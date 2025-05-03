import argparse
from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from kafka.errors import KafkaError
from kafka.admin import KafkaAdminClient, NewTopic, ConfigResource, ConfigResourceType
from kafka.structs import OffsetAndMetadata
from kafka.consumer.subscription_state import ConsumerRebalanceListener
import logging
import sys
import time
from threading import Thread, Lock, Event
from collections import defaultdict, deque
import json
from datetime import datetime
import signal
from tabulate import tabulate  # pip install tabulate, если нужно
import threading
import kafka
import os


def print_offsets_sync_table(sync_status_list, logger):
    """
    sync_status_list: список списков или кортежей вида
    [Group, Topic, Partition, Target Offset, Source Offset, Lag, Status]
    """
    table = tabulate(
        sync_status_list,
        headers=["Group", "Topic", "Partition", "Target Offset", "Source Offset", "Lag", "Status"],
        tablefmt="grid"
    )
    logger.info("\n" + table)

class MirroringMonitor:
    def __init__(self, max_history=1000):
        self.stats = {
            'total_mirrored': 0,
            'total_failed': 0,
            'total_retries': 0,
            'start_time': datetime.now(),
            'last_message_time': None,
            'by_topic': defaultdict(lambda: {
                'mirrored': 0,
                'failed': 0,
                'last_offset': {}
            }),
            'recent_errors': deque(maxlen=50),
            'throughput': deque(maxlen=60),
            'large_messages': 0
        }
        self.history = deque(maxlen=max_history)
        self.lock = Lock()

    def record_success(self, topic, partition, offset, message_size=0):
        with self.lock:
            now = datetime.now()
            self.stats['total_mirrored'] += 1
            self.stats['by_topic'][topic]['mirrored'] += 1
            self.stats['by_topic'][topic]['last_offset'][partition] = offset
            self.stats['last_message_time'] = now

            if message_size > 1024 * 1024 * 1024 * 1024:  # 1GB
                self.stats['large_messages'] += 1

            current_second = now.replace(microsecond=0)
            if not self.stats['throughput'] or self.stats['throughput'][-1][0] != current_second:
                self.stats['throughput'].append((current_second, 1))
            else:
                self.stats['throughput'][-1] = (current_second, self.stats['throughput'][-1][1] + 1)

            self.history.append(('SUCCESS', topic, partition, offset, now))

    def record_failure(self, topic, partition, offset, error, message_size=0):
        with self.lock:
            now = datetime.now()
            self.stats['total_failed'] += 1
            self.stats['by_topic'][topic]['failed'] += 1
            self.stats['recent_errors'].append((now, topic, partition, offset, str(error)))

            if message_size > 1024 * 1024 * 1024 * 1024:  # 1GB
                self.stats['large_messages'] += 1

            self.history.append(('FAILURE', topic, partition, offset, now, str(error)))

    def record_retry(self, topic, partition, offset):
        with self.lock:
            self.stats['total_retries'] += 1

    def get_stats(self):
        with self.lock:
            stats = self.stats.copy()
            stats['uptime'] = str(datetime.now() - stats['start_time'])
            stats['current_rate'] = sum(count for _, count in stats['throughput']) / 60 if stats['throughput'] else 0
            return stats

    def get_recent_errors(self, limit=10):
        with self.lock:
            return list(self.stats['recent_errors'])[-limit:]

    def get_history(self, limit=20):
        with self.lock:
            return list(self.history)[-limit:]

class RebalanceListener(ConsumerRebalanceListener):
    def __init__(self, mirror):
        self.mirror = mirror

    def on_partitions_assigned(self, assigned):
        self.mirror.logger.info(f"Assigned partitions: {assigned}")
        with self.mirror.lock:
            self.mirror.partition_assignments = {
                (tp.topic, tp.partition): tp for tp in assigned
            }

    def on_partitions_revoked(self, revoked):
        self.mirror.logger.info(f"Revoked partitions: {revoked}")
        with self.mirror.lock:
            for tp in revoked:
                self.mirror.partition_assignments.pop((tp.topic, tp.partition), None)

class KafkaMirror:
    def __init__(self, source_brokers, target_brokers, consumer_group, topics=None,
                 security_protocol='PLAINTEXT', ssl_config=None,
                 dynamic_config=False, max_retries=5, status_interval=10,
                 sasl_mechanism='PLAIN', source_sasl_username=None, source_sasl_password=None,
                 target_sasl_username=None, target_sasl_password=None,
                 max_message_size=1048576, mirror_all_topics=False, idle_timeout=30):

        self.source_brokers = source_brokers
        self.target_brokers = target_brokers
        self.consumer_group = consumer_group
        self.topics = topics
        self.security_protocol = security_protocol
        self.ssl_config = ssl_config or {}
        self.dynamic_config = dynamic_config
        self.max_retries = max_retries
        self.status_interval = status_interval
        self.sasl_mechanism = sasl_mechanism
        self.source_sasl_username = source_sasl_username
        self.source_sasl_password = source_sasl_password
        self.target_sasl_username = target_sasl_username
        self.target_sasl_password = target_sasl_password
        self.max_message_size = max_message_size
        self.mirror_all_topics = mirror_all_topics
        self.idle_timeout = idle_timeout  # Timeout in seconds

        self.consumer = None
        self.producer = None
        self.admin_client = None
        self.running = False
        self.partition_assignments = {}
        self.lock = Lock()
        self.retry_queue = defaultdict(list)
        self.active_topics = set()
        self.monitor = MirroringMonitor()
        self.status_event = Event()
        self.last_sync_status = None

        logging.basicConfig(
            format='%(asctime)s - %(levelname)s - %(message)s',
            level=logging.INFO
        )
        self.logger = logging.getLogger('kafka_mirror')

        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

    def signal_handler(self, signum, frame):
        self.logger.info(f"Received signal {signum}. Shutting down...")
        self.close()
        sys.exit(0)

    def check_connection(self):
        try:
            # Check source connection
            source_config = {
                'bootstrap_servers': self.source_brokers,
                'group_id': self.consumer_group,
                'security_protocol': self.security_protocol,
                'request_timeout_ms': 15000
            }
            if self.security_protocol in ['SASL_PLAINTEXT', 'SASL_SSL']:
                source_config.update({
                    'sasl_mechanism': self.sasl_mechanism,
                    'sasl_plain_username': self.source_sasl_username,
                    'sasl_plain_password': self.source_sasl_password
                })

            consumer = KafkaConsumer(**source_config)
            consumer.topics()
            consumer.close()

            # Check target connection
            target_config = {
                'bootstrap_servers': self.target_brokers,
                'security_protocol': self.security_protocol,
                'request_timeout_ms': 15000
            }
            if self.security_protocol in ['SASL_PLAINTEXT', 'SASL_SSL']:
                target_config.update({
                    'sasl_mechanism': self.sasl_mechanism,
                    'sasl_plain_username': self.target_sasl_username,
                    'sasl_plain_password': self.target_sasl_password
                })

            producer = KafkaProducer(**target_config)
            producer.metrics()
            producer.close()
            return True
        except Exception as e:
            self.logger.error(f"Connection error: {e}")
            return False

    def create_admin_client(self):
        config = {
            'bootstrap_servers': self.source_brokers,
            'security_protocol': self.security_protocol,
            'request_timeout_ms': 60000
        }

        if self.security_protocol in ['SASL_PLAINTEXT', 'SASL_SSL']:
            config.update({
                'sasl_mechanism': self.sasl_mechanism,
                'sasl_plain_username': self.source_sasl_username,
                'sasl_plain_password': self.source_sasl_password
            })

        if self.security_protocol == 'SSL':
            config.update(self.ssl_config)

        self.admin_client = KafkaAdminClient(**config)
        self.logger.info("Admin client created")

    def discover_topics(self):
        if not self.admin_client:
            self.create_admin_client()

        try:
            topics = self.admin_client.list_topics()
            return [t for t in topics if not t.startswith('__')]
        except Exception as e:
            self.logger.error(f"Error listing topics: {e}")
            return []

    def load_topics_from_config(self, config_path):
        try:
            with open(config_path) as f:
                config = json.load(f)
                return config.get('topics', [])
        except Exception as e:
            self.logger.error(f"Config load error: {e}")
            return []

    def create_consumer(self):
        consumer_config = {
            'bootstrap_servers': self.source_brokers,
            'group_id': self.consumer_group,
            'auto_offset_reset': 'earliest',
            'enable_auto_commit': False,
            'security_protocol': self.security_protocol,
            'max_poll_records': 100,
            'fetch_max_bytes': self.max_message_size,
            'max_partition_fetch_bytes': self.max_message_size,
            'request_timeout_ms': 60000,
            'session_timeout_ms': 30000,
            'max_poll_interval_ms': 60000,
            'api_version': (2, 6, 0)
        }

        if self.security_protocol in ['SASL_PLAINTEXT', 'SASL_SSL']:
            consumer_config.update({
                'sasl_mechanism': self.sasl_mechanism,
                'sasl_plain_username': self.source_sasl_username,
                'sasl_plain_password': self.source_sasl_password
            })

        if self.security_protocol == 'SSL':
            consumer_config.update(self.ssl_config)

        self.consumer = KafkaConsumer(**consumer_config)

        if self.mirror_all_topics:
            topics = self.discover_topics()
            if not topics:
                raise ValueError("No topics found in source cluster")
            self.active_topics.update(topics)
        elif isinstance(self.topics, str) and self.topics.endswith('.json'):
            topics = self.load_topics_from_config(self.topics)
            self.active_topics.update(topics)
        elif self.topics:
            topics = self.topics if isinstance(self.topics, list) else [self.topics]
            self.active_topics.update(topics)

        if not self.active_topics:
            raise ValueError("No topics specified for mirroring")

        rebalance_listener = RebalanceListener(self)
        self.consumer.subscribe(
            topics=list(self.active_topics),
            listener=rebalance_listener
        )
        self.logger.info(f"Subscribed to topics: {list(self.active_topics)}")

    def create_producer(self):
        producer_config = {
            'bootstrap_servers': self.target_brokers,
            'security_protocol': self.security_protocol,
            'max_request_size': self.max_message_size,
            'buffer_memory': 33554432,
            'compression_type': 'gzip',
            'retries': 3,
            'acks': 'all',
            'request_timeout_ms': 300000,
            'delivery_timeout_ms': 600000,
            'api_version': (2, 6, 0)
        }

        if self.security_protocol in ['SASL_PLAINTEXT', 'SASL_SSL']:
            producer_config.update({
                'sasl_mechanism': self.sasl_mechanism,
                'sasl_plain_username': self.target_sasl_username,
                'sasl_plain_password': self.target_sasl_password
            })

        if self.security_protocol == 'SSL':
            producer_config.update(self.ssl_config)

        self.producer = KafkaProducer(**producer_config)
        self.logger.info("Producer created")

    def process_retry_queue(self):
        while self.running:
            try:
                with self.lock:
                    for (topic, partition), messages in list(self.retry_queue.items()):
                        if not messages:
                            continue

                        message, attempt = messages.pop(0)
                        try:
                            message_size = len(message.value) if message.value else 0
                            future = self.producer.send(
                                topic,
                                key=message.key,
                                value=message.value,
                                headers=message.headers,
                                timestamp_ms=message.timestamp
                            )
                            future.get(timeout=30)
                            try:
                                self.consumer.commit()
                            except KafkaError as e:
                                if "UNKNOWN_MEMBER_ID" in str(e) or getattr(e, "code", None) == 25:
                                    self.logger.warning(f"Commit skipped on shutdown: {e}")
                                else:
                                    self.logger.error(f"Commit failed: {e}")
                            self.monitor.record_success(topic, partition, message.offset, message_size)
                        except Exception as e:
                            message_size = len(message.value) if message.value else 0
                            self.monitor.record_failure(topic, partition, message.offset, e, message_size)
                            if attempt < self.max_retries:
                                messages.append((message, attempt + 1))
                                self.monitor.record_retry(topic, partition, message.offset)
                            else:
                                self.logger.error(f"Max retries exceeded for message {topic}-{partition}:{message.offset}")

                time.sleep(1)
            except Exception as e:
                self.logger.error(f"Retry queue error: {e}")
                time.sleep(5)

    def mirror_messages(self):
        self.running = True
        last_message_time = time.time()

        retry_thread = Thread(target=self.process_retry_queue, daemon=True)
        retry_thread.start()

        status_thread = Thread(target=self.print_status, daemon=True)
        status_thread.start()

        self.logger.info("Starting message mirroring...")

        try:
            while self.running:
                msg_batch = self.consumer.poll(timeout_ms=1000)

                if msg_batch:
                    last_message_time = time.time()

                    for topic_partition, messages in msg_batch.items():
                        for message in messages:
                            try:
                                message_size = len(message.value) if message.value else 0

                                if message_size > self.max_message_size:
                                    self.logger.error(f"Message too large: {message_size} bytes (max {self.max_message_size})")
                                    continue

                                future = self.producer.send(
                                    topic_partition.topic,
                                    key=message.key,
                                    value=message.value,
                                    headers=message.headers,
                                    timestamp_ms=message.timestamp
                                )
                                future.get(timeout=30)
                                try:
                                    self.consumer.commit()
                                except KafkaError as e:
                                    if "UNKNOWN_MEMBER_ID" in str(e) or getattr(e, "code", None) == 25:
                                        self.logger.warning(f"Commit skipped on shutdown: {e}")
                                    else:
                                        self.logger.error(f"Commit failed: {e}")

                                self.monitor.record_success(
                                    topic_partition.topic,
                                    topic_partition.partition,
                                    message.offset,
                                    message_size
                                )

                            except KafkaError as e:
                                message_size = len(message.value) if message.value else 0
                                self.monitor.record_failure(
                                    topic_partition.topic,
                                    topic_partition.partition,
                                    message.offset,
                                    e,
                                    message_size
                                )

                                with self.lock:
                                    self.retry_queue[(topic_partition.topic, topic_partition.partition)].append(
                                        (message, 1)
                                    )
                elif time.time() - last_message_time > self.idle_timeout:
                    self.logger.info(f"No messages for {self.idle_timeout} seconds. Exiting.")
                    break

                self.status_event.set()
                time.sleep(2) # 0.1

        except KeyboardInterrupt:
            self.logger.info("Stopping mirroring...")
        except Exception as e:
            self.logger.error(f"Critical error: {e}")
        finally:
            try:
                self.consumer.commit()
                self.logger.info("Offsets committed before shutdown.")
            except Exception as e:
                # Подавляем ошибку UNKNOWN_MEMBER_ID
                if "UNKNOWN_MEMBER_ID" in str(e) or getattr(e, "code", None) == 25:
                    self.logger.info(f"Commit skipped on shutdown: {e}")
                else:
                    self.logger.warning(f"Commit on shutdown failed: {e}")
            try:
                self.consumer.close()
                self.logger.info("Consumer closed.")
            except Exception:
                pass
            self.close()

    def close(self):
        self.running = False
        if self.producer:
            self.producer.flush()
            self.producer.close()
        if self.admin_client:
            self.admin_client.close()
        self.logger.info("Mirroring stopped")

    def run(self):
        if not self.check_connection():
            raise ConnectionError("Failed to connect to Kafka clusters")

        if isinstance(self.topics, str) and self.topics.endswith('.json'):
            self.topics = self.load_topics_from_config(self.topics)

        if self.dynamic_config or self.mirror_all_topics:
            discovered_topics = self.discover_topics()
            if discovered_topics:
                self.active_topics.update(discovered_topics)
                self.logger.info(f"Discovered topics: {discovered_topics}")

        self.create_consumer()
        self.create_producer()

        if self.dynamic_config:
            discovery_thread = Thread(target=self.topic_discovery_loop, daemon=True)
            discovery_thread.start()

        running_event = threading.Event()
        running_event.set()
        self.mirror_messages()

    def topic_discovery_loop(self):
        while self.running:
            try:
                discovered_topics = set(self.discover_topics())
                new_topics = discovered_topics - self.active_topics

                if new_topics:
                    self.logger.info(f"Discovered new topics: {new_topics}")
                    self.active_topics.update(new_topics)
                    self.consumer.subscribe(list(self.active_topics), listener=RebalanceListener(self))

                time.sleep(60)
            except Exception as e:
                self.logger.error(f"Topic discovery error: {e}")
                time.sleep(30)

    def print_status(self):
        while self.running:
            try:
                stats = self.monitor.get_stats()
                recent_errors = self.monitor.get_recent_errors(3)

                self.logger.info("\n=== Mirroring Status ===")
                self.logger.info(f"Uptime: {stats['uptime']}")
                self.logger.info(f"Total mirrored: {stats['total_mirrored']}")
                self.logger.info(f"Total failed: {stats['total_failed']}")
                self.logger.info(f"Total retries: {stats['total_retries']}")
                self.logger.info(f"Large messages (>1GB): {stats['large_messages']}")
                self.logger.info(f"Current rate: {stats['current_rate']:.1f} msg/sec")

                if recent_errors:
                    self.logger.info("\nRecent errors:")
                    for error in recent_errors:
                        self.logger.info(f"{error[0]} - {error[1]}-{error[2]}:{error[3]} - {error[4]}")

                self.logger.info("\nTopic status:")
                for topic, data in stats['by_topic'].items():
                    self.logger.info(f"{topic}: success={data['mirrored']}, failed={data['failed']}")
                    for partition, offset in data['last_offset'].items():
                        self.logger.info(f"  Partition {partition}: last offset={offset}")

                self.logger.info("=" * 25 + "\n")

                self.status_event.wait(self.status_interval)
                self.status_event.clear()

            except Exception as e:
                self.logger.error(f"Status error: {e}")
                time.sleep(5)

    def status_thread_func(self, monitor, stop_event, interval=10):
        while not stop_event.is_set():
            monitor.print_status()
            time.sleep(interval)

def safe_commit(consumer, logger):
    try:
        consumer.commit()
    except Exception as e:
        # Не ругаемся на UNKNOWN_MEMBER_ID при завершении
        if "UNKNOWN_MEMBER_ID" in str(e) or getattr(e, "code", None) == 25:
            logger.info(f"Commit skipped on shutdown: {e}")
        else:
            logger.error(f"Commit failed: {e}")

def sync_offsets_confluent(source_conf, target_conf, consumer_group=None, logger=None):
    from confluent_kafka.admin import AdminClient
    from confluent_kafka import Consumer, TopicPartition
    from tabulate import tabulate

    # Преобразуем конфиги для confluent-kafka
    def to_confluent_conf(conf):
        out = {'bootstrap.servers': ','.join(conf['bootstrap_servers']) if isinstance(conf['bootstrap_servers'], list) else conf['bootstrap_servers']}
        if 'security_protocol' in conf:
            out['security.protocol'] = conf['security_protocol']
        if 'sasl_mechanism' in conf:
            out['sasl.mechanisms'] = conf['sasl_mechanism']
        if 'sasl_plain_username' in conf:
            out['sasl.username'] = conf['sasl_plain_username']
        if 'sasl_plain_password' in conf:
            out['sasl.password'] = conf['sasl_plain_password']
        if 'ssl_cafile' in conf:
            out['ssl.ca.location'] = conf['ssl_cafile']
        if 'ssl_certfile' in conf:
            out['ssl.certificate.location'] = conf['ssl_certfile']
        if 'ssl_keyfile' in conf:
            out['ssl.key.location'] = conf['ssl_keyfile']
        return out

    admin_src = AdminClient(to_confluent_conf(source_conf))
    admin_tgt = AdminClient(to_confluent_conf(target_conf))

    # Получаем список групп
    groups = []
    if consumer_group:
        groups = [consumer_group]
    else:
        groups = [g for g in admin_src.list_consumer_groups(timeout=10)]

    sync_status_list = []
    for group in groups:
        # Получаем offsets из исходного кластера
        src_consumer = Consumer({**to_confluent_conf(source_conf), 'group.id': group, 'enable.auto.commit': False})
        src_offsets = src_consumer.committed([TopicPartition(t, p) for t in admin_src.list_topics(timeout=10).topics for p in range(admin_src.list_topics(timeout=10).topics[t].partitions.__len__())], timeout=10)
        src_consumer.close()

        # Получаем offsets из целевого кластера
        tgt_consumer = Consumer({**to_confluent_conf(target_conf), 'group.id': group, 'enable.auto.commit': False})
        tgt_offsets = tgt_consumer.committed([TopicPartition(tp.topic, tp.partition) for tp in src_offsets], timeout=10)
        tgt_consumer.close()

        # Формируем таблицу мониторинга
        table = []
        for src_tp in src_offsets:
            tgt_tp = next((t for t in tgt_offsets if t.topic == src_tp.topic and t.partition == src_tp.partition), None)
            tgt_offset_val = tgt_tp.offset if tgt_tp else None
            lag = src_tp.offset - tgt_offset_val if tgt_offset_val is not None and src_tp.offset is not None else None
            table.append([
                group,
                src_tp.topic,
                src_tp.partition,
                tgt_offset_val if tgt_offset_val is not None else '',
                src_tp.offset if src_tp.offset is not None else '',
                lag if lag is not None else '',
                "OK" if src_tp.offset == tgt_offset_val else "SYNC NEEDED"
            ])
        if logger:
            logger.info("\n" + tabulate(
                table,
                headers=["Group", "Topic", "Partition", "Target Offset", "Source Offset", "Lag", "Status"],
                tablefmt="grid"
            ))

        # Синхронизируем offsets
        # Только если есть что переносить
        to_commit = [TopicPartition(tp.topic, tp.partition, tp.offset) for tp in src_offsets if tp.offset is not None and tp.offset >= 0]
        if to_commit:
            tgt_consumer = Consumer({**to_confluent_conf(target_conf), 'group.id': group, 'enable.auto.commit': False})
            tgt_consumer.assign([TopicPartition(tp.topic, tp.partition) for tp in to_commit])
            tgt_consumer.commit(offsets=to_commit, asynchronous=False)
            tgt_consumer.close()


def ensure_topics_exist(target_conf, topics, logger=None):
    from kafka.admin import KafkaAdminClient, NewTopic
    admin_tgt = None
    try:
        admin_tgt = KafkaAdminClient(**target_conf)
        existing = set(admin_tgt.list_topics())
        to_create = [t for t in topics if t not in existing]
        if to_create:
            new_topics = [NewTopic(name=t, num_partitions=1, replication_factor=1) for t in to_create]
            admin_tgt.create_topics(new_topics)
            if logger:
                logger.info(f"Created topics in target cluster: {to_create}")
    except Exception as e:
        if logger:
            logger.error(f"Error creating topics: {e}")
    finally:
        if admin_tgt is not None:
            admin_tgt.close()

def ensure_consumer_group_exists(target_conf, group_id, topics, logger=None):
    from kafka import KafkaConsumer
    import time
    if not topics:
        if logger:
            logger.error(f"No topics available to initialize group '{group_id}' in target cluster.")
        return
    try:
        consumer = KafkaConsumer(
            *topics,
            group_id=group_id,
            enable_auto_commit=False,
            auto_offset_reset='latest',
            **target_conf
        )
        for _ in range(3):
            consumer.poll(timeout_ms=1000)
            time.sleep(1)
        consumer.close()
        if logger:
            logger.info(f"Consumer group '{group_id}' initialized in target cluster.")
    except Exception as e:
        if logger:
            logger.error(f"Failed to initialize consumer group '{group_id}': {e}")

def parse_args():
    parser = argparse.ArgumentParser(description="Kafka Mirror & Offset Sync Enhanced")
    parser.add_argument('--source-brokers', required=True,
                       help='Source cluster brokers (comma separated)')
    parser.add_argument('--target-brokers', required=True,
                       help='Target cluster brokers (comma separated)')
    parser.add_argument('--consumer-group', required=True,
                       help='Consumer group ID')
    parser.add_argument('--topics', type=str, help='Topics to mirror (comma separated or .json file)')
    parser.add_argument('--security-protocol', default='PLAINTEXT',
                       help='Security protocol (PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL)')
    parser.add_argument('--ssl-cafile', help='CA certificate file for SSL')
    parser.add_argument('--ssl-certfile', help='Client certificate file for SSL')
    parser.add_argument('--ssl-keyfile', help='Client key file for SSL')
    parser.add_argument('--sasl-mechanism', default='PLAIN',
                       help='SASL mechanism (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512)')
    parser.add_argument('--source-sasl-username', help='SASL username for source cluster')
    parser.add_argument('--source-sasl-password', help='SASL password for source cluster')
    parser.add_argument('--target-sasl-username', help='SASL username for target cluster')
    parser.add_argument('--target-sasl-password', help='SASL password for target cluster')
    parser.add_argument('--dynamic-config', action='store_true',
                       help='Enable dynamic topic discovery')
    parser.add_argument('--mirror-all-topics', action='store_true',
                       help='Mirror all non-system topics')
    parser.add_argument('--max-retries', type=int, default=5,
                       help='Max retries for failed messages')
    parser.add_argument('--status-interval', type=int, default=10,
                       help='Status output interval (seconds)')
    parser.add_argument('--max-message-size', type=int, default=1048576,
                       help='Maximum message size in bytes (default: 1MB, max: 2147483647)')
    parser.add_argument('--idle-timeout', type=int, default=30,
                       help='Exit after N seconds without messages')
    parser.add_argument('--sync-offsets', action='store_true',
                       help='Sync consumer group offsets after mirroring')
    return parser.parse_args()

def main():
    args = parse_args()
    # Универсальная обработка topics
    if args.topics:
        if isinstance(args.topics, str) and args.topics.endswith('.json') and os.path.isfile(args.topics):
            with open(args.topics, 'r') as f:
                config = json.load(f)
                if isinstance(config, dict):
                    args.topics = config.get('topics', [])
                elif isinstance(config, list):
                    args.topics = config
                else:
                    args.topics = []
        elif isinstance(args.topics, str):
            args.topics = [t.strip() for t in args.topics.split(',')]

    ssl_config = None
    if args.security_protocol == 'SSL':
        ssl_config = {
            'ssl_cafile': args.ssl_cafile,
            'ssl_certfile': args.ssl_certfile,
            'ssl_keyfile': args.ssl_keyfile
        }

    mirror = KafkaMirror(
        source_brokers=args.source_brokers.split(','),
        target_brokers=args.target_brokers.split(','),
        consumer_group=args.consumer_group,
        topics=args.topics,
        security_protocol=args.security_protocol,
        ssl_config=ssl_config,
        dynamic_config=args.dynamic_config,
        max_retries=args.max_retries,
        status_interval=args.status_interval,
        sasl_mechanism=args.sasl_mechanism,
        source_sasl_username=args.source_sasl_username,
        source_sasl_password=args.source_sasl_password,
        target_sasl_username=args.target_sasl_username,
        target_sasl_password=args.target_sasl_password,
        max_message_size=args.max_message_size,
        mirror_all_topics=args.mirror_all_topics,
        idle_timeout=args.idle_timeout
    )

    try:
        mirror.run()
        if args.sync_offsets:
            source_conf = {
                'bootstrap_servers': args.source_brokers.split(','),
                'security_protocol': args.security_protocol,
            }
            target_conf = {
                'bootstrap_servers': args.target_brokers.split(','),
                'security_protocol': args.security_protocol,
            }
            if args.security_protocol in ['SASL_PLAINTEXT', 'SASL_SSL']:
                source_conf.update({
                    'sasl_mechanism': args.sasl_mechanism,
                    'sasl_plain_username': args.source_sasl_username,
                    'sasl_plain_password': args.source_sasl_password
                })
                target_conf.update({
                    'sasl_mechanism': args.sasl_mechanism,
                    'sasl_plain_username': args.target_sasl_username,
                    'sasl_plain_password': args.target_sasl_password
                })
            if args.security_protocol == 'SSL':
                source_conf.update(ssl_config or {})
                target_conf.update(ssl_config or {})

            if args.consumer_group:
                # Определяем список топиков для подписки
                topics = []
                if args.topics:
                    topics = args.topics
                elif hasattr(mirror, 'active_topics'):
                    topics = list(mirror.active_topics)
                # 1. Создать топики, если их нет
                ensure_topics_exist(target_conf, topics, logger=mirror.logger)
                # 2. Инициализировать группу
                ensure_consumer_group_exists(target_conf, args.consumer_group, topics, logger=mirror.logger)

            mirror.logger.info("Starting offsets synchronization...")
            sync_status_list = sync_offsets_confluent(source_conf, target_conf, consumer_group=args.consumer_group, logger=mirror.logger)
            mirror.logger.info("Offsets synchronization complete.")
            print(f"========== FINAL OFFSETS SYNC TABLE ==========")
            sync_status_list = sync_offsets_confluent(source_conf, target_conf, consumer_group=args.consumer_group, logger=mirror.logger)
    except Exception as e:
        mirror.logger.error(f"Fatal error: {e}")
        sys.exit(1)

    # Финальный вывод мониторинга
    final_stats = mirror.monitor.get_stats()
    mirror.logger.info("========== FINAL MIRRORING STATUS ==========")
    mirror.logger.info(f"Uptime: {final_stats['uptime']}")
    mirror.logger.info(f"Total mirrored: {final_stats['total_mirrored']}")
    mirror.logger.info(f"Total failed: {final_stats['total_failed']}")
    mirror.logger.info(f"Total retries: {final_stats['total_retries']}")
    mirror.logger.info(f"Large messages (>1GB): {final_stats.get('large_messages', 0)}")
    mirror.logger.info(f"Current rate: {final_stats.get('current_rate', 0):.2f} msg/sec")

if __name__ == '__main__':
    main()
