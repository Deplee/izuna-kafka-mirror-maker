# Kafka Mirror Script

## Назначение

Скрипт предназначен для зеркалирования (копирования) сообщений из топиков одного Kafka-кластера в другой, с возможностью синхронизации consumer offsets между кластерами.
Поддерживает SSL/SASL, работу с большими сообщениями, автоматическую синхронизацию оффсетов и мониторинг процесса.

## Как работает скрипт

1. Подключается к исходному и целевому Kafka-кластеру с заданными параметрами безопасности.
2. Читает сообщения из указанных топиков (или всех, если задано).
3. Пишет сообщения в целевой кластер с сохранением ключей, заголовков, таймстемпов.
4. Синхронизирует consumer offsets (если включено) — переносит прогресс чтения групп потребителей.
5. Ведёт мониторинг: считает количество скопированных, неудачных, больших сообщений, ошибок и т.д. и показ начальной таблицы
6. Синхронизация
6. Показывает финальный статус после завершения работы.

## Таблица параметров запуска

| Параметр | Обязательный | Описание | Пример значения |
|---------------------------|--------------|------------------------------------------------------------------------------------------|--------------------------------------|
| --source-brokers | Да | Адреса исходных брокеров Kafka (через запятую) | localhost:9092 |
| --target-brokers | Да | Адреса целевых брокеров Kafka (через запятую) | localhost:9093 |
| --consumer-group | Да | Имя consumer group для чтения | mirror-group |
| --topics | Нет* | Список топиков через запятую или путь к JSON-файлу | test-1,test-2 или topics.json |
| --mirror-all-topics | Нет | Зеркалировать все пользовательские топики | (флаг) |
| --security-protocol | Нет | Протокол безопасности (PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL) | SASL_PLAINTEXT |
| --ssl-cafile | Нет | CA-файл для SSL | /path/to/ca.pem |
| --ssl-certfile | Нет | Сертификат клиента для SSL | /path/to/cert.pem |
| --ssl-keyfile | Нет | Ключ клиента для SSL | /path/to/key.pem |
| --sasl-mechanism | Нет | Механизм SASL (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512) | PLAIN |
| --source-sasl-username | Нет | SASL username для исходного кластера | admin |
| --source-sasl-password | Нет | SASL password для исходного кластера | password |
| --target-sasl-username | Нет | SASL username для целевого кластера | admin |
| --target-sasl-password | Нет | SASL password для целевого кластера | password |
| --max-message-size | Нет | Максимальный размер сообщения (в байтах) | 2097152000 (2GB) |
| --idle-timeout | Нет | Время (сек), после которого скрипт завершится при отсутствии новых сообщений | 30 |
| --sync-offsets | Нет | Включить синхронизацию consumer offsets | (флаг) |
| --sync-interval | Нет | Интервал синхронизации offsets (сек) | 300 |
| --status-interval | Нет | Интервал вывода мониторинга (сек) | 10 |
| --max-retries | Нет | Максимальное число попыток повторной отправки сообщений | 5 |
| --target-cluster-alias | Нет | Алиас целевого кластера для чекпоинтов | target |
\* — либо --topics, либо --mirror-all-topics | Да | Список топиков либо все топики | --topics test-1,test-2 либо --topics.json |

## Особенности работы с offsets и статусом SYNC NEEDED

* topics.json: если передан путь к JSON-файлу, скрипт загрузит список топиков из файла.
* SYNC NEEDED появляется, если в целевом кластере offset для группы/топика/partition отсутствует или отстаёт от исходного.
* В этом случае скрипт покажет в финальной таблице, что требуется синхронизация.
* Если включена опция --sync-offsets, скрипт попытается автоматически перенести offsets из исходного кластера в целевой.
* После успешной синхронизации статус изменится на OK.

### Важно:

- Если в исходных топиках нет новых сообщений для вашей consumer group, mirrored будет 0.
- Если в JSON-файле указаны несуществующие топики, они будут созданы в целевом кластере, но останутся пустыми.
- Для корректной синхронизации offsets используйте уникальные consumer group для каждого запуска.


### Пример запуска

#### Список топиков через запятую:

```bash
python3 kafka_mirror_enhanced.py \
  --source-brokers ip:9092 \
  --target-brokers ip:9092 \
  --topics test-1,test-2,test-3 \
  --consumer-group my-mirror-group \
  --sync-offsets
```

#### Список топиков через JSON-файл:

```bash
python3 kafka_mirror_enhanced.py \
  --source-brokers ip:9092 \
  --target-brokers ip:9092 \
  --topics topics.json \
  --consumer-group my-mirror-group \
  --sync-offsets
```
Где topics.json

```json
{
  "topics": [
    "test-1",
    "test-2",
    "test-3"
  ]
}
```
### Зависимости

`requirements.txt`

```
kafka-python>=2.0.2
tabulate>=0.8.9
confluent-kafka
```


### Как установить и запустить

#### С виртуальным окружением:

```bash
python3 -m venv myenv
source myenv/bin/activate
pip install -r requirements.txt
python3 kafka_mirror_enhanced.py ... (параметры)
```

#### Без виртуального окружения:

```bash
pip install kafka-python tabulate
python3 kafka_mirror_enhanced.py ... (параметры)
```



Example:
```bash
python3 kafka_mirror_enhanced.py \
  --source-brokers ip:9092 \
  --target-brokers ip:9092 \
  --topics topics.json \
  --security-protocol SASL_PLAINTEXT \
  --sasl-mechanism PLAIN \
  --source-sasl-username admin \
  --source-sasl-password admin \
  --target-sasl-username admin \
  --target-sasl-password pwd \
  --idle-timeout 5 \
  --consumer-group t4 \
  --sync-offsets
```

```
python3 kafka_mirror_enhanced.py \
  --source-brokers ip:9092 \
  --target-brokers ip:9092 \
  --topics test-1,test-2,test-3 \
  --security-protocol SASL_PLAINTEXT \
  --sasl-mechanism PLAIN \
  --source-sasl-username admin \
  --source-sasl-password admin \
  --target-sasl-username admin \
  --target-sasl-password pwd \
  --idle-timeout 5 \
  --consumer-group t4 \
  --sync-offsets
```

```
python3 kafka_mirror_enhanced.py \
  --source-brokers ip:9092 \
  --target-brokers ip:9092 \
  --mirror-all-topics \
  --security-protocol SASL_PLAINTEXT \
  --sasl-mechanism PLAIN \
  --source-sasl-username admin \
  --source-sasl-password admin \
  --target-sasl-username admin \
  --target-sasl-password pwd \
  --idle-timeout 5 \
  --consumer-group t4 \
  --sync-offsets
```


### Описание таблицы FINAL OFFSETS SYNC TABLE

| Group   | Topic  | Partition   | Target Offset   | Source Offset   | Lag   | Status   |
|---------|--------|-------------|-----------------|-----------------|-------|----------|


Колонки таблицы:
1. Group
  - Consumer group ID, для которой выполняется синхронизация
  - Пример: t5
2. Topic
  - Имя топика
  - Может быть пользовательским `(test-1, test-2) `или системным `(__consumer_offsets)`
3. Partition
  - Номер партиции топика
  - Нумерация начинается с 0
4. Target Offset
  - Текущий офсет в целевом кластере
  - Может быть:
    - Число ≥ 0: позиция в партиции
    - -1001: партиция не используется (для __consumer_offsets)
    - Пустое поле: офсет не установлен
5. Source Offset
  - Текущий офсет в исходном кластере
  - Может быть:
    - Число ≥ 0: позиция в партиции
    - -1001: партиция не используется (для __consumer_offsets)
    - Пустое поле: офсет не установлен
6. Lag
  - Отставание между кластерами
  - Вычисляется как: `Source Offset` - `Target Offset`
  - Может быть:
    - Положительное число: target отстает
    - Отрицательное число: target опережает
    - 0: синхронизировано
    - Пустое поле: невозможно вычислить
7. Status
  - Статус синхронизации
  - Значения:
    - `OK:` офсеты совпадают (Source Offset = Target Offset)
    - `SYNC NEEDED:` требуется синхронизация (офсеты различаются)

#### Примеры интерпретации:

| Group | Topic   | Partition | Target Offset | Source Offset | Lag | Status |
|-------|---------|------------|---------------|---------------|-----|--------|
| t5    | test-1  | 0          | 5             | 5             | 0   | OK     |


- Топик полностью синхронизирован
- Оба офсета = 5
- Нет отставания (lag = 0)


| Group | Topic   | Partition | Target Offset | Source Offset | Lag | Status        |
|-------|---------|------------|---------------|---------------|-----|--------------|
| t5    | test-1  | 0          | 5             | 5             | 0   | OK           |
| t5    | test-2  | 0          | 2             | 5             | 3   | SYNC NEEDED  |


- Требуется синхронизация
- Target отстает на 3 сообщения
- Source ahead (опережает)


| Group | Topic               | Partition | Target Offset | Source Offset | Lag | Status       |
|-------|---------------------|------------|---------------|---------------|-----|--------------|
| t5    | test-1             | 0          | 5             | 5             | 0   | OK           |
| t5    | test-2             | 0          | 2             | 5             | 3   | SYNC NEEDED  |
| t5    | __consumer_offsets | 0          | -1001         | -1001         | 0   | OK           |


- Системный топик
- Партиция не используется (-1001)
- Статус OK (значения совпадают)
- Эта таблица помогает отслеживать процесс синхронизации и выявлять проблемы с отставанием между кластерами.




# Other Notes

1. Как Kafka хранит офсеты:

```text
В топике __consumer_offsets Kafka хранит записи вида:
- Ключ: (group.id, topic, partition)
- Значение: (offset, metadata)
```

2. Текущая синхронизация работает так:

```python
# 1. Читаем офсеты из исходного кластера
src_consumer = Consumer({**to_confluent_conf(source_conf), 'group.id': group})
src_offsets = src_consumer.committed([TopicPartition(t, p) ...])
# Это фактически чтение из __consumer_offsets в source кластере

# 2. Коммитим эти офсеты в целевой кластер
tgt_consumer = Consumer({**to_confluent_conf(target_conf), 'group.id': group})
tgt_consumer.commit(offsets=to_commit)
# Это запись в __consumer_offsets в target кластере
```

3. Что происходит под капотом:

```text
Source Cluster                     Target Cluster
----------------                   ----------------
__consumer_offsets                __consumer_offsets
    ↓ READ                            ↑ WRITE
    offset1 → → → → → → → → → → → → → offset1
    offset2 → → → → → → → → → → → → → offset2
    offset3 → → → → → → → → → → → → → offset3
```

4. Порядок операций:

```text
a. Создаем топики в target (если их нет)
b. Инициализируем consumer group в target
c. Читаем офсеты из __consumer_offsets в source
d. Записываем офсеты в __consumer_offsets в target
```

5. Важные моменты:

```text
- Мы не копируем сам топик __consumer_offsets
- Вместо этого мы:
  1. Читаем значения через Consumer API
  2. Записываем значения через Consumer API
- Это безопаснее, чем прямое копирование __consumer_offsets
```

6. Пример конкретных действий:

```python
# Для группы t5 и топика test-1, партиция 0:

# В source кластере читаем:
source_offset = 5  # из __consumer_offsets

# В target кластере записываем:
target_consumer.commit([
    TopicPartition('test-1', 0, offset=5)
])  # запишется в __consumer_offsets
```

7. Преимущества такого подхода:

```text
- Безопасная синхронизация без прямого доступа к __consumer_offsets
- Использование стандартного Kafka API
- Поддержка SASL/SSL аутентификации
- Атомарные операции записи офсетов
```

Таким образом, хотя офсеты хранятся в `__consumer_offsets`, мы не работаем с этим топиком напрямую, а используем `Consumer API` для безопасного переноса офсетов между кластерами.

8. Дополнение

Чтобы не было ошибок по завершению скрпита необходимо:
  - Создать `topics` которые мы передаем через `--topics` в целевом кластере kafka
  - Создать `consumer group` которую мы передаем через `--consumer-group` в целевом кластере kafka
  - По желанию подписать эту `--consumer-group` на `topics`

# Multiple Kafka Nodes

## Как это выглядит на практике

* Kafka-кластер — это несколько брокеров (контейнеров), которые вместе обслуживают топики и партиции.
* Zookeeper — нужен только для работы самого кластера Kafka, клиенту (вашему скрипту) он не нужен.
* Скрипт подключается к одному или нескольким брокерам из списка, автоматически узнаёт о других брокерах и партициях через Kafka-протокол.

### Пример запуска

```bash
python3 kafka_mirror_enhanced.py \
  --source-brokers kafka1:9092,kafka2:9092 \
  --target-brokers kafka3:9092,kafka4:9092 \
  --topics test-1,test-2 \
  --consumer-group my-mirror-group \
  --sync-offsets
```

## Важно

- Если вы укажете только один брокер, а он будет недоступен — скрипт не сможет работать.
- Поэтому всегда указывайте несколько брокеров из каждого кластера.
- Если кластеры находятся в разных сетях/докерах, проверьте доступность advertised.listeners.

# Вывод:

Скрипт полностью совместим с многоконтейнерными (многоброкерными) кластерами Kafka и не зависит от количества контейнеров или наличия Zookeeper — главное, чтобы были доступны брокеры Kafka.
