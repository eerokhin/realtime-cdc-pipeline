# Realtime CDC Pipeline: PostgreSQL → Debezium → Kafka → ClickHouse

Проект по построению near real-time CDC (Change Data Capture) pipeline. Изменения в PostgreSQL автоматически захватываются Debezium из WAL, передаются в Apache Kafka и загружаются в ClickHouse для дальнейшей аналитики.

В проекте реализованы:
- начальная загрузка данных (initial snapshot);
- автоматическое отслеживание INSERT, UPDATE и DELETE;
- передача изменений через Kafka;
- сохранение исходных CDC-событий в Raw-слое ClickHouse;
- автоматическое преобразование JSON-событий в типизированную таблицу;
- сохранение истории изменений без перезаписи предыдущих событий.

---

## 1. Архитектура проекта

```text
                         PostgreSQL
                              |
                              | WAL
                              v
                          Debezium
                         Kafka Connect
                              |
                              v
                            Kafka
                              |
                    shop.public.orders
                              |
                              v
                    ClickHouse Kafka Engine
                              |
                              v
                   orders_kafka_raw_mv
                              |
                              v
                      orders_cdc_raw
                         (Raw layer)
                              |
                              v
                    orders_raw_events_mv
                              |
                              v
                    orders_cdc_events
                        (Typed layer)
```

Основной поток данных:

1. PostgreSQL записывает изменения в Write-Ahead Log (WAL).
2. Debezium считывает изменения через logical replication.
3. Debezium формирует CDC-события и публикует их в Kafka.
4. ClickHouse Kafka Engine получает сообщения из Kafka.
5. Materialized View сохраняет исходные JSON-события в Raw-таблицу.
6. Вторая Materialized View преобразует JSON в типизированные колонки.
7. Результат сохраняется в ClickHouse MergeTree.

В результате каждое изменение становится отдельной записью в истории CDC-событий.

## 2. Технологический стек

| Компонент | Версия | Назначение |
|---|---|---|
| PostgreSQL | 17 | Источник данных |
| Debezium | 3.7.1.Final | Захват изменений из WAL |
| Kafka Connect | 3.7.1.Final | Управление Debezium Connector |
| Apache Kafka | 3.7.1.Final | Передача CDC-событий |
| Kafka KRaft | В составе Kafka | Управление метаданными Kafka без ZooKeeper |
| ClickHouse | 26.10.3.7 | Хранение и обработка CDC-событий |
| Kafka Engine | В составе ClickHouse | Чтение сообщений из Kafka |
| Docker Compose | — | Запуск инфраструктуры |
| PowerShell | — | Регистрация Connector и автоматизация |

Дополнительный интерфейс для просмотра Kafka:
- Kafka UI 0.7.2.

## 3. Структура проекта

```text
realtime-cdc-pipeline/
│
├── docker-compose.yml
├── .env
├── .gitignore
├── README.md
│
├── postgres/
│   └── init/
│       └── 01_init.sql
│
├── debezium/
│   └── connector.json
│
├── clickhouse/
│   └── init/
│       └── 01_init.sql
│
├── scripts/
│   └── register-connector.ps1
│
└── docs/
```

Назначение основных файлов:

| Файл | Назначение |
|---|---|
| `docker-compose.yml` | Описание и запуск сервисов |
| `.env` | Переменные окружения и пароли |
| `postgres/init/01_init.sql` | Создание исходных таблиц и тестовых данных |
| `debezium/connector.json` | Конфигурация Debezium Connector |
| `clickhouse/init/01_init.sql` | Создание ClickHouse-таблиц и Materialized Views |
| `scripts/register-connector.ps1` | Регистрация и обновление Connector |
| `.gitignore` | Исключение секретов и локальных файлов из Git |

## 4. Конфигурация окружения

Секреты и параметры подключения хранятся в `.env`.

```dotenv
POSTGRES_DB=shop
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres

CLICKHOUSE_DB=cdc
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=clickhouse
```

Файл `.env` исключён из Git через `.gitignore`.

В реальном окружении необходимо использовать собственные надёжные пароли, а не демонстрационные значения.

## 5. Docker Compose

Вся инфраструктура запускается через Docker Compose.

В проект входят следующие сервисы:

| Сервис | Container name | Порт |
|---|---|---|
| PostgreSQL | cdc-postgres | 5432 |
| Kafka | cdc-kafka | 9092, 29092 |
| Debezium | cdc-debezium | 8083 |
| ClickHouse | cdc-clickhouse | 8123, 9000 |
| Kafka UI | cdc-kafka-ui | 8080 |

Основные особенности конфигурации:

- PostgreSQL работает с `wal_level=logical`.
- Kafka работает в режиме KRaft, без ZooKeeper.
- Kafka использует один broker.
- Debezium подключается к Kafka по внутреннему адресу `kafka:9092`.
- ClickHouse получает сообщения из Kafka через внутреннюю Docker-сеть.
- Данные PostgreSQL, Kafka и ClickHouse хранятся в Docker volumes.

Kafka Connect использует три служебных топика:

```text
connect_configs
connect_offsets
connect_statuses
```

Они используются для хранения конфигурации, offsets и статусов Kafka Connect.

## 6. PostgreSQL: исходные таблицы

В PostgreSQL создаются две таблицы:

- `customers`;
- `orders`.

Схема таблицы `customers`:

```sql
CREATE TABLE customers (
    customer_id BIGSERIAL PRIMARY KEY,
    first_name VARCHAR(100),
    email VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

Схема таблицы `orders`:

```sql
CREATE TABLE orders (
    order_id BIGSERIAL PRIMARY KEY,
    customer_id BIGINT REFERENCES customers(customer_id),
    status VARCHAR(30),
    total_amount NUMERIC(12, 2),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

Для таблицы `orders` устанавливается:

```sql
ALTER TABLE orders REPLICA IDENTITY FULL;
```

`REPLICA IDENTITY FULL` позволяет PostgreSQL передавать полную старую версию строки при UPDATE и DELETE.

Это важно для формирования Debezium CDC-событий с заполненным полем `before`.

Начальные данные:

```sql
INSERT INTO customers (first_name, email)
VALUES
    ('Ivan', 'ivan@example.com'),
    ('Petr', 'petr@example.com'),
    ('Anna', 'anna@example.com');

INSERT INTO orders (customer_id, status, total_amount)
VALUES
    (1, 'NEW', 1500.00),
    (2, 'PAID', 2500.00),
    (3, 'NEW', 3200.00);
```

## 7. Настройка PostgreSQL для CDC

Для работы Debezium используется logical replication.

Основные параметры PostgreSQL:

```text
wal_level = logical
max_wal_senders = 10
max_replication_slots = 10
```

При регистрации Debezium Connector автоматически создаются:

**Replication slot**

```text
shop_cdc_slot
```

Replication slot позволяет Debezium считывать изменения из WAL и отслеживать позицию чтения.

**Publication**

```text
shop_publication
```

Publication определяет, изменения каких таблиц PostgreSQL доступны для logical replication.

В проекте используется:

```json
"publication.autocreate.mode": "filtered"
```

Это позволяет Debezium автоматически создать publication с учётом списка отслеживаемых таблиц.

## 8. Debezium Connector

Конфигурация находится в:

```text
debezium/connector.json
```

Основные параметры:

```json
{
  "name": "postgres-cdc-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "tasks.max": "1",

    "database.hostname": "postgres",
    "database.port": "5432",
    "database.user": "postgres",
    "database.password": "__POSTGRES_PASSWORD__",
    "database.dbname": "shop",

    "topic.prefix": "shop",

    "plugin.name": "pgoutput",
    "decimal.handling.mode": "string",
    "slot.name": "shop_cdc_slot",
    "publication.name": "shop_publication",
    "publication.autocreate.mode": "filtered",

    "schema.include.list": "public",
    "table.include.list": "public.customers,public.orders",

    "snapshot.mode": "initial"
  }
}
```

Назначение основных параметров:

| Параметр | Назначение |
|---|---|
| `connector.class` | Класс PostgreSQL Connector |
| `tasks.max` | Максимальное количество задач |
| `database.hostname` | Имя PostgreSQL в Docker-сети |
| `database.dbname` | Исходная база данных |
| `topic.prefix` | Префикс Kafka topics |
| `plugin.name` | Logical decoding plugin |
| `slot.name` | Имя replication slot |
| `publication.name` | Имя publication |
| `publication.autocreate.mode` | Автоматическое создание publication |
| `table.include.list` | Список отслеживаемых таблиц |
| `snapshot.mode` | Режим первоначальной загрузки |

### Формирование Kafka topics

Параметр:

```json
"topic.prefix": "shop"
```

Используется Debezium для формирования имён топиков.

Для проекта получаются:

```text
shop.public.customers
shop.public.orders
```

Структура имени:

```text
topic.prefix.schema.table
```

## 9. Регистрация Debezium Connector

Для регистрации используется PowerShell-скрипт:

```text
scripts/register-connector.ps1
```

Скрипт выполняет следующие действия:

1. Читает `.env`.
2. Получает пароль PostgreSQL.
3. Загружает `connector.json`.
4. Подставляет пароль вместо `__POSTGRES_PASSWORD__`.
5. Проверяет доступность Kafka Connect REST API.
6. Проверяет наличие Connector.
7. Регистрирует новый Connector или обновляет существующий.
8. Ожидает перехода Connector и его Task в состояние `RUNNING`.

Запуск:

```powershell
.\scripts\register-connector.ps1
```

Kafka Connect REST API:

```text
http://localhost:8083
```

Проверка зарегистрированных Connector:

```powershell
Invoke-RestMethod `
    -Uri "http://localhost:8083/connectors" `
    -Method Get
```

Проверка состояния:

```powershell
Invoke-RestMethod `
    -Uri "http://localhost:8083/connectors/postgres-cdc-connector/status" `
    -Method Get
```

Ожидаемое состояние:

```text
Connector: RUNNING
Task: 0 RUNNING
```

## 10. Начальный snapshot

При первой регистрации Connector используется:

```json
"snapshot.mode": "initial"
```

Debezium выполняет первоначальное чтение таблиц, которые указаны в `table.include.list`.

В рамках snapshot существующие строки передаются в Kafka как события с операцией:

```text
op = r
```

`r` означает read — чтение записи в рамках snapshot.

В проекте начальные данные содержат:

- 3 записи в `customers`;
- 3 записи в `orders`.

После первоначального snapshot Debezium продолжает отслеживать изменения через PostgreSQL WAL.

Таким образом, snapshot обеспечивает первоначальную загрузку, а logical replication — передачу последующих изменений.

## 11. Проверка Kafka

После регистрации Connector должны появиться два топика:

```text
shop.public.customers
shop.public.orders
```

Проверка:

```powershell
docker compose exec kafka /kafka/bin/kafka-topics.sh `
    --bootstrap-server kafka:9092 `
    --list
```

Чтение сообщений из топика `orders`:

```powershell
docker compose exec kafka /kafka/bin/kafka-console-consumer.sh `
    --bootstrap-server kafka:9092 `
    --topic shop.public.orders `
    --from-beginning `
    --max-messages 3
```

В сообщениях Debezium используется структура с полем `payload`.

Основные поля:

```json
{
  "payload": {
    "before": null,
    "after": {
      "order_id": 1,
      "customer_id": 1,
      "status": "NEW",
      "total_amount": "1500.00"
    },
    "op": "r"
  }
}
```

Назначение основных полей:

| Поле | Значение |
|---|---|
| `before` | Состояние строки до изменения |
| `after` | Состояние строки после изменения |
| `op` | Тип CDC-операции |
| `source` | Метаданные источника |
| `ts_ms` | Время события в миллисекундах |

Основные значения `op`:

| Операция | Значение |
|---|---|
| INSERT | `c` |
| UPDATE | `u` |
| DELETE | `d` |
| Snapshot | `r` |

## 12. ClickHouse: архитектура Raw и Typed

В ClickHouse реализована двухуровневая схема хранения CDC-событий.

### Raw layer

Таблица:

```text
cdc.orders_cdc_raw
```

Назначение:
- хранение исходного JSON-сообщения Debezium;
- сохранение истории полученных событий;
- возможность повторного анализа исходного payload;
- отделение получения данных от их преобразования.

Структура:

```sql
CREATE TABLE cdc.orders_cdc_raw
(
    event_time DateTime64(3),
    message String
)
ENGINE = MergeTree
ORDER BY event_time;
```

В `message` сохраняется исходное JSON-сообщение.

`event_time` в Raw-таблице — время поступления записи в ClickHouse.

### Typed layer

Таблица:

```text
cdc.orders_cdc_events
```

Назначение:
- преобразование JSON в типизированные колонки;
- хранение старых и новых значений;
- сохранение типа операции;
- хранение метаданных Debezium;
- подготовка данных для аналитических запросов.

Структура:

```sql
CREATE TABLE cdc.orders_cdc_events
(
    order_id UInt64,

    old_status Nullable(String),
    new_status Nullable(String),

    old_total_amount Nullable(Decimal(12, 2)),
    new_total_amount Nullable(Decimal(12, 2)),

    operation LowCardinality(String),
    source_table LowCardinality(String),

    event_time DateTime64(3),
    ingested_at DateTime64(3),

    lsn UInt64,
    tx_id Nullable(UInt64)
)
ENGINE = MergeTree
ORDER BY (order_id, event_time, lsn);
```

Особенности:
- `old_status` и `new_status` хранят старое и новое значение статуса;
- `old_total_amount` и `new_total_amount` хранят старую и новую сумму;
- `operation` хранит тип CDC-события;
- `source_table` содержит имя исходной таблицы;
- `event_time` — время события Debezium;
- `ingested_at` — время поступления в ClickHouse;
- `lsn` — позиция изменения в WAL;
- `tx_id` — идентификатор транзакции, если он присутствует в событии.

Поля старого и нового состояния nullable, поскольку для INSERT и DELETE одна из сторон отсутствует.

## 13. ClickHouse Kafka Engine и Materialized Views

Для чтения сообщений используется Kafka Engine.

### Kafka Engine table

```sql
CREATE TABLE cdc.orders_kafka
(
    raw String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'shop.public.orders',
    kafka_group_name = 'clickhouse_orders_consumer',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 1;
```

Kafka Engine подключается к топику:

```text
shop.public.orders
```

Формат `JSONAsString` позволяет получать целое Kafka-сообщение как строку.

### Materialized View: Kafka → Raw

```sql
CREATE MATERIALIZED VIEW cdc.orders_kafka_raw_mv
TO cdc.orders_cdc_raw
AS
SELECT
    now64(3) AS event_time,
    raw AS message
FROM cdc.orders_kafka;
```

Эта Materialized View автоматически сохраняет исходные сообщения Kafka в Raw-таблицу.

### Materialized View: Raw → Typed

```sql
CREATE MATERIALIZED VIEW cdc.orders_raw_events_mv
TO cdc.orders_cdc_events
AS
SELECT
    toUInt64(
        if(
            JSONExtractInt(
                message, 'payload', 'after', 'order_id'
            ) != 0,
            JSONExtractInt(
                message, 'payload', 'after', 'order_id'
            ),
            JSONExtractInt(
                message, 'payload', 'before', 'order_id'
            )
        )
    ) AS order_id,

    nullIf(
        JSONExtractString(
            message, 'payload', 'before', 'status'
        ),
        ''
    ) AS old_status,

    nullIf(
        JSONExtractString(
            message, 'payload', 'after', 'status'
        ),
        ''
    ) AS new_status,

    toDecimal64OrNull(
        nullIf(
            JSONExtractString(
                message, 'payload', 'before', 'total_amount'
            ),
            ''
        ),
        2
    ) AS old_total_amount,

    toDecimal64OrNull(
        nullIf(
            JSONExtractString(
                message, 'payload', 'after', 'total_amount'
            ),
            ''
        ),
        2
    ) AS new_total_amount,

    JSONExtractString(
        message, 'payload', 'op'
    ) AS operation,

    JSONExtractString(
        message, 'payload', 'source', 'table'
    ) AS source_table,

    fromUnixTimestamp64Milli(
        JSONExtractInt(message, 'payload', 'ts_ms')
    ) AS event_time,

    ingested_at,

    toUInt64(
        JSONExtractInt(message, 'payload', 'source', 'lsn')
    ) AS lsn,

    nullIf(
        toUInt64(
            JSONExtractInt(message, 'payload', 'source', 'txId')
        ),
        0
    ) AS tx_id

FROM
(
    SELECT
        message,
        event_time AS ingested_at
    FROM cdc.orders_cdc_raw
);
```

Эта Materialized View срабатывает при вставке новых блоков в Raw-таблицу и преобразует их в типизированные записи.

### Почему используются две Materialized Views

В проекте Kafka Engine читается только одной Materialized View.

Схема:

```text
Kafka Engine
     |
     v
Kafka → Raw MV
     |
     v
Raw MergeTree
     |
     v
Raw → Typed MV
     |
     v
Typed MergeTree
```

Это позволяет сохранить каждое сообщение в Raw и одновременно сформировать типизированную запись.

Если создать две независимые Materialized Views непосредственно из Kafka Engine, они будут конкурировать за чтение сообщений через Kafka consumer group. Поэтому в проекте используется каскадная схема через Raw-таблицу.

## 14. Особенности обработки CDC-событий

### INSERT

При вставке новой записи:

```text
before = NULL
after  = новая версия строки
op     = c
```

В Typed-таблице:
- старые значения — `NULL`;
- новые значения заполнены.

### UPDATE

При обновлении записи:

```text
before = старая версия строки
after  = новая версия строки
op     = u
```

В Typed-таблице сохраняются обе версии.

### DELETE

При удалении записи:

```text
before = последняя версия строки
after  = NULL
op     = d
```

Для DELETE идентификатор заказа извлекается из `before`, поскольку `after` отсутствует.

### Snapshot

При начальной загрузке:

```text
before = NULL
after  = существующая строка
op     = r
```

Snapshot не является обычным INSERT. Это отдельный тип события, обозначающий первоначальное чтение существующей записи.

## 15. Запуск проекта

### 15.1. Проверка Docker Compose

Из корневой директории проекта:

```powershell
docker compose config --quiet
```

### 15.2. Запуск инфраструктуры

```powershell
docker compose up -d
```

Проверка контейнеров:

```powershell
docker compose ps
```

PostgreSQL и ClickHouse должны перейти в состояние `healthy`.

### 15.3. Проверка Kafka Connect

```powershell
Invoke-RestMethod `
    -Uri "http://localhost:8083/connectors" `
    -Method Get
```

### 15.4. Регистрация Debezium Connector

```powershell
.\scripts\register-connector.ps1
```

### 15.5. Проверка PostgreSQL CDC

Replication slot:

```powershell
docker compose exec postgres psql -U postgres -d shop -c "SELECT slot_name, plugin, slot_type, active FROM pg_replication_slots;"
```

Publication:

```powershell
docker compose exec postgres psql -U postgres -d shop -c "SELECT pubname, puballtables FROM pg_publication;"
```

Replica identity:

```powershell
docker compose exec postgres psql -U postgres -d shop -c "SELECT relname, relreplident FROM pg_class WHERE relname = 'orders';"
```

### 15.6. Проверка Kafka topics

```powershell
docker compose exec kafka /kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list
```

### 15.7. Проверка ClickHouse

```powershell
docker compose exec clickhouse clickhouse-client --user default --password clickhouse --query "SHOW TABLES FROM cdc"
```

## 16. Проверка работоспособности pipeline

После чистого запуска проекта были выполнены проверки snapshot и всех основных CDC-операций.

### 16.1. Проверка initial snapshot

После регистрации Connector в Kafka появились:

```text
shop.public.customers
shop.public.orders
```

В топике `shop.public.orders` обнаружены три snapshot-события.

Результат:

| order_id | status | operation |
|---:|---|---|
| 1 | NEW | `r` |
| 2 | PAID | `r` |
| 3 | NEW | `r` |

В ClickHouse проверено количество записей:

| Таблица | Количество |
|---|---:|
| `orders_cdc_raw` | 3 |
| `orders_cdc_events` | 3 |

Типизированные данные:

| order_id | old_status | new_status | old_amount | new_amount | operation |
|---:|---|---|---:|---:|---|
| 1 | NULL | NEW | NULL | 1500 | r |
| 2 | NULL | PAID | NULL | 2500 | r |
| 3 | NULL | NEW | NULL | 3200 | r |

### 16.2. Проверка UPDATE

В PostgreSQL выполнен запрос:

```sql
UPDATE orders
SET
    status = 'PROCESSING',
    total_amount = 1750.00,
    updated_at = CURRENT_TIMESTAMP
WHERE order_id = 1;
```

В ClickHouse получены две записи:

| order_id | old_status | new_status | old_amount | new_amount | operation |
|---:|---|---|---:|---:|---|
| 1 | NULL | NEW | NULL | 1500 | r |
| 1 | NEW | PROCESSING | 1500 | 1750 | u |

Результат подтверждает, что Debezium передаёт старую и новую версии строки.

### 16.3. Проверка DELETE

В PostgreSQL выполнен запрос:

```sql
DELETE FROM orders
WHERE order_id = 2;
```

В ClickHouse получены:

| order_id | old_status | new_status | old_amount | new_amount | operation |
|---:|---|---|---:|---:|---|
| 2 | NULL | PAID | NULL | 2500 | r |
| 2 | PAID | NULL | 2500 | NULL | d |

Результат подтверждает корректную обработку DELETE.

Старые значения и идентификатор заказа извлекаются из `before`.

### 16.4. Проверка INSERT

В PostgreSQL выполнен запрос:

```sql
INSERT INTO orders (
    customer_id,
    status,
    total_amount
)
VALUES (
    1,
    'NEW',
    4100.00
)
RETURNING order_id;
```

PostgreSQL вернул:

```text
order_id = 4
```

В ClickHouse получено:

| order_id | old_status | new_status | old_amount | new_amount | operation |
|---:|---|---|---:|---:|---|
| 4 | NULL | NEW | NULL | 4100 | c |

Результат подтверждает автоматическую передачу нового INSERT-события.

### 16.5. Итог функционального тестирования

| Сценарий | Операция | Результат |
|---|---|---|
| Initial snapshot | `r` | Успешно |
| INSERT | `c` | Успешно |
| UPDATE | `u` | Успешно |
| DELETE | `d` | Успешно |
| Raw ingestion | JSON → Raw | Успешно |
| Typed ingestion | Raw → Typed | Успешно |

Все перечисленные сценарии были проверены на работающем локальном окружении.

## 17. Полезные команды

### Docker

Посмотреть состояние контейнеров:

```powershell
docker compose ps
```

Посмотреть логи всех сервисов:

```powershell
docker compose logs -f
```

Логи конкретного сервиса:

```powershell
docker compose logs -f postgres
docker compose logs -f kafka
docker compose logs -f debezium
docker compose logs -f clickhouse
```

Остановить контейнеры, сохранив данные:

```powershell
docker compose down
```

Остановить контейнеры и удалить volumes:

```powershell
docker compose down -v
```

**Внимание:** `down -v` удаляет данные PostgreSQL, Kafka и ClickHouse. Использовать только при необходимости полностью пересоздать окружение.

### Kafka

Список топиков:

```powershell
docker compose exec kafka /kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list
```

Чтение сообщений:

```powershell
docker compose exec kafka /kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic shop.public.orders --from-beginning
```

### ClickHouse

Список таблиц:

```sql
SHOW TABLES FROM cdc;
```

Количество Raw-событий:

```sql
SELECT count()
FROM cdc.orders_cdc_raw;
```

Количество Typed-событий:

```sql
SELECT count()
FROM cdc.orders_cdc_events;
```

Просмотр истории изменений:

```sql
SELECT
    order_id,
    old_status,
    new_status,
    old_total_amount,
    new_total_amount,
    operation,
    event_time,
    lsn
FROM cdc.orders_cdc_events
ORDER BY order_id, event_time, lsn;
```

Количество событий по типам:

```sql
SELECT
    operation,
    count() AS events
FROM cdc.orders_cdc_events
GROUP BY operation
ORDER BY operation;
```

---

## 18. Что демонстрирует проект

Проект демонстрирует практическую реализацию CDC pipeline и работу со следующими технологиями и механизмами:

- PostgreSQL Logical Replication;
- Write-Ahead Log (WAL);
- Replication Slots и Publications;
- Debezium PostgreSQL Connector;
- Kafka Connect REST API;
- Apache Kafka и KRaft;
- Kafka topics и consumer groups;
- CDC-события `before` / `after`;
- Initial Snapshot;
- ClickHouse Kafka Engine;
- ClickHouse Materialized Views;
- Raw и Typed layers;
- JSON parsing в ClickHouse;
- MergeTree;
- обработка INSERT, UPDATE и DELETE;
- автоматизация регистрации Connector через PowerShell;
- контейнеризация инфраструктуры с Docker Compose.

**Результат:** реализован и проверен локальный near real-time CDC pipeline, который автоматически переносит изменения из PostgreSQL через Debezium и Kafka в ClickHouse, сохраняя как исходные JSON-события, так и типизированную историю изменений.
