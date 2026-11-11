-- =====================================================
-- 1. Database
-- =====================================================

CREATE DATABASE IF NOT EXISTS cdc;


-- =====================================================
-- 2. Raw table
-- Хранение исходных JSON-событий Debezium
-- =====================================================

CREATE TABLE IF NOT EXISTS cdc.orders_cdc_raw
(
    event_time DateTime64(3),
    message String
)
ENGINE = MergeTree
ORDER BY event_time;


-- =====================================================
-- 3. Typed CDC events table
-- Хранение разобранных CDC-событий
-- =====================================================

CREATE TABLE IF NOT EXISTS cdc.orders_cdc_events
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


-- =====================================================
-- 4. Kafka Engine
-- Чтение сообщений из Kafka
-- =====================================================

CREATE TABLE IF NOT EXISTS cdc.orders_kafka
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


-- =====================================================
-- 5. Materialized View: Raw -> Typed
--
-- Создаём ПЕРЕД Kafka MV, чтобы typed MV
-- уже существовала к моменту начала чтения Kafka.
-- =====================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS cdc.orders_raw_events_mv
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


-- =====================================================
-- 6. Materialized View: Kafka -> Raw
--
-- Создаём ПОСЛЕДНЕЙ.
-- Это единственная MV, которая читает Kafka.
-- =====================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS cdc.orders_kafka_raw_mv
TO cdc.orders_cdc_raw
AS
SELECT
    now64(3) AS event_time,
    raw AS message
FROM cdc.orders_kafka;
