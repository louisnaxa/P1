CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

CREATE TABLE IF NOT EXISTS candles (
    symbol_id   INT         NOT NULL,
    resolution  VARCHAR(4)  NOT NULL,
    bucket_time TIMESTAMPTZ NOT NULL,
    open        BIGINT      NOT NULL,
    high        BIGINT      NOT NULL,
    low         BIGINT      NOT NULL,
    close       BIGINT      NOT NULL,
    volume      BIGINT      NOT NULL,
    PRIMARY KEY (symbol_id, resolution, bucket_time)
);

SELECT create_hypertable('candles', 'bucket_time', if_not_exists => TRUE);
