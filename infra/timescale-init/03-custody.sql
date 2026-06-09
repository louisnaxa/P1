-- Maps an on-chain deposit address to an internal user account.
-- Populated by ops when creating a new user deposit address (MPC-derived).
-- NOTE: this table contains sensitive on-chain/user data — restrict read access.
CREATE TABLE IF NOT EXISTS deposit_addresses (
    address  VARCHAR(42)  NOT NULL PRIMARY KEY,  -- checksummed EVM address
    uid      BIGINT       NOT NULL,               -- internal user id (EngineCommand.uid)
    currency INT          NOT NULL                -- TigerBeetle ledger id
);

-- Transactional outbox for on-chain deposit events.
-- UNIQUE(tx_hash, log_index) is the idempotency gate: the custody-watcher inserts
-- ON CONFLICT DO NOTHING, so a re-delivered on-chain event never double-publishes to Kafka.
CREATE TABLE IF NOT EXISTS custody_events (
    id          BIGSERIAL    PRIMARY KEY,
    tx_hash     VARCHAR(66)  NOT NULL,
    log_index   INT          NOT NULL,
    uid         BIGINT       NOT NULL,
    currency    INT          NOT NULL,
    amount      BIGINT       NOT NULL,
    state       VARCHAR(20)  NOT NULL DEFAULT 'PENDING',  -- PENDING | EMITTED
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (tx_hash, log_index)
);

-- Tracks the highest EVM block number fully processed by the custody-watcher.
-- Singleton row (singleton_id = 1). Reset to an older block to trigger re-scan.
CREATE TABLE IF NOT EXISTS custody_sync (
    singleton_id INT    NOT NULL DEFAULT 1 PRIMARY KEY CHECK (singleton_id = 1),
    last_block   BIGINT NOT NULL DEFAULT 0
);
INSERT INTO custody_sync DEFAULT VALUES ON CONFLICT DO NOTHING;
