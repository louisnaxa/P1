-- Withdrawal state machine.
-- Source of truth for on-chain withdrawal lifecycle.
--
-- State transitions:
--   LOCKED → BROADCAST → CONFIRMED
--   LOCKED → VOID
--
-- tb_pending_id: TigerBeetle PENDING transfer ID, created atomically before this row.
--   UNIQUE ensures one TB transfer per withdrawal row.
--   Derived as (1L << 62) | (id << 1) — bit 62 namespaces withdrawals away from trade legs.
--
-- tx_hash: set once at BROADCAST, never changed.
--   UNIQUE prevents two withdrawal rows from sharing the same on-chain tx.
--   NULLs are allowed (multiple rows can be LOCKED simultaneously).
--
-- nonce, raw_tx: populated by the MPC signer sub-lot (currently NULL).
CREATE TABLE IF NOT EXISTS withdrawals (
    id                  BIGINT       PRIMARY KEY,
    uid                 BIGINT       NOT NULL,
    currency            INT          NOT NULL,
    amount              BIGINT       NOT NULL,
    destination_address VARCHAR(42)  NOT NULL,
    tb_pending_id       BIGINT       NOT NULL UNIQUE,
    state               VARCHAR(20)  NOT NULL DEFAULT 'LOCKED'
                            CHECK (state IN ('LOCKED', 'BROADCAST', 'CONFIRMED', 'VOID')),
    tx_hash             VARCHAR(66)  UNIQUE,   -- null until BROADCAST; immutable once set
    nonce               BIGINT,                -- reserved: MPC nonce sub-lot
    raw_tx              TEXT,                  -- reserved: MPC raw tx sub-lot
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
