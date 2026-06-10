-- B4 — Droits économiques (loyers)
--
-- rent_distributions enregistre chaque distribution de loyers pour auditabilité et idempotence.
-- distribution_key : clé fournie par l'appelant (admin), unique par (property_id, distribution_key).
-- Sur crash-et-retry avec la même clé : le même id est retrouvé → mêmes rentTransferId → TB Exists.
--
-- Limites des identifiants de transfert (voir TD-16) :
--   distributionId occupe 32 bits dans le rentTransferId → max ~4.3 milliards de distributions/bien.
--   holderIndex    occupe 16 bits dans le rentTransferId → max 65 535 détenteurs par run.

CREATE TABLE IF NOT EXISTS rent_distributions (
    id                BIGSERIAL    PRIMARY KEY,
    property_id       BIGINT       NOT NULL REFERENCES properties(id),
    quote_ledger_id   INT          NOT NULL,
    total_amount      BIGINT       NOT NULL CHECK (total_amount > 0),
    distribution_key  BIGINT       NOT NULL,
    distributed_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    remainder         BIGINT       NOT NULL DEFAULT 0 CHECK (remainder >= 0),
    UNIQUE (property_id, distribution_key)
);
