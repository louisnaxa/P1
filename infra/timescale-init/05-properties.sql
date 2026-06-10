-- B2 — Gestion par bien (property/asset management)
--
-- Chaque bien immobilier = un ledger TigerBeetle dédié (property_ledger_id).
-- Les tokens sont off-chain : TigerBeetle fait foi pour les montants.
-- property_holders est une projection de TB, reconstruisible à tout moment.
--
-- Contrainte 24 bits sur property_ledger_id : voir TD-14 dans TECH_DEBT.md.

CREATE TABLE IF NOT EXISTS properties (
    id                 BIGSERIAL    PRIMARY KEY,
    name               VARCHAR(255) NOT NULL,
    jurisdiction       VARCHAR(10)  NOT NULL,
    property_ledger_id INT          NOT NULL UNIQUE,  -- TigerBeetle ledger dédié au bien
    total_tokens       BIGINT       NOT NULL CHECK (total_tokens > 0)
);

-- Fait correspondre un symbolId exchange-core à la paire de ledgers TigerBeetle.
-- base_ledger_id  = ledger de tokens du bien ; quote_ledger_id = ledger stablecoin.
-- Remplace le TODO hardcodé (baseLedger=10, quoteLedger=11) dans TradeConsumer.
CREATE TABLE IF NOT EXISTS symbols (
    id               INT    PRIMARY KEY,              -- symbolId utilisé par exchange-core
    property_id      BIGINT NOT NULL REFERENCES properties(id),
    base_ledger_id   INT    NOT NULL,
    quote_ledger_id  INT    NOT NULL
);

-- Projection des soldes TigerBeetle pour chaque (bien, détenteur).
-- TigerBeetle est la source de vérité — cette table est reconstruisible
-- en scannant tous les comptes (userId, propertyLedger) dans TB.
-- Mise à jour après chaque settlement de trade et à la création du bien.
CREATE TABLE IF NOT EXISTS property_holders (
    property_id      BIGINT      NOT NULL REFERENCES properties(id),
    uid              BIGINT      NOT NULL,
    balance_snapshot BIGINT      NOT NULL DEFAULT 0,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (property_id, uid)
);
