-- B7 — Métadonnées de bien (habillage, non money-path)
-- Jamais lue par settlement ni engine — lecture seule par l'API de présentation.
-- Insérée par PropertyCommandConsumer après création du bien dans properties.

CREATE TABLE IF NOT EXISTS property_metadata (
    property_id  BIGINT       NOT NULL REFERENCES properties(id) PRIMARY KEY,
    description  TEXT,
    location     VARCHAR(255),
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
