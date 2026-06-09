-- Maps Keycloak subject (UUID string) to the internal Long uid used by exchange-core.
-- keycloak_sub is the JWT "sub" claim; internal_uid is what circulates in EngineCommand.uid.
-- Populated by an admin when creating a new user account (not auto-created on signup).
CREATE TABLE IF NOT EXISTS users (
    keycloak_sub VARCHAR(255) NOT NULL PRIMARY KEY,
    internal_uid BIGINT       NOT NULL UNIQUE
);
