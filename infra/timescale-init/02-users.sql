-- Maps Keycloak subject (UUID string) to the internal Long uid used by exchange-core.
-- keycloak_sub is the JWT "sub" claim; internal_uid is what circulates in EngineCommand.uid.
-- Populated by an admin when creating a new user account (not auto-created on signup).
--
-- account_status: pilots what the account can hold, receive and trigger (brique B1).
-- jurisdiction: non-null only for CITIZEN_APPROVED — enforced by DB constraint.
-- status_updated_at / status_updated_by: audit trail for regulatory auditability.
-- The status is mutable; it must never enter the Kafka journal or the TigerBeetle accountId.
CREATE TABLE IF NOT EXISTS users (
    keycloak_sub      VARCHAR(255) NOT NULL PRIMARY KEY,
    internal_uid      BIGINT       NOT NULL UNIQUE,
    account_status    VARCHAR(30)  NOT NULL DEFAULT 'UNVERIFIED'
                          CHECK (account_status IN ('UNVERIFIED','FOREIGN_SPECULATIVE','CITIZEN_APPROVED','SUSPENDED')),
    jurisdiction      VARCHAR(10),
    status_updated_at TIMESTAMPTZ,
    status_updated_by VARCHAR(255),
    CONSTRAINT chk_citizen_jurisdiction
        CHECK (account_status <> 'CITIZEN_APPROVED' OR jurisdiction IS NOT NULL)
);
