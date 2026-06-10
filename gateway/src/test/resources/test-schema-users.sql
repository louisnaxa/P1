-- H2-compatible schema for AccountStatusTest.
-- Uses TIMESTAMP instead of TIMESTAMPTZ (H2 does not support the abbreviation).
-- Production schema (02-users.sql) uses TIMESTAMPTZ for real PostgreSQL.
CREATE TABLE IF NOT EXISTS users (
    keycloak_sub      VARCHAR(255) NOT NULL PRIMARY KEY,
    internal_uid      BIGINT       NOT NULL UNIQUE,
    account_status    VARCHAR(30)  NOT NULL DEFAULT 'UNVERIFIED'
                          CHECK (account_status IN ('UNVERIFIED','FOREIGN_SPECULATIVE','CITIZEN_APPROVED','SUSPENDED')),
    jurisdiction      VARCHAR(10),
    status_updated_at TIMESTAMP,
    status_updated_by VARCHAR(255),
    CONSTRAINT chk_citizen_jurisdiction
        CHECK (account_status <> 'CITIZEN_APPROVED' OR jurisdiction IS NOT NULL)
);
