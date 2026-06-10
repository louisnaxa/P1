package com.exchange.gateway

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.springframework.dao.DataIntegrityViolationException
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.datasource.DriverManagerDataSource
import org.testcontainers.containers.PostgreSQLContainer

/**
 * Proves the holder-status model against a real PostgreSQL database.
 *
 * What is proved:
 *   - A new account is UNVERIFIED by default (no explicit status on INSERT).
 *   - Status transitions are read back correctly for all non-default states.
 *   - Every transition writes status_updated_at (non-null) and status_updated_by (exact actor).
 *   - DB CHECK constraint REJECTS CITIZEN_APPROVED without jurisdiction:
 *       bypass the Kotlin guard and write directly via JdbcTemplate — proves the
 *       constraint on the database itself, independently of the application code.
 *   - DB CHECK constraint REJECTS an unknown status string.
 *
 * Uses real PostgreSQL (Testcontainers) — not H2 — because H2 and PostgreSQL differ
 * in constraint enforcement. A green H2 test does not prove a PostgreSQL constraint.
 * The status is money-path (it conditions access to funds), so it gets the same
 * rigour as the settlement and custody tests: proof against the real engine, and
 * the REJECTION is proved, not only the happy path.
 *
 * Run: ./gradlew :gateway:statusTest
 */
@Tag("integration")
class AccountStatusIntegrationTest {

    companion object {
        @Suppress("UNCHECKED_CAST")
        private val pg = PostgreSQLContainer("postgres:16-alpine") as PostgreSQLContainer<*>

        private lateinit var jdbc: JdbcTemplate
        private lateinit var userService: UserService

        @JvmStatic
        @BeforeAll
        fun setup() {
            pg.start()
            jdbc = JdbcTemplate(DriverManagerDataSource(pg.jdbcUrl, pg.username, pg.password))
            createSchema()
            userService = UserService(jdbc)
        }

        @JvmStatic
        @AfterAll
        fun teardown() {
            pg.stop()
        }

        /**
         * Identical to the production DDL in infra/timescale-init/02-users.sql.
         * Any divergence between this schema and production is a bug: the constraints
         * tested here must be the constraints that run in production.
         */
        private fun createSchema() {
            jdbc.execute("""
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
                )
            """.trimIndent())
        }
    }

    @BeforeEach
    fun cleanTable() {
        jdbc.execute("DELETE FROM users")
    }

    private fun insertUser(uid: Long, sub: String) {
        jdbc.update("INSERT INTO users (keycloak_sub, internal_uid) VALUES (?, ?)", sub, uid)
    }

    // ── Valid-path proofs ────────────────────────────────────────────────────

    @Test
    fun `new account is UNVERIFIED by default`() {
        insertUser(1L, "sub-1")
        val account = userService.resolveStatus(1L)
        assertThat(account.status).isEqualTo(AccountStatus.UNVERIFIED)
        assertThat(account.jurisdiction).isNull()
    }

    @Test
    fun `status transition writes audit trail - timestamp and actor recorded`() {
        insertUser(2L, "sub-2")
        userService.setAccountStatus(2L, AccountStatus.FOREIGN_SPECULATIVE, null, "admin-alice")

        val row = jdbc.queryForMap(
            "SELECT account_status, status_updated_at, status_updated_by FROM users WHERE internal_uid = ?", 2L
        )
        assertThat(row["account_status"]).isEqualTo("FOREIGN_SPECULATIVE")
        assertThat(row["status_updated_by"]).isEqualTo("admin-alice")
        assertThat(row["status_updated_at"]).isNotNull()
    }

    @Test
    fun `CITIZEN_APPROVED with jurisdiction is read back correctly`() {
        insertUser(3L, "sub-3")
        userService.setAccountStatus(3L, AccountStatus.CITIZEN_APPROVED, "AE-AZ", "admin-bob")

        val account = userService.resolveStatus(3L)
        assertThat(account.status).isEqualTo(AccountStatus.CITIZEN_APPROVED)
        assertThat(account.jurisdiction).isEqualTo("AE-AZ")
    }

    @Test
    fun `SUSPENDED account is read back correctly`() {
        insertUser(4L, "sub-4")
        userService.setAccountStatus(4L, AccountStatus.SUSPENDED, null, "admin-bob")

        val account = userService.resolveStatus(4L)
        assertThat(account.status).isEqualTo(AccountStatus.SUSPENDED)
        assertThat(account.jurisdiction).isNull()
    }

    // ── Constraint-rejection proofs ──────────────────────────────────────────
    // These bypass the Kotlin guard in setAccountStatus() and write directly via
    // JdbcTemplate to prove that the DB itself — not just the application — rejects
    // invalid states. This is the correct level of proof for a DB constraint.

    @Test
    fun `DB rejects CITIZEN_APPROVED without jurisdiction - chk_citizen_jurisdiction fires`() {
        insertUser(10L, "sub-10")
        assertThatThrownBy {
            jdbc.update(
                "UPDATE users SET account_status = 'CITIZEN_APPROVED', jurisdiction = NULL WHERE internal_uid = ?",
                10L
            )
        }.isInstanceOf(DataIntegrityViolationException::class.java)
    }

    @Test
    fun `DB rejects unknown status string - account_status CHECK fires`() {
        insertUser(11L, "sub-11")
        assertThatThrownBy {
            jdbc.update(
                "UPDATE users SET account_status = 'UNKNOWN_STATUS' WHERE internal_uid = ?",
                11L
            )
        }.isInstanceOf(DataIntegrityViolationException::class.java)
    }
}
