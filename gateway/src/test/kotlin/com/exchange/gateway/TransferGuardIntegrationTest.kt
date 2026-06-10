package com.exchange.gateway

import org.assertj.core.api.Assertions.assertThatCode
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.springframework.http.HttpStatus
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.datasource.DriverManagerDataSource
import org.springframework.web.server.ResponseStatusException
import org.testcontainers.containers.PostgreSQLContainer

/**
 * Proves TransferGuard access rules against a real PostgreSQL database.
 *
 * What is proved (money-path — the guard conditions access to the order book):
 *   R1 — UNVERIFIED is rejected
 *   R2 — SUSPENDED is rejected
 *   R3 — FOREIGN_SPECULATIVE is allowed for any symbol (including unknown ones —
 *         no property lookup, early return before the DB join)
 *   R4 — CITIZEN_APPROVED(J) × property(J) is allowed
 *   R5 — CITIZEN_APPROVED(J) × property(J') is rejected
 *   R6 — CITIZEN_APPROVED × unknown symbol is rejected (fail-closed: no property
 *         → cannot verify jurisdiction → deny rather than open)
 *
 * Each rejection test proves the DB state (status + jurisdiction) is what drives
 * the decision — not application-level guards that could be bypassed.
 *
 * Uses real PostgreSQL (Testcontainers) — not H2 — for the same reason as
 * AccountStatusIntegrationTest: the status check reads from a real DB, and the
 * guard behaviour must be proved against the real engine.
 *
 * Run: ./gradlew :gateway:transferTest
 */
@Tag("transfer")
class TransferGuardIntegrationTest {

    companion object {
        const val UID_UNVERIFIED  = 201L
        const val UID_SUSPENDED   = 202L
        const val UID_FOREIGN     = 203L
        const val UID_CITIZEN_AZ  = 204L   // CITIZEN_APPROVED, jurisdiction = AE-AZ
        const val UID_CITIZEN_DU  = 205L   // CITIZEN_APPROVED, jurisdiction = AE-DU

        const val SYMBOL_AE_AZ = 3001      // property in AE-AZ
        const val LEDGER_AE_AZ = 3001
        const val SYMBOL_UNKNOWN = 9999    // not in symbols table

        @Suppress("UNCHECKED_CAST")
        private val pg = PostgreSQLContainer("postgres:16-alpine") as PostgreSQLContainer<*>

        private lateinit var jdbc: JdbcTemplate
        private lateinit var userService: UserService
        private lateinit var transferGuard: TransferGuard

        @JvmStatic
        @BeforeAll
        fun setup() {
            pg.start()
            jdbc = JdbcTemplate(DriverManagerDataSource(pg.jdbcUrl, pg.username, pg.password))
            createSchema()
            userService = UserService(jdbc)
            transferGuard = TransferGuard(userService, jdbc)
            seedData()
        }

        @JvmStatic
        @AfterAll
        fun teardown() {
            pg.stop()
        }

        /**
         * Identical to the production DDL in:
         *   infra/timescale-init/02-users.sql     (users)
         *   infra/timescale-init/05-properties.sql (properties, symbols)
         * Any divergence is a bug.
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
            jdbc.execute("""
                CREATE TABLE IF NOT EXISTS properties (
                    id                 BIGSERIAL    PRIMARY KEY,
                    name               VARCHAR(255) NOT NULL,
                    jurisdiction       VARCHAR(10)  NOT NULL,
                    property_ledger_id INT          NOT NULL UNIQUE,
                    total_tokens       BIGINT       NOT NULL CHECK (total_tokens > 0)
                )
            """.trimIndent())
            jdbc.execute("""
                CREATE TABLE IF NOT EXISTS symbols (
                    id               INT    PRIMARY KEY,
                    property_id      BIGINT NOT NULL REFERENCES properties(id),
                    base_ledger_id   INT    NOT NULL,
                    quote_ledger_id  INT    NOT NULL
                )
            """.trimIndent())
        }

        private fun seedData() {
            insertUser(UID_UNVERIFIED, "sub-unverified", "UNVERIFIED",           null)
            insertUser(UID_SUSPENDED,  "sub-suspended",  "SUSPENDED",            null)
            insertUser(UID_FOREIGN,    "sub-foreign",    "FOREIGN_SPECULATIVE",  null)
            insertUser(UID_CITIZEN_AZ, "sub-citizen-az", "CITIZEN_APPROVED",     "AE-AZ")
            insertUser(UID_CITIZEN_DU, "sub-citizen-du", "CITIZEN_APPROVED",     "AE-DU")

            val propId = jdbc.queryForObject(
                "INSERT INTO properties (name, jurisdiction, property_ledger_id, total_tokens) VALUES (?,?,?,?) RETURNING id",
                Long::class.java, "Burj Test", "AE-AZ", LEDGER_AE_AZ, 1_000_000L
            )!!
            jdbc.update(
                "INSERT INTO symbols (id, property_id, base_ledger_id, quote_ledger_id) VALUES (?,?,?,?)",
                SYMBOL_AE_AZ, propId, LEDGER_AE_AZ, 100
            )
        }

        private fun insertUser(uid: Long, sub: String, status: String, jurisdiction: String?) {
            jdbc.update(
                "INSERT INTO users (keycloak_sub, internal_uid, account_status, jurisdiction) VALUES (?,?,?,?)",
                sub, uid, status, jurisdiction
            )
        }

        private fun assertForbidden(block: () -> Unit) {
            assertThatThrownBy { block() }
                .isInstanceOf(ResponseStatusException::class.java)
                .satisfies({ ex ->
                    org.assertj.core.api.Assertions.assertThat(
                        (ex as ResponseStatusException).statusCode
                    ).isEqualTo(HttpStatus.FORBIDDEN)
                })
        }
    }

    // ── Rejection proofs ─────────────────────────────────────────────────────

    @Test
    fun `R1 UNVERIFIED account is rejected`() {
        assertForbidden { transferGuard.checkOrderAccess(UID_UNVERIFIED, SYMBOL_AE_AZ) }
    }

    @Test
    fun `R2 SUSPENDED account is rejected`() {
        assertForbidden { transferGuard.checkOrderAccess(UID_SUSPENDED, SYMBOL_AE_AZ) }
    }

    @Test
    fun `R5 CITIZEN_APPROVED in non-matching jurisdiction is rejected`() {
        // UID_CITIZEN_DU is AE-DU, the property is AE-AZ
        assertForbidden { transferGuard.checkOrderAccess(UID_CITIZEN_DU, SYMBOL_AE_AZ) }
    }

    @Test
    fun `R6 CITIZEN_APPROVED with unknown symbol is rejected - fail closed`() {
        assertForbidden { transferGuard.checkOrderAccess(UID_CITIZEN_AZ, SYMBOL_UNKNOWN) }
    }

    // ── Allow proofs ─────────────────────────────────────────────────────────

    @Test
    fun `R3 FOREIGN_SPECULATIVE is allowed for any symbol including unknown`() {
        // Known symbol — no exception
        assertThatCode { transferGuard.checkOrderAccess(UID_FOREIGN, SYMBOL_AE_AZ) }
            .doesNotThrowAnyException()
        // Unknown symbol — FOREIGN bypasses property lookup entirely, still no exception
        assertThatCode { transferGuard.checkOrderAccess(UID_FOREIGN, SYMBOL_UNKNOWN) }
            .doesNotThrowAnyException()
    }

    @Test
    fun `R4 CITIZEN_APPROVED in matching jurisdiction is allowed`() {
        // UID_CITIZEN_AZ is AE-AZ, the property is AE-AZ
        assertThatCode { transferGuard.checkOrderAccess(UID_CITIZEN_AZ, SYMBOL_AE_AZ) }
            .doesNotThrowAnyException()
    }
}
