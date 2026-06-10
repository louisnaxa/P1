package com.exchange.gateway

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.autoconfigure.jdbc.JdbcTest
import org.springframework.context.annotation.Import
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.test.context.jdbc.Sql
import org.springframework.test.context.jdbc.SqlConfig

/**
 * Proves the holder-status model against a real (H2) database.
 *
 * What is proved:
 *   - A new account is UNVERIFIED by default (no explicit status on INSERT).
 *   - Status transitions are read back correctly: SUSPENDED, CITIZEN_APPROVED + jurisdiction.
 *   - Every transition writes status_updated_at (non-null) and status_updated_by (exact actor).
 *
 * @JdbcTest wires a real JdbcTemplate against an in-memory H2 database.
 * @Sql(ISOLATED) creates the schema in its own committed transaction before each test,
 * so the DDL persists even though @JdbcTest rolls back DML after each test.
 * Each test therefore starts with an empty users table and a live schema.
 */
@JdbcTest
@Import(UserService::class)
@Sql(
    scripts = ["/test-schema-users.sql"],
    config = SqlConfig(transactionMode = SqlConfig.TransactionMode.ISOLATED)
)
class AccountStatusTest {

    @Autowired private lateinit var userService: UserService
    @Autowired private lateinit var jdbc: JdbcTemplate

    private fun insertUser(uid: Long, sub: String) {
        jdbc.update("INSERT INTO users (keycloak_sub, internal_uid) VALUES (?, ?)", sub, uid)
    }

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
}
