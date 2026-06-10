package com.exchange.gateway

import org.springframework.http.HttpStatus
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Service
import org.springframework.web.server.ResponseStatusException
import java.util.concurrent.ConcurrentHashMap

data class ResolvedAccount(
    val uid: Long,
    val status: AccountStatus,
    val jurisdiction: String?
)

/**
 * Resolves a Keycloak subject (JWT "sub" claim) to the internal Long uid used
 * by exchange-core and TigerBeetle, and reads the current account status.
 *
 * The uid mapping is immutable once set — cached in memory. The account_status
 * is mutable and is NEVER cached: it is always read fresh from the DB at the
 * point of control, before any durable write.
 *
 * Invariant: account_status must never enter the Kafka journal nor the
 * TigerBeetle accountId. Both are immutable identifiers that underpin
 * idempotence — a status change would orphan all historical records.
 *
 * Unknown sub: throws 403. NEVER falls back to a default uid.
 */
@Service
class UserService(private val jdbc: JdbcTemplate) {

    private val cache = ConcurrentHashMap<String, Long>()

    fun resolveUid(keycloakSub: String): Long {
        cache[keycloakSub]?.let { return it }

        val uid = jdbc.query(
            "SELECT internal_uid FROM users WHERE keycloak_sub = ?",
            { rs, _ -> rs.getLong("internal_uid") },
            keycloakSub
        ).firstOrNull()
            ?: throw ResponseStatusException(
                HttpStatus.FORBIDDEN,
                "Authenticated user has no registered account"
            )

        cache[keycloakSub] = uid
        return uid
    }

    /**
     * Reads the current status and jurisdiction of an account by uid.
     * NOT cached — account_status is mutable. Always hits the DB.
     * Called at the point of control, before any durable write.
     */
    fun resolveStatus(uid: Long): ResolvedAccount {
        return jdbc.query(
            "SELECT internal_uid, account_status, jurisdiction FROM users WHERE internal_uid = ?",
            { rs, _ -> ResolvedAccount(
                uid  = rs.getLong("internal_uid"),
                status = AccountStatus.valueOf(rs.getString("account_status")),
                jurisdiction = rs.getString("jurisdiction")
            )},
            uid
        ).firstOrNull()
            ?: throw ResponseStatusException(HttpStatus.NOT_FOUND, "Account not found: uid=$uid")
    }

    /**
     * Transitions an account to a new status and writes the audit trail.
     * CITIZEN_APPROVED requires a non-null jurisdiction, enforced both here
     * and by the DB CHECK constraint. actorSub identifies the admin making the change.
     */
    fun setAccountStatus(uid: Long, newStatus: AccountStatus, jurisdiction: String?, actorSub: String) {
        if (newStatus == AccountStatus.CITIZEN_APPROVED && jurisdiction.isNullOrBlank()) {
            throw ResponseStatusException(HttpStatus.BAD_REQUEST, "CITIZEN_APPROVED requires a jurisdiction")
        }
        val updated = jdbc.update(
            """UPDATE users
               SET account_status    = ?,
                   jurisdiction      = ?,
                   status_updated_at = NOW(),
                   status_updated_by = ?
               WHERE internal_uid = ?""",
            newStatus.name, jurisdiction, actorSub, uid
        )
        if (updated == 0) {
            throw ResponseStatusException(HttpStatus.NOT_FOUND, "Account not found: uid=$uid")
        }
    }
}
