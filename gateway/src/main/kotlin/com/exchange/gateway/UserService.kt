package com.exchange.gateway

import org.springframework.http.HttpStatus
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Service
import org.springframework.web.server.ResponseStatusException
import java.util.concurrent.ConcurrentHashMap

/**
 * Resolves a Keycloak subject (JWT "sub" claim) to the internal Long uid used
 * by exchange-core and TigerBeetle.
 *
 * The mapping lives in the `users` table (keycloak_sub → internal_uid).
 * An admin must insert a row there when provisioning a new user account.
 *
 * Sub → uid results are cached in memory after the first lookup.
 * Cache invalidation is not required: keycloak_sub and internal_uid are both
 * immutable once set (no user renames, no uid reassignment).
 *
 * Unknown sub: throws 403.  NEVER falls back to a default uid — a sub not in
 * the table means the user has no registered account, and the order must be
 * rejected before reaching Kafka.
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
}
