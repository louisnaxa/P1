package com.exchange.gateway

import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Service
import org.springframework.web.server.ResponseStatusException

/**
 * Access control gate for order placement.
 *
 * Must be called in OrderController.placeOrder() BEFORE publisher.publish() —
 * before any durable write, never after. Throwing here means the command never
 * enters the Kafka journal, so no replay path can bypass the check.
 *
 * Rules (statut × juridiction du bien) :
 *   UNVERIFIED              → REJET
 *   SUSPENDED               → REJET
 *   FOREIGN_SPECULATIVE     → AUTORISÉ pour tout symbole (retour rapide, pas de lookup propriété)
 *   CITIZEN_APPROVED(J)     × bien(juridiction=J)   → AUTORISÉ
 *   CITIZEN_APPROVED(J)     × bien(juridiction=J')  → REJET
 *   Symbole inconnu (absent de `symbols`)           → REJET (fail-closed)
 *
 * The status is read fresh from DB on every call (UserService.resolveStatus is never cached).
 * The property jurisdiction is resolved from the `symbols → properties` join.
 *
 * Race window: the check and the publish are not atomic. A status change in the
 * sub-millisecond gap would let one order through after revocation. This is
 * architecturally acceptable — status changes eventually block all subsequent orders.
 * Eliminating the window would require a distributed lock, which is not warranted.
 *
 * Proved in TransferGuardIntegrationTest (R1–R6, real PostgreSQL).
 * Wiring proved in SecurityTest (R7, @WebMvcTest, mock throws → 403).
 * Run: ./gradlew :gateway:transferTest
 */
@Service
class TransferGuard(
    private val userService: UserService,
    private val jdbc: JdbcTemplate
) {
    private val log = LoggerFactory.getLogger(javaClass)

    fun checkOrderAccess(uid: Long, symbolId: Int) {
        val account = userService.resolveStatus(uid)

        when (account.status) {
            AccountStatus.UNVERIFIED, AccountStatus.SUSPENDED -> {
                log.info("Order rejected: uid={} status={}", uid, account.status)
                throw ResponseStatusException(
                    HttpStatus.FORBIDDEN,
                    "Account status ${account.status} is not permitted to place orders"
                )
            }
            AccountStatus.FOREIGN_SPECULATIVE -> return  // no property check — early exit
            AccountStatus.CITIZEN_APPROVED -> {
                val propertyJurisdiction = resolvePropertyJurisdiction(symbolId)
                    ?: run {
                        log.warn("Order rejected: uid={} symbolId={} — symbol has no associated property (fail-closed)", uid, symbolId)
                        throw ResponseStatusException(
                            HttpStatus.FORBIDDEN,
                            "Symbol $symbolId is not associated with a known property — access denied"
                        )
                    }
                if (account.jurisdiction != propertyJurisdiction) {
                    log.info("Order rejected: uid={} jurisdiction={} propertyJurisdiction={}", uid, account.jurisdiction, propertyJurisdiction)
                    throw ResponseStatusException(
                        HttpStatus.FORBIDDEN,
                        "CITIZEN_APPROVED(${account.jurisdiction}) cannot trade property in jurisdiction $propertyJurisdiction"
                    )
                }
            }
        }
    }

    private fun resolvePropertyJurisdiction(symbolId: Int): String? =
        jdbc.query(
            "SELECT p.jurisdiction FROM symbols s JOIN properties p ON s.property_id = p.id WHERE s.id = ?",
            { rs, _ -> rs.getString("jurisdiction") },
            symbolId
        ).firstOrNull()
}
