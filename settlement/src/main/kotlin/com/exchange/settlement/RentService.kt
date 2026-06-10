package com.exchange.settlement

import org.slf4j.LoggerFactory
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Service

/**
 * Distributes rent to eligible CITIZEN_APPROVED holders of a property.
 *
 * Flow:
 *   depositRent(propertyId, quoteLedgerId, amount, depositTransferId)
 *     → deposits stablecoin into the per-property rent pool
 *       (external(quoteLedgerId) → available(RENT_POOL_UID_BASE + propertyId, quoteLedgerId)).
 *
 *   distributeRent(propertyId, distributionKey)
 *     → reads eligible holders: CITIZEN_APPROVED + jurisdiction matches property + TB token balance > 0
 *     → distributes proportionally to each holder's token share among eligible holders only
 *       (compaction: foreigners holding tokens don't dilute the agréés' yield)
 *     → undivided remainder (integer division) stays in the pool, carries over to next distribution
 *     → records the distribution in rent_distributions for audit and idempotency
 *
 * Idempotency:
 *   distributeRent is idempotent for a given distributionKey: crash-and-retry with the same key
 *   finds the existing distributionId → same rentTransferId per holder → TB Exists → no double payment.
 *
 * Jurisdiction check is explicit (defense in depth):
 *   B3 prevents wrong-jurisdiction CITIZEN_APPROVED from acquiring tokens via the order book,
 *   but rent distribution re-verifies at pay time — never trust upstream gate alone for money movement.
 *
 * rentTransferId bit layout (64-bit):
 *   bits 63–48 : 3              (namespace, distinct from trade=0, emission=1)
 *   bits 47–16 : distributionId (32 bits, max ~4.3B per property — TD-16)
 *   bits 15–0  : holderIndex    (16 bits, max 65,535 holders per run — TD-16)
 *
 * Iteration order: ORDER BY internal_uid in the eligible-holders query ensures holderIndex
 * is stable across retries for the same distributionKey.
 *
 * Proved in RentDistributionTest (L1–L6, real TigerBeetle + real PostgreSQL).
 * Run: ./gradlew :settlement:rentTest
 */
@Service
class RentService(
    private val settlementService: SettlementService,
    private val jdbc: JdbcTemplate
) {
    private val log = LoggerFactory.getLogger(javaClass)

    /**
     * Deposits stablecoin into the per-property rent pool.
     * Idempotent: TB rejects duplicate depositTransferId as Exists.
     */
    fun depositRent(propertyId: Long, quoteLedgerId: Int, amount: Long, depositTransferId: Long) {
        settlementService.ensureAccount(AccountIds.RENT_POOL_UID_BASE + propertyId, quoteLedgerId)
        settlementService.deposit(AccountIds.RENT_POOL_UID_BASE + propertyId, quoteLedgerId, amount, depositTransferId)
        log.info("Rent deposited: propertyId={} amount={} transferId={}", propertyId, amount, depositTransferId)
    }

    /**
     * Distributes all stablecoin in the rent pool to eligible holders.
     *
     * @param distributionKey caller-provided key, unique per (propertyId, distribution).
     * @return distributionId (primary key in rent_distributions)
     */
    fun distributeRent(propertyId: Long, distributionKey: Long): Long {
        data class PropInfo(val jurisdiction: String, val propertyLedgerId: Int, val quoteLedgerId: Int)

        val prop = jdbc.queryForObject(
            """SELECT p.jurisdiction, p.property_ledger_id, s.quote_ledger_id
               FROM properties p
               JOIN symbols s ON s.property_id = p.id
               WHERE p.id = ?""",
            { rs, _ -> PropInfo(
                rs.getString("jurisdiction"),
                rs.getInt("property_ledger_id"),
                rs.getInt("quote_ledger_id")
            )},
            propertyId
        )!!

        // Resolve or create distribution record — establishes total_amount for idempotent retries
        data class DistRecord(val id: Long, val totalAmount: Long)

        val existing = jdbc.query(
            "SELECT id, total_amount FROM rent_distributions WHERE property_id = ? AND distribution_key = ?",
            { rs, _ -> DistRecord(rs.getLong("id"), rs.getLong("total_amount")) },
            propertyId, distributionKey
        ).firstOrNull()

        val (distributionId, distributionAmount) = if (existing != null) {
            existing.id to existing.totalAmount
        } else {
            val poolBalance = settlementService.getBalance(
                AccountIds.RENT_POOL_UID_BASE + propertyId, prop.quoteLedgerId
            )
            require(poolBalance > 0L) { "Rent pool for property $propertyId is empty" }
            val newId = jdbc.queryForObject(
                """INSERT INTO rent_distributions (property_id, quote_ledger_id, total_amount, distribution_key)
                   VALUES (?,?,?,?) RETURNING id""",
                Long::class.java, propertyId, prop.quoteLedgerId, poolBalance, distributionKey
            )!!
            newId to poolBalance
        }
        require(distributionId < (1L shl 32)) {
            "distributionId $distributionId exceeds 32-bit limit — TB transferId overflow (TD-16)"
        }

        // Eligible holders: CITIZEN_APPROVED + matching jurisdiction, ordered for stable holderIndex
        val eligibleUids = jdbc.query(
            """SELECT internal_uid FROM users
               WHERE account_status = 'CITIZEN_APPROVED' AND jurisdiction = ?
               ORDER BY internal_uid""",
            { rs, _ -> rs.getLong("internal_uid") },
            prop.jurisdiction
        )
        val eligibleBalances = eligibleUids
            .associateWith { uid -> settlementService.getBalance(uid, prop.propertyLedgerId) }
            .filter { (_, bal) -> bal > 0L }
        require(eligibleBalances.isNotEmpty()) { "No eligible CITIZEN_APPROVED holders for property $propertyId" }

        val totalEligibleTokens = eligibleBalances.values.sum()

        var totalDistributed = 0L
        eligibleBalances.entries.forEachIndexed { holderIndex, (uid, tokenBalance) ->
            require(holderIndex < 65_536) {
                "holderIndex $holderIndex >= 65,536 — too many holders per distribution run (TD-16)"
            }
            val share = distributionAmount * tokenBalance / totalEligibleTokens
            if (share > 0L) {
                val transferId = (3L shl 48) or (distributionId shl 16) or holderIndex.toLong()
                settlementService.ensureAccount(uid, prop.quoteLedgerId)
                settlementService.transfer(
                    AccountIds.RENT_POOL_UID_BASE + propertyId, uid, prop.quoteLedgerId, share, transferId
                )
                totalDistributed += share
            }
        }

        val remainder = distributionAmount - totalDistributed
        jdbc.update("UPDATE rent_distributions SET remainder = ? WHERE id = ?", remainder, distributionId)

        log.info(
            "Rent distributed: propertyId={} distributionId={} distributed={} remainder={}",
            propertyId, distributionId, totalDistributed, remainder
        )
        return distributionId
    }
}
