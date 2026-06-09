package com.exchange.settlement

import com.exchange.common.OrderSide
import com.exchange.common.TradeEvent
import org.slf4j.LoggerFactory
import org.springframework.context.ConfigurableApplicationContext
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

/**
 * Balance-level reconciliation: engine balance == TigerBeetle balance.
 *
 * "Engine balance" is reconstructed from the same Kafka streams the engine uses:
 *   - AdjustBalanceConsumer feeds initial credits (replayed from commands offset 0)
 *   - TradeConsumer feeds per-trade debits/credits (replayed from trades offset 0)
 *
 * Because the trades topic accumulates one copy per engine restart, trade events
 * are deduplicated by tradeId so each trade is counted exactly once.
 *
 * Every 30 s the scheduler compares expected vs actual TigerBeetle balance for
 * every tracked (uid, ledger) pair.  Any divergence → ERROR log + ctx.close().
 *
 * Limitation: the 30 s initial delay is a heuristic to let Kafka replay finish
 * before the first check.  For very large histories, increase initialDelay.
 */
@Component
class ReconciliationService(
    private val settlementService: SettlementService,
    private val ctx: ConfigurableApplicationContext
) {
    private val log = LoggerFactory.getLogger(javaClass)

    // uid → (ledger → expected balance).  AtomicLong supports concurrent updates
    // from AdjustBalanceConsumer and TradeConsumer threads.
    private val expected = ConcurrentHashMap<Long, ConcurrentHashMap<Int, AtomicLong>>()

    // Trades topic accumulates duplicates on each engine restart; deduplicate here.
    private val seenTradeIds = ConcurrentHashMap.newKeySet<Long>()

    /** Called by AdjustBalanceConsumer for each credit applied from the command log. */
    fun recordCredit(uid: Long, ledger: Int, amount: Long) {
        expected.computeIfAbsent(uid) { ConcurrentHashMap() }
            .computeIfAbsent(ledger) { AtomicLong(0L) }
            .addAndGet(amount)
    }

    /**
     * Called by TradeConsumer after each settlement.
     * Deduplicates by tradeId — safe to call multiple times for the same trade.
     */
    fun recordTrade(trade: TradeEvent, baseLedger: Int, quoteLedger: Int) {
        if (!seenTradeIds.add(trade.tradeId)) return  // duplicate from engine replay

        val (buyerUid, sellerUid) = if (trade.takerSide == OrderSide.BID)
            trade.takerUserId to trade.makerUserId
        else
            trade.makerUserId to trade.takerUserId
        val quoteAmount = trade.price * trade.quantity
        val baseAmount  = trade.quantity

        recordCredit(buyerUid,  baseLedger,   baseAmount)   // buyer  receives base
        recordCredit(buyerUid,  quoteLedger, -quoteAmount)  // buyer  pays     quote
        recordCredit(sellerUid, quoteLedger,  quoteAmount)  // seller receives quote
        recordCredit(sellerUid, baseLedger,  -baseAmount)   // seller pays     base
    }

    @Scheduled(fixedDelay = 30_000, initialDelay = 30_000)
    fun reconcile() {
        val accountCount = expected.values.sumOf { it.size }
        if (accountCount == 0) {
            log.debug("Reconciliation skipped — no accounts tracked yet")
            return
        }

        var mismatches = 0
        for ((uid, ledgers) in expected) {
            for ((ledger, exp) in ledgers) {
                val actual = settlementService.getBalance(uid, ledger)
                if (actual != exp.get()) {
                    log.error(
                        "RECONCILIATION MISMATCH uid={} ledger={} expected={} actual={}",
                        uid, ledger, exp.get(), actual
                    )
                    mismatches++
                }
            }
        }

        if (mismatches > 0) {
            log.error(
                "Reconciliation FAILED: {}/{} account(s) diverged — stopping service",
                mismatches, accountCount
            )
            ctx.close()
        } else {
            log.info("Reconciliation OK — {}/{} account(s) match TigerBeetle", accountCount, accountCount)
        }
    }
}
