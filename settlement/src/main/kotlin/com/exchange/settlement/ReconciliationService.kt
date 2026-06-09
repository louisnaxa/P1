package com.exchange.settlement

import com.exchange.common.OrderSide
import com.exchange.common.TradeEvent
import org.slf4j.LoggerFactory
import org.springframework.context.ConfigurableApplicationContext
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong

/**
 * Balance-level reconciliation: engine balance == TigerBeetle balance.
 *
 * Expected balance per (uid, ledger) is built from two Kafka streams:
 *   - AdjustBalanceConsumer: credits from the commands log
 *   - TradeConsumer: per-trade debits and credits
 *
 * The reconcile() job runs every 30 s but ONLY compares balances once both
 * consumers have signalled that their initial Kafka replay is complete.
 * Until then it skips without touching TigerBeetle.  This eliminates the
 * heuristic initialDelay: the guard is a real condition, not a timing bet.
 *
 * Signals are emitted by each consumer after the last replay record has been
 * fully settled in TigerBeetle AND counted in expected — never earlier.
 * Both signals are reset on partition rebalance so a re-assigned consumer
 * must complete a fresh replay before reconciliation resumes.
 *
 * Trade events are deduplicated by tradeId: the trades topic accumulates one
 * copy per engine restart, so each trade must be counted exactly once.
 */
@Component
class ReconciliationService(
    private val settlementService: SettlementService,
    private val ctx: ConfigurableApplicationContext
) {
    private val log = LoggerFactory.getLogger(javaClass)

    // ── Replay-completion gates ───────────────────────────────────────────────

    private val commandsCaughtUp = AtomicBoolean(false)
    private val tradesCaughtUp   = AtomicBoolean(false)

    fun signalCommandsCaughtUp() {
        if (commandsCaughtUp.compareAndSet(false, true))
            log.info("Commands replay complete — balance tracking active for commands stream")
    }

    fun signalTradesCaughtUp() {
        if (tradesCaughtUp.compareAndSet(false, true))
            log.info("Trades replay complete — balance tracking active for trades stream")
    }

    /** Called on partition rebalance: replay must be confirmed again before reconciling. */
    fun resetCommandsCaughtUp() { commandsCaughtUp.set(false) }
    fun resetTradesCaughtUp()   { tradesCaughtUp.set(false)   }

    // ── Expected-balance tracking ─────────────────────────────────────────────

    // uid → (ledger → expected balance).
    // AtomicLong supports concurrent updates from different consumer threads.
    private val expected = ConcurrentHashMap<Long, ConcurrentHashMap<Int, AtomicLong>>()

    // Deduplicate: the trades topic accumulates one copy per engine restart.
    private val seenTradeIds = ConcurrentHashMap.newKeySet<Long>()

    /** Called by AdjustBalanceConsumer for each adjustBalance credit applied. */
    fun recordCredit(uid: Long, ledger: Int, amount: Long) {
        expected.computeIfAbsent(uid) { ConcurrentHashMap() }
            .computeIfAbsent(ledger) { AtomicLong(0L) }
            .addAndGet(amount)
    }

    /**
     * Called by TradeConsumer after each settlement.
     * Idempotent: repeated calls for the same tradeId are silently ignored.
     */
    fun recordTrade(trade: TradeEvent, baseLedger: Int, quoteLedger: Int) {
        if (!seenTradeIds.add(trade.tradeId)) return

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

    // ── Scheduled reconciliation ──────────────────────────────────────────────

    /**
     * Compares expected vs actual TigerBeetle balance for every tracked account.
     * Skips entirely until both replay-completion signals have been received.
     * Any divergence logs ERROR and stops the service.
     */
    @Scheduled(fixedDelay = 30_000)
    fun reconcile() {
        if (!commandsCaughtUp.get() || !tradesCaughtUp.get()) {
            log.info("Reconciliation skipped — replay not complete (commands={}, trades={})",
                commandsCaughtUp.get(), tradesCaughtUp.get())
            return
        }

        val accountCount = expected.values.sumOf { it.size }
        if (accountCount == 0) {
            log.debug("Reconciliation: no accounts tracked yet")
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
            log.error("Reconciliation FAILED: {}/{} account(s) diverged — stopping service",
                mismatches, accountCount)
            ctx.close()
        } else {
            log.info("Reconciliation OK — {}/{} account(s) match TigerBeetle", accountCount, accountCount)
        }
    }
}
