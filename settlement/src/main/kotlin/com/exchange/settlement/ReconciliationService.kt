package com.exchange.settlement

import com.exchange.common.OrderSide
import com.exchange.common.TradeEvent
import com.tigerbeetle.Client
import com.tigerbeetle.IdBatch
import org.slf4j.LoggerFactory
import org.springframework.context.ConfigurableApplicationContext
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.util.concurrent.ConcurrentLinkedQueue

/**
 * Verifies that every trade settled by this service is durably recorded in
 * TigerBeetle ("engine balance = register balance").
 *
 * Source of truth for the engine side: the Kafka trades topic (same stream
 * the matching engine replays from on restart). For every TradeEvent consumed,
 * both TigerBeetle transfers must exist after settlement.
 *
 * A missing transfer means a settlement was silently lost.  The service stops
 * immediately to prevent further balance divergence.
 */
@Component
class ReconciliationService(
    private val tb: Client,
    private val ctx: ConfigurableApplicationContext
) {
    private val log = LoggerFactory.getLogger(javaClass)

    private data class SettledTrade(
        val tradeId: Long,
        val buyerUid: Long,
        val sellerUid: Long
    )

    private val settled = ConcurrentLinkedQueue<SettledTrade>()

    /** Called by TradeConsumer immediately after each successful settleTrade(). */
    fun record(trade: TradeEvent) {
        val (buyerUid, sellerUid) = if (trade.takerSide == OrderSide.BID)
            trade.takerUserId to trade.makerUserId
        else
            trade.makerUserId to trade.takerUserId
        settled.add(SettledTrade(trade.tradeId, buyerUid, sellerUid))
    }

    /**
     * Every 30 s (after an initial 30 s warm-up to let Kafka replay finish):
     * look up both TigerBeetle transfers for every recorded trade and verify
     * they exist.  Any missing transfer → log ERROR and stop the service.
     */
    @Scheduled(fixedDelay = 30_000, initialDelay = 30_000)
    fun reconcile() {
        val snapshot = settled.toList()
        if (snapshot.isEmpty()) {
            log.debug("Reconciliation skipped — no trades recorded yet")
            return
        }

        var missing = 0
        for (st in snapshot) {
            val leg0 = (st.tradeId shl 4) or 0L
            val leg1 = (st.tradeId shl 4) or 1L
            val ids = IdBatch(2)
            ids.add(); ids.setId(leg0, 0L)
            ids.add(); ids.setId(leg1, 0L)
            val found = tb.lookupTransfers(ids).length
            if (found != 2) {
                log.error(
                    "RECONCILIATION MISMATCH tradeId={} buyer={} seller={}: " +
                        "expected 2 TigerBeetle transfers, found {}",
                    st.tradeId, st.buyerUid, st.sellerUid, found
                )
                missing++
            }
        }

        if (missing > 0) {
            log.error(
                "Reconciliation FAILED: {}/{} trades missing from TigerBeetle — stopping service",
                missing, snapshot.size
            )
            ctx.close()
        } else {
            log.info("Reconciliation OK — {}/{} trades verified in TigerBeetle", snapshot.size, snapshot.size)
        }
    }
}
