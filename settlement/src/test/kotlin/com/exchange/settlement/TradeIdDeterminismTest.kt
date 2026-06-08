package com.exchange.settlement

import com.exchange.common.EngineCommand
import com.exchange.common.OrderSide
import com.exchange.common.TradeEvent
import com.exchange.engine.MatchingEngineService
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

/**
 * Unit test — no external dependencies, no Docker, no mocks of TigerBeetle.
 *
 * Verifies the tradeId formula:
 *   tradeId = (commandOffset shl 16) or matchIndex.toLong()
 *
 * The formula must be deterministic: replaying the same command sequence at the
 * same Kafka offsets must produce an identical tradeId.  This is the root
 * guarantee that makes TigerBeetle's transferId deduplication work — if the
 * engine replays after a crash, the re-emitted trades carry the same transferIds
 * and TigerBeetle silently ignores the duplicates.
 *
 * The integration tests in SettlementChaosIntegrationTest verify the full
 * end-to-end behaviour against real containers; this test acts as an instant
 * regression guard at the formula level.
 */
class TradeIdDeterminismTest {

    @Test
    fun `same offsets produce identical tradeId across engine restarts`() {
        fun runEngine(): Long {
            val trades = mutableListOf<TradeEvent>()
            val engine = MatchingEngineService { trades.add(it) }
            engine.init()
            var offset = 0L
            fun cmd(c: EngineCommand) = engine.processCommand(offset++, c)
            cmd(EngineCommand.addSymbol(1, 10, 11))               // offset 0
            cmd(EngineCommand.addUser(101L))                       // offset 1
            cmd(EngineCommand.addUser(102L))                       // offset 2
            cmd(EngineCommand.adjustBalance(101L, 11, 1_000_000_000L)) // offset 3
            cmd(EngineCommand.adjustBalance(101L, 10, 1_000_000L))     // offset 4
            cmd(EngineCommand.adjustBalance(102L, 10, 1_000_000L))     // offset 5
            cmd(EngineCommand.adjustBalance(102L, 11, 1_000_000_000L)) // offset 6
            cmd(EngineCommand.placeOrder(1, 102L, 1L, 100L, 5L, OrderSide.ASK)) // offset 7
            cmd(EngineCommand.placeOrder(1, 101L, 2L, 100L, 5L, OrderSide.BID)) // offset 8 → match
            engine.shutdown()
            assertThat(trades).`as`("engine must produce exactly one trade").hasSize(1)
            return trades[0].tradeId
        }

        val firstRun  = runEngine()
        val secondRun = runEngine()

        assertThat(secondRun)
            .`as`("tradeId must be deterministic: same offsets → same tradeId")
            .isEqualTo(firstRun)
    }
}
