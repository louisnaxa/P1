package com.exchange.engine

import com.exchange.common.EngineCommand
import com.exchange.common.OrderSide
import com.exchange.common.TradeEvent
import exchange.core2.core.common.cmd.CommandResultCode
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.concurrent.CopyOnWriteArrayList

class MatchingEngineTest {

    private val publishedTrades = CopyOnWriteArrayList<TradeEvent>()
    private lateinit var engine: MatchingEngineService
    private var nextOffset = 0L

    companion object {
        const val SYMBOL_ID = 1
        const val BASE_CURRENCY = 10
        const val QUOTE_CURRENCY = 11
        const val ALICE_UID = 101L
        const val BOB_UID = 102L
    }

    private fun cmd(command: EngineCommand): CommandResultCode =
        engine.processCommand(nextOffset++, command)

    @BeforeEach
    fun setUp() {
        publishedTrades.clear()
        nextOffset = 0L
        engine = MatchingEngineService(tradePublisher = { publishedTrades.add(it) })
        engine.init()

        cmd(EngineCommand.addSymbol(SYMBOL_ID, BASE_CURRENCY, QUOTE_CURRENCY))
        cmd(EngineCommand.addUser(ALICE_UID))
        cmd(EngineCommand.addUser(BOB_UID))
        cmd(EngineCommand.adjustBalance(ALICE_UID, QUOTE_CURRENCY, 1_000_000_000L))
        cmd(EngineCommand.adjustBalance(ALICE_UID, BASE_CURRENCY, 1_000_000L))
        cmd(EngineCommand.adjustBalance(BOB_UID, BASE_CURRENCY, 1_000_000L))
        cmd(EngineCommand.adjustBalance(BOB_UID, QUOTE_CURRENCY, 1_000_000_000L))
    }

    @AfterEach
    fun tearDown() {
        engine.shutdown()
    }

    @Test
    fun `orders at same price should match`() {
        val askResult = cmd(EngineCommand.placeOrder(SYMBOL_ID, BOB_UID, 1L, 100L, 10L, OrderSide.ASK))
        assertThat(askResult).isEqualTo(CommandResultCode.SUCCESS)

        val bidOffset = nextOffset
        val bidResult = cmd(EngineCommand.placeOrder(SYMBOL_ID, ALICE_UID, 2L, 100L, 10L, OrderSide.BID))
        assertThat(bidResult).isEqualTo(CommandResultCode.SUCCESS)

        assertThat(publishedTrades).hasSize(1)
        val trade = publishedTrades[0]
        assertThat(trade.price).isEqualTo(100L)
        assertThat(trade.quantity).isEqualTo(10L)
        assertThat(trade.takerUserId).isEqualTo(ALICE_UID)
        assertThat(trade.makerUserId).isEqualTo(BOB_UID)
        assertThat(trade.takerSide).isEqualTo(OrderSide.BID)

        // tradeId is deterministic: f(commandOffset, matchIndex)
        val expectedTradeId = (bidOffset shl 16) or 0L
        assertThat(trade.tradeId).isEqualTo(expectedTradeId)
    }

    @Test
    fun `bid below ask should not match - both rest on book`() {
        val askResult = cmd(EngineCommand.placeOrder(SYMBOL_ID, BOB_UID, 10L, 110L, 5L, OrderSide.ASK))
        assertThat(askResult).isEqualTo(CommandResultCode.SUCCESS)

        val bidResult = cmd(EngineCommand.placeOrder(SYMBOL_ID, ALICE_UID, 11L, 100L, 5L, OrderSide.BID))
        assertThat(bidResult).isEqualTo(CommandResultCode.SUCCESS)

        assertThat(publishedTrades).isEmpty()
    }

    @Test
    fun `partial fill - taker order larger than maker`() {
        cmd(EngineCommand.placeOrder(SYMBOL_ID, BOB_UID, 20L, 100L, 5L, OrderSide.ASK))

        val bidResult = cmd(EngineCommand.placeOrder(SYMBOL_ID, ALICE_UID, 21L, 100L, 10L, OrderSide.BID))
        assertThat(bidResult).isEqualTo(CommandResultCode.SUCCESS)

        assertThat(publishedTrades).hasSize(1)
        assertThat(publishedTrades[0].quantity).isEqualTo(5L)
    }

    @Test
    fun `multiple makers fill a single taker`() {
        for (i in 30L..32L) {
            cmd(EngineCommand.placeOrder(SYMBOL_ID, BOB_UID, i, 100L, 3L, OrderSide.ASK))
        }

        val takerOffset = nextOffset
        val result = cmd(EngineCommand.placeOrder(SYMBOL_ID, ALICE_UID, 33L, 100L, 9L, OrderSide.BID))
        assertThat(result).isEqualTo(CommandResultCode.SUCCESS)

        assertThat(publishedTrades).hasSize(3)
        assertThat(publishedTrades.sumOf { it.quantity }).isEqualTo(9L)

        // All 3 trades derive from the same command offset
        publishedTrades.forEachIndexed { idx, trade ->
            val expectedId = (takerOffset shl 16) or idx.toLong()
            assertThat(trade.tradeId).isEqualTo(expectedId)
        }
    }

    @Test
    fun `place then cancel via offset-identity — matching order sees no trade`() {
        // Simulate the gateway writing PLACE_ORDER with no orderId in the payload (defaults to 0).
        // processCommand(offset, cmd) uses offset — not cmd.orderId — as the exchange-core orderId.
        val placeOffset = nextOffset   // mirrors the offset the gateway would return in the 202
        val placeResult = cmd(
            EngineCommand(
                type = EngineCommand.PLACE_ORDER,
                uid = BOB_UID, symbolId = SYMBOL_ID,
                side = OrderSide.ASK, price = 100L, size = 10L
                // orderId absent (= 0): exactly what the gateway writes
            )
        )
        assertThat(placeResult).isEqualTo(CommandResultCode.SUCCESS)

        // The gateway returns { "orderId": placeOffset } in the 202.
        // The client sends DELETE /orders/{placeOffset}; the gateway writes:
        val cancelResult = cmd(EngineCommand.cancelOrder(SYMBOL_ID, BOB_UID, placeOffset))
        assertThat(cancelResult).isEqualTo(CommandResultCode.SUCCESS)

        // Book must be empty: Alice's matching BID finds nothing — the ASK was cancelled.
        cmd(EngineCommand.placeOrder(SYMBOL_ID, ALICE_UID, 0L, 100L, 10L, OrderSide.BID))
        assertThat(publishedTrades).isEmpty()
    }

    @Test
    fun `replay produces identical trade IDs`() {
        // First run
        cmd(EngineCommand.placeOrder(SYMBOL_ID, BOB_UID, 1L, 100L, 5L, OrderSide.ASK))
        cmd(EngineCommand.placeOrder(SYMBOL_ID, ALICE_UID, 2L, 100L, 5L, OrderSide.BID))

        val firstRunTrades = publishedTrades.toList()
        assertThat(firstRunTrades).hasSize(1)

        // Simulate restart: new engine, replay same commands with same offsets
        engine.shutdown()
        publishedTrades.clear()

        engine = MatchingEngineService(tradePublisher = { publishedTrades.add(it) })
        engine.init()

        // Replay with identical offsets
        nextOffset = 0L
        cmd(EngineCommand.addSymbol(SYMBOL_ID, BASE_CURRENCY, QUOTE_CURRENCY))
        cmd(EngineCommand.addUser(ALICE_UID))
        cmd(EngineCommand.addUser(BOB_UID))
        cmd(EngineCommand.adjustBalance(ALICE_UID, QUOTE_CURRENCY, 1_000_000_000L))
        cmd(EngineCommand.adjustBalance(ALICE_UID, BASE_CURRENCY, 1_000_000L))
        cmd(EngineCommand.adjustBalance(BOB_UID, BASE_CURRENCY, 1_000_000L))
        cmd(EngineCommand.adjustBalance(BOB_UID, QUOTE_CURRENCY, 1_000_000_000L))
        cmd(EngineCommand.placeOrder(SYMBOL_ID, BOB_UID, 1L, 100L, 5L, OrderSide.ASK))
        cmd(EngineCommand.placeOrder(SYMBOL_ID, ALICE_UID, 2L, 100L, 5L, OrderSide.BID))

        val replayTrades = publishedTrades.toList()
        assertThat(replayTrades).hasSize(1)

        // Same offset → same tradeId
        assertThat(replayTrades[0].tradeId).isEqualTo(firstRunTrades[0].tradeId)
        assertThat(replayTrades[0].price).isEqualTo(firstRunTrades[0].price)
        assertThat(replayTrades[0].quantity).isEqualTo(firstRunTrades[0].quantity)
    }
}
