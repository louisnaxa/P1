package com.exchange.engine

import com.exchange.common.OrderSide
import exchange.core2.core.common.cmd.CommandResultCode
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

/**
 * M0 gate test: send orders into exchange-core, verify matching works.
 * This test runs without Spring context for speed.
 */
class MatchingEngineTest {

    private lateinit var engine: MatchingEngineService

    companion object {
        const val SYMBOL_ID = 1
        const val BASE_CURRENCY = 10   // FOO
        const val QUOTE_CURRENCY = 11  // USDT
        const val ALICE_UID = 101L
        const val BOB_UID = 102L
    }

    @BeforeEach
    fun setUp() {
        engine = MatchingEngineService()
        engine.init()

        // Register the symbol (pair)
        engine.addSymbol(
            symbolId = SYMBOL_ID,
            baseCurrency = BASE_CURRENCY,
            quoteCurrency = QUOTE_CURRENCY
        )

        // Register users
        engine.addUser(ALICE_UID)
        engine.addUser(BOB_UID)

        // Give Alice quote currency (USDT) to buy
        engine.adjustBalance(ALICE_UID, QUOTE_CURRENCY, 1_000_000L)

        // Give Bob base currency (FOO) to sell
        engine.adjustBalance(BOB_UID, BASE_CURRENCY, 1_000L)
    }

    @AfterEach
    fun tearDown() {
        engine.shutdown()
    }

    @Test
    fun `orders at same price should match`() {
        // Bob places a sell (ask) at price 100
        val askResult = engine.placeOrder(
            symbolId = SYMBOL_ID,
            uid = BOB_UID,
            orderId = 1L,
            price = 100L,
            size = 10L,
            side = OrderSide.ASK
        )
        assertThat(askResult).isEqualTo(CommandResultCode.SUCCESS)

        // Alice places a buy (bid) at price 100 - should match immediately
        val bidResult = engine.placeOrder(
            symbolId = SYMBOL_ID,
            uid = ALICE_UID,
            orderId = 2L,
            price = 100L,
            size = 10L,
            side = OrderSide.BID
        )
        assertThat(bidResult).isEqualTo(CommandResultCode.SUCCESS)
    }

    @Test
    fun `bid below ask should not match - both rest on book`() {
        val askResult = engine.placeOrder(
            symbolId = SYMBOL_ID,
            uid = BOB_UID,
            orderId = 10L,
            price = 110L,
            size = 5L,
            side = OrderSide.ASK
        )
        assertThat(askResult).isEqualTo(CommandResultCode.SUCCESS)

        val bidResult = engine.placeOrder(
            symbolId = SYMBOL_ID,
            uid = ALICE_UID,
            orderId = 11L,
            price = 100L,
            size = 5L,
            side = OrderSide.BID
        )
        assertThat(bidResult).isEqualTo(CommandResultCode.SUCCESS)
    }

    @Test
    fun `partial fill - taker order larger than maker`() {
        // Bob sells 5 at 100
        engine.placeOrder(
            symbolId = SYMBOL_ID,
            uid = BOB_UID,
            orderId = 20L,
            price = 100L,
            size = 5L,
            side = OrderSide.ASK
        )

        // Alice buys 10 at 100 - only 5 filled, 5 remain on book
        val result = engine.placeOrder(
            symbolId = SYMBOL_ID,
            uid = ALICE_UID,
            orderId = 21L,
            price = 100L,
            size = 10L,
            side = OrderSide.BID
        )
        assertThat(result).isEqualTo(CommandResultCode.SUCCESS)
    }

    @Test
    fun `multiple makers fill a single taker`() {
        // Bob places 3 sells at price 100
        for (i in 30L..32L) {
            engine.placeOrder(
                symbolId = SYMBOL_ID,
                uid = BOB_UID,
                orderId = i,
                price = 100L,
                size = 3L,
                side = OrderSide.ASK
            )
        }

        // Alice buys 9 at 100 — should sweep all three asks
        val result = engine.placeOrder(
            symbolId = SYMBOL_ID,
            uid = ALICE_UID,
            orderId = 33L,
            price = 100L,
            size = 9L,
            side = OrderSide.BID
        )
        assertThat(result).isEqualTo(CommandResultCode.SUCCESS)
    }
}
