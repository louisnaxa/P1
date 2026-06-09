package com.exchange.engine

import com.exchange.common.EngineCommand
import com.exchange.common.OrderBookEvent
import com.exchange.common.OrderSide
import com.exchange.common.PriceLevel
import com.exchange.common.TradeEvent
import exchange.core2.core.ExchangeApi
import exchange.core2.core.ExchangeCore
import exchange.core2.core.common.CoreSymbolSpecification
import exchange.core2.core.common.MatcherEventType
import exchange.core2.core.common.OrderAction
import exchange.core2.core.common.OrderType
import exchange.core2.core.common.SymbolType
import exchange.core2.core.common.api.ApiAddUser
import exchange.core2.core.common.api.ApiAdjustUserBalance
import exchange.core2.core.common.api.ApiCancelOrder
import exchange.core2.core.common.api.ApiPlaceOrder
import exchange.core2.core.common.api.binary.BatchAddSymbolsCommand
import exchange.core2.core.common.cmd.CommandResultCode
import exchange.core2.core.common.cmd.OrderCommand
import exchange.core2.core.common.config.ExchangeConfiguration
import exchange.core2.core.common.config.InitialStateConfiguration
import exchange.core2.core.common.config.SerializationConfiguration
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import jakarta.annotation.PostConstruct
import jakarta.annotation.PreDestroy

/**
 * Wraps exchange-core. All commands enter through [processCommand] with a
 * deterministic offset from the durable command log (Kafka).
 *
 * Recovery strategy: full-replay from the commands topic.
 * exchange-core always starts clean (no snapshots) and rebuilds state.
 */
@Service
class MatchingEngineService(
    private val tradePublisher: (TradeEvent) -> Unit = {},
    private val orderbookPublisher: (OrderBookEvent) -> Unit = {},
    @Value("\${exchange.orderbook.depth:20}") private val orderbookDepth: Int = 20
) {

    private val log = LoggerFactory.getLogger(javaClass)
    private val txIdSeq = AtomicLong(1)

    // Set by EngineCommandConsumer once the initial Kafka replay is complete.
    // Orderbook snapshots are suppressed during replay to avoid flooding the topic.
    private val replayComplete = AtomicBoolean(false)

    fun signalReplayComplete() {
        if (replayComplete.compareAndSet(false, true))
            log.info("Engine replay complete — orderbook snapshot publication active")
    }

    private lateinit var exchangeCore: ExchangeCore
    private lateinit var api: ExchangeApi

    // Intermediate trade data collected from the Disruptor callback.
    // Only accessed sequentially: Disruptor thread writes during .get() block,
    // consumer thread reads after .get() returns (happens-before via Future).
    private val pendingRawTrades = mutableListOf<RawTrade>()

    private data class RawTrade(
        val symbolId: Int,
        val takerOrderId: Long,
        val takerUserId: Long,
        val takerSide: OrderSide,
        val makerOrderId: Long,
        val makerUserId: Long,
        val price: Long,
        val size: Long
    )

    @PostConstruct
    fun init() {
        val conf = ExchangeConfiguration.defaultBuilder()
            .initStateCfg(InitialStateConfiguration.CLEAN_TEST)
            .serializationCfg(SerializationConfiguration.DEFAULT)
            .build()

        exchangeCore = ExchangeCore.builder()
            .resultsConsumer { cmd, _ -> onResult(cmd) }
            .exchangeConfiguration(conf)
            .build()

        exchangeCore.startup()
        api = exchangeCore.api

        log.info("Matching engine started (full-replay mode, no snapshots)")
    }

    @PreDestroy
    fun shutdown() {
        exchangeCore.shutdown()
        log.info("Matching engine stopped")
    }

    /**
     * Process a command from the durable log.
     *
     * @param offset Kafka offset of the command — used to derive deterministic trade IDs.
     * @param cmd    the command to process
     * @return the exchange-core result code
     */
    fun processCommand(offset: Long, cmd: EngineCommand): CommandResultCode {
        pendingRawTrades.clear()

        val result = when (cmd.type) {
            EngineCommand.PLACE_ORDER -> submitPlaceOrder(cmd, offset)
            EngineCommand.CANCEL_ORDER -> submitCancelOrder(cmd)
            EngineCommand.ADD_SYMBOL -> submitAddSymbol(cmd)
            EngineCommand.ADD_USER -> submitAddUser(cmd)
            EngineCommand.ADJUST_BALANCE -> submitAdjustBalance(cmd)
            else -> throw IllegalArgumentException("Unknown command type: ${cmd.type}")
        }

        // After exchange-core processed the command, assign deterministic IDs and publish.
        // tradeId = (commandOffset << 16) | matchIndex
        pendingRawTrades.forEachIndexed { matchIndex, raw ->
            val tradeId = (offset shl 16) or matchIndex.toLong()
            val trade = TradeEvent(
                tradeId = tradeId,
                symbolId = raw.symbolId,
                takerOrderId = raw.takerOrderId,
                takerUserId = raw.takerUserId,
                makerOrderId = raw.makerOrderId,
                makerUserId = raw.makerUserId,
                price = raw.price,
                quantity = raw.size,
                takerSide = raw.takerSide,
                timestampNs = System.nanoTime()
            )
            tradePublisher(trade)
            log.info("Trade id={} {} {}@{} taker={} maker={}",
                tradeId, raw.takerSide, raw.size, raw.price, raw.takerUserId, raw.makerUserId)
        }

        // Publish L2 snapshot AFTER all trades are durable in Kafka.
        // Fire-and-forget, isolated: failure here never affects trade or settlement.
        // Suppressed during replay so the orderbook topic stays lean.
        if (replayComplete.get() &&
            (cmd.type == EngineCommand.PLACE_ORDER || cmd.type == EngineCommand.CANCEL_ORDER)) {
            publishSnapshot(cmd.symbolId)
        }

        return result
    }

    private fun publishSnapshot(symbolId: Int) {
        try {
            val l2 = api.requestOrderBookAsync(symbolId, orderbookDepth).get()
            val bids = (0 until l2.bidSize).map { i -> PriceLevel(l2.bidPrices[i], l2.bidVolumes[i]) }
            val asks = (0 until l2.askSize).map { i -> PriceLevel(l2.askPrices[i], l2.askVolumes[i]) }
            orderbookPublisher(OrderBookEvent(symbolId, bids, asks))
        } catch (e: Exception) {
            log.warn("Orderbook snapshot failed for symbol={}: {}", symbolId, e.message)
        }
    }

    // -- Disruptor callback (runs on Disruptor thread) --

    private fun onResult(cmd: OrderCommand) {
        var event = cmd.matcherEvent
        while (event != null) {
            if (event.eventType == MatcherEventType.TRADE) {
                val takerSide = if (cmd.action == OrderAction.BID) OrderSide.BID else OrderSide.ASK
                pendingRawTrades.add(
                    RawTrade(
                        symbolId = cmd.symbol,
                        takerOrderId = cmd.orderId,
                        takerUserId = cmd.uid,
                        takerSide = takerSide,
                        makerOrderId = event.matchedOrderId,
                        makerUserId = event.matchedOrderUid,
                        price = event.price,
                        size = event.size
                    )
                )
            }
            event = event.nextEvent
        }
    }

    // -- Command dispatchers --

    // orderId = Kafka offset of this command — the single source of order identity (Option A).
    // cmd.orderId is ignored here; the gateway does not include it in PLACE_ORDER commands.
    private fun submitPlaceOrder(cmd: EngineCommand, offset: Long): CommandResultCode {
        val action = when (cmd.side) {
            OrderSide.BID -> OrderAction.BID
            OrderSide.ASK -> OrderAction.ASK
            null -> throw IllegalArgumentException("PlaceOrder requires a side")
        }
        val order = ApiPlaceOrder.builder()
            .uid(cmd.uid)
            .orderId(offset)
            .price(cmd.price)
            .reservePrice(if (cmd.side == OrderSide.BID) cmd.price else 0L)
            .size(cmd.size)
            .action(action)
            .orderType(OrderType.GTC)
            .symbol(cmd.symbolId)
            .build()
        return api.submitCommandAsync(order).get()
    }

    private fun submitCancelOrder(cmd: EngineCommand): CommandResultCode =
        api.submitCommandAsync(ApiCancelOrder(cmd.orderId, cmd.uid, cmd.symbolId)).get()

    private fun submitAddSymbol(cmd: EngineCommand): CommandResultCode {
        val spec = CoreSymbolSpecification.builder()
            .symbolId(cmd.symbolId)
            .type(SymbolType.CURRENCY_EXCHANGE_PAIR)
            .baseCurrency(cmd.baseCurrency)
            .quoteCurrency(cmd.quoteCurrency)
            .baseScaleK(cmd.baseScaleK)
            .quoteScaleK(cmd.quoteScaleK)
            .takerFee(0)
            .makerFee(0)
            .build()
        return api.submitBinaryDataAsync(BatchAddSymbolsCommand(spec)).get()
    }

    private fun submitAddUser(cmd: EngineCommand): CommandResultCode =
        api.submitCommandAsync(ApiAddUser(cmd.uid)).get()

    private fun submitAdjustBalance(cmd: EngineCommand): CommandResultCode =
        api.submitCommandAsync(
            ApiAdjustUserBalance(cmd.uid, cmd.currency, cmd.amount, txIdSeq.getAndIncrement())
        ).get()
}
