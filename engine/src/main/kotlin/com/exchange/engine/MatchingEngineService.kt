package com.exchange.engine

import com.exchange.common.OrderSide
import com.exchange.common.TradeEvent
import exchange.core2.core.ExchangeApi
import exchange.core2.core.ExchangeCore
import exchange.core2.core.common.CoreSymbolSpecification
import exchange.core2.core.common.OrderAction
import exchange.core2.core.common.OrderType
import exchange.core2.core.common.SymbolType
import exchange.core2.core.common.api.ApiAddUser
import exchange.core2.core.common.api.ApiAdjustUserBalance
import exchange.core2.core.common.api.ApiPlaceOrder
import exchange.core2.core.common.api.binary.BatchAddSymbolsCommand
import exchange.core2.core.common.cmd.CommandResultCode
import exchange.core2.core.common.config.ExchangeConfiguration
import exchange.core2.core.common.config.InitialStateConfiguration
import exchange.core2.core.common.config.LoggingConfiguration
import exchange.core2.core.common.config.OrdersProcessingConfiguration
import exchange.core2.core.common.config.PerformanceConfiguration
import exchange.core2.core.common.config.ReportsQueriesConfiguration
import exchange.core2.core.common.config.SerializationConfiguration
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicLong
import jakarta.annotation.PostConstruct
import jakarta.annotation.PreDestroy

@Service
class MatchingEngineService {

    private val log = LoggerFactory.getLogger(javaClass)

    private val tradeIdSeq = AtomicLong(1)
    private val txIdSeq = AtomicLong(1)
    private val trades = CopyOnWriteArrayList<TradeEvent>()

    private lateinit var exchangeCore: ExchangeCore
    private lateinit var api: ExchangeApi

    @PostConstruct
    fun init() {
        val conf = ExchangeConfiguration.defaultBuilder()
            .initStateCfg(InitialStateConfiguration.CLEAN_TEST)
            .serializationCfg(SerializationConfiguration.DEFAULT)
            .build()

        exchangeCore = ExchangeCore.builder()
            .resultsConsumer { cmd, _ ->
                log.trace("Result: {}", cmd)
            }
            .exchangeConfiguration(conf)
            .build()

        exchangeCore.startup()
        api = exchangeCore.api

        log.info("Matching engine started")
    }

    @PreDestroy
    fun shutdown() {
        exchangeCore.shutdown()
        log.info("Matching engine stopped")
    }

    fun addSymbol(symbolId: Int, baseCurrency: Int, quoteCurrency: Int, baseScaleK: Long = 1, quoteScaleK: Long = 1) {
        val spec = CoreSymbolSpecification.builder()
            .symbolId(symbolId)
            .type(SymbolType.CURRENCY_EXCHANGE_PAIR)
            .baseCurrency(baseCurrency)
            .quoteCurrency(quoteCurrency)
            .baseScaleK(baseScaleK)
            .quoteScaleK(quoteScaleK)
            .takerFee(0)
            .makerFee(0)
            .build()

        val future = api.submitBinaryDataAsync(BatchAddSymbolsCommand(spec))
        val result = future.get()
        check(result == CommandResultCode.SUCCESS) { "addSymbol failed: $result" }
    }

    fun addUser(uid: Long) {
        val result = api.submitCommandAsync(ApiAddUser(uid)).get()
        check(result == CommandResultCode.SUCCESS) { "addUser($uid) failed: $result" }
    }

    fun adjustBalance(uid: Long, currency: Int, amount: Long) {
        val result = api.submitCommandAsync(
            ApiAdjustUserBalance(uid, currency, amount, txIdSeq.getAndIncrement())
        ).get()
        check(result == CommandResultCode.SUCCESS) { "adjustBalance failed: $result" }
    }

    fun placeOrder(
        symbolId: Int,
        uid: Long,
        orderId: Long,
        price: Long,
        size: Long,
        side: OrderSide
    ): CommandResultCode {
        val action = when (side) {
            OrderSide.BID -> OrderAction.BID
            OrderSide.ASK -> OrderAction.ASK
        }

        val cmd = ApiPlaceOrder.builder()
            .uid(uid)
            .orderId(orderId)
            .price(price)
            .reservePrice(if (side == OrderSide.BID) price else 0L)
            .size(size)
            .action(action)
            .orderType(OrderType.GTC)
            .symbol(symbolId)
            .build()

        val result = api.submitCommandAsync(cmd).get()

        if (result == CommandResultCode.SUCCESS) {
            log.debug("Order placed: uid={}, orderId={}, {}@{} {}", uid, orderId, size, price, side)
        }

        return result
    }

    fun getRecentTrades(): List<TradeEvent> = trades.toList()

    fun clearTrades() {
        trades.clear()
    }
}
