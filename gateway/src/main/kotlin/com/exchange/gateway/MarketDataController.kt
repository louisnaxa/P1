package com.exchange.gateway

import com.exchange.common.OrderBookEvent
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*

/**
 * Serves the latest cached market data snapshot for new clients connecting to the WebSocket.
 * Clients fetch the current state once via REST, then subscribe to live updates via STOMP.
 */
@RestController
@RequestMapping
class MarketDataController(
    private val orderBookConsumer: OrderBookConsumer,
    private val tradeStreamConsumer: TradeStreamConsumer,
    private val candleAggregator: CandleAggregator
) {

    @GetMapping("/orderbook/{symbolId}")
    fun orderBook(@PathVariable symbolId: Int): ResponseEntity<OrderBookEvent> =
        orderBookConsumer.latest(symbolId)
            ?.let { ResponseEntity.ok(it) }
            ?: ResponseEntity.notFound().build()

    @GetMapping("/ticker/{symbolId}")
    fun ticker(@PathVariable symbolId: Int): ResponseEntity<TickerEvent> =
        tradeStreamConsumer.latestTicker(symbolId)
            ?.let { ResponseEntity.ok(it) }
            ?: ResponseEntity.notFound().build()

    @GetMapping("/candles/{symbolId}")
    fun candles(
        @PathVariable symbolId: Int,
        @RequestParam(defaultValue = "1m") resolution: String,
        @RequestParam(defaultValue = "200") limit: Int
    ): List<CandleDto> = candleAggregator.getCandles(symbolId, resolution, limit.coerceAtMost(1000))
}
