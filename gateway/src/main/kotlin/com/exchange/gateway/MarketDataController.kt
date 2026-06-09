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
    private val tradeStreamConsumer: TradeStreamConsumer
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
}
