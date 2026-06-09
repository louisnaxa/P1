package com.exchange.gateway

import com.exchange.common.TradeEvent
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.listener.ConsumerSeekAware
import org.springframework.kafka.support.Acknowledgment
import org.springframework.messaging.simp.SimpMessagingTemplate
import org.springframework.stereotype.Component
import java.util.concurrent.ConcurrentHashMap

data class TickerEvent(
    val symbolId: Int,
    val last: Long,
    val high: Long,
    val low: Long,
    val volume: Long
)

/**
 * Consumes the trades topic and broadcasts trade events and ticker updates.
 *
 * Seeks to the END on startup — only new real-time trades are relayed to clients.
 * Historical trades are not replayed to WebSocket subscribers.
 *
 * Ticker (last/high/low/vol) accumulates from the current session.
 * It becomes accurate over a full 24h window of continuous operation.
 *
 * Pure relay: no order-book state, no balances, no decisions.
 */
@Component
class TradeStreamConsumer(
    private val messaging: SimpMessagingTemplate
) : ConsumerSeekAware {

    private val log = LoggerFactory.getLogger(javaClass)

    private val tickerBySymbol = ConcurrentHashMap<Int, TickerEvent>()

    fun latestTicker(symbolId: Int): TickerEvent? = tickerBySymbol[symbolId]

    override fun onPartitionsAssigned(
        assignments: Map<TopicPartition, Long>,
        callback: ConsumerSeekAware.ConsumerSeekCallback
    ) {
        // Seek to end: only live trades reach WebSocket clients, not historical replay.
        assignments.keys.forEach { tp ->
            callback.seekToEnd(tp.topic(), tp.partition())
        }
    }

    @KafkaListener(
        topics = ["trades"],
        groupId = "gateway-trades",
        containerFactory = "kafkaListenerContainerFactory"
    )
    fun onTrade(record: ConsumerRecord<String, TradeEvent>, ack: Acknowledgment) {
        val trade = record.value()

        messaging.convertAndSend("/topic/trades/${trade.symbolId}", trade)

        val ticker = tickerBySymbol.compute(trade.symbolId) { _, prev ->
            if (prev == null) TickerEvent(trade.symbolId, trade.price, trade.price, trade.price, trade.quantity)
            else TickerEvent(
                symbolId = trade.symbolId,
                last   = trade.price,
                high   = maxOf(prev.high, trade.price),
                low    = minOf(prev.low,  trade.price),
                volume = prev.volume + trade.quantity
            )
        }!!
        messaging.convertAndSend("/topic/ticker/${trade.symbolId}", ticker)

        log.debug("Relayed trade {} symbol={} {}@{}", trade.tradeId, trade.symbolId, trade.quantity, trade.price)
        ack.acknowledge()
    }
}
