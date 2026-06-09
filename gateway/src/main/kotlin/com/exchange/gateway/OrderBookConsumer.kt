package com.exchange.gateway

import com.exchange.common.OrderBookEvent
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.listener.ConsumerSeekAware
import org.springframework.kafka.support.Acknowledgment
import org.springframework.messaging.simp.SimpMessagingTemplate
import org.springframework.stereotype.Component
import java.util.concurrent.ConcurrentHashMap

/**
 * Consumes the orderbook topic and broadcasts L2 snapshots to WebSocket subscribers.
 *
 * Seeks to the beginning on startup to rebuild the latest-snapshot cache per symbol
 * (when the topic uses log compaction, this is O(num_symbols) not O(num_commands)).
 * New REST clients can query the cached snapshot via GET /orderbook/{symbolId}.
 * WebSocket subscribers receive live updates on /topic/orderbook/{symbolId}.
 *
 * Pure relay: this component holds no order-book logic and makes no decisions.
 */
@Component
class OrderBookConsumer(
    private val messaging: SimpMessagingTemplate
) : ConsumerSeekAware {

    private val log = LoggerFactory.getLogger(javaClass)

    private val latestBySymbol = ConcurrentHashMap<Int, OrderBookEvent>()

    fun latest(symbolId: Int): OrderBookEvent? = latestBySymbol[symbolId]

    override fun onPartitionsAssigned(
        assignments: Map<TopicPartition, Long>,
        callback: ConsumerSeekAware.ConsumerSeekCallback
    ) {
        assignments.keys.forEach { tp ->
            callback.seekToBeginning(tp.topic(), tp.partition())
        }
    }

    @KafkaListener(
        topics = ["orderbook"],
        groupId = "gateway-orderbook",
        containerFactory = "kafkaListenerContainerFactory"
    )
    fun onOrderBook(record: ConsumerRecord<String, OrderBookEvent>, ack: Acknowledgment) {
        val event = record.value()
        latestBySymbol[event.symbolId] = event
        messaging.convertAndSend("/topic/orderbook/${event.symbolId}", event)
        log.debug("Relayed orderbook snapshot symbol={} bids={} asks={}", event.symbolId, event.bids.size, event.asks.size)
        ack.acknowledge()
    }
}
