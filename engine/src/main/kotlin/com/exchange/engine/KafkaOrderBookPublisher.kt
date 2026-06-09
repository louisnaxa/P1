package com.exchange.engine

import com.exchange.common.OrderBookEvent
import org.slf4j.LoggerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component

/**
 * Publishes L2 orderbook snapshots to Kafka as a fire-and-forget diffusion side-effect.
 *
 * Intentionally no .get(): snapshot delivery failure must never block or fail the engine.
 * The topic is keyed by symbolId so that log compaction retains only the latest snapshot
 * per symbol (configure cleanup.policy=compact on the orderbook topic in production).
 */
@Component
class KafkaOrderBookPublisher(
    private val kafkaTemplate: KafkaTemplate<String, OrderBookEvent>
) : (OrderBookEvent) -> Unit {

    private val log = LoggerFactory.getLogger(javaClass)

    override fun invoke(event: OrderBookEvent) {
        kafkaTemplate.send(TOPIC, event.symbolId.toString(), event)
            .whenComplete { _, ex ->
                if (ex != null) log.warn("Orderbook snapshot send failed symbol={}: {}", event.symbolId, ex.message)
            }
    }

    companion object {
        const val TOPIC = "orderbook"
    }
}
