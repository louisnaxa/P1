package com.exchange.engine

import com.exchange.common.TradeEvent
import org.slf4j.LoggerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component

/**
 * Publishes trades to Kafka synchronously.
 * The send blocks until the broker acknowledges, so that the command offset
 * is never advanced before the trade is durably stored.
 */
@Component
class KafkaTradePublisher(
    private val kafkaTemplate: KafkaTemplate<String, TradeEvent>
) : (TradeEvent) -> Unit {

    private val log = LoggerFactory.getLogger(javaClass)

    override fun invoke(trade: TradeEvent) {
        val key = "${trade.symbolId}"
        kafkaTemplate.send(TOPIC, key, trade).get()
        log.debug("Published trade {} to Kafka", trade.tradeId)
    }

    companion object {
        const val TOPIC = "trades"
    }
}
