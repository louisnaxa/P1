package com.exchange.settlement

import com.exchange.common.TradeEvent
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.Acknowledgment
import org.springframework.stereotype.Component

/**
 * Consumes trades from Kafka and settles them in TigerBeetle.
 * Offset is committed only AFTER TigerBeetle write succeeds.
 * If settlement fails, the message is redelivered on restart.
 */
@Component
class TradeConsumer(private val settlementService: SettlementService) {

    private val log = LoggerFactory.getLogger(javaClass)

    @KafkaListener(
        topics = ["trades"],
        groupId = "settlement",
        containerFactory = "kafkaListenerContainerFactory"
    )
    fun onTrade(record: ConsumerRecord<String, TradeEvent>, ack: Acknowledgment) {
        val trade = record.value()
        log.info("Received trade {} for symbol {} (offset={})", trade.tradeId, trade.symbolId, record.offset())

        // TODO: symbol → ledger mapping will be managed properly in M2
        settlementService.settleTrade(trade, baseLedger = 10, quoteLedger = 11)

        // Commit only after TigerBeetle write succeeded
        ack.acknowledge()
    }
}
