package com.exchange.settlement

import com.exchange.common.TradeEvent
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.listener.ConsumerSeekAware
import org.springframework.kafka.support.Acknowledgment
import org.springframework.stereotype.Component

/**
 * Consumes trades from Kafka, settles them in TigerBeetle, and records each
 * settlement in ReconciliationService for balance-level verification.
 *
 * Seeks to the beginning of the trades topic on every startup — same strategy
 * as EngineCommandConsumer — so the full trade history is replayed into the
 * expected-balance tracker on each restart.  TigerBeetle idempotency (duplicate
 * transferIds return EXISTS silently) makes the re-settlement safe.
 * ReconciliationService deduplicates by tradeId so each trade is counted once.
 *
 * Offset is still committed after each record (harmless since seek-to-beginning
 * overrides it on the next restart).
 *
 * TODO: symbol → (baseLedger, quoteLedger) mapping will be managed in M2.
 */
@Component
class TradeConsumer(
    private val settlementService: SettlementService,
    private val reconciliation: ReconciliationService
) : ConsumerSeekAware {

    private val log = LoggerFactory.getLogger(javaClass)

    override fun onPartitionsAssigned(
        assignments: MutableMap<TopicPartition, Long>,
        callback: ConsumerSeekAware.ConsumerSeekCallback
    ) {
        assignments.keys.forEach { tp ->
            log.info("TradeConsumer: seeking to beginning of {}", tp)
            callback.seekToBeginning(tp.topic(), tp.partition())
        }
    }

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
        reconciliation.recordTrade(trade, baseLedger = 10, quoteLedger = 11)

        // Commit only after TigerBeetle write succeeded (seek-to-beginning on restart
        // re-processes all records anyway, so this commit is informational only)
        ack.acknowledge()
    }
}
