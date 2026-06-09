package com.exchange.settlement

import com.exchange.common.TradeEvent
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.OffsetSpec
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.listener.ConsumerSeekAware
import org.springframework.kafka.support.Acknowledgment
import org.springframework.stereotype.Component
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit

/**
 * Consumes trades from Kafka, settles them in TigerBeetle, and records each
 * settlement in ReconciliationService for balance-level verification.
 *
 * Seeks to the beginning of the trades topic on every startup so that the full
 * trade history is replayed into the expected-balance tracker.  TigerBeetle
 * idempotency (duplicate transferIds return EXISTS) makes the re-settlement
 * safe.  ReconciliationService deduplicates by tradeId so each trade is
 * counted exactly once regardless of how many copies appear in the topic.
 *
 * Replay-completion detection
 * ───────────────────────────
 * Mirrors the same end-offset strategy as AdjustBalanceConsumer: end offsets
 * are snapshotted in onPartitionsAssigned via AdminClient.  Empty partitions
 * (endOffset == 0, position 0 >= endOffset 0) signal immediately; non-empty
 * partitions signal in onTrade after the last replay record is fully settled
 * in TigerBeetle and counted in expected balances.
 *
 * On rebalance, replay targets and the caught-up signal are reset.
 */
@Component
class TradeConsumer(
    private val settlementService: SettlementService,
    private val reconciliation: ReconciliationService,
    @Value("\${spring.kafka.bootstrap-servers}") private val bootstrapServers: String
) : ConsumerSeekAware {

    private val log = LoggerFactory.getLogger(javaClass)

    private val replayTargets = ConcurrentHashMap<TopicPartition, Long>()

    override fun onPartitionsAssigned(
        assignments: Map<TopicPartition, Long>,
        callback: ConsumerSeekAware.ConsumerSeekCallback
    ) {
        replayTargets.clear()
        reconciliation.resetTradesCaughtUp()

        assignments.keys.forEach { tp ->
            callback.seekToBeginning(tp.topic(), tp.partition())
        }

        AdminClient.create(
            mapOf(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers)
        ).use { admin ->
            val latestOffsets = admin.listOffsets(
                assignments.keys.associateWith { OffsetSpec.latest() }
            ).all().get(10, TimeUnit.SECONDS)

            assignments.keys.forEach { tp ->
                val endOffset = latestOffsets[tp]?.offset() ?: 0L
                log.info("TradeConsumer partition={} endOffset={}", tp, endOffset)
                if (endOffset > 0L) {
                    replayTargets[tp] = endOffset
                }
            }

            if (replayTargets.isEmpty()) {
                log.info("Trades topic fully empty at assignment — signalling caught up immediately")
                reconciliation.signalTradesCaughtUp()
            }
        }
    }

    @KafkaListener(
        topics = ["trades"],
        groupId = "settlement",
        containerFactory = "kafkaListenerContainerFactory"
    )
    fun onTrade(record: ConsumerRecord<String, TradeEvent>, ack: Acknowledgment) {
        val trade = record.value()
        log.info("Received trade {} symbol={} offset={}", trade.tradeId, trade.symbolId, record.offset())

        // TODO: symbol → (baseLedger, quoteLedger) mapping will be managed in M2
        settlementService.settleTrade(trade, baseLedger = 10, quoteLedger = 11)

        // Update expected balance BEFORE emitting the caught-up signal below
        reconciliation.recordTrade(trade, baseLedger = 10, quoteLedger = 11)

        // Signal AFTER settlement and expected-balance update, never before
        val tp = TopicPartition(record.topic(), record.partition())
        val target = replayTargets[tp]
        if (target != null && record.offset() >= target - 1) {
            replayTargets.remove(tp)
            if (replayTargets.isEmpty()) {
                log.info("Trades replay complete at offset={}", record.offset())
                reconciliation.signalTradesCaughtUp()
            }
        }

        ack.acknowledge()
    }
}
