package com.exchange.settlement

import com.exchange.common.EngineCommand
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
 * Mirrors adjustBalance commands from the durable command log into TigerBeetle
 * as credit transfers from the external account.
 *
 * Seeks to the beginning of the commands topic on every startup (same replay
 * strategy as the matching engine).  All operations are idempotent: account
 * creation and deposit use deterministic IDs so TigerBeetle silently ignores
 * duplicates on replay.
 *
 * Replay-completion detection
 * ───────────────────────────
 * In onPartitionsAssigned, the end offsets of all assigned partitions are
 * queried via AdminClient (latest OffsetSpec).  After seekToBeginning, the
 * consumer position is 0.  If endOffset == 0 the partition is empty and
 * position (0) >= endOffset (0): the consumer is already caught up and
 * signals ReconciliationService immediately — no record needs to arrive.
 * For non-empty partitions the signal fires in onCommand AFTER the last
 * replay record has been fully settled in TigerBeetle and counted in the
 * expected-balance tracker.
 *
 * On rebalance, replay targets are reset and the caught-up signal is cleared
 * so ReconciliationService resumes only after the fresh replay completes.
 *
 * NOT M4 stablecoin deposit logic.  No external value enters here — only
 * commands already present in the durable Kafka log are applied.
 */
@Component
class AdjustBalanceConsumer(
    private val settlementService: SettlementService,
    private val reconciliation: ReconciliationService,
    @Value("\${spring.kafka.bootstrap-servers}") private val bootstrapServers: String
) : ConsumerSeekAware {

    private val log = LoggerFactory.getLogger(javaClass)

    // partition → endOffset captured at assignment (only partitions with endOffset > 0)
    private val replayTargets = ConcurrentHashMap<TopicPartition, Long>()

    override fun onPartitionsAssigned(
        assignments: Map<TopicPartition, Long>,
        callback: ConsumerSeekAware.ConsumerSeekCallback
    ) {
        // Clear previous state on rebalance
        replayTargets.clear()
        reconciliation.resetCommandsCaughtUp()

        // Seek to beginning — consumer position becomes 0 for all assigned partitions
        assignments.keys.forEach { tp ->
            callback.seekToBeginning(tp.topic(), tp.partition())
        }

        // Snapshot end offsets: the last record of the initial replay is at endOffset - 1.
        // After seekToBeginning, position = 0.  If endOffset == 0 then position >= endOffset
        // already (empty partition) → signal immediately without waiting for a record.
        AdminClient.create(
            mapOf(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers)
        ).use { admin ->
            val latestOffsets = admin.listOffsets(
                assignments.keys.associateWith { OffsetSpec.latest() }
            ).all().get(10, TimeUnit.SECONDS)

            assignments.keys.forEach { tp ->
                val endOffset = latestOffsets[tp]?.offset() ?: 0L
                log.info("AdjustBalanceConsumer partition={} endOffset={}", tp, endOffset)
                if (endOffset > 0L) {
                    replayTargets[tp] = endOffset
                }
                // else endOffset == 0: position (0) >= endOffset (0) — caught up immediately
            }

            if (replayTargets.isEmpty()) {
                log.info("Commands topic fully empty at assignment — signalling caught up immediately")
                reconciliation.signalCommandsCaughtUp()
            }
        }
    }

    @KafkaListener(
        topics = ["commands"],
        groupId = "settlement-balance",
        containerFactory = "kafkaListenerContainerFactory"
    )
    fun onCommand(record: ConsumerRecord<String, EngineCommand>, ack: Acknowledgment) {
        val cmd = record.value()

        // When a new trading pair is activated, create external+fees accounts for
        // both ledgers immediately so they are ready before the first trade settles.
        if (cmd.type == EngineCommand.ADD_SYMBOL) {
            settlementService.ensureSystemAccounts(cmd.baseCurrency)
            settlementService.ensureSystemAccounts(cmd.quoteCurrency)
        }

        // Only adjustBalance commands are applied to TigerBeetle; all other types
        // (addUser, addSymbol, placeOrder) are skipped for settlement purposes.
        if (cmd.type == EngineCommand.ADJUST_BALANCE) {
            settlementService.ensureSystemAccounts(cmd.currency)
            settlementService.ensureAccount(cmd.uid, cmd.currency)

            // transferId: (partition << 32) | (offset + 1)
            //   - offset + 1 avoids TigerBeetle's reserved ID 0
            //   - partition-scoped so multi-partition topics never collide
            //   - values stay well below trade transferIds (tradeId << 4)
            val transferId = (record.partition().toLong() shl 32) or (record.offset() + 1)
            settlementService.deposit(cmd.uid, cmd.currency, cmd.amount, transferId)

            // Update expected balance BEFORE emitting the caught-up signal below
            reconciliation.recordCredit(cmd.uid, cmd.currency, cmd.amount)
        }

        // Signal replay complete AFTER any TigerBeetle write and expected-balance update
        // for this record.  The check applies to ALL command types, not just
        // adjustBalance — the last record in the log may be a placeOrder or addUser.
        val tp = TopicPartition(record.topic(), record.partition())
        val target = replayTargets[tp]
        if (target != null && record.offset() >= target - 1) {
            replayTargets.remove(tp)
            if (replayTargets.isEmpty()) {
                log.info("Commands replay complete at offset={}", record.offset())
                reconciliation.signalCommandsCaughtUp()
            }
        }

        ack.acknowledge()
    }
}
