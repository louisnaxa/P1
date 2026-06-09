package com.exchange.settlement

import com.exchange.common.EngineCommand
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.listener.ConsumerSeekAware
import org.springframework.kafka.support.Acknowledgment
import org.springframework.stereotype.Component

/**
 * Mirrors adjustBalance commands from the durable command log into TigerBeetle
 * as credit transfers from the external account.
 *
 * Seeks to the beginning of the commands topic on every startup — identical to
 * the matching engine's own replay strategy — so TigerBeetle balances are fully
 * reconstructed from the Kafka log.  All operations are idempotent: ensureAccount
 * and deposit use deterministic IDs derived from the Kafka offset, so TigerBeetle
 * silently ignores duplicates on replay.
 *
 * NOT M4 stablecoin deposit logic.  No external value enters here — we apply
 * commands that already exist in the durable log.
 */
@Component
class AdjustBalanceConsumer(
    private val settlementService: SettlementService,
    private val reconciliation: ReconciliationService
) : ConsumerSeekAware {

    private val log = LoggerFactory.getLogger(javaClass)

    override fun onPartitionsAssigned(
        assignments: MutableMap<TopicPartition, Long>,
        callback: ConsumerSeekAware.ConsumerSeekCallback
    ) {
        assignments.keys.forEach { tp ->
            log.info("AdjustBalanceConsumer: seeking to beginning of {}", tp)
            callback.seekToBeginning(tp.topic(), tp.partition())
        }
    }

    @KafkaListener(
        topics = ["commands"],
        groupId = "settlement-balance",
        containerFactory = "kafkaListenerContainerFactory"
    )
    fun onCommand(record: ConsumerRecord<String, EngineCommand>, ack: Acknowledgment) {
        val cmd = record.value()
        if (cmd.type != EngineCommand.ADJUST_BALANCE) {
            ack.acknowledge()
            return
        }

        // System accounts (external, fees) must exist before any deposit transfer
        settlementService.ensureSystemAccounts(cmd.currency)
        settlementService.ensureAccount(cmd.uid, cmd.currency)

        // transferId: (partition << 32) | (offset + 1)
        //   - non-zero (offset + 1 avoids TigerBeetle's reserved ID 0)
        //   - partition-scoped to prevent collisions on multi-partition topics
        //   - much smaller than trade transferIds (tradeId << 4), no overlap
        val transferId = (record.partition().toLong() shl 32) or (record.offset() + 1)
        settlementService.deposit(cmd.uid, cmd.currency, cmd.amount, transferId)

        reconciliation.recordCredit(cmd.uid, cmd.currency, cmd.amount)
        ack.acknowledge()
        log.debug("Applied adjustBalance uid={} currency={} amount={} transferId={}",
            cmd.uid, cmd.currency, cmd.amount, transferId)
    }
}
