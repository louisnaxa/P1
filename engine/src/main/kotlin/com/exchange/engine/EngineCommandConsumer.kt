package com.exchange.engine

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
import org.springframework.stereotype.Component
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit

/**
 * Consumes the durable command log and feeds exchange-core.
 *
 * On every startup, seeks to the beginning of the commands topic
 * and replays all commands (exchange-core starts clean, no snapshots).
 * Offsets are never committed — replay is always full.
 *
 * During replay, trades are re-published to the trades topic.
 * Settlement is idempotent (TigerBeetle rejects duplicate transfer IDs).
 *
 * Replay-completion detection
 * ───────────────────────────
 * End offsets are snapshotted in onPartitionsAssigned via AdminClient.
 * Once all partitions reach their end offset, engine.signalReplayComplete()
 * is called so that orderbook snapshot publication can begin.
 * Empty partitions signal immediately (no record will arrive).
 */
@Component
class EngineCommandConsumer(
    private val engine: MatchingEngineService,
    @Value("\${spring.kafka.bootstrap-servers}") private val bootstrapServers: String
) : ConsumerSeekAware {

    private val log = LoggerFactory.getLogger(javaClass)
    private val replayTargets = ConcurrentHashMap<TopicPartition, Long>()

    override fun onPartitionsAssigned(
        assignments: MutableMap<TopicPartition, Long>,
        callback: ConsumerSeekAware.ConsumerSeekCallback
    ) {
        replayTargets.clear()

        assignments.keys.forEach { tp ->
            log.info("Seeking to beginning of {}", tp)
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
                log.info("EngineCommandConsumer partition={} endOffset={}", tp, endOffset)
                if (endOffset > 0L) {
                    replayTargets[tp] = endOffset
                }
            }

            if (replayTargets.isEmpty()) {
                log.info("Commands topic empty at assignment — engine replay complete immediately")
                engine.signalReplayComplete()
            }
        }
    }

    @KafkaListener(
        topics = ["commands"],
        groupId = "engine",
        properties = ["enable.auto.commit=false"]
    )
    fun onCommand(record: ConsumerRecord<String, EngineCommand>) {
        log.debug("Processing command offset={} type={}", record.offset(), record.value().type)
        engine.processCommand(record.offset(), record.value())
        // No offset commit — we always replay from the beginning on restart.

        val tp = TopicPartition(record.topic(), record.partition())
        val target = replayTargets[tp]
        if (target != null && record.offset() >= target - 1) {
            replayTargets.remove(tp)
            if (replayTargets.isEmpty()) {
                log.info("Engine replay complete at offset={}", record.offset())
                engine.signalReplayComplete()
            }
        }
    }
}
