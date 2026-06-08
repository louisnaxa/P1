package com.exchange.engine

import com.exchange.common.EngineCommand
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.listener.ConsumerSeekAware
import org.apache.kafka.common.TopicPartition
import org.springframework.stereotype.Component

/**
 * Consumes the durable command log and feeds exchange-core.
 *
 * On every startup, seeks to the beginning of the commands topic
 * and replays all commands (exchange-core starts clean, no snapshots).
 * Offsets are never committed — replay is always full.
 *
 * During replay, trades are re-published to the trades topic.
 * Settlement is idempotent (TigerBeetle rejects duplicate transfer IDs).
 */
@Component
class EngineCommandConsumer(
    private val engine: MatchingEngineService
) : ConsumerSeekAware {

    private val log = LoggerFactory.getLogger(javaClass)

    override fun onPartitionsAssigned(
        assignments: MutableMap<TopicPartition, Long>,
        callback: ConsumerSeekAware.ConsumerSeekCallback
    ) {
        assignments.keys.forEach { tp ->
            log.info("Seeking to beginning of {}", tp)
            callback.seekToBeginning(tp.topic(), tp.partition())
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
    }
}
