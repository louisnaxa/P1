package com.exchange.gateway

import com.exchange.common.EngineCommand
import org.apache.kafka.clients.producer.ProducerRecord
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component

/**
 * Publishes a command to the commands topic and waits for the broker ACK
 * (acks=all, configured in application.yml).
 *
 * Returns the Kafka offset assigned to the record.  With Option A, this offset
 * is the canonical orderId of the order — the engine derives its identity from
 * record.offset(), not from any field inside the command.
 */
@Component
class CommandPublisher(private val kafka: KafkaTemplate<String, EngineCommand>) {

    fun publish(topic: String, key: String, command: EngineCommand): Long {
        val record = ProducerRecord<String, EngineCommand>(topic, key, command)
        val result = kafka.send(record).get()   // blocks until broker confirms durability
        return result.recordMetadata.offset()
    }
}
