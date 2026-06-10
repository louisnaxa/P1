package com.exchange.gateway

import org.apache.kafka.clients.producer.ProducerRecord
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component

/**
 * Publishes a command to any topic and waits for the broker ACK (acks=all).
 *
 * Returns the Kafka offset assigned to the record.  For PLACE_ORDER, this offset
 * is the canonical orderId — the engine derives its identity from record.offset(),
 * not from any field inside the command.
 *
 * Accepts Any so that both EngineCommand (orders/balance) and PropertyCommand
 * (property creation) can be published through the same publisher.
 */
@Component
class CommandPublisher(private val kafka: KafkaTemplate<String, Any>) {

    fun publish(topic: String, key: String, command: Any): Long {
        val record = ProducerRecord<String, Any>(topic, key, command)
        val result = kafka.send(record).get()   // blocks until broker confirms durability
        return result.recordMetadata.offset()
    }
}
