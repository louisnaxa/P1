package com.exchange.settlement

import com.exchange.common.PropertyCommand
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.dao.DataIntegrityViolationException
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.Acknowledgment
import org.springframework.stereotype.Component

/**
 * Consumes CREATE_PROPERTY commands from the property_commands topic.
 *
 * Settlement responsibility (after durability boundary):
 *   - Integrity validation via DB constraints (UNIQUE on property_ledger_id,
 *     FK from symbols → properties). Does NOT re-check access control —
 *     that was enforced by the gateway before the command entered the durable log
 *     (same principle as TransferGuard / TradeConsumer).
 *
 * Idempotence (re-delivery handling):
 *   - PropertyService.createProperty → DB UNIQUE on property_ledger_id → second INSERT
 *     throws DataIntegrityViolationException → caught here → ack and skip.
 *   - TB emission transferId is deterministic (emissionTransferId = (1L shl 48) + propertyId)
 *     → even if deposit() were called twice it would return TB Exists (no double emission).
 *
 * Proved in CI: PropertyIntegrationTest P5.
 */
@Component
class PropertyCommandConsumer(
    private val propertyService: PropertyService,
    private val jdbc: JdbcTemplate
) {
    private val log = LoggerFactory.getLogger(javaClass)

    @KafkaListener(
        topics = ["property_commands"],
        groupId = "settlement-property",
        containerFactory = "kafkaListenerContainerFactory"
    )
    fun onCreateProperty(record: ConsumerRecord<String, PropertyCommand>, ack: Acknowledgment) {
        val cmd = record.value()
        if (cmd.type != PropertyCommand.CREATE_PROPERTY) {
            log.warn("Unknown property command type: {}, ack and skip", cmd.type)
            ack.acknowledge()
            return
        }
        try {
            val propertyId = propertyService.createProperty(
                cmd.name, cmd.jurisdiction, cmd.propertyLedgerId,
                cmd.quoteLedgerId, cmd.symbolId, cmd.totalTokens
            )
            // Metadata: habillage — insert separately, never read by settlement/engine.
            if (cmd.description.isNotBlank() || cmd.location.isNotBlank()) {
                jdbc.update(
                    """INSERT INTO property_metadata (property_id, description, location)
                       VALUES (?, ?, ?)
                       ON CONFLICT (property_id) DO NOTHING""",
                    propertyId,
                    cmd.description.ifBlank { null },
                    cmd.location.ifBlank { null }
                )
            }
            log.info("Property created: propertyId={} ledger={} jurisdiction={}",
                propertyId, cmd.propertyLedgerId, cmd.jurisdiction)
        } catch (ex: DataIntegrityViolationException) {
            // Re-delivery: property with this ledger already exists — ack and skip.
            // TB is unaffected: emissionTransferId is idempotent (TB Exists on retry).
            log.info("Property ledger={} already exists (re-delivery), ack and skip",
                cmd.propertyLedgerId)
        }
        ack.acknowledge()
    }
}
