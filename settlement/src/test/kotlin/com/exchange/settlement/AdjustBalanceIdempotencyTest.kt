package com.exchange.settlement

import com.exchange.common.EngineCommand
import com.tigerbeetle.Client
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.MethodOrderer
import org.junit.jupiter.api.Order
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestMethodOrder
import org.mockito.Mockito.mock
import org.springframework.kafka.support.Acknowledgment

/**
 * TD-12 — Layer-2 idempotency: same on-chain deposit at two distinct Kafka offsets.
 *
 * Proves that if layer-1 (custody_events UNIQUE gate) were breached and the same
 * ADJUST_BALANCE command arrived at two different Kafka offsets, AdjustBalanceConsumer's
 * SHA-256 transferId path would still prevent double-credit in TigerBeetle.
 *
 * Neither idempotency layer alone is a single point of failure:
 *   - Layer 1 (Postgres UNIQUE) stops the duplicate before it reaches Kafka.
 *   - Layer 2 (SHA-256 transferId) stops it in TigerBeetle even if layer 1 is bypassed.
 *
 * Containers: TigerBeetle only. No Kafka, no Anvil, no Postgres needed.
 */
@Tag("integration")
@TestMethodOrder(MethodOrderer.OrderAnnotation::class)
class AdjustBalanceIdempotencyTest {

    companion object {
        private const val UID      = 7001L
        private const val CURRENCY = 20
        private const val AMOUNT   = 500L
        // Represents "$txHash:$logIndex" as stored in onChainRef
        private const val ON_CHAIN_REF = "0xdeadbeefcafe000000000000000000000000000000000000000000000000abcd:0"

        private val tb = TigerBeetleContainer()
        private lateinit var tbClient: Client
        private lateinit var settlement: SettlementService
        private lateinit var consumer: AdjustBalanceConsumer

        private val noOpAck = Acknowledgment { }

        @JvmStatic
        @BeforeAll
        fun setup() {
            tb.start()
            tbClient  = Client(ByteArray(16), arrayOf(tb.address))
            settlement = SettlementService(tbClient)
            settlement.ensureSystemAccounts(CURRENCY)
            settlement.ensureAccount(UID, CURRENCY)
            // bootstrapServers unused: we call onCommand() directly, never onPartitionsAssigned()
            consumer = AdjustBalanceConsumer(
                settlement,
                mock(ReconciliationService::class.java),
                "unused:9092"
            )
        }

        @JvmStatic
        @AfterAll
        fun teardown() {
            tbClient.close()
            tb.close()
        }

        private fun record(offset: Long) = ConsumerRecord<String, EngineCommand>(
            "commands", 0, offset, null,
            EngineCommand(
                type       = EngineCommand.ADJUST_BALANCE,
                uid        = UID,
                currency   = CURRENCY,
                amount     = AMOUNT,
                onChainRef = ON_CHAIN_REF
            )
        )
    }

    @Test
    @Order(1)
    fun `layer-2 idempotency D1 - first delivery credits TigerBeetle exactly once`() {
        consumer.onCommand(record(5L), noOpAck)
        assertThat(settlement.getBalance(UID, CURRENCY))
            .`as`("balance after first delivery").isEqualTo(AMOUNT)
    }

    @Test
    @Order(2)
    fun `layer-2 idempotency D2 - same onChainRef at a different Kafka offset is a TigerBeetle no-op`() {
        // Simulates layer-1 breach: watcher re-publishes the same event at offset 17 instead of 5.
        // SHA-256(onChainRef) is identical → same 64-bit transferId → TigerBeetle silent no-op.
        consumer.onCommand(record(17L), noOpAck)
        assertThat(settlement.getBalance(UID, CURRENCY))
            .`as`("balance must equal exactly one deposit, not two").isEqualTo(AMOUNT)
    }
}
