package com.exchange.settlement

import com.exchange.common.EngineCommand
import com.exchange.common.OrderSide
import com.exchange.common.TradeEvent
import com.exchange.engine.MatchingEngineService
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.tigerbeetle.Client
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.utility.DockerImageName
import java.time.Duration
import java.util.UUID
import java.util.concurrent.atomic.AtomicLong

/**
 * Chaos integration tests — real TigerBeetle, real Kafka, no mocks.
 *
 * Each test uses distinct user IDs so TigerBeetle state does not leak between
 * tests (all three share one long-running container pair).
 *
 * "Crash" fidelity within a single JVM:
 *
 *   Chaos 1 — Kafka at-least-once re-delivery
 *     The Kafka producer sends the same TradeEvent twice (simulates a broker
 *     retry or a lost ack that causes the producer to re-send).
 *     A consumer settles both messages against real TigerBeetle.
 *
 *   Chaos 2 — Engine crash before Kafka publish ack
 *     MatchingEngineService is shut down and re-created with the same offsets
 *     (full-replay, identical to the real startup path).  The replayed trade
 *     has the same tradeId → same transferIds → second settleTrade is a no-op.
 *
 *   Chaos 3 — Settlement crash after TigerBeetle write, before Kafka commit
 *     Consumer-1 (group G) settles the trade, then closes WITHOUT committing.
 *     Consumer-2 (same group G) re-reads the uncommitted offset and settles
 *     again.  TigerBeetle rejects the duplicate transferId.
 *
 * Final assertion in every scenario: real TigerBeetle balance == value of
 * exactly one trade, queried via SettlementService.getBalance().
 *
 * CI note: Docker ≥ 25 blocks io_uring; TigerBeetleContainer sets
 * seccomp=unconfined on both TB containers.  Passes on GitHub Actions
 * ubuntu-latest; may require explicit seccomp policy on hardened runners.
 */
@Tag("integration")
class SettlementChaosIntegrationTest {

    companion object {
        // Distinct user IDs per scenario — avoids TB account collisions
        const val ALICE_C1 = 1001L;  const val BOB_C1 = 1002L   // Chaos 1
        const val ALICE_C2 = 2001L;  const val BOB_C2 = 2002L   // Chaos 2
        const val ALICE_C3 = 3001L;  const val BOB_C3 = 3002L   // Chaos 3
        const val ALICE_C4 = 4001L;  const val BOB_C4 = 4002L   // Chaos 4

        const val BASE_LEDGER  = 10
        const val QUOTE_LEDGER = 11

        const val ALICE_QUOTE = 1_000_000_000L
        const val ALICE_BASE  = 1_000_000L
        const val BOB_BASE    = 1_000_000L
        const val BOB_QUOTE   = 1_000_000_000L

        // Deposit transfer IDs must not collide with trade transfer IDs.
        // Trade transferIds in these tests top out around 5_000_000_000.
        // Starting at 10_000_000_000 gives a safe buffer.
        private val depositSeq = AtomicLong(10_000_000_000L)

        private val mapper = jacksonObjectMapper()

        // Containers are started once and shared across all tests in the class
        val tb    = TigerBeetleContainer()
        val kafka = KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.4.0"))

        @JvmStatic
        @BeforeAll
        fun startContainers() {
            tb.start()
            kafka.start()
            createTopics("trades-chaos1", "trades-chaos3")
        }

        @JvmStatic
        @AfterAll
        fun stopContainers() {
            tb.close()
            kafka.stop()
        }

        private fun createTopics(vararg names: String) {
            AdminClient.create(
                mapOf(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to kafka.bootstrapServers)
            ).use { admin ->
                admin.createTopics(names.map { NewTopic(it, 1, 1.toShort()) }).all().get()
            }
        }
    }

    // ── Per-test service instances ─────────────────────────────────────────

    private lateinit var tbClient: Client
    private lateinit var service: SettlementService

    @BeforeEach
    fun setUp() {
        tbClient = Client(ByteArray(16) /* cluster-id = 0 */, arrayOf(tb.address))
        service  = SettlementService(tbClient)
        // Idempotent — safe to call on every test even if accounts already exist
        service.ensureSystemAccounts(BASE_LEDGER)
        service.ensureSystemAccounts(QUOTE_LEDGER)
    }

    @AfterEach
    fun tearDown() {
        tbClient.close()
    }

    // ── Helpers ───────────────────────────────────────────────────────────

    private fun fund(aliceUid: Long, bobUid: Long) {
        listOf(aliceUid, bobUid).forEach { uid ->
            service.ensureAccount(uid, BASE_LEDGER)
            service.ensureAccount(uid, QUOTE_LEDGER)
        }
        service.deposit(aliceUid, QUOTE_LEDGER, ALICE_QUOTE, depositSeq.getAndIncrement())
        service.deposit(aliceUid, BASE_LEDGER,  ALICE_BASE,  depositSeq.getAndIncrement())
        service.deposit(bobUid,   BASE_LEDGER,  BOB_BASE,    depositSeq.getAndIncrement())
        service.deposit(bobUid,   QUOTE_LEDGER, BOB_QUOTE,   depositSeq.getAndIncrement())
    }

    /** Assert that real TB balances reflect exactly one BID-side trade. */
    private fun assertSingleTrade(aliceUid: Long, bobUid: Long, price: Long, qty: Long) {
        val quoteAmount = price * qty
        assertThat(service.getBalance(aliceUid, QUOTE_LEDGER))
            .`as`("Alice quote balance").isEqualTo(ALICE_QUOTE - quoteAmount)
        assertThat(service.getBalance(aliceUid, BASE_LEDGER))
            .`as`("Alice base balance").isEqualTo(ALICE_BASE + qty)
        assertThat(service.getBalance(bobUid, QUOTE_LEDGER))
            .`as`("Bob quote balance").isEqualTo(BOB_QUOTE + quoteAmount)
        assertThat(service.getBalance(bobUid, BASE_LEDGER))
            .`as`("Bob base balance").isEqualTo(BOB_BASE - qty)
    }

    private fun kafkaProducer(): KafkaProducer<String, String> =
        KafkaProducer(mapOf(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG      to kafka.bootstrapServers,
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG   to StringSerializer::class.java,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
        ))

    private fun kafkaConsumer(groupId: String): KafkaConsumer<String, String> =
        KafkaConsumer(mapOf(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG        to kafka.bootstrapServers,
            ConsumerConfig.GROUP_ID_CONFIG                 to groupId,
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG        to "earliest",
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG       to "false",
            ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG       to "6000",
            ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG    to "1000",
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG   to StringDeserializer::class.java,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
        ))

    /** Polls until one record arrives or 20 s elapses. */
    private fun pollOne(consumer: KafkaConsumer<String, String>): ConsumerRecord<String, String> {
        val deadline = System.currentTimeMillis() + 20_000
        while (System.currentTimeMillis() < deadline) {
            val batch = consumer.poll(Duration.ofSeconds(1))
            if (batch.count() > 0) return batch.iterator().next()
        }
        error("No Kafka record arrived within 20 s")
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Chaos 1 — Double delivery via Kafka at-least-once
    //
    // The broker (or producer) delivers the same TradeEvent message twice.
    // The consumer calls settleTrade for each delivery.
    // TigerBeetle must reject the duplicate transferId on the second call.
    // ─────────────────────────────────────────────────────────────────────────
    @Test
    fun `chaos 1 - Kafka double delivery is idempotent in TigerBeetle`() {
        fund(ALICE_C1, BOB_C1)

        // tradeId uses offset=100 to avoid collisions with Chaos 2 (offset=8) and Chaos 3
        val trade = TradeEvent(
            tradeId      = (100L shl 16) or 0L,
            symbolId     = 1,
            takerOrderId = 2L,
            takerUserId  = ALICE_C1,
            makerOrderId = 1L,
            makerUserId  = BOB_C1,
            price        = 100L,
            quantity     = 5L,
            takerSide    = OrderSide.BID,
            timestampNs  = 0L
        )
        val json = mapper.writeValueAsString(trade)

        // Publish the same message twice — simulates at-least-once re-delivery
        kafkaProducer().use { producer ->
            repeat(2) { producer.send(ProducerRecord("trades-chaos1", "1", json)).get() }
        }

        // Consume both records and settle each against real TigerBeetle
        kafkaConsumer("chaos1-${UUID.randomUUID()}").use { consumer ->
            consumer.subscribe(listOf("trades-chaos1"))
            var settled = 0
            val deadline = System.currentTimeMillis() + 20_000
            while (settled < 2 && System.currentTimeMillis() < deadline) {
                for (record in consumer.poll(Duration.ofSeconds(1))) {
                    service.settleTrade(
                        mapper.readValue(record.value(), TradeEvent::class.java),
                        BASE_LEDGER, QUOTE_LEDGER
                    )
                    settled++
                }
            }
            assertThat(settled).`as`("both deliveries consumed").isEqualTo(2)
        }

        // Real TigerBeetle: transferId deduplicated → exactly one trade settled
        assertSingleTrade(ALICE_C1, BOB_C1, price = 100L, qty = 5L)
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Chaos 2 — Engine crash between match and Kafka publish
    //
    // MatchingEngineService is shut down (crash) and restarted with identical
    // command offsets (full-replay from offset 0, same as the real engine).
    // The replayed order produces the identical tradeId → identical transferIds.
    // Both the first and replayed trade are settled → TigerBeetle deduplicates.
    // ─────────────────────────────────────────────────────────────────────────
    @Test
    fun `chaos 2 - engine crash and replay produces same tradeId, TigerBeetle idempotent`() {
        fund(ALICE_C2, BOB_C2)

        /**
         * Run a fresh MatchingEngineService from offset 0 with the standard
         * command sequence.  For each matched trade, calls [onTrade] and
         * collects the published trade.  Shuts the engine down before returning.
         */
        fun runEngine(onTrade: (TradeEvent) -> Unit): TradeEvent {
            val captured = mutableListOf<TradeEvent>()
            val engine = MatchingEngineService { t -> captured.add(t); onTrade(t) }
            engine.init()
            var offset = 0L
            fun cmd(c: EngineCommand) = engine.processCommand(offset++, c)
            cmd(EngineCommand.addSymbol(1, BASE_LEDGER, QUOTE_LEDGER))   // offset 0
            cmd(EngineCommand.addUser(ALICE_C2))                         // offset 1
            cmd(EngineCommand.addUser(BOB_C2))                           // offset 2
            cmd(EngineCommand.adjustBalance(ALICE_C2, QUOTE_LEDGER, ALICE_QUOTE)) // offset 3
            cmd(EngineCommand.adjustBalance(ALICE_C2, BASE_LEDGER,  ALICE_BASE))  // offset 4
            cmd(EngineCommand.adjustBalance(BOB_C2,   BASE_LEDGER,  BOB_BASE))    // offset 5
            cmd(EngineCommand.adjustBalance(BOB_C2,   QUOTE_LEDGER, BOB_QUOTE))   // offset 6
            cmd(EngineCommand.placeOrder(1, BOB_C2,   1L, 100L, 5L, OrderSide.ASK)) // offset 7
            cmd(EngineCommand.placeOrder(1, ALICE_C2, 2L, 100L, 5L, OrderSide.BID)) // offset 8 → match
            engine.shutdown()
            assertThat(captured).`as`("engine must produce exactly one trade").hasSize(1)
            return captured[0]
        }

        // First run: trade matched and settled — then engine "crashes"
        val trade1 = runEngine { t -> service.settleTrade(t, BASE_LEDGER, QUOTE_LEDGER) }

        // Restart: full replay from offset 0 with same offsets → same tradeId
        val trade2 = runEngine { t -> service.settleTrade(t, BASE_LEDGER, QUOTE_LEDGER) }

        // Determinism guarantee: identical offsets → identical tradeId
        assertThat(trade2.tradeId).`as`("replayed tradeId must match").isEqualTo(trade1.tradeId)

        // Real TigerBeetle: second settleTrade was a no-op → exactly one trade settled
        assertSingleTrade(ALICE_C2, BOB_C2, price = 100L, qty = 5L)
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Chaos 3 — Settlement crash after TigerBeetle write, before Kafka commit
    //
    // Consumer-1 settles the trade (TigerBeetle write succeeds) then closes
    // WITHOUT committing the Kafka offset — simulating a process crash.
    // Consumer-2 (same group-id) reconnects and re-reads the uncommitted
    // message (Kafka redelivers since offset was never committed).
    // The second settleTrade hits TigerBeetle with identical transferIds → no-op.
    // ─────────────────────────────────────────────────────────────────────────
    @Test
    fun `chaos 3 - settlement crash after TB write before commit is idempotent`() {
        fund(ALICE_C3, BOB_C3)

        // tradeId uses offset=300 — distinct from Chaos 1 (100) and Chaos 2 (engine offset 8)
        val trade = TradeEvent(
            tradeId      = (300L shl 16) or 0L,
            symbolId     = 1,
            takerOrderId = 2L,
            takerUserId  = ALICE_C3,
            makerOrderId = 1L,
            makerUserId  = BOB_C3,
            price        = 200L,
            quantity     = 3L,
            takerSide    = OrderSide.BID,
            timestampNs  = 0L
        )

        kafkaProducer().use { producer ->
            producer.send(ProducerRecord("trades-chaos3", "1", mapper.writeValueAsString(trade))).get()
        }

        // Shared group-id so consumer-2 inherits consumer-1's uncommitted offset
        val groupId = "chaos3-${UUID.randomUUID()}"

        // Consumer 1: TigerBeetle write succeeds, then process "crashes" (close without commit)
        kafkaConsumer(groupId).use { consumer ->
            consumer.subscribe(listOf("trades-chaos3"))
            val record = pollOne(consumer)
            service.settleTrade(
                mapper.readValue(record.value(), TradeEvent::class.java),
                BASE_LEDGER, QUOTE_LEDGER
            )
            // close() sends LeaveGroup to broker — no commitSync() → offset NOT committed
        }

        // Consumer 2: same group-id → Kafka redelivers from the last committed offset
        // (none committed) → receives the same message via auto.offset.reset=earliest
        kafkaConsumer(groupId).use { consumer ->
            consumer.subscribe(listOf("trades-chaos3"))
            val record = pollOne(consumer)
            val redelivered = mapper.readValue(record.value(), TradeEvent::class.java)

            assertThat(redelivered.tradeId)
                .`as`("Kafka must redeliver the same TradeEvent")
                .isEqualTo(trade.tradeId)

            // Second settleTrade: TigerBeetle sees duplicate transferIds → silent no-op
            service.settleTrade(redelivered, BASE_LEDGER, QUOTE_LEDGER)
            consumer.commitSync()
        }

        // Real TigerBeetle: exactly one trade settled despite two settleTrade calls
        assertSingleTrade(ALICE_C3, BOB_C3, price = 200L, qty = 3L)
    }

    // ─────────────────────────────────────────────────────────────────────────
    // M2 regression — missing LINKED flag: leg 1 failure leaves leg 0 applied
    //
    // settleTrade submits both legs in ONE createTransfers(batch) call without
    // TransferFlags.LINKED.  The legs are independent: if leg 1 fails (e.g. a
    // balance-constraint or missing-account error in M2), leg 0 stays committed.
    //
    // This cannot happen in M1 — user accounts have no upper-bound constraint
    // and both legs always fit.  It becomes a correctness risk in M2 when locked
    // accounts and real margin checks are introduced (see TD-4 in TECH_DEBT.md).
    //
    // Test scenario:
    //   1. Bob's base account is intentionally absent → leg 1 (base: Bob→Alice)
    //      returns DEBIT_ACCOUNT_NOT_FOUND; leg 0 (quote: Alice→Bob) succeeds.
    //   2. Bob's base account is created and funded (operator remediation).
    //   3. Full settleTrade replayed → leg 0 EXISTS (no-op), leg 1 applied.
    //   4. Balances == exactly one complete trade; leg 0 not double-counted.
    // ─────────────────────────────────────────────────────────────────────────
    @Test
    @Disabled("régression M2 — voir TD-4")
    fun `m2 regression - without LINKED flag leg 0 survives leg 1 failure and replay completes trade`() {
        // Set up accounts for both users — Bob's base account intentionally omitted
        service.ensureAccount(ALICE_C4, BASE_LEDGER)
        service.ensureAccount(ALICE_C4, QUOTE_LEDGER)
        service.ensureAccount(BOB_C4,   QUOTE_LEDGER)
        service.deposit(ALICE_C4, QUOTE_LEDGER, ALICE_QUOTE, depositSeq.getAndIncrement())
        service.deposit(ALICE_C4, BASE_LEDGER,  ALICE_BASE,  depositSeq.getAndIncrement())
        service.deposit(BOB_C4,   QUOTE_LEDGER, BOB_QUOTE,   depositSeq.getAndIncrement())

        // tradeId offset=400 — distinct from C1 (100), C2 (engine ~8), C3 (300)
        val trade = TradeEvent(
            tradeId      = (400L shl 16) or 0L,
            symbolId     = 1,
            takerOrderId = 2L,
            takerUserId  = ALICE_C4,   // BID taker → buyer
            makerOrderId = 1L,
            makerUserId  = BOB_C4,     // maker → seller
            price        = 100L,
            quantity     = 5L,
            takerSide    = OrderSide.BID,
            timestampNs  = 0L
        )
        val quoteAmount = trade.price * trade.quantity  // 500
        val baseAmount  = trade.quantity                // 5

        // ── First call: leg 0 succeeds, leg 1 fails (Bob has no base account) ──
        // SettlementService logs a warn for errors.length > 0 and returns normally
        service.settleTrade(trade, BASE_LEDGER, QUOTE_LEDGER)

        // Half-settled: quote transferred, base untouched
        assertThat(service.getBalance(ALICE_C4, QUOTE_LEDGER))
            .`as`("Alice quote: leg 0 applied").isEqualTo(ALICE_QUOTE - quoteAmount)
        assertThat(service.getBalance(BOB_C4,   QUOTE_LEDGER))
            .`as`("Bob quote: leg 0 applied").isEqualTo(BOB_QUOTE + quoteAmount)
        assertThat(service.getBalance(ALICE_C4, BASE_LEDGER))
            .`as`("Alice base: leg 1 not yet applied").isEqualTo(ALICE_BASE)
        assertThat(service.getBalance(BOB_C4,   BASE_LEDGER))
            .`as`("Bob base account missing → balance 0").isEqualTo(0L)

        // ── Operator remediation: create Bob's base account and fund it ────────
        service.ensureAccount(BOB_C4, BASE_LEDGER)
        service.deposit(BOB_C4, BASE_LEDGER, BOB_BASE, depositSeq.getAndIncrement())

        // ── Replay: leg 0 → EXISTS (no-op), leg 1 → applied ──────────────────
        service.settleTrade(trade, BASE_LEDGER, QUOTE_LEDGER)

        // Leg 0 must NOT be double-counted
        assertThat(service.getBalance(ALICE_C4, QUOTE_LEDGER))
            .`as`("leg 0 must not be double-counted").isEqualTo(ALICE_QUOTE - quoteAmount)
        assertThat(service.getBalance(ALICE_C4, BASE_LEDGER))
            .`as`("leg 1 applied by replay").isEqualTo(ALICE_BASE + baseAmount)

        // Full invariant: real TigerBeetle balances == one complete trade
        assertSingleTrade(ALICE_C4, BOB_C4, price = trade.price, qty = trade.quantity)

        // A second replay is a complete no-op (both legs exist)
        service.settleTrade(trade, BASE_LEDGER, QUOTE_LEDGER)
        assertSingleTrade(ALICE_C4, BOB_C4, price = trade.price, qty = trade.quantity)
    }
}
