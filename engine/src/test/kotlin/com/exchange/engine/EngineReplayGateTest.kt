package com.exchange.engine

import com.exchange.common.EngineCommand
import com.exchange.common.OrderBookEvent
import com.exchange.common.OrderSide
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.kafka.listener.ConsumerSeekAware
import org.springframework.kafka.test.EmbeddedKafkaBroker
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.kafka.test.utils.KafkaTestUtils
import org.springframework.test.annotation.DirtiesContext
import java.time.Duration
import java.util.UUID

// ── Unit tests: gate behaviour in MatchingEngineService ─────────────────────

class MatchingEngineReplayGateTest {

    private val snapshots = mutableListOf<OrderBookEvent>()
    private lateinit var engine: MatchingEngineService
    private var nextOffset = 0L

    private fun cmd(c: EngineCommand) = engine.processCommand(nextOffset++, c)

    @BeforeEach fun setUp() {
        snapshots.clear()
        nextOffset = 0L
        engine = MatchingEngineService(orderbookPublisher = { snapshots.add(it) })
        engine.init()
        cmd(EngineCommand.addSymbol(1, 10, 11))
        cmd(EngineCommand.addUser(101L))
        cmd(EngineCommand.addUser(102L))
        cmd(EngineCommand.adjustBalance(101L, 11, 1_000_000L))
        cmd(EngineCommand.adjustBalance(102L, 10, 1_000_000L))
    }

    @AfterEach fun tearDown() { engine.shutdown() }

    @Test
    fun `no snapshot before signalReplayComplete`() {
        // PLACE_ORDER during replay — replayComplete=false
        cmd(EngineCommand.placeOrder(1, 102L, 0L, 100L, 5L, OrderSide.ASK))
        assertThat(snapshots).isEmpty()
        assertThat(engine.isReplayComplete()).isFalse()
    }

    @Test
    fun `snapshot published after signalReplayComplete`() {
        // Simulate: consumer processed all replay records and signals
        engine.signalReplayComplete()
        assertThat(engine.isReplayComplete()).isTrue()

        // New live order — snapshot must be published
        cmd(EngineCommand.placeOrder(1, 102L, 0L, 100L, 5L, OrderSide.ASK))
        assertThat(snapshots).hasSize(1)
        assertThat(snapshots[0].symbolId).isEqualTo(1)
    }

    @Test
    fun `no snapshot during replay, then snapshot after signal`() {
        // Place order during replay — no snapshot
        cmd(EngineCommand.placeOrder(1, 102L, 0L, 100L, 5L, OrderSide.ASK))
        assertThat(snapshots).isEmpty()

        engine.signalReplayComplete()

        // Cancel live — snapshot fires
        val cancelOffset = nextOffset - 1  // offset of the PLACE_ORDER above
        cmd(EngineCommand.cancelOrder(1, 102L, cancelOffset))
        assertThat(snapshots).hasSize(1)
    }
}

// ── Integration test: EngineCommandConsumer replay-complete detection ────────

@EmbeddedKafka(
    partitions = 1,
    topics = ["commands"],
    bootstrapServersProperty = "spring.kafka.bootstrap-servers"
)
@DirtiesContext
class EngineCommandConsumerReplayTest(private val embeddedKafka: EmbeddedKafkaBroker) {

    private val mapper = ObjectMapper().registerKotlinModule()
    private lateinit var engine: MatchingEngineService
    private val snapshots = mutableListOf<OrderBookEvent>()

    @BeforeEach fun setUp() {
        snapshots.clear()
        engine = MatchingEngineService(orderbookPublisher = { snapshots.add(it) })
        engine.init()
        engine.processCommand(0L, EngineCommand.addSymbol(1, 10, 11))
        engine.processCommand(1L, EngineCommand.addUser(101L))
        engine.processCommand(2L, EngineCommand.addUser(102L))
        engine.processCommand(3L, EngineCommand.adjustBalance(101L, 11, 1_000_000L))
        engine.processCommand(4L, EngineCommand.adjustBalance(102L, 10, 1_000_000L))
    }

    @AfterEach fun tearDown() { engine.shutdown() }

    /**
     * Proves that signalReplayComplete() fires correctly even when
     * all records are already present at assignment time — without needing
     * any new records to arrive after onPartitionsAssigned.
     *
     * Also proves no snapshot is emitted during replay (ADD_USER commands),
     * and that the first PLACE_ORDER after replay triggers a snapshot.
     */
    @Test
    fun `signal fires after all pre-existing records consumed, no snapshot during replay`() {
        val brokers = embeddedKafka.brokersAsString

        // Pre-populate the commands topic with 3 records before the consumer starts
        buildProducer(brokers).use { producer ->
            listOf(
                EngineCommand.addUser(201L),
                EngineCommand.addUser(202L),
                EngineCommand.adjustBalance(201L, 11, 500_000L)
            ).forEach { cmd ->
                producer.send(ProducerRecord("commands", "1", mapper.writeValueAsString(cmd))).get()
            }
        }
        // topic now has endOffset=3, startOffset=0 — 3 records to replay

        val consumer = EngineCommandConsumer(engine, brokers)

        // Simulate what Spring Kafka does on partition assignment
        val tp = TopicPartition("commands", 0)
        consumer.onPartitionsAssigned(mutableMapOf(tp to 0L), NoOpSeekCallback)
        assertThat(engine.isReplayComplete()).isFalse()  // not yet — records exist

        // Pull all 3 records from Kafka and drive them through the consumer
        val records = drainRecords(brokers, "commands", count = 3)
        assertThat(records).hasSize(3)

        records.forEach { (offset, json) ->
            val cmd = mapper.readValue(json, EngineCommand::class.java)
            val cr = org.apache.kafka.clients.consumer.ConsumerRecord("commands", 0, offset, "1", cmd)
            consumer.onCommand(cr)
        }

        // After last record (offset=2 = endOffset-1=3-1), signal must have fired
        assertThat(engine.isReplayComplete()).isTrue()

        // No snapshot during replay (ADD_USER / ADJUST_BALANCE commands)
        assertThat(snapshots).isEmpty()

        // Live PLACE_ORDER after replay — snapshot must now be published
        val placeOffset = 5L  // beyond setup offsets
        engine.processCommand(placeOffset, EngineCommand.placeOrder(1, 102L, 0L, 100L, 5L, OrderSide.ASK))
        // Note: engine.processCommand is called directly (bypass consumer) since we need
        // a real order in exchange-core; signalReplayComplete was already called above.
        assertThat(snapshots).hasSize(1)
        assertThat(snapshots[0].symbolId).isEqualTo(1)
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    private fun buildProducer(brokers: String): KafkaProducer<String, String> {
        val props = mapOf(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to brokers,
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java.name,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java.name,
            ProducerConfig.ACKS_CONFIG to "all"
        )
        return KafkaProducer(props)
    }

    private fun drainRecords(
        @Suppress("UNUSED_PARAMETER") brokers: String,
        topic: String,
        count: Int
    ): List<Pair<Long, String>> {
        val props = KafkaTestUtils.consumerProps(
            "drain-${UUID.randomUUID()}", "false", embeddedKafka
        )
        props[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java.name
        props[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java.name
        props[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"

        KafkaConsumer<String, String>(props).use { c ->
            c.subscribe(listOf(topic))
            val result = mutableListOf<Pair<Long, String>>()
            val deadline = System.currentTimeMillis() + 10_000
            while (result.size < count && System.currentTimeMillis() < deadline) {
                c.poll(Duration.ofMillis(200)).forEach { result.add(it.offset() to it.value()) }
            }
            assertThat(result).hasSize(count)
            return result
        }
    }
}

// ── No-op helpers ────────────────────────────────────────────────────────────

private object NoOpSeekCallback : ConsumerSeekAware.ConsumerSeekCallback {
    override fun seek(topic: String, partition: Int, offset: Long) {}
    override fun seek(topic: String, partition: Int, offsetComputeFunction: java.util.function.Function<Long, Long>) {}
    override fun seekToBeginning(topic: String, partition: Int) {}
    override fun seekToEnd(topic: String, partition: Int) {}
    override fun seekRelative(topic: String, partition: Int, offset: Long, toCurrent: Boolean) {}
    override fun seekToTimestamp(topic: String, partition: Int, timestamp: Long) {}
    override fun seekToTimestamp(partitions: Collection<TopicPartition>, timestamp: Long) {}
}
