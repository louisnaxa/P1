package com.exchange.gateway

import com.exchange.common.EngineCommand
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.whenever
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.mock.mockito.MockBean
import org.springframework.boot.test.web.client.TestRestTemplate
import org.springframework.http.HttpEntity
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpMethod
import org.springframework.http.HttpStatus
import org.springframework.kafka.test.EmbeddedKafkaBroker
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.security.oauth2.jwt.Jwt
import org.springframework.security.oauth2.jwt.JwtDecoder
import org.springframework.test.annotation.DirtiesContext
import java.time.Duration
import java.util.*

/**
 * HTTP → Kafka layer: proves that
 *   1. POST /orders writes a PLACE_ORDER with orderId=0 in the payload, and the offset
 *      returned in the 202 body matches the Kafka record's actual offset.
 *   2. DELETE /orders/{orderId} writes a CANCEL_ORDER whose cmd.orderId equals the
 *      offset returned by POST — completing the identity chain.
 *
 * Passes through the real security filter chain using a mocked JwtDecoder — the chain
 * is exercised (not bypassed), uid is resolved from the token subject via the mocked
 * UserService, and the test focuses on the HTTP→Kafka identity guarantee.
 *
 * Engine-level proof (offset→orderId→cancel) is in PlaceCancelLifecycleTest (engine module).
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(
    partitions = 1,
    topics = ["commands"],
    bootstrapServersProperty = "spring.kafka.bootstrap-servers"
)
@DirtiesContext
class OrderLifecycleTest {

    @Autowired
    private lateinit var restTemplate: TestRestTemplate

    @Autowired
    private lateinit var embeddedKafka: EmbeddedKafkaBroker

    @MockBean
    private lateinit var userService: UserService

    @MockBean
    private lateinit var jwtDecoder: JwtDecoder

    // Prevent these listeners from subscribing to topics not created by @EmbeddedKafka
    @MockBean
    private lateinit var orderBookConsumer: OrderBookConsumer

    @MockBean
    private lateinit var tradeStreamConsumer: TradeStreamConsumer

    @MockBean
    private lateinit var candleAggregator: CandleAggregator

    private val mapper = ObjectMapper()

    // Stable fake JWT used in every request — subject is irrelevant, what matters is
    // that the real BearerTokenAuthenticationFilter authenticates successfully and
    // authentication.name ("test-sub") is resolved to uid=42L by the mocked UserService.
    private val fakeJwt: Jwt = Jwt.withTokenValue("test-token")
        .header("alg", "RS256")
        .subject("test-sub")
        .build()

    private val authHeaders = HttpHeaders().apply {
        set("Authorization", "Bearer test-token")
    }

    @BeforeEach
    fun setUp() {
        whenever(jwtDecoder.decode(any())).thenReturn(fakeJwt)
        whenever(userService.resolveUid(any())).thenReturn(42L)
    }

    @Test
    fun `place then cancel - CANCEL_ORDER carries the exact offset returned by POST`() {
        // ── Place a BID order via HTTP ──────────────────────────────────────────
        val placeResp = restTemplate.exchange(
            "/orders",
            HttpMethod.POST,
            HttpEntity(
                PlaceOrderRequest(symbolId = 1, side = com.exchange.common.OrderSide.BID, price = 100L, quantity = 5L),
                authHeaders
            ),
            OrderResponse::class.java
        )
        assertThat(placeResp.statusCode).isEqualTo(HttpStatus.ACCEPTED)
        val orderId = placeResp.body!!.orderId

        // ── Cancel using the orderId returned by POST ───────────────────────────
        restTemplate.exchange(
            "/orders/$orderId?symbolId=1",
            HttpMethod.DELETE,
            HttpEntity<Void>(null, authHeaders),
            Void::class.java
        )

        // ── Read both commands from the topic and verify the identity chain ─────
        kafkaConsumer().use { consumer ->
            consumer.subscribe(listOf("commands"))
            val records = pollExactly(consumer, expected = 2)

            val placeRecord = records[0]
            val placeJson = mapper.readTree(placeRecord.value())
            assertThat(placeJson["type"].asText()).isEqualTo(EngineCommand.PLACE_ORDER)
            // Gateway writes no orderId — defaults to 0 in the command payload.
            // The engine will use record.offset() instead.
            assertThat(placeJson["orderId"].asLong()).isEqualTo(0L)
            // The offset of the Kafka record IS the orderId returned to the client.
            assertThat(placeRecord.offset()).isEqualTo(orderId)

            val cancelRecord = records[1]
            val cancelJson = mapper.readTree(cancelRecord.value())
            assertThat(cancelJson["type"].asText()).isEqualTo(EngineCommand.CANCEL_ORDER)
            // Cancel carries the offset (= the orderId from the 202).
            assertThat(cancelJson["orderId"].asLong()).isEqualTo(orderId)
        }
    }

    // ── Helpers ─────────────────────────────────────────────────────────────────

    private fun kafkaConsumer(): KafkaConsumer<String, String> {
        val props = Properties().apply {
            put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.brokersAsString)
            put(ConsumerConfig.GROUP_ID_CONFIG, "test-${UUID.randomUUID()}")
            put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
            put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
        }
        return KafkaConsumer(props)
    }

    private fun pollExactly(
        consumer: KafkaConsumer<String, String>,
        expected: Int,
        timeoutMs: Long = 10_000
    ): List<ConsumerRecord<String, String>> {
        val records = mutableListOf<ConsumerRecord<String, String>>()
        val deadline = System.currentTimeMillis() + timeoutMs
        while (records.size < expected && System.currentTimeMillis() < deadline) {
            consumer.poll(Duration.ofMillis(200)).forEach { records.add(it) }
        }
        assertThat(records).hasSize(expected)
        return records
    }
}
