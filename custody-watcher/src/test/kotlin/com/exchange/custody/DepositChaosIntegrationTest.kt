package com.exchange.custody

import com.exchange.common.EngineCommand
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.MethodOrderer
import org.junit.jupiter.api.Order
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestMethodOrder
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.datasource.DriverManagerDataSource
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName
import org.testcontainers.utility.MountableFile
import org.web3j.abi.FunctionEncoder
import org.web3j.abi.TypeReference
import org.web3j.abi.datatypes.Address
import org.web3j.abi.datatypes.generated.Uint256
import org.web3j.protocol.Web3j
import org.web3j.protocol.core.methods.request.Transaction
import org.web3j.protocol.http.HttpService
import java.math.BigInteger
import java.time.Duration
import java.util.UUID

/**
 * Chaos integration test for M4 stablecoin deposit.
 *
 * Proves two things:
 *   Chaos D1 — Correct credit
 *     A Transfer event on-chain is detected by the watcher and a single
 *     ADJUST_BALANCE command is published to Kafka with the right uid,
 *     currency, amount and onChainRef.
 *
 *   Chaos D2 — No double-publish on watcher re-delivery (real web3j path)
 *     custody_sync.last_block is reset to the block BEFORE the deposit,
 *     forcing the watcher to re-scan via web3j and re-see the same Transfer
 *     event. UNIQUE(tx_hash, log_index) → ON CONFLICT DO NOTHING → no second
 *     INSERT → no second Kafka command.
 *
 * The second safety net (TigerBeetle SHA-256 transferId idempotency via
 * AdjustBalanceConsumer) is the settlement module's responsibility and is proven
 * there. Together: at-most-once Kafka delivery + at-most-once TB transfer.
 *
 * Containers: Anvil (EVM) + Postgres + Kafka.
 */
@Tag("integration")
@TestMethodOrder(MethodOrderer.OrderAnnotation::class)
class DepositChaosIntegrationTest {

    companion object {
        // Anvil deterministic pre-funded account #0 (no signing required on Anvil)
        private const val SENDER = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"
        // Anvil account #1 — used as the deposit address
        private const val DEPOSIT_ADDR = "0x70997970C51812dc3A010C7d01b50e0d17dc79C8"

        private const val DEPOSIT_UID = 5001L
        private const val CURRENCY    = 20
        private const val DEPOSIT_AMT = 500L
        private const val CMD_TOPIC   = "commands"

        private val mapper = jacksonObjectMapper()

        val kafka = KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.4.0"))

        @Suppress("UNCHECKED_CAST")
        val pg = PostgreSQLContainer("postgres:16-alpine") as PostgreSQLContainer<*>

        @Suppress("UNCHECKED_CAST")
        val anvil = GenericContainer<Nothing>("ghcr.io/foundry-rs/foundry:latest")
            .also { it.withCommand("anvil --host 0.0.0.0 --port 8545") }
            .also { it.withExposedPorts(8545) }
            .also { it.waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofSeconds(60))) }

        // Populated in @BeforeAll
        lateinit var jdbc: JdbcTemplate
        lateinit var web3j: Web3j
        lateinit var watcher: CustodyWatcher
        lateinit var tokenAddress: String

        @JvmStatic
        @BeforeAll
        fun startAll() {
            kafka.start(); pg.start(); anvil.start()

            // ── Postgres ──────────────────────────────────────────────────────
            jdbc = JdbcTemplate(DriverManagerDataSource(pg.jdbcUrl, pg.username, pg.password))
            createSchema()

            // ── Kafka ─────────────────────────────────────────────────────────
            AdminClient.create(
                mapOf(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to kafka.bootstrapServers)
            ).use { it.createTopics(listOf(NewTopic(CMD_TOPIC, 1, 1.toShort()))).all().get() }

            // ── Anvil ─────────────────────────────────────────────────────────
            web3j = Web3j.build(HttpService("http://${anvil.host}:${anvil.getMappedPort(8545)}"))
            tokenAddress = deployMockToken()

            jdbc.update(
                "INSERT INTO deposit_addresses (address, uid, currency) VALUES (?, ?, ?)",
                DEPOSIT_ADDR.lowercase(), DEPOSIT_UID, CURRENCY
            )

            // ── CustodyWatcher ────────────────────────────────────────────────
            val producer = KafkaProducer<String, String>(mapOf(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG      to kafka.bootstrapServers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG   to StringSerializer::class.java.name,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java.name,
                ProducerConfig.ACKS_CONFIG                   to "all"
            ))
            watcher = CustodyWatcher(
                web3j         = web3j,
                jdbc          = jdbc,
                producer      = producer,
                tokenAddress  = tokenAddress,
                commandsTopic = CMD_TOPIC,
                confirmations = 0L   // instant mining on Anvil — no confirmation wait needed
            )
        }

        @JvmStatic
        @AfterAll
        fun stopAll() {
            web3j.shutdown()
            kafka.stop(); pg.stop(); anvil.stop()
        }

        /**
         * Copies MockToken.sol into the Anvil container and deploys it via
         * `forge create` (solc bundled in the foundry image — no CI compiler needed).
         * Returns the deployed contract address.
         */
        private fun deployMockToken(): String {
            anvil.copyFileToContainer(
                MountableFile.forClasspathResource("contracts/MockToken.sol"),
                "/tmp/MockToken.sol"
            )
            val result = anvil.execInContainer(
                "forge", "create",
                "/tmp/MockToken.sol:MockToken",
                "--rpc-url", "http://127.0.0.1:8545",
                "--private-key", "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80",
                "--legacy"
            )
            check(result.exitCode == 0) {
                "forge create failed (exit ${result.exitCode}):\nstdout:${result.stdout}\nstderr:${result.stderr}"
            }
            return result.stdout.lines()
                .firstOrNull { it.trimStart().startsWith("Deployed to:") }
                ?.substringAfter("Deployed to:")
                ?.trim()
                ?: error("Could not parse deployed address from:\n${result.stdout}")
        }

        private fun createSchema() {
            jdbc.execute("""CREATE TABLE IF NOT EXISTS deposit_addresses (
                address VARCHAR(42) NOT NULL PRIMARY KEY, uid BIGINT NOT NULL, currency INT NOT NULL
            )""")
            jdbc.execute("""CREATE TABLE IF NOT EXISTS custody_events (
                id BIGSERIAL PRIMARY KEY,
                tx_hash   VARCHAR(66) NOT NULL, log_index INT NOT NULL,
                uid BIGINT NOT NULL, currency INT NOT NULL, amount BIGINT NOT NULL,
                state VARCHAR(20) NOT NULL DEFAULT 'PENDING',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE (tx_hash, log_index)
            )""")
            jdbc.execute("""CREATE TABLE IF NOT EXISTS custody_sync (
                singleton_id INT NOT NULL DEFAULT 1 PRIMARY KEY CHECK (singleton_id = 1),
                last_block BIGINT NOT NULL DEFAULT 0
            )""")
            jdbc.update("INSERT INTO custody_sync DEFAULT VALUES ON CONFLICT DO NOTHING")
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Chaos D1 — Correct credit: Transfer event on-chain → Kafka command
    // ─────────────────────────────────────────────────────────────────────────
    @Test
    @Order(1)
    fun `chaos D1 - Transfer event is detected and published as ADJUST_BALANCE command`() {
        // Encode transfer(address to, uint256 value) and send via eth_sendTransaction
        // (Anvil account #0 is pre-funded and unlocked — no signing needed)
        @Suppress("UNCHECKED_CAST")
        val function = org.web3j.abi.datatypes.Function(
            "transfer",
            listOf(Address(DEPOSIT_ADDR), Uint256(BigInteger.valueOf(DEPOSIT_AMT))),
            emptyList<TypeReference<*>>()
        )
        val tx = Transaction.createFunctionCallTransaction(
            SENDER, null, BigInteger.ZERO, BigInteger.valueOf(1_000_000L),
            tokenAddress, FunctionEncoder.encode(function)
        )
        val sendResult = web3j.ethSendTransaction(tx).send()
        check(!sendResult.hasError()) { "ethSendTransaction: ${sendResult.error?.message}" }

        watcher.poll()

        // ── Postgres: one EMITTED row ─────────────────────────────────────────
        val events = jdbc.queryForList("SELECT * FROM custody_events")
        assertThat(events).hasSize(1)
        assertThat(events[0]["uid"]).isEqualTo(DEPOSIT_UID)
        assertThat(events[0]["currency"]).isEqualTo(CURRENCY)
        assertThat(events[0]["amount"]).isEqualTo(DEPOSIT_AMT)
        assertThat(events[0]["state"]).isEqualTo("EMITTED")

        // ── Kafka: exactly one ADJUST_BALANCE command ─────────────────────────
        newConsumer().use { consumer ->
            consumer.subscribe(listOf(CMD_TOPIC))
            val records = consumer.poll(Duration.ofSeconds(10))
            assertThat(records.count()).isEqualTo(1)
            val cmd = mapper.readValue<EngineCommand>(records.first().value())
            assertThat(cmd.type).isEqualTo(EngineCommand.ADJUST_BALANCE)
            assertThat(cmd.uid).isEqualTo(DEPOSIT_UID)
            assertThat(cmd.currency).isEqualTo(CURRENCY)
            assertThat(cmd.amount).isEqualTo(DEPOSIT_AMT)
            // onChainRef = "$txHash:$logIndex" — AdjustBalanceConsumer hashes this for TB transferId
            assertThat(cmd.onChainRef).matches(".+:\\d+")
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Chaos D2 — No double-publish on watcher restart from older block
    //
    // Resets custody_sync.last_block to the block BEFORE the deposit so the
    // watcher re-reads the Transfer event via web3j. UNIQUE(tx_hash, log_index)
    // stops the re-insert → no second Kafka command.
    // ─────────────────────────────────────────────────────────────────────────
    @Test
    @Order(2)
    fun `chaos D2 - watcher re-scan from older block does not double-publish`() {
        val txHash = jdbc.queryForObject("SELECT tx_hash FROM custody_events LIMIT 1", String::class.java)!!
        val depositBlock = web3j.ethGetTransactionReceipt(txHash).send()
            .transactionReceipt.get().blockNumber.toLong()

        // Simulate watcher restart from a block before the deposit
        jdbc.update("UPDATE custody_sync SET last_block = ?", depositBlock - 1)

        val countBefore = totalKafkaMessages()

        // Re-scan: web3j re-fetches the same Transfer event
        watcher.poll()

        // UNIQUE constraint blocked the re-insert → still 1 row in custody_events
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM custody_events", Long::class.java))
            .`as`("custody_events row count after re-poll").isEqualTo(1L)

        // No new Kafka message
        assertThat(totalKafkaMessages())
            .`as`("Kafka command count after re-poll — must not increase").isEqualTo(countBefore)
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private fun newConsumer(): KafkaConsumer<String, String> = KafkaConsumer(mapOf(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG        to kafka.bootstrapServers,
        ConsumerConfig.GROUP_ID_CONFIG                 to "chaos-d-${UUID.randomUUID()}",
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG        to "earliest",
        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG       to "false",
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG   to StringDeserializer::class.java.name,
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java.name,
    ))

    /** Drains all records from the topic (from offset 0, unique group) within 3 s. */
    private fun totalKafkaMessages(): Int = newConsumer().use { consumer ->
        consumer.subscribe(listOf(CMD_TOPIC))
        val deadline = System.currentTimeMillis() + 3_000
        var count = 0
        while (System.currentTimeMillis() < deadline) {
            count += consumer.poll(Duration.ofMillis(500)).count()
        }
        count
    }
}
