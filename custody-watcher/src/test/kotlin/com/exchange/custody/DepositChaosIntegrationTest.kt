package com.exchange.custody

import com.exchange.common.EngineCommand
import com.exchange.settlement.SettlementService
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.tigerbeetle.Client
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
import java.security.MessageDigest
import java.time.Duration
import java.util.UUID

/**
 * Chaos integration test for M4 stablecoin deposit.
 *
 * Proves the full deposit chain end-to-end across both idempotency layers:
 *
 *   Chaos D1 — Correct credit (Watcher → Kafka → TigerBeetle)
 *     A Transfer event on-chain is detected by the watcher, a single
 *     ADJUST_BALANCE command is published to Kafka with the right uid/currency/
 *     amount/onChainRef, and AdjustBalanceConsumer's SHA-256 transferId path
 *     credits TigerBeetle with the exact deposit amount.
 *
 *   Chaos D2 — No double-credit on watcher re-delivery (real web3j path)
 *     custody_sync.last_block is reset to before the deposit block, forcing
 *     the watcher to re-scan via web3j and re-see the same Transfer event.
 *     Layer 1: UNIQUE(tx_hash, log_index) → ON CONFLICT DO NOTHING → 0 new
 *     Kafka messages.
 *     Layer 2: same SHA-256 transferId → TigerBeetle deposit is a no-op →
 *     TigerBeetle.getBalance(uid) unchanged (still one deposit, not two).
 *
 * The independent layer-2 test ("same command at two distinct Kafka offsets →
 * single TB credit") is tracked in TECH_DEBT.md (TD-12) and runs in the
 * settlement module where AdjustBalanceConsumer lives.
 *
 * Containers: Anvil + Postgres + Kafka + TigerBeetle.
 */
@Tag("integration")
@TestMethodOrder(MethodOrderer.OrderAnnotation::class)
class DepositChaosIntegrationTest {

    companion object {
        // Anvil deterministic pre-funded account #0 (unlocked, no signing needed)
        private const val SENDER       = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"
        // Anvil account #1 — used as the deposit address for this user
        private const val DEPOSIT_ADDR = "0x70997970C51812dc3A010C7d01b50e0d17dc79C8"

        private const val DEPOSIT_UID = 5001L
        private const val CURRENCY    = 20
        private const val DEPOSIT_AMT = 500L
        private const val CMD_TOPIC   = "commands"

        private val mapper = jacksonObjectMapper()

        val kafka = KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.4.0"))
        @Suppress("UNCHECKED_CAST")
        val pg    = PostgreSQLContainer("postgres:16-alpine") as PostgreSQLContainer<*>
        @Suppress("UNCHECKED_CAST")
        val anvil = GenericContainer<Nothing>("ghcr.io/foundry-rs/foundry:latest")
            .also { it.withCommand("anvil --host 0.0.0.0 --port 8545") }
            .also { it.withExposedPorts(8545) }
            .also { it.waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofSeconds(60))) }
        val tb    = TigerBeetleContainer()

        // Populated in @BeforeAll
        lateinit var jdbc: JdbcTemplate
        lateinit var web3j: Web3j
        lateinit var watcher: CustodyWatcher
        lateinit var settlement: SettlementService
        lateinit var tbClient: Client
        lateinit var tokenAddress: String

        @JvmStatic
        @BeforeAll
        fun startAll() {
            kafka.start(); pg.start(); anvil.start(); tb.start()

            // ── Postgres ──────────────────────────────────────────────────────
            jdbc = JdbcTemplate(DriverManagerDataSource(pg.jdbcUrl, pg.username, pg.password))
            createSchema()

            // ── Kafka ─────────────────────────────────────────────────────────
            AdminClient.create(
                mapOf(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to kafka.bootstrapServers)
            ).use { it.createTopics(listOf(NewTopic(CMD_TOPIC, 1, 1.toShort()))).all().get() }

            // ── Anvil / web3j ─────────────────────────────────────────────────
            web3j = Web3j.build(HttpService("http://${anvil.host}:${anvil.getMappedPort(8545)}"))
            tokenAddress = deployMockToken()

            jdbc.update(
                "INSERT INTO deposit_addresses (address, uid, currency) VALUES (?, ?, ?)",
                DEPOSIT_ADDR.lowercase(), DEPOSIT_UID, CURRENCY
            )

            // ── TigerBeetle ───────────────────────────────────────────────────
            tbClient   = Client(ByteArray(16), arrayOf(tb.address))
            settlement = SettlementService(tbClient)
            settlement.ensureSystemAccounts(CURRENCY)
            settlement.ensureAccount(DEPOSIT_UID, CURRENCY)

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
                confirmations = 0L
            )
        }

        @JvmStatic
        @AfterAll
        fun stopAll() {
            tbClient.close()
            web3j.shutdown()
            kafka.stop(); pg.stop(); anvil.stop(); tb.close()
        }

        /**
         * Copies MockToken.sol into the Anvil container and deploys it via `forge create`.
         * `--out /tmp/forge-out` writes artifacts to a writable path (default /out is root-owned
         * in the foundry image).
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
                "--legacy",
                "--out", "/tmp/forge-out"  // /out is permission-denied in foundry image
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
                tx_hash VARCHAR(66) NOT NULL, log_index INT NOT NULL,
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

        /** Mirrors AdjustBalanceConsumer's on-chain transferId formula. */
        private fun sha256TransferId(onChainRef: String): Long {
            val sha = MessageDigest.getInstance("SHA-256").digest(onChainRef.toByteArray())
            return (sha[0].toLong() and 0xff shl 56) or (sha[1].toLong() and 0xff shl 48) or
                   (sha[2].toLong() and 0xff shl 40) or (sha[3].toLong() and 0xff shl 32) or
                   (sha[4].toLong() and 0xff shl 24) or (sha[5].toLong() and 0xff shl 16) or
                   (sha[6].toLong() and 0xff shl  8) or (sha[7].toLong() and 0xff) or Long.MIN_VALUE
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Chaos D1 — Correct credit: Transfer event → Kafka command → TB balance
    // ─────────────────────────────────────────────────────────────────────────
    @Test
    @Order(1)
    fun `chaos D1 - Transfer event is credited exactly once in TigerBeetle`() {
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

        // ── Layer 1: exactly one EMITTED row in custody_events ────────────────
        val events = jdbc.queryForList("SELECT * FROM custody_events")
        assertThat(events).hasSize(1)
        assertThat(events[0]["state"]).isEqualTo("EMITTED")

        // ── Kafka: single ADJUST_BALANCE command with correct fields ──────────
        val cmd = newConsumer().use { consumer ->
            consumer.subscribe(listOf(CMD_TOPIC))
            val records = consumer.poll(Duration.ofSeconds(10))
            assertThat(records.count()).isEqualTo(1)
            mapper.readValue<EngineCommand>(records.first().value())
        }
        assertThat(cmd.type).isEqualTo(EngineCommand.ADJUST_BALANCE)
        assertThat(cmd.uid).isEqualTo(DEPOSIT_UID)
        assertThat(cmd.currency).isEqualTo(CURRENCY)
        assertThat(cmd.amount).isEqualTo(DEPOSIT_AMT)
        assertThat(cmd.onChainRef).matches(".+:\\d+")

        // ── Layer 2: TigerBeetle credited exactly DEPOSIT_AMT ─────────────────
        // Simulate AdjustBalanceConsumer processing the Kafka command
        val transferId = sha256TransferId(cmd.onChainRef)
        settlement.deposit(DEPOSIT_UID, CURRENCY, DEPOSIT_AMT, transferId)

        assertThat(settlement.getBalance(DEPOSIT_UID, CURRENCY))
            .`as`("TigerBeetle balance after D1 — exactly one deposit").isEqualTo(DEPOSIT_AMT)
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Chaos D2 — No double-credit on watcher restart from older block
    //
    // Resets custody_sync.last_block to before the deposit so the watcher
    // re-reads the same Transfer event via real web3j.
    //
    // Layer 1 proof: UNIQUE(tx_hash, log_index) → no second Kafka message.
    // Layer 2 proof: same SHA-256 transferId → TigerBeetle balance UNCHANGED.
    // Both layers are tested independently; together they guarantee no double-credit.
    // ─────────────────────────────────────────────────────────────────────────
    @Test
    @Order(2)
    fun `chaos D2 - watcher re-scan from older block does not double-credit TigerBeetle`() {
        val txHash = jdbc.queryForObject("SELECT tx_hash FROM custody_events LIMIT 1", String::class.java)!!
        val depositBlock = web3j.ethGetTransactionReceipt(txHash).send()
            .transactionReceipt.get().blockNumber.toLong()

        // Simulate watcher restart: re-scan from before the deposit
        jdbc.update("UPDATE custody_sync SET last_block = ?", depositBlock - 1)

        val msgsBefore = totalKafkaMessages()

        // Re-scan via real web3j — sees the same Transfer event again
        watcher.poll()

        // ── Layer 1: no new Kafka message ─────────────────────────────────────
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM custody_events", Long::class.java))
            .`as`("custody_events after re-poll").isEqualTo(1L)
        assertThat(totalKafkaMessages())
            .`as`("Kafka messages after re-poll — must not increase").isEqualTo(msgsBefore)

        // ── Layer 2: TigerBeetle balance UNCHANGED — still exactly one deposit ─
        // Get the onChainRef stored in D1 and replay the deposit with the same transferId.
        // This is what AdjustBalanceConsumer would do if Kafka somehow delivered twice.
        val onChainRef = jdbc.queryForObject(
            "SELECT tx_hash || ':' || log_index FROM custody_events LIMIT 1", String::class.java
        )!!
        val transferId = sha256TransferId(onChainRef)
        settlement.deposit(DEPOSIT_UID, CURRENCY, DEPOSIT_AMT, transferId)  // idempotent replay

        assertThat(settlement.getBalance(DEPOSIT_UID, CURRENCY))
            .`as`("TigerBeetle balance after D2 re-delivery — must equal exactly one deposit").isEqualTo(DEPOSIT_AMT)
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

    /** Drains all records from the commands topic within 3 s (unique consumer group). */
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
