package com.exchange.custody

import com.exchange.common.EngineCommand
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.slf4j.LoggerFactory
import org.springframework.jdbc.core.JdbcTemplate
import org.web3j.protocol.Web3j
import org.web3j.protocol.core.DefaultBlockParameter
import org.web3j.protocol.core.methods.request.EthFilter
import org.web3j.protocol.core.methods.response.Log
import java.math.BigInteger

/**
 * Polls the stablecoin token contract for ERC-20 Transfer events directed to
 * known deposit addresses, then publishes ADJUST_BALANCE commands to Kafka.
 *
 * Idempotency — two layers:
 *   1. Postgres UNIQUE(tx_hash, log_index): INSERT ON CONFLICT DO NOTHING prevents
 *      a re-delivered on-chain event (watcher restart, block reorg) from publishing
 *      a second Kafka command.
 *   2. TigerBeetle transferId = SHA-256(onChainRef): even if Kafka re-delivers the
 *      same command, AdjustBalanceConsumer derives the same transferId → TB no-op.
 *
 * Not a Spring @Component — wired via @Bean in CustodyWatcherConfig so that the
 * @Scheduled wrapper controls the poll interval without double-registering.
 */
class CustodyWatcher(
    private val web3j: Web3j,
    private val jdbc: JdbcTemplate,
    private val producer: KafkaProducer<String, String>,
    private val tokenAddress: String,
    private val commandsTopic: String = "commands",
    private val confirmations: Long = 6L
) {

    companion object {
        /** keccak256("Transfer(address,address,uint256)") */
        const val TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
    }

    private val log = LoggerFactory.getLogger(javaClass)
    private val mapper = jacksonObjectMapper()

    /**
     * Scans blocks (lastBlock+1 .. latestBlock-confirmations) for Transfer events
     * to known deposit addresses and publishes commands for any new ones.
     */
    fun poll() {
        val lastBlock = jdbc.queryForObject("SELECT last_block FROM custody_sync", Long::class.java)
            ?: return
        val latestBlock = web3j.ethBlockNumber().send().blockNumber.toLong() - confirmations
        if (latestBlock <= lastBlock) return

        val filter = EthFilter(
            DefaultBlockParameter.valueOf(BigInteger.valueOf(lastBlock + 1)),
            DefaultBlockParameter.valueOf(BigInteger.valueOf(latestBlock)),
            tokenAddress
        )
        filter.addSingleTopic(TRANSFER_TOPIC)

        val logs = web3j.ethGetLogs(filter).send().logs
        log.debug("poll blocks [{},{}]: {} Transfer logs", lastBlock + 1, latestBlock, logs.size)

        for (result in logs) {
            processTransferLog(result.get() as Log)
        }

        jdbc.update("UPDATE custody_sync SET last_block = ?", latestBlock)
    }

    /**
     * Re-publishes any PENDING events from a previous crashed run.
     * Call once at startup, before starting the scheduled poll.
     */
    fun recoverPending() {
        val rows = jdbc.queryForList(
            "SELECT tx_hash, log_index, uid, currency, amount FROM custody_events WHERE state = 'PENDING'"
        )
        for (row in rows) {
            val txHash   = row["tx_hash"]   as String
            val logIndex = row["log_index"] as Int
            val uid      = row["uid"]       as Long
            val currency = row["currency"]  as Int
            val amount   = row["amount"]    as Long
            publishCommand(txHash, logIndex, uid, currency, amount)
            markEmitted(txHash, logIndex)
        }
        if (rows.isNotEmpty()) log.info("Recovered {} PENDING custody events", rows.size)
    }

    private fun processTransferLog(ethLog: Log) {
        val txHash   = ethLog.transactionHash
        val logIndex = ethLog.logIndex.toInt()
        // topic[2] = "to" address, 32-byte padded: last 20 bytes (40 hex chars) = address
        val toAddress = "0x" + ethLog.topics[2].removePrefix("0x").takeLast(40)
        val amount    = BigInteger(ethLog.data.removePrefix("0x"), 16).toLong()

        val rows = jdbc.queryForList(
            "SELECT uid, currency FROM deposit_addresses WHERE LOWER(address) = LOWER(?)",
            toAddress
        )
        if (rows.isEmpty()) return  // not one of our deposit addresses

        val uid      = rows[0]["uid"]      as Long
        val currency = rows[0]["currency"] as Int

        val inserted = jdbc.update(
            """INSERT INTO custody_events (tx_hash, log_index, uid, currency, amount)
               VALUES (?, ?, ?, ?, ?)
               ON CONFLICT (tx_hash, log_index) DO NOTHING""",
            txHash, logIndex, uid, currency, amount
        )
        if (inserted == 0) {
            log.info("Skipping already-seen deposit tx={} logIndex={}", txHash, logIndex)
            return
        }

        publishCommand(txHash, logIndex, uid, currency, amount)
        markEmitted(txHash, logIndex)
    }

    private fun publishCommand(txHash: String, logIndex: Int, uid: Long, currency: Int, amount: Long) {
        val cmd = EngineCommand(
            type       = EngineCommand.ADJUST_BALANCE,
            uid        = uid,
            currency   = currency,
            amount     = amount,
            onChainRef = "$txHash:$logIndex"
        )
        val json = mapper.writeValueAsString(cmd)
        producer.send(ProducerRecord(commandsTopic, "$txHash:$logIndex", json)).get()
        log.info("Published deposit cmd uid={} currency={} amount={} ref={}:{}", uid, currency, amount, txHash, logIndex)
    }

    private fun markEmitted(txHash: String, logIndex: Int) {
        jdbc.update(
            "UPDATE custody_events SET state = 'EMITTED' WHERE tx_hash = ? AND log_index = ?",
            txHash, logIndex
        )
    }
}
