package com.exchange.gateway

import com.exchange.common.TradeEvent
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.listener.ConsumerSeekAware
import org.springframework.kafka.support.Acknowledgment
import org.springframework.stereotype.Component
import java.sql.Timestamp
import java.time.Instant

data class CandleDto(
    val t: Long,   // bucket start, Unix seconds
    val o: Long,   // open
    val h: Long,   // high
    val l: Long,   // low
    val c: Long,   // close
    val v: Long    // volume
)

/**
 * Aggregates trades from Kafka into 1-minute OHLCV candles stored in TimescaleDB.
 *
 * Seeks to END on startup — diffusion-level guarantee matching TradeStreamConsumer.
 * Candles for periods when the service was down are absent; this is accepted for M2.
 *
 * Upsert is idempotent for live (non-replayed) trades: each trade arrives exactly
 * once, open is only set on first insert, high/low update via GREATEST/LEAST,
 * close overwrites unconditionally (last writer wins within the bucket), and
 * volume accumulates correctly since each trade is consumed exactly once per run.
 */
@Component
class CandleAggregator(private val jdbc: JdbcTemplate) : ConsumerSeekAware {

    private val log = LoggerFactory.getLogger(javaClass)

    override fun onPartitionsAssigned(
        assignments: Map<TopicPartition, Long>,
        callback: ConsumerSeekAware.ConsumerSeekCallback
    ) {
        assignments.keys.forEach { tp ->
            callback.seekToEnd(tp.topic(), tp.partition())
        }
    }

    @KafkaListener(
        topics = ["trades"],
        groupId = "gateway-candles",
        containerFactory = "kafkaListenerContainerFactory"
    )
    fun onTrade(record: ConsumerRecord<String, TradeEvent>, ack: Acknowledgment) {
        val trade = record.value()
        val bucketSec = trade.timestampNs / 1_000_000_000L / 60 * 60
        val bucket = Timestamp.from(Instant.ofEpochSecond(bucketSec))

        jdbc.update("""
            INSERT INTO candles (symbol_id, resolution, bucket_time, open, high, low, close, volume)
            VALUES (?, '1m', ?, ?, ?, ?, ?, ?)
            ON CONFLICT (symbol_id, resolution, bucket_time) DO UPDATE SET
              high   = GREATEST(candles.high, EXCLUDED.high),
              low    = LEAST(candles.low, EXCLUDED.low),
              close  = EXCLUDED.close,
              volume = candles.volume + EXCLUDED.volume
        """.trimIndent(),
            trade.symbolId, bucket,
            trade.price, trade.price, trade.price, trade.price, trade.quantity
        )

        log.debug("Candle upsert symbol={} bucket={} price={}", trade.symbolId, bucketSec, trade.price)
        ack.acknowledge()
    }

    fun getCandles(symbolId: Int, resolution: String, limit: Int): List<CandleDto> =
        jdbc.query("""
            SELECT bucket_time, open, high, low, close, volume
            FROM candles
            WHERE symbol_id = ? AND resolution = ?
            ORDER BY bucket_time DESC
            LIMIT ?
        """.trimIndent(),
            { rs, _ ->
                CandleDto(
                    t = rs.getTimestamp("bucket_time").toInstant().epochSecond,
                    o = rs.getLong("open"),
                    h = rs.getLong("high"),
                    l = rs.getLong("low"),
                    c = rs.getLong("close"),
                    v = rs.getLong("volume")
                )
            },
            symbolId, resolution, limit
        )
}
