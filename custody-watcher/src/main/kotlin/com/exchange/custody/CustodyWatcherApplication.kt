package com.exchange.custody

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import jakarta.annotation.PostConstruct
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.scheduling.annotation.EnableScheduling
import org.springframework.scheduling.annotation.Scheduled
import org.web3j.protocol.Web3j
import org.web3j.protocol.http.HttpService

@SpringBootApplication
@EnableScheduling
class CustodyWatcherApplication

fun main(args: Array<String>) {
    runApplication<CustodyWatcherApplication>(*args)
}

@Configuration
class CustodyWatcherConfig(
    @Value("\${custody.rpc-url}") private val rpcUrl: String,
    @Value("\${custody.token-address}") private val tokenAddress: String,
    @Value("\${custody.commands-topic:commands}") private val commandsTopic: String,
    @Value("\${custody.confirmations:6}") private val confirmations: Long,
    @Value("\${spring.kafka.bootstrap-servers}") private val bootstrapServers: String,
    private val jdbc: JdbcTemplate
) {

    @Bean
    fun web3j(): Web3j = Web3j.build(HttpService(rpcUrl))

    @Bean
    fun custodyCommandProducer(): KafkaProducer<String, String> = KafkaProducer(
        mapOf(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG      to bootstrapServers,
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG   to StringSerializer::class.java.name,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java.name,
            ProducerConfig.ACKS_CONFIG                   to "all",
            ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG     to "true"
        )
    )

    @Bean
    fun custodyWatcher(web3j: Web3j, producer: KafkaProducer<String, String>): CustodyWatcher =
        CustodyWatcher(web3j, jdbc, producer, tokenAddress, commandsTopic, confirmations)
}

@Configuration
class CustodyScheduler(private val watcher: CustodyWatcher) {

    @PostConstruct
    fun recover() = watcher.recoverPending()

    @Scheduled(fixedDelayString = "\${custody.poll-interval-ms:5000}")
    fun poll() = watcher.poll()
}
