package com.exchange.settlement

import com.tigerbeetle.Client
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import jakarta.annotation.PreDestroy

@Configuration
class TigerBeetleConfig {

    @Value("\${tigerbeetle.addresses}")
    private lateinit var addresses: String

    @Value("\${tigerbeetle.cluster-id}")
    private var clusterId: Long = 0

    private var client: Client? = null

    @Bean
    fun tigerBeetleClient(): Client {
        val clusterIdBytes = ByteArray(16)
        clusterIdBytes[0] = clusterId.toByte()
        val c = Client(clusterIdBytes, arrayOf(addresses))
        client = c
        return c
    }

    @PreDestroy
    fun close() {
        client?.close()
    }
}
