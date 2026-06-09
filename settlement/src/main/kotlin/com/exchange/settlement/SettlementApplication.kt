package com.exchange.settlement

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.scheduling.annotation.EnableScheduling

@SpringBootApplication
@EnableScheduling
class SettlementApplication

fun main(args: Array<String>) {
    runApplication<SettlementApplication>(*args)
}
