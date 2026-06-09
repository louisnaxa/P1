package com.exchange.gateway

import com.exchange.common.EngineCommand
import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.*

data class CreditRequest(val uid: Long, val currency: Int, val amount: Long)

@RestController
class AdminController(private val publisher: CommandPublisher) {

    /**
     * Credits a user's balance by writing an ADJUST_BALANCE command to the
     * commands topic. Responds 202 once the broker confirms the write (acks=all).
     * The engine processes the command deterministically on replay.
     */
    @PostMapping("/admin/credit")
    @ResponseStatus(HttpStatus.ACCEPTED)
    fun credit(@RequestBody req: CreditRequest) {
        publisher.publish("commands", req.uid.toString(), EngineCommand.adjustBalance(req.uid, req.currency, req.amount))
    }
}
