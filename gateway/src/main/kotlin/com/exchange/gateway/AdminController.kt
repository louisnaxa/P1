package com.exchange.gateway

import com.exchange.common.EngineCommand
import org.springframework.http.HttpStatus
import org.springframework.security.core.Authentication
import org.springframework.web.bind.annotation.*

data class CreditRequest(val uid: Long, val currency: Int, val amount: Long)

data class SetAccountStatusRequest(
    val uid: Long,
    val status: AccountStatus,
    val jurisdiction: String? = null
)

@RestController
class AdminController(
    private val publisher: CommandPublisher,
    private val userService: UserService
) {

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

    /**
     * Transitions an account to a new status and writes the audit trail.
     * The actor (admin performing the change) is read from the JWT — not from the
     * request body. Jurisdiction is required for CITIZEN_APPROVED, null otherwise.
     */
    @PostMapping("/admin/account-status")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    fun setAccountStatus(
        @RequestBody req: SetAccountStatusRequest,
        authentication: Authentication
    ) {
        userService.setAccountStatus(req.uid, req.status, req.jurisdiction, authentication.name)
    }
}
