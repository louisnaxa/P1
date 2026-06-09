package com.exchange.gateway

import com.exchange.common.EngineCommand
import com.exchange.common.OrderSide
import org.springframework.http.ResponseEntity
import org.springframework.security.core.Authentication
import org.springframework.web.bind.annotation.*

data class PlaceOrderRequest(
    // uid is intentionally absent: it is resolved from the verified JWT token,
    // not supplied by the client.  Accepting uid from the client would allow
    // any authenticated user to trade on behalf of any other user.
    val symbolId: Int,
    val side: OrderSide,
    val price: Long,
    val quantity: Long
)

data class OrderResponse(val orderId: Long)

@RestController
@RequestMapping("/orders")
class OrderController(
    private val publisher: CommandPublisher,
    private val userService: UserService
) {

    /**
     * Places an order by writing a PLACE_ORDER command to the commands topic.
     * Responds only after the broker confirms the write (acks=all).
     * orderId in the response = Kafka offset = canonical identity of the order.
     *
     * uid is resolved from Authentication.name (the JWT "sub" claim).
     * An unknown sub is rejected with 403 before the command reaches Kafka.
     *
     * TD-5: no Idempotency-Key — a retried POST creates a second order.
     */
    @PostMapping
    fun placeOrder(
        @RequestBody req: PlaceOrderRequest,
        authentication: Authentication
    ): ResponseEntity<OrderResponse> {
        val uid = userService.resolveUid(authentication.name)
        val cmd = EngineCommand(
            type = EngineCommand.PLACE_ORDER,
            uid = uid,
            symbolId = req.symbolId,
            side = req.side,
            price = req.price,
            size = req.quantity
        )
        val orderId = publisher.publish("commands", req.symbolId.toString(), cmd)
        return ResponseEntity.accepted().body(OrderResponse(orderId))
    }

    /**
     * Cancels an order by writing a CANCEL_ORDER command to the commands topic.
     * orderId path parameter is the Kafka offset returned by POST /orders.
     * uid is resolved from the JWT token — a user can only cancel their own orders.
     */
    @DeleteMapping("/{orderId}")
    fun cancelOrder(
        @PathVariable orderId: Long,
        @RequestParam symbolId: Int,
        authentication: Authentication
    ): ResponseEntity<Void> {
        val uid = userService.resolveUid(authentication.name)
        val cmd = EngineCommand.cancelOrder(symbolId, uid, orderId)
        publisher.publish("commands", symbolId.toString(), cmd)
        return ResponseEntity.accepted().build()
    }
}
