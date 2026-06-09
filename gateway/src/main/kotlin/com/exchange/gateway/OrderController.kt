package com.exchange.gateway

import com.exchange.common.EngineCommand
import com.exchange.common.OrderSide
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*

data class PlaceOrderRequest(
    val uid: Long,
    val symbolId: Int,
    val side: OrderSide,
    val price: Long,
    val quantity: Long
)

data class OrderResponse(val orderId: Long)

@RestController
@RequestMapping("/orders")
class OrderController(private val publisher: CommandPublisher) {

    /**
     * Places an order by writing a PLACE_ORDER command to the commands topic.
     * Responds only after the broker confirms the write (acks=all).
     * orderId in the response = Kafka offset = canonical identity of the order.
     *
     * TD-5: no Idempotency-Key — a retried POST creates a second order.
     */
    @PostMapping
    fun placeOrder(@RequestBody req: PlaceOrderRequest): ResponseEntity<OrderResponse> {
        val cmd = EngineCommand(
            type = EngineCommand.PLACE_ORDER,
            uid = req.uid,
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
     * Responds only after the broker confirms the write.
     */
    @DeleteMapping("/{orderId}")
    fun cancelOrder(
        @PathVariable orderId: Long,
        @RequestParam uid: Long,
        @RequestParam symbolId: Int
    ): ResponseEntity<Void> {
        val cmd = EngineCommand.cancelOrder(symbolId, uid, orderId)
        publisher.publish("commands", symbolId.toString(), cmd)
        return ResponseEntity.accepted().build()
    }
}
