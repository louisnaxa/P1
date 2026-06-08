package com.exchange.common

data class TradeEvent(
    val tradeId: Long,
    val symbol: String,
    val makerOrderId: Long,
    val takerOrderId: Long,
    val makerUserId: Long,
    val takerUserId: Long,
    val price: Long,
    val quantity: Long,
    val takerSide: OrderSide,
    val timestampNs: Long
)
