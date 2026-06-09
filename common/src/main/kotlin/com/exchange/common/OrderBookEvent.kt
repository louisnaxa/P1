package com.exchange.common

data class PriceLevel(val price: Long, val volume: Long)

data class OrderBookEvent(
    val symbolId: Int,
    val bids: List<PriceLevel>,
    val asks: List<PriceLevel>
)
