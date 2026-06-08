package com.exchange.common

data class EngineCommand(
    val type: String,
    // PlaceOrder
    val symbolId: Int = 0,
    val uid: Long = 0,
    val orderId: Long = 0,
    val price: Long = 0,
    val size: Long = 0,
    val side: OrderSide? = null,
    // AdjustBalance
    val currency: Int = 0,
    val amount: Long = 0,
    // AddSymbol
    val baseCurrency: Int = 0,
    val quoteCurrency: Int = 0,
    val baseScaleK: Long = 1,
    val quoteScaleK: Long = 1
) {
    companion object {
        const val ADD_SYMBOL = "ADD_SYMBOL"
        const val ADD_USER = "ADD_USER"
        const val ADJUST_BALANCE = "ADJUST_BALANCE"
        const val PLACE_ORDER = "PLACE_ORDER"

        fun addSymbol(symbolId: Int, baseCurrency: Int, quoteCurrency: Int, baseScaleK: Long = 1, quoteScaleK: Long = 1) =
            EngineCommand(type = ADD_SYMBOL, symbolId = symbolId, baseCurrency = baseCurrency, quoteCurrency = quoteCurrency, baseScaleK = baseScaleK, quoteScaleK = quoteScaleK)

        fun addUser(uid: Long) =
            EngineCommand(type = ADD_USER, uid = uid)

        fun adjustBalance(uid: Long, currency: Int, amount: Long) =
            EngineCommand(type = ADJUST_BALANCE, uid = uid, currency = currency, amount = amount)

        fun placeOrder(symbolId: Int, uid: Long, orderId: Long, price: Long, size: Long, side: OrderSide) =
            EngineCommand(type = PLACE_ORDER, symbolId = symbolId, uid = uid, orderId = orderId, price = price, size = size, side = side)
    }
}
