package com.exchange.common

data class PropertyCommand(
    val type: String,
    val name: String = "",
    val jurisdiction: String = "",
    val propertyLedgerId: Int = 0,
    val quoteLedgerId: Int = 0,
    val symbolId: Int = 0,
    val totalTokens: Long = 0L,
    // Metadata (habillage — presentation only, never read by settlement/engine)
    val description: String = "",
    val location: String = ""
) {
    companion object {
        const val CREATE_PROPERTY = "CREATE_PROPERTY"
    }
}
