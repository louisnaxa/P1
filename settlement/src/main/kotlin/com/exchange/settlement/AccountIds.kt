package com.exchange.settlement

/**
 * Deterministic account ID encoding for TigerBeetle.
 * Layout: (userId << 32) | (ledgerId << 8) | accountType
 */
object AccountIds {
    const val AVAILABLE: Int = 0x01
    const val LOCKED: Int = 0x02

    const val SYSTEM_EXTERNAL_USER: Long = 0L
    const val SYSTEM_FEES_USER: Long = 1L

    fun encode(userId: Long, ledgerId: Int, accountType: Int): Long =
        (userId shl 32) or (ledgerId.toLong() shl 8) or accountType.toLong()

    fun available(userId: Long, ledgerId: Int): Long = encode(userId, ledgerId, AVAILABLE)
    fun locked(userId: Long, ledgerId: Int): Long = encode(userId, ledgerId, LOCKED)
    fun external(ledgerId: Int): Long = encode(SYSTEM_EXTERNAL_USER, ledgerId, AVAILABLE)
    fun fees(ledgerId: Int): Long = encode(SYSTEM_FEES_USER, ledgerId, AVAILABLE)
}
