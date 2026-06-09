package com.exchange.custody

/**
 * Signs a withdrawal and returns the raw signed transaction.
 *
 * In production this delegates to the MPC key-management service.
 * In tests a mock implementation is injected.
 *
 * Nonce assignment is intentionally outside this interface for M4.
 * The MPC sub-lot (next) will add nonce reservation, raw_tx persistence,
 * and the crash-3b recovery path (intent stored before calling sign()).
 */
interface WithdrawalSigner {
    data class SignResult(
        val txHash: String,
        val nonce: Long,   // -1L sentinel in mock; real nonce managed by next sub-lot
        val rawTx: String
    )

    fun sign(withdrawalId: Long, destination: String, amount: Long, currency: Int): SignResult
}
