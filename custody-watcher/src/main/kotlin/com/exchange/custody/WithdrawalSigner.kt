package com.exchange.custody

/**
 * Signs a withdrawal and returns the raw signed transaction.
 *
 * In production this delegates to the MPC key-management service.
 * In tests a mock implementation is injected.
 *
 * Nonce is assigned by the application (NEXTVAL in DB, committed before sign()) and
 * passed in as an input. The signer must use exactly this nonce — no reassignment.
 * This enables crash-3b recovery: on restart the same nonce is reused without calling
 * NEXTVAL again, so the on-chain transaction remains identical.
 */
interface WithdrawalSigner {
    data class SignResult(
        val txHash: String,
        val rawTx: String
    )

    fun sign(withdrawalId: Long, destination: String, amount: Long, currency: Int, nonce: Long): SignResult
}
