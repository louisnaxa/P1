package com.exchange.custody

import com.exchange.settlement.SettlementService
import org.slf4j.LoggerFactory
import org.springframework.jdbc.core.JdbcTemplate

/**
 * Orchestrates the withdrawal state machine: LOCKED → BROADCAST → CONFIRMED | VOID.
 *
 * Sequence (see TECH_DEBT.md and withdrawal design doc for failure analysis):
 *   1. initiate()        — TB PENDING transfer + DB row (LOCKED). Nothing sent on-chain.
 *   2. broadcastPending() — scan LOCKED → sign → persist tx_hash → BROADCAST.
 *   3. confirmBroadcast() — scan BROADCAST → TB postPending → CONFIRMED.
 *                          Entry point for crash-2 recovery. NEVER calls signer.
 *   4. voidWithdrawal()  — TB voidPending → VOID. Funds fully restored to user.
 *
 * tb_pending_id formula: (1L shl 62) or (withdrawalId shl 1)
 *   - Bit 62 namespaces withdrawals away from trade leg IDs (bits 0-3).
 *   - Bit 63 is reserved for on-chain deposit SHA-256 transferIds.
 *   - Bit 0 is always 0 for pending; the resolving transfer uses pendingId or 1L.
 */
class WithdrawalService(
    private val settlement: SettlementService,
    private val jdbc: JdbcTemplate,
    private val signer: WithdrawalSigner
) {
    private val log = LoggerFactory.getLogger(javaClass)

    /**
     * Reserve funds and persist the withdrawal intent.
     *
     * Step 1 (TB PENDING) happens before Step 2 (DB insert):
     * if the process crashes between them, the TB PENDING is orphaned but no funds
     * move — a scan of TB PENDING transfers can recover the state. In normal operation,
     * the crash is recovered by the caller retrying initiate() with the same withdrawalId
     * (TB Exists + DB ON CONFLICT DO NOTHING = both steps are idempotent).
     *
     * Throws if TB rejects the PENDING transfer (e.g. insufficient funds).
     */
    fun initiate(uid: Long, currency: Int, amount: Long, destination: String, withdrawalId: Long) {
        val tbPendingId = (1L shl 62) or (withdrawalId shl 1)
        settlement.pendingWithdrawal(uid, currency, amount, tbPendingId)
        jdbc.update(
            """INSERT INTO withdrawals (id, uid, currency, amount, destination_address, tb_pending_id)
               VALUES (?, ?, ?, ?, ?, ?)
               ON CONFLICT (id) DO NOTHING""",
            withdrawalId, uid, currency, amount, destination, tbPendingId
        )
    }

    /**
     * Broadcast all LOCKED withdrawals.
     *
     * Anti-double-broadcast: reads state before calling signer; the UPDATE uses
     * WHERE state = 'LOCKED' so a concurrent transition (e.g. another instance
     * already broadcast this row) makes the UPDATE a no-op.
     *
     * Called by the scheduler. Also serves as crash-1 recovery on restart:
     * LOCKED rows whose broadcast was not started are naturally picked up here.
     */
    fun broadcastPending() {
        val rows = jdbc.queryForList("SELECT id, destination_address, amount, currency FROM withdrawals WHERE state = 'LOCKED'")
        for (row in rows) {
            val id       = row["id"] as Long
            val dest     = row["destination_address"] as String
            val amount   = row["amount"] as Long
            val currency = row["currency"] as Int
            val signed = signer.sign(id, dest, amount, currency)
            val updated = jdbc.update(
                """UPDATE withdrawals
                   SET state = 'BROADCAST', tx_hash = ?, nonce = ?, raw_tx = ?, updated_at = NOW()
                   WHERE id = ? AND state = 'LOCKED'""",
                signed.txHash, signed.nonce, signed.rawTx, id
            )
            if (updated == 0) {
                log.warn("broadcastPending: withdrawal {} already transitioned — skipping", id)
            } else {
                log.info("broadcastPending: withdrawal {} → BROADCAST txHash={}", id, signed.txHash)
            }
        }
    }

    /**
     * Confirm all BROADCAST withdrawals.
     *
     * Crash-2 recovery entry point: if the process crashed after broadcast but before
     * confirmation, rows stay in BROADCAST state. On restart, this method picks them up
     * and calls postPendingWithdrawal() — which is idempotent (TB Exists on replay).
     *
     * NEVER calls signer. The anti-double guarantee is structural, not conditional:
     * there is no reference to signer in this method.
     */
    fun confirmBroadcast() {
        val rows = jdbc.queryForList("SELECT id, uid, currency, amount, tb_pending_id FROM withdrawals WHERE state = 'BROADCAST'")
        for (row in rows) {
            val id          = row["id"] as Long
            val uid         = row["uid"] as Long
            val currency    = row["currency"] as Int
            val amount      = row["amount"] as Long
            val tbPendingId = row["tb_pending_id"] as Long
            settlement.postPendingWithdrawal(tbPendingId, uid, currency, amount)
            jdbc.update(
                "UPDATE withdrawals SET state = 'CONFIRMED', updated_at = NOW() WHERE id = ? AND state = 'BROADCAST'",
                id
            )
            log.info("confirmBroadcast: withdrawal {} → CONFIRMED", id)
        }
    }

    /**
     * Void a LOCKED withdrawal: release the TB PENDING lock and mark VOID.
     * Used when the withdrawal cannot proceed (on-chain failure, expired nonce,
     * or manual cancellation). Funds are fully restored to the user's available balance.
     */
    fun voidWithdrawal(withdrawalId: Long) {
        val row = jdbc.queryForMap("SELECT uid, currency, tb_pending_id FROM withdrawals WHERE id = ?", withdrawalId)
        val uid         = row["uid"] as Long
        val currency    = row["currency"] as Int
        val tbPendingId = row["tb_pending_id"] as Long
        settlement.voidPendingWithdrawal(tbPendingId, uid, currency)
        jdbc.update(
            "UPDATE withdrawals SET state = 'VOID', updated_at = NOW() WHERE id = ? AND state = 'LOCKED'",
            withdrawalId
        )
        log.info("voidWithdrawal: withdrawal {} → VOID", withdrawalId)
    }
}
