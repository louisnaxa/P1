package com.exchange.settlement

import com.exchange.common.OrderSide
import com.exchange.common.TradeEvent
import com.tigerbeetle.AccountBatch
import com.tigerbeetle.AccountFlags
import com.tigerbeetle.Client
import com.tigerbeetle.IdBatch
import com.tigerbeetle.CreateTransferResult
import com.tigerbeetle.TransferBatch
import com.tigerbeetle.TransferFlags
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service

@Service
class SettlementService(private val tb: Client) {

    private val log = LoggerFactory.getLogger(javaClass)

    /**
     * Ensure user available account exists for a given ledger.
     * TigerBeetle returns "exists" for duplicate IDs (idempotent).
     */
    fun ensureAccount(userId: Long, ledgerId: Int) {
        val batch = AccountBatch(2)

        // Available account — holds spendable funds
        batch.add()
        batch.setId(AccountIds.available(userId, ledgerId))
        batch.setLedger(ledgerId)
        batch.setCode(1)
        batch.setFlags(AccountFlags.DEBITS_MUST_NOT_EXCEED_CREDITS)

        // Locked account — holds reserved funds for two-phase settlement (M2)
        batch.add()
        batch.setId(AccountIds.locked(userId, ledgerId))
        batch.setLedger(ledgerId)
        batch.setCode(2)
        batch.setFlags(AccountFlags.DEBITS_MUST_NOT_EXCEED_CREDITS)

        tb.createAccounts(batch)
    }

    fun ensureSystemAccounts(ledgerId: Int) {
        val batch = AccountBatch(2)

        // External account (no balance constraint — represents the outside world)
        batch.add()
        batch.setId(AccountIds.external(ledgerId))
        batch.setLedger(ledgerId)
        batch.setCode(10)
        batch.setFlags(AccountFlags.NONE)

        // Fees account
        batch.add()
        batch.setId(AccountIds.fees(ledgerId))
        batch.setLedger(ledgerId)
        batch.setCode(11)
        batch.setFlags(AccountFlags.DEBITS_MUST_NOT_EXCEED_CREDITS)

        tb.createAccounts(batch)
    }

    /**
     * Credit a user's available account from the external account (deposit).
     */
    fun deposit(userId: Long, ledgerId: Int, amount: Long, transferId: Long) {
        val batch = TransferBatch(1)
        batch.add()
        batch.setId(transferId)
        batch.setDebitAccountId(AccountIds.external(ledgerId))
        batch.setCreditAccountId(AccountIds.available(userId, ledgerId))
        batch.setAmount(amount)
        batch.setLedger(ledgerId)
        batch.setCode(100)
        val errors = tb.createTransfers(batch)
        if (errors.length > 0) {
            log.debug("Deposit transfer may already exist (idempotent)")
        }
    }

    /**
     * Settle a trade: move funds between available accounts.
     *
     * For M1, the locked/available split lives only in exchange-core.
     * TigerBeetle tracks net positions via available accounts.
     * Locked accounts and two-phase settlement come in M2.
     *
     * Zero fees for M1. Fee transfers will be added later.
     */
    fun settleTrade(trade: TradeEvent, baseLedger: Int, quoteLedger: Int) {
        val quoteAmount = trade.price * trade.quantity
        val baseAmount = trade.quantity

        val (buyerUid, sellerUid) = if (trade.takerSide == OrderSide.BID) {
            trade.takerUserId to trade.makerUserId
        } else {
            trade.makerUserId to trade.takerUserId
        }

        val batch = TransferBatch(2)

        // Transfer 1: quote currency (buyer → seller). LINKED to transfer 2:
        // if leg 1 fails, TigerBeetle rolls back leg 0 atomically (closes TD-4).
        batch.add()
        batch.setId(transferId(trade.tradeId, 0))
        batch.setDebitAccountId(AccountIds.available(buyerUid, quoteLedger))
        batch.setCreditAccountId(AccountIds.available(sellerUid, quoteLedger))
        batch.setAmount(quoteAmount)
        batch.setLedger(quoteLedger)
        batch.setCode(1)
        batch.setFlags(TransferFlags.LINKED)

        // Transfer 2: base currency (seller → buyer). Chain terminator — no LINKED.
        batch.add()
        batch.setId(transferId(trade.tradeId, 1))
        batch.setDebitAccountId(AccountIds.available(sellerUid, baseLedger))
        batch.setCreditAccountId(AccountIds.available(buyerUid, baseLedger))
        batch.setAmount(baseAmount)
        batch.setLedger(baseLedger)
        batch.setCode(1)

        val errors = tb.createTransfers(batch)
        if (errors.length > 0) {
            val errorList = buildList {
                while (errors.next()) add(errors.getIndex() to errors.getResult())
            }
            if (errorList.all { (_, r) -> r == CreateTransferResult.Exists }) {
                log.debug("Trade {} already settled — idempotent replay", trade.tradeId)
            } else {
                // LINKED batch rejected: no funds were moved (atomicity held), but the trade
                // is NOT settled. Manual intervention required (see TD-9 in TECH_DEBT.md).
                log.error(
                    "SETTLEMENT FAILURE trade={} — LINKED batch rejected, trade NOT settled. " +
                    "Ensure all participant accounts exist and re-emit via /admin/credit. errors={}",
                    trade.tradeId,
                    errorList.joinToString { (i, r) -> "leg[$i]=$r" }
                )
            }
        } else {
            log.info("Settled trade {}: {} base @ {} quote, buyer={} seller={}",
                trade.tradeId, baseAmount, quoteAmount, buyerUid, sellerUid)
        }
    }

    /**
     * Reserve funds for a pending withdrawal.
     * Creates a PENDING transfer: available(userId) → external(ledgerId).
     * The debit lands in debits_pending (not debits_posted), so DEBITS_MUST_NOT_EXCEED_CREDITS
     * still blocks a second debit — the funds are effectively locked.
     * Throws if TB rejects for any reason other than Exists (e.g. ExceedsCredits = insufficient funds).
     */
    fun pendingWithdrawal(userId: Long, ledgerId: Int, amount: Long, transferId: Long) {
        val batch = TransferBatch(1)
        batch.add()
        batch.setId(transferId)
        batch.setDebitAccountId(AccountIds.available(userId, ledgerId))
        batch.setCreditAccountId(AccountIds.external(ledgerId))
        batch.setAmount(amount)
        batch.setLedger(ledgerId)
        batch.setCode(200)
        batch.setFlags(TransferFlags.PENDING)
        val errors = tb.createTransfers(batch)
        if (errors.length > 0) {
            while (errors.next()) {
                val result = errors.getResult()
                if (result != CreateTransferResult.Exists) {
                    throw IllegalStateException("pendingWithdrawal rejected by TigerBeetle: $result")
                }
            }
        }
    }

    /**
     * Finalise a pending withdrawal: debits_pending → debits_posted on the user account.
     * The resolving transfer ID is derived as pendingId or 1L (bit 0 distinguishes it from
     * the pending transfer, whose ID always has bit 0 clear by construction).
     * Idempotent: Exists is silently ignored.
     */
    fun postPendingWithdrawal(pendingId: Long) {
        val batch = TransferBatch(1)
        batch.add()
        batch.setId(pendingId or 1L)
        batch.setPendingId(pendingId)
        batch.setFlags(TransferFlags.POST_PENDING_TRANSFER)
        // debit_account_id, credit_account_id, ledger, code, amount = 0 → inherited from pending transfer
        val errors = tb.createTransfers(batch)
        if (errors.length > 0) {
            while (errors.next()) {
                val result = errors.getResult()
                if (result != CreateTransferResult.Exists) {
                    throw IllegalStateException("postPendingWithdrawal failed pendingId=$pendingId resolvingId=${pendingId or 1L}: $result")
                }
            }
        }
    }

    /**
     * Reverse a pending withdrawal: release the locked funds back to available.
     * Uses the same resolving ID formula as postPendingWithdrawal — post and void are
     * mutually exclusive so they share the ID slot.
     * Idempotent: Exists is silently ignored.
     */
    fun voidPendingWithdrawal(pendingId: Long) {
        val batch = TransferBatch(1)
        batch.add()
        batch.setId(pendingId or 1L)
        batch.setPendingId(pendingId)
        batch.setFlags(TransferFlags.VOID_PENDING_TRANSFER)
        val errors = tb.createTransfers(batch)
        if (errors.length > 0) {
            while (errors.next()) {
                val result = errors.getResult()
                if (result != CreateTransferResult.Exists) {
                    throw IllegalStateException("voidPendingWithdrawal failed pendingId=$pendingId resolvingId=${pendingId or 1L}: $result")
                }
            }
        }
    }

    /**
     * Query the net balance of a user's available account.
     * Balance = credits_posted - debits_posted.
     */
    fun getBalance(userId: Long, ledgerId: Int): Long {
        val ids = IdBatch(1)
        ids.add()
        // IdBatch.setId has no single-Long overload — use (low, high) for 128-bit ID
        ids.setId(AccountIds.available(userId, ledgerId), 0L)
        val results = tb.lookupAccounts(ids)
        if (results.length == 0) return 0
        results.next() // advance cursor from -1 to first element before reading
        return results.getCreditsPosted().toLong() - results.getDebitsPosted().toLong()
    }

    /**
     * Spendable balance = credits_posted - debits_posted - debits_pending.
     * Unlike getBalance(), this reflects funds that are locked in a PENDING withdrawal:
     * they haven't been debited (posted) yet, but they cannot be spent again.
     */
    fun getSpendableBalance(userId: Long, ledgerId: Int): Long {
        val ids = IdBatch(1)
        ids.add()
        ids.setId(AccountIds.available(userId, ledgerId), 0L)
        val results = tb.lookupAccounts(ids)
        if (results.length == 0) return 0
        results.next()
        return results.getCreditsPosted().toLong() -
               results.getDebitsPosted().toLong() -
               results.getDebitsPending().toLong()
    }

    /**
     * Deterministic transfer ID: (tradeId << 4) | legIndex.
     * 4 bits for legIndex (max 15 legs), remaining 60 bits for tradeId.
     */
    private fun transferId(tradeId: Long, legIndex: Int): Long =
        (tradeId shl 4) or legIndex.toLong()
}
