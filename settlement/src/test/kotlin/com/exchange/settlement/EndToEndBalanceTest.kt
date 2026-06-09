package com.exchange.settlement

import com.exchange.common.EngineCommand
import com.exchange.common.OrderSide
import com.exchange.engine.MatchingEngineService
import com.tigerbeetle.Client
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import org.springframework.context.ConfigurableApplicationContext

/**
 * End-to-end balance conservation test — M2 level, M1 rigour.
 *
 * Proves that a multi-account trading session produces no balance drift:
 *   (a) Σ BASE and Σ QUOTE are strictly conserved across all accounts
 *   (b) every per-account balance engine == TigerBeetle (via ReconciliationService)
 *   (c) no account carries a negative balance
 *
 * Session: 5 accounts, 6 trades, including a partial fill and three gap-2 accounts
 * (Alice, Eve, Bob have no prior "receive" account for the traded asset).
 *
 * Architecture: real TigerBeetle (Testcontainer) + real MatchingEngineService.
 * Kafka is intentionally absent: the conservation property lives in the settlement
 * math, not in the transport.  The tradePublisher callback replicates the exact
 * logic of TradeConsumer.onTrade (ensureAccount ordering guarantee → settleTrade →
 * recordTrade), minus the Kafka ack wrapper which adds nothing to conservation.
 * Kafka-specific behaviours (at-least-once, crash recovery) are proven separately
 * in SettlementChaosIntegrationTest.
 */
@Tag("integration")
class EndToEndBalanceTest {

    companion object {
        const val BASE_LEDGER  = 10
        const val QUOTE_LEDGER = 11
        const val SYMBOL_ID    = 2   // distinct from chaos tests (symbolId = 1)

        val tb = TigerBeetleContainer()

        @JvmStatic @BeforeAll fun startContainers() { tb.start() }
        @JvmStatic @AfterAll  fun stopContainers()  { tb.close() }
    }

    // User IDs distinct from chaos tests (1001–4002)
    private val ALICE = 5001L
    private val BOB   = 5002L
    private val CAROL = 5003L
    private val DAVE  = 5004L
    private val EVE   = 5005L

    private lateinit var tbClient: Client
    private lateinit var service: SettlementService
    private lateinit var reconciliation: ReconciliationService
    private lateinit var engine: MatchingEngineService
    private var offset = 0L

    @BeforeEach
    fun setUp() {
        tbClient = Client(ByteArray(16) /* cluster-id = 0 */, arrayOf(tb.address))
        service  = SettlementService(tbClient)

        // If reconcile() finds a mismatch it calls ctx.close() — fail the test loudly.
        val mockCtx = Mockito.mock(ConfigurableApplicationContext::class.java)
        Mockito.doThrow(AssertionError("ReconciliationService found a balance mismatch — ctx.close() was called"))
            .`when`(mockCtx).close()
        reconciliation = ReconciliationService(service, mockCtx)

        engine = MatchingEngineService(
            tradePublisher = { trade ->
                // Mirror TradeConsumer.onTrade.
                // ORDER IS A CORRECTNESS GUARANTEE: all ensureAccount calls must complete
                // before settleTrade (gap-2 prevention).  See TradeConsumer for the original.
                service.ensureAccount(trade.takerUserId, BASE_LEDGER)
                service.ensureAccount(trade.takerUserId, QUOTE_LEDGER)
                service.ensureAccount(trade.makerUserId, BASE_LEDGER)
                service.ensureAccount(trade.makerUserId, QUOTE_LEDGER)
                service.settleTrade(trade, BASE_LEDGER, QUOTE_LEDGER)
                reconciliation.recordTrade(trade, BASE_LEDGER, QUOTE_LEDGER)
            }
        )
        engine.init()
        offset = 0L

        service.ensureSystemAccounts(BASE_LEDGER)
        service.ensureSystemAccounts(QUOTE_LEDGER)
    }

    @AfterEach
    fun tearDown() {
        engine.shutdown()
        tbClient.close()
    }

    /** Process one command at the next monotonic offset. */
    private fun cmd(c: EngineCommand) = engine.processCommand(offset++, c)

    /**
     * Register a deposit in both the engine and TigerBeetle + reconciliation.
     * Mirrors the joint work of EngineCommandConsumer + AdjustBalanceConsumer:
     *   - engine.processCommand handles the adjustBalance for exchange-core's internal balance
     *   - deposit() + recordCredit() handle the TigerBeetle and reconciliation sides
     * transferId = commandOffset + 1, matching AdjustBalanceConsumer's formula.
     */
    private fun adjustAndDeposit(uid: Long, ledger: Int, amount: Long) {
        cmd(EngineCommand.adjustBalance(uid, ledger, amount))
        val transferId = offset   // cmd() used (offset-1), so offset = cmdOffset + 1
        service.ensureAccount(uid, ledger)
        service.deposit(uid, ledger, amount, transferId)
        reconciliation.recordCredit(uid, ledger, amount)
    }

    @Test
    fun `E2E - 5-account session conserves total BASE and QUOTE, no balance drift`() {

        // ── Symbol + users ────────────────────────────────────────────────────
        cmd(EngineCommand.addSymbol(SYMBOL_ID, BASE_LEDGER, QUOTE_LEDGER))
        cmd(EngineCommand.addUser(ALICE))
        cmd(EngineCommand.addUser(BOB))
        cmd(EngineCommand.addUser(CAROL))
        cmd(EngineCommand.addUser(DAVE))
        cmd(EngineCommand.addUser(EVE))

        // ── Deposits ──────────────────────────────────────────────────────────
        // Alice and Eve: QUOTE only (gap-2 for BASE on first buy)
        // Bob: BASE only (gap-2 for QUOTE on first sell proceeds)
        // Carol, Dave: both BASE and QUOTE (mixed — buyer and seller in session)
        adjustAndDeposit(ALICE, QUOTE_LEDGER, 1000L)
        adjustAndDeposit(BOB,   BASE_LEDGER,  10L)
        adjustAndDeposit(CAROL, QUOTE_LEDGER, 600L)
        adjustAndDeposit(CAROL, BASE_LEDGER,  5L)
        adjustAndDeposit(DAVE,  QUOTE_LEDGER, 400L)
        adjustAndDeposit(DAVE,  BASE_LEDGER,  3L)
        adjustAndDeposit(EVE,   QUOTE_LEDGER, 600L)

        // Conservation invariants established at deposit time
        val initQuote = 1000L + 600L + 400L + 600L  // = 2600
        val initBase  = 10L + 5L + 3L               // = 18

        // ── Trading session ───────────────────────────────────────────────────
        //
        // Bob ASK 5@100 — resting, no trade
        cmd(EngineCommand.placeOrder(SYMBOL_ID, BOB,   0L, 100L, 5L, OrderSide.ASK))

        // T1 — Alice BID 2@100: PARTIAL FILL of Bob's ASK (3 units remain)
        //   Alice: QUOTE -200, BASE +2   ← gap-2: Alice has no BASE account yet
        //   Bob:   BASE  -2,  QUOTE +200 ← gap-2: Bob has no QUOTE account yet
        cmd(EngineCommand.placeOrder(SYMBOL_ID, ALICE, 0L, 100L, 2L, OrderSide.BID))

        // Carol ASK 3@80 — resting
        cmd(EngineCommand.placeOrder(SYMBOL_ID, CAROL, 0L, 80L, 3L, OrderSide.ASK))

        // T2 — Eve BID 3@80: fills Carol's ASK
        //   Eve:   QUOTE -240, BASE +3   ← gap-2: Eve has no BASE account yet
        //   Carol: BASE  -3,  QUOTE +240
        cmd(EngineCommand.placeOrder(SYMBOL_ID, EVE,   0L, 80L, 3L, OrderSide.BID))

        // Bob ASK 1@90 — new separate order, resting
        cmd(EngineCommand.placeOrder(SYMBOL_ID, BOB,   0L, 90L, 1L, OrderSide.ASK))

        // T3 — Dave BID 1@90: fills Bob's second ASK
        //   Dave: QUOTE -90, BASE +1
        //   Bob:  BASE  -1,  QUOTE +90
        cmd(EngineCommand.placeOrder(SYMBOL_ID, DAVE,  0L, 90L, 1L, OrderSide.BID))

        // Dave ASK 2@110 — resting
        cmd(EngineCommand.placeOrder(SYMBOL_ID, DAVE,  0L, 110L, 2L, OrderSide.ASK))

        // T4 — Carol BID 2@110: Carol switches from seller (T2) to buyer
        //   Carol: QUOTE -220, BASE +2
        //   Dave:  BASE  -2,  QUOTE +220
        cmd(EngineCommand.placeOrder(SYMBOL_ID, CAROL, 0L, 110L, 2L, OrderSide.BID))

        // Carol ASK 1@95 — resting
        cmd(EngineCommand.placeOrder(SYMBOL_ID, CAROL, 0L, 95L, 1L, OrderSide.ASK))

        // T5 — Alice BID 1@95
        //   Alice: QUOTE -95, BASE +1
        //   Carol: BASE  -1,  QUOTE +95  (Carol sells again — T2 seller, T4 buyer, T5 seller)
        cmd(EngineCommand.placeOrder(SYMBOL_ID, ALICE, 0L, 95L, 1L, OrderSide.BID))

        // T6 — Eve BID 3@100: fills the remaining 3 of Bob's original ASK 5@100
        //   Eve:   QUOTE -300, BASE +3   ← partial fill completion
        //   Bob:   BASE  -3,  QUOTE +300
        cmd(EngineCommand.placeOrder(SYMBOL_ID, EVE,   0L, 100L, 3L, OrderSide.BID))

        // ── Signal replay complete ────────────────────────────────────────────
        // No Kafka replay in this test, but both gates must be set before reconcile().
        reconciliation.signalCommandsCaughtUp()
        reconciliation.signalTradesCaughtUp()

        val users = listOf(ALICE, BOB, CAROL, DAVE, EVE)

        // ── (a) Conservation: Σ assets unchanged vs initial deposits ─────────
        val finalQuote = users.sumOf { service.getBalance(it, QUOTE_LEDGER) }
        val finalBase  = users.sumOf { service.getBalance(it, BASE_LEDGER) }
        assertThat(finalQuote).`as`("Σ QUOTE conserved").isEqualTo(initQuote)
        assertThat(finalBase) .`as`("Σ BASE conserved").isEqualTo(initBase)

        // ── (b) Per-account: engine expected == TigerBeetle actual ───────────
        // reconcile() calls ctx.close() on any mismatch → throws AssertionError above
        reconciliation.reconcile()

        // ── (c) No negative balances ──────────────────────────────────────────
        // TigerBeetle enforces DEBITS_MUST_NOT_EXCEED_CREDITS at transfer time,
        // so negatives cannot exist in practice. Asserted explicitly for clarity.
        for (uid in users) {
            assertThat(service.getBalance(uid, QUOTE_LEDGER)).`as`("uid=$uid QUOTE ≥ 0").isGreaterThanOrEqualTo(0L)
            assertThat(service.getBalance(uid, BASE_LEDGER)) .`as`("uid=$uid BASE ≥ 0").isGreaterThanOrEqualTo(0L)
        }

        // ── Spot-check individual balances ────────────────────────────────────
        // These follow from (a)+(b), but listed here so a regression identifies the exact account.
        assertThat(service.getBalance(ALICE, QUOTE_LEDGER)).`as`("Alice QUOTE 1000-200-95")   .isEqualTo(705L)
        assertThat(service.getBalance(ALICE, BASE_LEDGER)) .`as`("Alice BASE  0+2+1")         .isEqualTo(3L)
        assertThat(service.getBalance(BOB,   QUOTE_LEDGER)).`as`("Bob QUOTE   0+200+90+300")  .isEqualTo(590L)
        assertThat(service.getBalance(BOB,   BASE_LEDGER)) .`as`("Bob BASE    10-2-1-3")      .isEqualTo(4L)
        assertThat(service.getBalance(CAROL, QUOTE_LEDGER)).`as`("Carol QUOTE 600+240-220+95").isEqualTo(715L)
        assertThat(service.getBalance(CAROL, BASE_LEDGER)) .`as`("Carol BASE  5-3+2-1")       .isEqualTo(3L)
        assertThat(service.getBalance(DAVE,  QUOTE_LEDGER)).`as`("Dave QUOTE  400-90+220")    .isEqualTo(530L)
        assertThat(service.getBalance(DAVE,  BASE_LEDGER)) .`as`("Dave BASE   3+1-2")         .isEqualTo(2L)
        assertThat(service.getBalance(EVE,   QUOTE_LEDGER)).`as`("Eve QUOTE   600-240-300")   .isEqualTo(60L)
        assertThat(service.getBalance(EVE,   BASE_LEDGER)) .`as`("Eve BASE    0+3+3")         .isEqualTo(6L)
    }
}
