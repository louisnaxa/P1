package com.exchange.custody

import com.exchange.settlement.AccountIds
import com.exchange.settlement.SettlementService
import com.tigerbeetle.Client
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.MethodOrderer
import org.junit.jupiter.api.Order
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestMethodOrder
import org.mockito.Mockito.anyInt
import org.mockito.Mockito.anyLong
import org.mockito.Mockito.anyString
import org.mockito.Mockito.clearInvocations
import org.mockito.Mockito.eq
import org.mockito.Mockito.mock
import org.mockito.Mockito.never
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.`when`
import org.springframework.jdbc.datasource.DriverManagerDataSource
import org.springframework.jdbc.core.JdbcTemplate
import org.testcontainers.containers.PostgreSQLContainer

/**
 * Withdrawal state machine — chaos integration tests.
 *
 * Proves the financially critical invariants of the withdrawal lifecycle
 * with a mock signer (no MPC, no on-chain tx):
 *
 *   W1 — Lock reserves funds: TB PENDING blocks a second debit.
 *   W2 — Lock is idempotent: same withdrawalId is a TB and DB no-op.
 *   W3 — broadcastPending calls signer exactly once per LOCKED row.
 *   W4 — confirmBroadcast finalizes TB and conservation holds (zero-sum).
 *   W5 — Crash-1 recovery: LOCKED row is picked up by broadcastPending on restart.
 *   W6 — Crash-2 recovery: BROADCAST row confirms without ever calling signer.
 *   W7 — voidWithdrawal returns funds exactly: void conservation invariant.
 *
 * Containers: TigerBeetle + Postgres. No Kafka, no Anvil, no MPC.
 */
@Tag("integration")
@TestMethodOrder(MethodOrderer.OrderAnnotation::class)
class WithdrawalChaosTest {

    companion object {
        // Main test user — goes through full LOCKED→BROADCAST→CONFIRMED cycle (W1-W4)
        private const val UID      = 8001L
        private const val CURRENCY = 20
        private const val AMOUNT   = 500L
        private const val DEST     = "0x70997970C51812dc3A010C7d01b50e0d17dc79C8"

        private val tb = TigerBeetleContainer()
        @Suppress("UNCHECKED_CAST")
        private val pg = PostgreSQLContainer("postgres:16-alpine") as PostgreSQLContainer<*>

        private lateinit var tbClient: Client
        lateinit var settlement: SettlementService
        lateinit var signer: WithdrawalSigner
        lateinit var service: WithdrawalService
        lateinit var jdbc: JdbcTemplate

        @JvmStatic
        @BeforeAll
        fun setup() {
            tb.start(); pg.start()

            tbClient   = Client(ByteArray(16), arrayOf(tb.address))
            settlement = SettlementService(tbClient)
            settlement.ensureSystemAccounts(CURRENCY)

            // UID: main test user — AMOUNT deposited, available for W1-W4
            settlement.ensureAccount(UID, CURRENCY)
            settlement.deposit(UID, CURRENCY, AMOUNT, transferId = 1L)

            jdbc = JdbcTemplate(DriverManagerDataSource(pg.jdbcUrl, pg.username, pg.password))
            createSchema()

            signer = mock(WithdrawalSigner::class.java)
            `when`(signer.sign(anyLong(), anyString(), anyLong(), anyInt()))
                .thenAnswer { inv ->
                    WithdrawalSigner.SignResult(
                        txHash = "0xmock_${inv.getArgument<Long>(0)}",
                        nonce  = -1L,
                        rawTx  = ""
                    )
                }

            service = WithdrawalService(settlement, jdbc, signer)
        }

        @JvmStatic
        @AfterAll
        fun teardown() {
            tbClient.close(); tb.close(); pg.stop()
        }

        private fun createSchema() {
            jdbc.execute("""CREATE TABLE withdrawals (
                id                  BIGINT       PRIMARY KEY,
                uid                 BIGINT       NOT NULL,
                currency            INT          NOT NULL,
                amount              BIGINT       NOT NULL,
                destination_address VARCHAR(42)  NOT NULL,
                tb_pending_id       BIGINT       NOT NULL UNIQUE,
                state               VARCHAR(20)  NOT NULL DEFAULT 'LOCKED'
                                        CHECK (state IN ('LOCKED','BROADCAST','CONFIRMED','VOID')),
                tx_hash             VARCHAR(66)  UNIQUE,
                nonce               BIGINT,
                raw_tx              TEXT,
                created_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
                updated_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW()
            )""")
        }

        /** Extra user for W5/W6/W7: deposits AMOUNT and returns the deposit transferId used. */
        private fun setupExtraUser(uid: Long, depositTransferId: Long) {
            settlement.ensureAccount(uid, CURRENCY)
            settlement.deposit(uid, CURRENCY, AMOUNT, depositTransferId)
        }
    }

    @BeforeEach
    fun resetMockInvocations() {
        clearInvocations(signer)
    }

    // ─────────────────────────────────────────────────────────────────────────
    // W1 — Lock reserves funds: second withdrawal rejected by TigerBeetle
    // ─────────────────────────────────────────────────────────────────────────
    @Test
    @Order(1)
    fun `W1 - lock reserves funds — second withdrawal rejected while PENDING active`() {
        service.initiate(UID, CURRENCY, AMOUNT, DEST, withdrawalId = 1L)

        // Funds are locked: spendable = 0 (debits_pending = AMOUNT, debits_posted = 0)
        assertThat(settlement.getSpendableBalance(UID, CURRENCY))
            .`as`("spendable balance must be 0 — AMOUNT is in debits_pending").isEqualTo(0L)

        // TB rejects a new debit: debits_pending + debits_posted + 1 > credits_posted
        assertThatThrownBy { service.initiate(UID, CURRENCY, 1L, DEST, withdrawalId = 2L) }
            .isInstanceOf(IllegalStateException::class.java)
            .hasMessageContaining("TigerBeetle")

        // State: 1 LOCKED row, no second row created
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM withdrawals", Long::class.java))
            .isEqualTo(1L)
        assertThat(jdbc.queryForObject("SELECT state FROM withdrawals WHERE id = 1", String::class.java))
            .isEqualTo("LOCKED")
    }

    // ─────────────────────────────────────────────────────────────────────────
    // W2 — Lock is idempotent
    // ─────────────────────────────────────────────────────────────────────────
    @Test
    @Order(2)
    fun `W2 - lock is idempotent — same withdrawalId is a TB and DB no-op`() {
        // id=1 already LOCKED from W1 — calling initiate again must be a no-op
        service.initiate(UID, CURRENCY, AMOUNT, DEST, withdrawalId = 1L)

        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM withdrawals WHERE id = 1", Long::class.java))
            .`as`("exactly one row despite two initiate() calls").isEqualTo(1L)
        assertThat(jdbc.queryForObject("SELECT state FROM withdrawals WHERE id = 1", String::class.java))
            .isEqualTo("LOCKED")
    }

    // ─────────────────────────────────────────────────────────────────────────
    // W3 — broadcastPending calls signer exactly once per LOCKED row
    // ─────────────────────────────────────────────────────────────────────────
    @Test
    @Order(3)
    fun `W3 - broadcastPending transitions LOCKED to BROADCAST and calls signer once`() {
        service.broadcastPending()

        verify(signer, times(1)).sign(eq(1L), anyString(), eq(AMOUNT), eq(CURRENCY))
        assertThat(jdbc.queryForObject("SELECT state FROM withdrawals WHERE id = 1", String::class.java))
            .isEqualTo("BROADCAST")
        assertThat(jdbc.queryForObject("SELECT tx_hash FROM withdrawals WHERE id = 1", String::class.java))
            .isNotBlank()
    }

    // ─────────────────────────────────────────────────────────────────────────
    // W4 — confirmBroadcast finalizes TB — conservation invariant
    // ─────────────────────────────────────────────────────────────────────────
    @Test
    @Order(4)
    fun `W4 - confirmBroadcast finalizes TB debit — exact amount + zero-sum conservation`() {
        // Capture external balance before confirmation to assert the exact delta
        val externalBefore = settlement.getBalance(AccountIds.SYSTEM_EXTERNAL_USER, CURRENCY)

        service.confirmBroadcast()

        assertThat(jdbc.queryForObject("SELECT state FROM withdrawals WHERE id = 1", String::class.java))
            .isEqualTo("CONFIRMED")

        val userBalance    = settlement.getBalance(UID, CURRENCY)
        val externalAfter  = settlement.getBalance(AccountIds.SYSTEM_EXTERNAL_USER, CURRENCY)

        // Exact amount: user lost AMOUNT (was AMOUNT, now 0)
        assertThat(userBalance).`as`("user balance after confirmed withdrawal of $AMOUNT").isEqualTo(0L)
        // Exact amount: external gained AMOUNT (not 400, not 600 — exactly AMOUNT)
        assertThat(externalAfter - externalBefore)
            .`as`("external account gained exactly $AMOUNT — correct amount moved").isEqualTo(AMOUNT)
        // Conservation: zero-sum across all accounts
        assertThat(userBalance + externalAfter)
            .`as`("zero-sum conservation: user + external = 0").isEqualTo(0L)
    }

    // ─────────────────────────────────────────────────────────────────────────
    // W5 — Crash-1 recovery: LOCKED row picked up by broadcastPending on restart
    // ─────────────────────────────────────────────────────────────────────────
    @Test
    @Order(5)
    fun `W5 - crash-1 recovery — LOCKED row is picked up by broadcastPending`() {
        setupExtraUser(uid = UID + 1, depositTransferId = 10L)
        // Simulate crash between initiate() and broadcastPending(): the row is LOCKED
        service.initiate(UID + 1, CURRENCY, AMOUNT, DEST, withdrawalId = 100L)

        assertThat(jdbc.queryForObject("SELECT state FROM withdrawals WHERE id = 100", String::class.java))
            .isEqualTo("LOCKED")

        // Simulate restart: broadcastPending() is the recovery path
        service.broadcastPending()

        verify(signer, times(1)).sign(eq(100L), anyString(), eq(AMOUNT), eq(CURRENCY))
        assertThat(jdbc.queryForObject("SELECT state FROM withdrawals WHERE id = 100", String::class.java))
            .isEqualTo("BROADCAST")
    }

    // ─────────────────────────────────────────────────────────────────────────
    // W6 — Crash-2 recovery: BROADCAST row confirms without calling signer
    //
    // This is the key anti-double-diffusion proof.
    // The BROADCAST state is produced by the real path (initiate → broadcastPending),
    // not fabricated manually. Then confirmBroadcast() is called as the crash-2 recovery.
    // The signer must never be called during confirmation.
    // ─────────────────────────────────────────────────────────────────────────
    @Test
    @Order(6)
    fun `W6 - crash-2 recovery — BROADCAST row confirmed without calling signer`() {
        setupExtraUser(uid = UID + 2, depositTransferId = 11L)
        service.initiate(UID + 2, CURRENCY, AMOUNT, DEST, withdrawalId = 200L)
        service.broadcastPending()  // real path — produces BROADCAST state

        assertThat(jdbc.queryForObject("SELECT state FROM withdrawals WHERE id = 200", String::class.java))
            .isEqualTo("BROADCAST")

        // Simulate crash-2: process restarted after broadcast, before confirmation.
        // confirmBroadcast() is the recovery entry point.
        clearInvocations(signer)
        service.confirmBroadcast()

        assertThat(jdbc.queryForObject("SELECT state FROM withdrawals WHERE id = 200", String::class.java))
            .isEqualTo("CONFIRMED")
        // THE KEY ASSERTION: signer must never be called during confirmation
        verify(signer, never()).sign(anyLong(), anyString(), anyLong(), anyInt())
    }

    // ─────────────────────────────────────────────────────────────────────────
    // W7 — voidWithdrawal returns funds exactly — void conservation invariant
    // ─────────────────────────────────────────────────────────────────────────
    @Test
    @Order(7)
    fun `W7 - voidWithdrawal restores full balance — void conservation invariant`() {
        setupExtraUser(uid = UID + 3, depositTransferId = 12L)
        service.initiate(UID + 3, CURRENCY, AMOUNT, DEST, withdrawalId = 300L)

        // Funds are locked after initiate
        assertThat(settlement.getSpendableBalance(UID + 3, CURRENCY))
            .`as`("spendable balance must be 0 after initiate — funds locked").isEqualTo(0L)

        service.voidWithdrawal(300L)

        // Full restoration: spendable returns to AMOUNT (pending debit released)
        assertThat(settlement.getSpendableBalance(UID + 3, CURRENCY))
            .`as`("spendable balance fully restored after void").isEqualTo(AMOUNT)
        // Posted balance is unchanged (PENDING never became POSTED)
        assertThat(settlement.getBalance(UID + 3, CURRENCY))
            .`as`("posted balance unchanged by void — no funds were debited").isEqualTo(AMOUNT)
        // DB state
        assertThat(jdbc.queryForObject("SELECT state FROM withdrawals WHERE id = 300", String::class.java))
            .isEqualTo("VOID")
    }
}
