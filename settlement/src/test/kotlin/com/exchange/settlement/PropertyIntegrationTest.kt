package com.exchange.settlement

import com.exchange.common.OrderSide
import com.exchange.common.TradeEvent
import com.tigerbeetle.Client
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.springframework.dao.DataIntegrityViolationException
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.datasource.DriverManagerDataSource
import org.testcontainers.containers.PostgreSQLContainer

/**
 * Property integration tests — real TigerBeetle + real PostgreSQL, no mocks.
 *
 * What is proved:
 *   P1 — Emission invariant: after createProperty(totalTokens=N),
 *        TB balance of SYSTEM_PROPERTY_OWNER_USER == N and property_holders
 *        has exactly one row (owner, N).
 *   P2 — Token conservation: after owner sells a fraction to a buyer via settleTrade,
 *        owner_balance + buyer_balance == N in both TB and property_holders after sync.
 *   P3 — Projection fidelity: property_holders is stale before syncHolder and correct
 *        after — proves it is a manually-synced projection, not an auto-maintained mirror.
 *   P4 — Jurisdiction link: property.jurisdiction is stored and read back correctly.
 *
 * TigerBeetle accounts cannot be deleted between tests, so each test uses a
 * distinct property_ledger_id (2001–2004) to isolate TB state.
 *
 * Run: ./gradlew :settlement:propertyTest
 */
@Tag("property")
class PropertyIntegrationTest {

    companion object {
        const val STABLECOIN_LEDGER = 100

        // Distinct ledger / symbol IDs per test — TB accounts cannot be cleared between tests
        const val LEDGER_P1 = 2001;  const val SYM_P1 = 2001
        const val LEDGER_P2 = 2002;  const val SYM_P2 = 2002
        const val LEDGER_P3 = 2003;  const val SYM_P3 = 2003
        const val LEDGER_P4 = 2004;  const val SYM_P4 = 2004
        const val LEDGER_P5 = 2005;  const val SYM_P5 = 2005

        const val BUYER_UID = 9001L
        const val USER_P3   = 9002L

        @Suppress("UNCHECKED_CAST")
        private val pg = PostgreSQLContainer("postgres:16-alpine") as PostgreSQLContainer<*>
        private val tbContainer = TigerBeetleContainer()

        private lateinit var jdbc: JdbcTemplate
        private lateinit var tbClient: Client
        private lateinit var settlementService: SettlementService
        private lateinit var propertyService: PropertyService

        @JvmStatic
        @BeforeAll
        fun setup() {
            tbContainer.start()
            pg.start()

            jdbc = JdbcTemplate(DriverManagerDataSource(pg.jdbcUrl, pg.username, pg.password))
            tbClient = Client(ByteArray(16) /* cluster-id = 0 */, arrayOf(tbContainer.address))

            createSchema()

            settlementService = SettlementService(tbClient)
            propertyService   = PropertyService(settlementService, jdbc)

            // Stablecoin system accounts are shared across all property tests
            settlementService.ensureSystemAccounts(STABLECOIN_LEDGER)
        }

        @JvmStatic
        @AfterAll
        fun teardown() {
            tbClient.close()
            tbContainer.close()
            pg.stop()
        }

        /**
         * Identical to the production DDL in infra/timescale-init/05-properties.sql.
         * Any divergence is a bug: the constraints tested here must be the constraints
         * that run in production.
         */
        private fun createSchema() {
            jdbc.execute("""
                CREATE TABLE IF NOT EXISTS properties (
                    id                 BIGSERIAL    PRIMARY KEY,
                    name               VARCHAR(255) NOT NULL,
                    jurisdiction       VARCHAR(10)  NOT NULL,
                    property_ledger_id INT          NOT NULL UNIQUE,
                    total_tokens       BIGINT       NOT NULL CHECK (total_tokens > 0)
                )
            """.trimIndent())
            jdbc.execute("""
                CREATE TABLE IF NOT EXISTS symbols (
                    id               INT    PRIMARY KEY,
                    property_id      BIGINT NOT NULL REFERENCES properties(id),
                    base_ledger_id   INT    NOT NULL,
                    quote_ledger_id  INT    NOT NULL
                )
            """.trimIndent())
            jdbc.execute("""
                CREATE TABLE IF NOT EXISTS property_holders (
                    property_id      BIGINT      NOT NULL REFERENCES properties(id),
                    uid              BIGINT      NOT NULL,
                    balance_snapshot BIGINT      NOT NULL DEFAULT 0,
                    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (property_id, uid)
                )
            """.trimIndent())
        }
    }

    @BeforeEach
    fun cleanTables() {
        // Delete in dependency order (FK constraints)
        jdbc.execute("DELETE FROM property_holders")
        jdbc.execute("DELETE FROM symbols")
        jdbc.execute("DELETE FROM properties")
    }

    // ── P1 — Emission invariant ────────────────────────────────────────────

    @Test
    fun `P1 owner balance equals total tokens after property creation`() {
        val propId = propertyService.createProperty(
            name = "Burj Al Arab",
            jurisdiction = "AE-AZ",
            propertyLedgerId = LEDGER_P1,
            quoteLedgerId = STABLECOIN_LEDGER,
            symbolId = SYM_P1,
            totalTokens = 1_000_000L
        )

        // TB: issuer holds 100% of tokens
        val ownerBalance = settlementService.getBalance(AccountIds.SYSTEM_PROPERTY_OWNER_USER, LEDGER_P1)
        assertThat(ownerBalance).isEqualTo(1_000_000L)

        // DB: exactly one holder row with the full amount
        val holders = jdbc.queryForList(
            "SELECT uid, balance_snapshot FROM property_holders WHERE property_id = ?", propId
        )
        assertThat(holders).hasSize(1)
        assertThat(holders[0]["uid"]).isEqualTo(AccountIds.SYSTEM_PROPERTY_OWNER_USER)
        assertThat(holders[0]["balance_snapshot"] as Long).isEqualTo(1_000_000L)
    }

    // ── P2 — Token conservation after trade ───────────────────────────────

    @Test
    fun `P2 token sum conserved in TB and property_holders after owner sells fraction`() {
        val totalTokens = 1_000_000L
        val saleQty     = 300_000L
        val price       = 100L    // stablecoin units per token

        val propId = propertyService.createProperty(
            name = "Marina Tower",
            jurisdiction = "AE-DU",
            propertyLedgerId = LEDGER_P2,
            quoteLedgerId = STABLECOIN_LEDGER,
            symbolId = SYM_P2,
            totalTokens = totalTokens
        )

        // Fund buyer: needs price * saleQty stablecoin to pay the owner
        settlementService.ensureAccount(BUYER_UID, STABLECOIN_LEDGER)
        settlementService.ensureAccount(BUYER_UID, LEDGER_P2)
        settlementService.deposit(BUYER_UID, STABLECOIN_LEDGER, price * saleQty, 99_001L)

        // Owner needs a stablecoin account to receive payment (token accounts exist from createProperty)
        settlementService.ensureAccount(AccountIds.SYSTEM_PROPERTY_OWNER_USER, STABLECOIN_LEDGER)

        // Trade: buyer (taker BID) buys saleQty tokens from owner (maker)
        val trade = TradeEvent(
            tradeId      = 20020L,
            symbolId     = SYM_P2,
            makerOrderId = 1L, takerOrderId = 2L,
            makerUserId  = AccountIds.SYSTEM_PROPERTY_OWNER_USER,
            takerUserId  = BUYER_UID,
            price        = price,
            quantity     = saleQty,
            takerSide    = OrderSide.BID,
            timestampNs  = 0L
        )
        settlementService.settleTrade(trade, LEDGER_P2, STABLECOIN_LEDGER)

        // Sync projection for both participants
        propertyService.syncHolder(propId, AccountIds.SYSTEM_PROPERTY_OWNER_USER, LEDGER_P2)
        propertyService.syncHolder(propId, BUYER_UID, LEDGER_P2)

        // TB: individual balances are correct and sum to total_tokens
        val ownerTb = settlementService.getBalance(AccountIds.SYSTEM_PROPERTY_OWNER_USER, LEDGER_P2)
        val buyerTb = settlementService.getBalance(BUYER_UID, LEDGER_P2)
        assertThat(ownerTb).`as`("owner TB balance").isEqualTo(totalTokens - saleQty)
        assertThat(buyerTb).`as`("buyer TB balance").isEqualTo(saleQty)
        assertThat(ownerTb + buyerTb).`as`("TB sum = total_tokens").isEqualTo(totalTokens)

        // DB: property_holders sum equals total_tokens
        val dbSum = jdbc.queryForObject(
            "SELECT SUM(balance_snapshot) FROM property_holders WHERE property_id = ?",
            Long::class.java, propId
        )!!
        assertThat(dbSum).`as`("property_holders sum = total_tokens").isEqualTo(totalTokens)
    }

    // ── P3 — Projection fidelity ──────────────────────────────────────────

    @Test
    fun `P3 syncHolder writes TB balance to property_holders - stale before sync correct after`() {
        val totalTokens = 500_000L
        val saleQty     = 200_000L
        val price       = 50L

        val propId = propertyService.createProperty(
            name = "Saadiyat Villa",
            jurisdiction = "AE-AZ",
            propertyLedgerId = LEDGER_P3,
            quoteLedgerId = STABLECOIN_LEDGER,
            symbolId = SYM_P3,
            totalTokens = totalTokens
        )

        // Initial state: projection shows the full balance (set by createProperty)
        val initial = jdbc.queryForObject(
            "SELECT balance_snapshot FROM property_holders WHERE property_id = ? AND uid = ?",
            Long::class.java, propId, AccountIds.SYSTEM_PROPERTY_OWNER_USER
        )!!
        assertThat(initial).`as`("initial snapshot").isEqualTo(totalTokens)

        // Trade: owner sells saleQty tokens to USER_P3
        settlementService.ensureAccount(USER_P3, LEDGER_P3)
        settlementService.ensureAccount(USER_P3, STABLECOIN_LEDGER)
        settlementService.ensureAccount(AccountIds.SYSTEM_PROPERTY_OWNER_USER, STABLECOIN_LEDGER)
        settlementService.deposit(USER_P3, STABLECOIN_LEDGER, price * saleQty, 99_002L)

        settlementService.settleTrade(
            TradeEvent(
                tradeId      = 20030L,
                symbolId     = SYM_P3,
                makerOrderId = 1L, takerOrderId = 2L,
                makerUserId  = AccountIds.SYSTEM_PROPERTY_OWNER_USER,
                takerUserId  = USER_P3,
                price        = price,
                quantity     = saleQty,
                takerSide    = OrderSide.BID,
                timestampNs  = 0L
            ),
            LEDGER_P3, STABLECOIN_LEDGER
        )

        // BEFORE syncHolder: projection is stale (still shows old balance)
        val stale = jdbc.queryForObject(
            "SELECT balance_snapshot FROM property_holders WHERE property_id = ? AND uid = ?",
            Long::class.java, propId, AccountIds.SYSTEM_PROPERTY_OWNER_USER
        )!!
        assertThat(stale).`as`("stale snapshot before sync").isEqualTo(totalTokens)

        // AFTER syncHolder: projection matches TB exactly
        propertyService.syncHolder(propId, AccountIds.SYSTEM_PROPERTY_OWNER_USER, LEDGER_P3)
        val fresh = jdbc.queryForObject(
            "SELECT balance_snapshot FROM property_holders WHERE property_id = ? AND uid = ?",
            Long::class.java, propId, AccountIds.SYSTEM_PROPERTY_OWNER_USER
        )!!
        val tbBalance = settlementService.getBalance(AccountIds.SYSTEM_PROPERTY_OWNER_USER, LEDGER_P3)
        assertThat(fresh).`as`("fresh snapshot = totalTokens - saleQty").isEqualTo(totalTokens - saleQty)
        assertThat(fresh).`as`("fresh snapshot = TB balance").isEqualTo(tbBalance)
    }

    // ── P5 — Double-creation idempotence (B7 proof) ───────────────────────

    /**
     * Proves that re-delivering the same CREATE_PROPERTY command (same property_ledger_id)
     * does NOT double-emit tokens.
     *
     * Mechanism:
     *   1st call : DB INSERT succeeds, TB emission transfer fires — balance = totalTokens.
     *   2nd call : DB INSERT fails with DataIntegrityViolationException (UNIQUE on
     *              property_ledger_id) — TB deposit is never reached, so no second emission.
     *
     * PropertyCommandConsumer catches DataIntegrityViolationException and acks (re-delivery
     * treated as already-processed). The TB balance invariant is preserved.
     */
    @Test
    fun `P5 double createProperty on same ledger - DB uniqueness prevents double token emission`() {
        val totalTokens = 200_000L

        // First creation: succeeds, tokens emitted
        propertyService.createProperty(
            name = "Idempotence Tower",
            jurisdiction = "AE-AZ",
            propertyLedgerId = LEDGER_P5,
            quoteLedgerId = STABLECOIN_LEDGER,
            symbolId = SYM_P5,
            totalTokens = totalTokens
        )
        val balanceAfterFirst = settlementService.getBalance(AccountIds.SYSTEM_PROPERTY_OWNER_USER, LEDGER_P5)
        assertThat(balanceAfterFirst).`as`("balance after first creation = totalTokens").isEqualTo(totalTokens)

        // Second creation (re-delivery simulation): DB UNIQUE constraint fires
        assertThatThrownBy {
            propertyService.createProperty(
                name = "Idempotence Tower",
                jurisdiction = "AE-AZ",
                propertyLedgerId = LEDGER_P5,
                quoteLedgerId = STABLECOIN_LEDGER,
                symbolId = SYM_P5,
                totalTokens = totalTokens
            )
        }.isInstanceOf(DataIntegrityViolationException::class.java)

        // TB balance is unchanged: no double emission
        val balanceAfterSecond = settlementService.getBalance(AccountIds.SYSTEM_PROPERTY_OWNER_USER, LEDGER_P5)
        assertThat(balanceAfterSecond).`as`("balance after re-delivery = still totalTokens").isEqualTo(totalTokens)
    }

    // ── P4 — Jurisdiction link ────────────────────────────────────────────

    @Test
    fun `P4 property carries its jurisdiction - stored and read back correctly`() {
        val propId = propertyService.createProperty(
            name = "Dubai Hills Villa",
            jurisdiction = "AE-DU",
            propertyLedgerId = LEDGER_P4,
            quoteLedgerId = STABLECOIN_LEDGER,
            symbolId = SYM_P4,
            totalTokens = 100_000L
        )

        val row = jdbc.queryForMap(
            "SELECT name, jurisdiction, property_ledger_id, total_tokens FROM properties WHERE id = ?", propId
        )
        assertThat(row["name"]).isEqualTo("Dubai Hills Villa")
        assertThat(row["jurisdiction"]).isEqualTo("AE-DU")
        assertThat(row["property_ledger_id"]).isEqualTo(LEDGER_P4)
        assertThat(row["total_tokens"] as Long).isEqualTo(100_000L)

        // Symbol is also wired to the property
        val symRow = jdbc.queryForMap("SELECT property_id, base_ledger_id FROM symbols WHERE id = ?", SYM_P4)
        assertThat(symRow["property_id"]).isEqualTo(propId)
        assertThat(symRow["base_ledger_id"]).isEqualTo(LEDGER_P4)
    }
}
