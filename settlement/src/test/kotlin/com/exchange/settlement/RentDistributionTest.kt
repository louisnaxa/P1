package com.exchange.settlement

import com.tigerbeetle.Client
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.datasource.DriverManagerDataSource
import org.testcontainers.containers.PostgreSQLContainer

/**
 * Rent distribution integration tests — real TigerBeetle + real PostgreSQL, no mocks.
 *
 * What is proved (money-path — distribution d'argent réel) :
 *   L1 — Montant exact : chaque CITIZEN_APPROVED reçoit la part exacte proportionnelle à ses
 *        tokens parmi les détenteurs éligibles.
 *   L2 — Étranger reçoit zéro : FOREIGN_SPECULATIVE avec des tokens n'obtient aucun stablecoin.
 *   L3 — Mauvaise juridiction reçoit zéro : CITIZEN_APPROVED(AE-DU) ne touche pas les loyers
 *        d'un bien AE-AZ — le contrôle est ré-appliqué à l'instant du paiement (défense en profondeur).
 *   L4 — Conservation : pool_avant = somme(gains détenteurs) + pool_après. Aucun stablecoin créé ni perdu.
 *   L5 — Idempotence : deux appels avec la même distributionKey → TB Exists → paiement unique (pas de double).
 *   L6 — Reste : si rentAmount ne se divise pas exactement, le reste demeure dans le pool ;
 *        somme_distribuée + reste = total_amount.
 *
 * Deposit transfer IDs use dedicated ranges (8_001–8_099) to avoid collisions with
 * PropertyIntegrationTest (99_001–99_002) and trade IDs (20_000 range).
 *
 * Run: ./gradlew :settlement:rentTest
 */
@Tag("rent")
class RentDistributionTest {

    companion object {
        const val STABLECOIN_LEDGER = 100

        // One distinct property ledger per test — TB accounts cannot be deleted between tests
        const val LEDGER_L1 = 5001; const val SYM_L1 = 5001
        const val LEDGER_L2 = 5002; const val SYM_L2 = 5002
        const val LEDGER_L3 = 5003; const val SYM_L3 = 5003
        const val LEDGER_L4 = 5004; const val SYM_L4 = 5004
        const val LEDGER_L5 = 5005; const val SYM_L5 = 5005
        const val LEDGER_L6 = 5006; const val SYM_L6 = 5006

        // User IDs — well below RENT_POOL_UID_BASE (10^9), above system UIDs (0–2)
        const val CITIZEN_AZ_1 = 6001L  // CITIZEN_APPROVED, jurisdiction = AE-AZ
        const val CITIZEN_AZ_2 = 6002L  // CITIZEN_APPROVED, jurisdiction = AE-AZ
        const val FOREIGN      = 6003L  // FOREIGN_SPECULATIVE
        const val CITIZEN_DU   = 6004L  // CITIZEN_APPROVED, jurisdiction = AE-DU

        @Suppress("UNCHECKED_CAST")
        private val pg = PostgreSQLContainer("postgres:16-alpine") as PostgreSQLContainer<*>
        private val tbContainer = TigerBeetleContainer()

        private lateinit var jdbc: JdbcTemplate
        private lateinit var tbClient: Client
        private lateinit var settlementService: SettlementService
        private lateinit var propertyService: PropertyService
        private lateinit var rentService: RentService

        @JvmStatic
        @BeforeAll
        fun setup() {
            tbContainer.start()
            pg.start()
            jdbc = JdbcTemplate(DriverManagerDataSource(pg.jdbcUrl, pg.username, pg.password))
            tbClient = Client(ByteArray(16), arrayOf(tbContainer.address))
            createSchema()
            settlementService = SettlementService(tbClient)
            propertyService   = PropertyService(settlementService, jdbc)
            rentService       = RentService(settlementService, jdbc)
            settlementService.ensureSystemAccounts(STABLECOIN_LEDGER)
            seedUsers()
        }

        @JvmStatic
        @AfterAll
        fun teardown() {
            tbClient.close()
            tbContainer.close()
            pg.stop()
        }

        /**
         * Identical DDL to production:
         *   02-users.sql, 05-properties.sql, 06-rent.sql
         * Any divergence here is a bug.
         */
        private fun createSchema() {
            jdbc.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    keycloak_sub      VARCHAR(255) NOT NULL PRIMARY KEY,
                    internal_uid      BIGINT       NOT NULL UNIQUE,
                    account_status    VARCHAR(30)  NOT NULL DEFAULT 'UNVERIFIED'
                                          CHECK (account_status IN ('UNVERIFIED','FOREIGN_SPECULATIVE','CITIZEN_APPROVED','SUSPENDED')),
                    jurisdiction      VARCHAR(10),
                    status_updated_at TIMESTAMPTZ,
                    status_updated_by VARCHAR(255),
                    CONSTRAINT chk_citizen_jurisdiction
                        CHECK (account_status <> 'CITIZEN_APPROVED' OR jurisdiction IS NOT NULL)
                )
            """.trimIndent())
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
            jdbc.execute("""
                CREATE TABLE IF NOT EXISTS rent_distributions (
                    id                BIGSERIAL    PRIMARY KEY,
                    property_id       BIGINT       NOT NULL REFERENCES properties(id),
                    quote_ledger_id   INT          NOT NULL,
                    total_amount      BIGINT       NOT NULL CHECK (total_amount > 0),
                    distribution_key  BIGINT       NOT NULL,
                    distributed_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
                    remainder         BIGINT       NOT NULL DEFAULT 0 CHECK (remainder >= 0),
                    UNIQUE (property_id, distribution_key)
                )
            """.trimIndent())
        }

        private fun seedUsers() {
            jdbc.update(
                "INSERT INTO users (keycloak_sub, internal_uid, account_status, jurisdiction) VALUES (?,?,?,?)",
                "sub-az-1", CITIZEN_AZ_1, "CITIZEN_APPROVED", "AE-AZ"
            )
            jdbc.update(
                "INSERT INTO users (keycloak_sub, internal_uid, account_status, jurisdiction) VALUES (?,?,?,?)",
                "sub-az-2", CITIZEN_AZ_2, "CITIZEN_APPROVED", "AE-AZ"
            )
            jdbc.update(
                "INSERT INTO users (keycloak_sub, internal_uid, account_status, jurisdiction) VALUES (?,?,?,?)",
                "sub-foreign", FOREIGN, "FOREIGN_SPECULATIVE", null
            )
            jdbc.update(
                "INSERT INTO users (keycloak_sub, internal_uid, account_status, jurisdiction) VALUES (?,?,?,?)",
                "sub-du", CITIZEN_DU, "CITIZEN_APPROVED", "AE-DU"
            )
        }
    }

    @BeforeEach
    fun cleanTables() {
        // Delete in FK dependency order; users are NOT cleaned (shared, status unchanged)
        jdbc.execute("DELETE FROM rent_distributions")
        jdbc.execute("DELETE FROM property_holders")
        jdbc.execute("DELETE FROM symbols")
        jdbc.execute("DELETE FROM properties")
    }

    // ── L1 — Montant exact par agréé ────────────────────────────────────────────

    @Test
    fun `L1 each eligible holder receives exact proportional share`() {
        val propId = propertyService.createProperty("L1 Prop", "AE-AZ", LEDGER_L1, STABLECOIN_LEDGER, SYM_L1, 1_000L)

        settlementService.ensureAccount(CITIZEN_AZ_1, LEDGER_L1)
        settlementService.ensureAccount(CITIZEN_AZ_2, LEDGER_L1)
        settlementService.deposit(CITIZEN_AZ_1, LEDGER_L1, 300L, 8_001L)
        settlementService.deposit(CITIZEN_AZ_2, LEDGER_L1, 700L, 8_002L)
        rentService.depositRent(propId, STABLECOIN_LEDGER, 1_000L, 8_003L)

        val az1Before = settlementService.getBalance(CITIZEN_AZ_1, STABLECOIN_LEDGER)
        val az2Before = settlementService.getBalance(CITIZEN_AZ_2, STABLECOIN_LEDGER)

        rentService.distributeRent(propId, distributionKey = 1L)

        assertThat(settlementService.getBalance(CITIZEN_AZ_1, STABLECOIN_LEDGER) - az1Before)
            .`as`("AZ_1: 1000 * 300/1000 = 300").isEqualTo(300L)
        assertThat(settlementService.getBalance(CITIZEN_AZ_2, STABLECOIN_LEDGER) - az2Before)
            .`as`("AZ_2: 1000 * 700/1000 = 700").isEqualTo(700L)
    }

    // ── L2 — Étranger reçoit zéro ────────────────────────────────────────────────

    @Test
    fun `L2 FOREIGN_SPECULATIVE holder receives zero rent`() {
        val propId = propertyService.createProperty("L2 Prop", "AE-AZ", LEDGER_L2, STABLECOIN_LEDGER, SYM_L2, 1_000L)

        settlementService.ensureAccount(CITIZEN_AZ_1, LEDGER_L2)
        settlementService.ensureAccount(FOREIGN, LEDGER_L2)
        settlementService.deposit(CITIZEN_AZ_1, LEDGER_L2, 800L, 8_011L)
        settlementService.deposit(FOREIGN, LEDGER_L2, 200L, 8_012L)
        rentService.depositRent(propId, STABLECOIN_LEDGER, 1_000L, 8_013L)
        settlementService.ensureAccount(FOREIGN, STABLECOIN_LEDGER)

        val foreignBefore = settlementService.getBalance(FOREIGN, STABLECOIN_LEDGER)
        val az1Before     = settlementService.getBalance(CITIZEN_AZ_1, STABLECOIN_LEDGER)

        rentService.distributeRent(propId, distributionKey = 2L)

        // AZ_1 is the only eligible holder → receives 100% of rent
        assertThat(settlementService.getBalance(CITIZEN_AZ_1, STABLECOIN_LEDGER) - az1Before)
            .`as`("AZ_1 receives 100% — only eligible holder").isEqualTo(1_000L)
        // FOREIGN stablecoin balance is unchanged
        assertThat(settlementService.getBalance(FOREIGN, STABLECOIN_LEDGER))
            .`as`("FOREIGN stablecoin unchanged").isEqualTo(foreignBefore)
    }

    // ── L3 — Mauvaise juridiction reçoit zéro ────────────────────────────────────

    @Test
    fun `L3 CITIZEN_APPROVED wrong jurisdiction receives zero - defense in depth`() {
        val propId = propertyService.createProperty("L3 Prop", "AE-AZ", LEDGER_L3, STABLECOIN_LEDGER, SYM_L3, 1_000L)

        // AE-DU citizen somehow holds AE-AZ tokens (e.g. via bug or direct admin credit bypassing B3)
        settlementService.ensureAccount(CITIZEN_AZ_1, LEDGER_L3)
        settlementService.ensureAccount(CITIZEN_DU, LEDGER_L3)
        settlementService.deposit(CITIZEN_AZ_1, LEDGER_L3, 600L, 8_021L)
        settlementService.deposit(CITIZEN_DU, LEDGER_L3, 400L, 8_022L)
        rentService.depositRent(propId, STABLECOIN_LEDGER, 1_000L, 8_023L)
        settlementService.ensureAccount(CITIZEN_DU, STABLECOIN_LEDGER)

        val duBefore  = settlementService.getBalance(CITIZEN_DU, STABLECOIN_LEDGER)
        val az1Before = settlementService.getBalance(CITIZEN_AZ_1, STABLECOIN_LEDGER)

        rentService.distributeRent(propId, distributionKey = 3L)

        // AZ_1 is the only eligible holder (jurisdiction matches) → 100% of rent
        assertThat(settlementService.getBalance(CITIZEN_AZ_1, STABLECOIN_LEDGER) - az1Before)
            .`as`("AZ_1 receives 100% — jurisdiction AE-AZ matches property").isEqualTo(1_000L)
        // CITIZEN_DU(AE-DU) gets nothing — jurisdiction mismatch re-verified at pay time
        assertThat(settlementService.getBalance(CITIZEN_DU, STABLECOIN_LEDGER))
            .`as`("CITIZEN_DU(AE-DU) stablecoin unchanged").isEqualTo(duBefore)
    }

    // ── L4 — Conservation ────────────────────────────────────────────────────────

    @Test
    fun `L4 rent is conserved - pool_before equals sum of holder gains plus remainder`() {
        val propId = propertyService.createProperty("L4 Prop", "AE-AZ", LEDGER_L4, STABLECOIN_LEDGER, SYM_L4, 1_000L)

        settlementService.ensureAccount(CITIZEN_AZ_1, LEDGER_L4)
        settlementService.ensureAccount(CITIZEN_AZ_2, LEDGER_L4)
        settlementService.deposit(CITIZEN_AZ_1, LEDGER_L4, 400L, 8_031L)
        settlementService.deposit(CITIZEN_AZ_2, LEDGER_L4, 600L, 8_032L)
        rentService.depositRent(propId, STABLECOIN_LEDGER, 1_000L, 8_033L)

        val poolUid   = AccountIds.RENT_POOL_UID_BASE + propId
        val poolBefore = settlementService.getBalance(poolUid, STABLECOIN_LEDGER)
        val az1Before  = settlementService.getBalance(CITIZEN_AZ_1, STABLECOIN_LEDGER)
        val az2Before  = settlementService.getBalance(CITIZEN_AZ_2, STABLECOIN_LEDGER)

        rentService.distributeRent(propId, distributionKey = 4L)

        val az1Gain   = settlementService.getBalance(CITIZEN_AZ_1, STABLECOIN_LEDGER) - az1Before
        val az2Gain   = settlementService.getBalance(CITIZEN_AZ_2, STABLECOIN_LEDGER) - az2Before
        val poolAfter = settlementService.getBalance(poolUid, STABLECOIN_LEDGER)

        // Conservation: no stablecoin created or destroyed
        assertThat(az1Gain + az2Gain + poolAfter)
            .`as`("conservation: gains + remainder = pool_before").isEqualTo(poolBefore)
    }

    // ── L5 — Idempotence ─────────────────────────────────────────────────────────

    @Test
    fun `L5 distributeRent with same distributionKey is idempotent - holder paid exactly once`() {
        val propId = propertyService.createProperty("L5 Prop", "AE-AZ", LEDGER_L5, STABLECOIN_LEDGER, SYM_L5, 1_000L)

        settlementService.ensureAccount(CITIZEN_AZ_1, LEDGER_L5)
        settlementService.deposit(CITIZEN_AZ_1, LEDGER_L5, 1_000L, 8_041L)
        rentService.depositRent(propId, STABLECOIN_LEDGER, 500L, 8_042L)

        val az1Before = settlementService.getBalance(CITIZEN_AZ_1, STABLECOIN_LEDGER)

        rentService.distributeRent(propId, distributionKey = 5L)
        val afterFirst = settlementService.getBalance(CITIZEN_AZ_1, STABLECOIN_LEDGER)
        assertThat(afterFirst - az1Before).`as`("first distribution: 500 paid").isEqualTo(500L)

        // Same distributionKey → TB Exists for all transfers → no double payment
        rentService.distributeRent(propId, distributionKey = 5L)
        assertThat(settlementService.getBalance(CITIZEN_AZ_1, STABLECOIN_LEDGER))
            .`as`("idempotent: balance unchanged after second call").isEqualTo(afterFirst)
    }

    // ── L6 — Reste dans le pool ───────────────────────────────────────────────────

    @Test
    fun `L6 undivided remainder stays in pool and distributed plus remainder equals total`() {
        val propId = propertyService.createProperty("L6 Prop", "AE-AZ", LEDGER_L6, STABLECOIN_LEDGER, SYM_L6, 1_000L)

        // 1 token : 2 tokens → rent of 10 yields 3 + 6 with remainder 1
        settlementService.ensureAccount(CITIZEN_AZ_1, LEDGER_L6)
        settlementService.ensureAccount(CITIZEN_AZ_2, LEDGER_L6)
        settlementService.deposit(CITIZEN_AZ_1, LEDGER_L6, 1L, 8_051L)
        settlementService.deposit(CITIZEN_AZ_2, LEDGER_L6, 2L, 8_052L)
        rentService.depositRent(propId, STABLECOIN_LEDGER, 10L, 8_053L)

        val poolUid   = AccountIds.RENT_POOL_UID_BASE + propId
        val az1Before = settlementService.getBalance(CITIZEN_AZ_1, STABLECOIN_LEDGER)
        val az2Before = settlementService.getBalance(CITIZEN_AZ_2, STABLECOIN_LEDGER)

        val distributionId = rentService.distributeRent(propId, distributionKey = 6L)

        val az1Gain   = settlementService.getBalance(CITIZEN_AZ_1, STABLECOIN_LEDGER) - az1Before
        val az2Gain   = settlementService.getBalance(CITIZEN_AZ_2, STABLECOIN_LEDGER) - az2Before
        val poolAfter = settlementService.getBalance(poolUid, STABLECOIN_LEDGER)

        // Exact shares (integer division)
        assertThat(az1Gain).`as`("AZ_1: 10 * 1/3 = 3").isEqualTo(3L)
        assertThat(az2Gain).`as`("AZ_2: 10 * 2/3 = 6").isEqualTo(6L)

        // Remainder stays in pool
        assertThat(poolAfter).`as`("remainder = 1 stays in pool").isEqualTo(1L)

        // DB records the remainder
        val dbRemainder = jdbc.queryForObject(
            "SELECT remainder FROM rent_distributions WHERE id = ?",
            Long::class.java, distributionId
        )!!
        assertThat(dbRemainder).`as`("DB remainder = 1").isEqualTo(1L)

        // Full conservation
        assertThat(az1Gain + az2Gain + poolAfter)
            .`as`("distributed + remainder = total_amount").isEqualTo(10L)
    }
}
