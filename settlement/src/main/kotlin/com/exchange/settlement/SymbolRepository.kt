package com.exchange.settlement

import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Component

/**
 * Resolves a symbolId (exchange-core) to the pair of TigerBeetle ledger IDs
 * (base = property token ledger, quote = stablecoin ledger) via the `symbols` table.
 *
 * Replaces the hardcoded baseLedger=10/quoteLedger=11 TODO in TradeConsumer.
 * An unknown symbolId is a configuration error — fails fast.
 */
@Component
class SymbolRepository(private val jdbc: JdbcTemplate) {

    data class SymbolLedgers(val baseLedgerId: Int, val quoteLedgerId: Int)

    fun getLedgers(symbolId: Int): SymbolLedgers =
        jdbc.queryForObject(
            "SELECT base_ledger_id, quote_ledger_id FROM symbols WHERE id = ?",
            { rs, _ -> SymbolLedgers(rs.getInt("base_ledger_id"), rs.getInt("quote_ledger_id")) },
            symbolId
        ) ?: throw IllegalStateException("Unknown symbolId: $symbolId — add a row to the symbols table")
}
