package com.exchange.gateway

import com.exchange.common.PropertyCommand
import org.springframework.dao.EmptyResultDataAccessException
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.security.core.Authentication
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken
import org.springframework.web.bind.annotation.*
import org.springframework.web.server.ResponseStatusException

data class CreatePropertyRequest(
    val name: String,
    val jurisdiction: String,
    val propertyLedgerId: Int,
    val quoteLedgerId: Int,
    val symbolId: Int,
    val totalTokens: Long,
    // Metadata (habillage — presentation only)
    val description: String = "",
    val location: String = ""
)

data class PropertyResponse(
    val propertyLedgerId: Int,
    val name: String,
    val jurisdiction: String,
    val totalTokens: Long
)

@RestController
@RequestMapping("/properties")
class PropertyController(
    private val publisher: CommandPublisher,
    private val jdbc: JdbcTemplate
) {

    /**
     * Creates a property by publishing a CREATE_PROPERTY command to the
     * property_commands topic. Returns 202 after the broker confirms the write.
     * Settlement processes the command asynchronously.
     *
     * Access control (money-path — enforced BEFORE publication, same principle as B3):
     *   - ROLE_exchange-admin: may create properties in any jurisdiction.
     *   - ROLE_subsidiary: jurisdiction in the request body MUST match the
     *     "subsidiary_jurisdiction" JWT claim (uid-from-token pattern — a filiale
     *     cannot register a property in another filiale's jurisdiction).
     *
     * Bounds validations (gateway, before durability boundary):
     *   - name: non-blank
     *   - totalTokens: > 0
     *   - propertyLedgerId: 1..16_777_215 (24-bit, see TD-14)
     *   - quoteLedgerId: > 0
     *   - symbolId: > 0
     *   - jurisdiction: non-blank
     *
     * On any failure: 400/403 returned, nothing published to Kafka.
     */
    @PostMapping
    fun createProperty(
        @RequestBody req: CreatePropertyRequest,
        authentication: Authentication
    ): ResponseEntity<Map<String, Int>> {
        val isAdmin = authentication.authorities.any { it.authority == "ROLE_exchange-admin" }

        if (!isAdmin) {
            // Subsidiary: jurisdiction in body must match the JWT "subsidiary_jurisdiction" claim.
            // Reading from the token, never from the request body — mirrors B3's uid-from-token pattern.
            val jwtJurisdiction = subsidiaryJurisdiction(authentication)
            if (jwtJurisdiction == null || jwtJurisdiction != req.jurisdiction) {
                throw ResponseStatusException(
                    HttpStatus.FORBIDDEN,
                    "Jurisdiction mismatch: JWT=$jwtJurisdiction body=${req.jurisdiction}"
                )
            }
        }

        if (req.name.isBlank())
            throw ResponseStatusException(HttpStatus.BAD_REQUEST, "name must not be blank")
        if (req.totalTokens <= 0)
            throw ResponseStatusException(HttpStatus.BAD_REQUEST, "totalTokens must be > 0")
        if (req.propertyLedgerId < 1 || req.propertyLedgerId >= 16_777_216)
            throw ResponseStatusException(HttpStatus.BAD_REQUEST,
                "propertyLedgerId must be in 1..16_777_215 (24-bit range, see TD-14)")
        if (req.quoteLedgerId <= 0)
            throw ResponseStatusException(HttpStatus.BAD_REQUEST, "quoteLedgerId must be > 0")
        if (req.symbolId <= 0)
            throw ResponseStatusException(HttpStatus.BAD_REQUEST, "symbolId must be > 0")
        if (req.jurisdiction.isBlank())
            throw ResponseStatusException(HttpStatus.BAD_REQUEST, "jurisdiction must not be blank")

        val cmd = PropertyCommand(
            type = PropertyCommand.CREATE_PROPERTY,
            name = req.name,
            jurisdiction = req.jurisdiction,
            propertyLedgerId = req.propertyLedgerId,
            quoteLedgerId = req.quoteLedgerId,
            symbolId = req.symbolId,
            totalTokens = req.totalTokens,
            description = req.description,
            location = req.location
        )
        publisher.publish("property_commands", req.propertyLedgerId.toString(), cmd)
        return ResponseEntity.accepted().body(mapOf("propertyLedgerId" to req.propertyLedgerId))
    }

    /**
     * Returns the property identified by its TigerBeetle ledger ID.
     * Reads from Postgres — settlement populates the record asynchronously
     * after processing the CREATE_PROPERTY command.
     */
    @GetMapping("/{propertyLedgerId}")
    fun getProperty(@PathVariable propertyLedgerId: Int): ResponseEntity<PropertyResponse> {
        return try {
            val row = jdbc.queryForMap(
                "SELECT name, jurisdiction, total_tokens FROM properties WHERE property_ledger_id = ?",
                propertyLedgerId
            )
            ResponseEntity.ok(
                PropertyResponse(
                    propertyLedgerId = propertyLedgerId,
                    name = row["name"] as String,
                    jurisdiction = row["jurisdiction"] as String,
                    totalTokens = row["total_tokens"] as Long
                )
            )
        } catch (e: EmptyResultDataAccessException) {
            throw ResponseStatusException(HttpStatus.NOT_FOUND, "Property not found: $propertyLedgerId")
        }
    }

    private fun subsidiaryJurisdiction(authentication: Authentication): String? {
        if (authentication !is JwtAuthenticationToken) return null
        return authentication.token.getClaim<String>("subsidiary_jurisdiction")
    }
}
