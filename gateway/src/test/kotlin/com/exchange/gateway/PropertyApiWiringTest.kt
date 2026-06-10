package com.exchange.gateway

import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.never
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest
import org.springframework.boot.test.mock.mockito.MockBean
import org.springframework.context.annotation.Import
import org.springframework.http.MediaType
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.security.core.authority.SimpleGrantedAuthority
import org.springframework.security.oauth2.jwt.JwtDecoder
import org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.jwt
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.status

/**
 * Wiring tests for PropertyController — @WebMvcTest slice, no containers.
 *
 * What is proved (money-path):
 *   PA1 — Subsidiary with matching JWT jurisdiction → 202, publisher called once
 *   PA2 — Subsidiary with mismatching jurisdiction (body ≠ JWT claim) → 403, publisher NEVER called
 *   PA3 — Authenticated user without subsidiary/admin role → 403, publisher NEVER called
 *   PA4 — Invalid bounds (totalTokens=0 / ledger out of range) → 400, publisher NEVER called
 *   PA5 — Admin bypasses jurisdiction check → 202, publisher called once
 *
 * "Publisher never called" proves the refusal stops the command BEFORE it becomes
 * durable on the Kafka topic — the same structural guarantee as B3 / TransferGuard.
 */
@WebMvcTest(PropertyController::class)
@Import(SecurityConfig::class)
class PropertyApiWiringTest {

    @Autowired
    private lateinit var mvc: MockMvc

    @MockBean
    private lateinit var publisher: CommandPublisher

    @MockBean
    private lateinit var jdbc: JdbcTemplate

    @MockBean
    private lateinit var jwtDecoder: JwtDecoder

    private val validBody = """
        {
          "name": "Burj Al Arab",
          "jurisdiction": "AE-AZ",
          "propertyLedgerId": 3001,
          "quoteLedgerId": 100,
          "symbolId": 3001,
          "totalTokens": 1000000
        }
    """.trimIndent()

    // ── PA1 — Subsidiary with matching jurisdiction → 202 ──────────────────

    @Test
    fun `PA1 subsidiary with matching jurisdiction creates property - 202 publisher called`() {
        whenever(publisher.publish(any(), any(), any())).thenReturn(0L)

        mvc.perform(
            post("/properties")
                .with(jwt()
                    .authorities(SimpleGrantedAuthority("ROLE_subsidiary"))
                    .jwt { it.subject("sub-filiale").claim("subsidiary_jurisdiction", "AE-AZ") }
                )
                .contentType(MediaType.APPLICATION_JSON)
                .content(validBody)
        ).andExpect(status().isAccepted)

        verify(publisher).publish(any(), any(), any())
    }

    // ── PA2 — Subsidiary with mismatching jurisdiction → 403, nothing published ───

    @Test
    fun `PA2 subsidiary jurisdiction mismatch returns 403 - publisher never called`() {
        // JWT claim is AE-DU but body declares AE-AZ
        mvc.perform(
            post("/properties")
                .with(jwt()
                    .authorities(SimpleGrantedAuthority("ROLE_subsidiary"))
                    .jwt { it.subject("sub-filiale").claim("subsidiary_jurisdiction", "AE-DU") }
                )
                .contentType(MediaType.APPLICATION_JSON)
                .content(validBody)
        ).andExpect(status().isForbidden)

        verify(publisher, never()).publish(any(), any(), any())
    }

    // ── PA3 — No subsidiary/admin role → 403, nothing published ──────────

    @Test
    fun `PA3 authenticated user without subsidiary or admin role returns 403 - publisher never called`() {
        mvc.perform(
            post("/properties")
                .with(jwt().jwt { it.subject("sub-user") })  // no roles
                .contentType(MediaType.APPLICATION_JSON)
                .content(validBody)
        ).andExpect(status().isForbidden)

        verify(publisher, never()).publish(any(), any(), any())
    }

    // ── PA4 — Invalid bounds → 400, nothing published ────────────────────

    @Test
    fun `PA4 totalTokens zero returns 400 - publisher never called`() {
        mvc.perform(
            post("/properties")
                .with(jwt()
                    .authorities(SimpleGrantedAuthority("ROLE_subsidiary"))
                    .jwt { it.subject("sub-filiale").claim("subsidiary_jurisdiction", "AE-AZ") }
                )
                .contentType(MediaType.APPLICATION_JSON)
                .content("""{"name":"Test","jurisdiction":"AE-AZ","propertyLedgerId":3001,"quoteLedgerId":100,"symbolId":3001,"totalTokens":0}""")
        ).andExpect(status().isBadRequest)

        verify(publisher, never()).publish(any(), any(), any())
    }

    @Test
    fun `PA4 propertyLedgerId outside 24-bit range returns 400 - publisher never called`() {
        mvc.perform(
            post("/properties")
                .with(jwt()
                    .authorities(SimpleGrantedAuthority("ROLE_subsidiary"))
                    .jwt { it.subject("sub-filiale").claim("subsidiary_jurisdiction", "AE-AZ") }
                )
                .contentType(MediaType.APPLICATION_JSON)
                .content("""{"name":"Test","jurisdiction":"AE-AZ","propertyLedgerId":16777216,"quoteLedgerId":100,"symbolId":3001,"totalTokens":100}""")
        ).andExpect(status().isBadRequest)

        verify(publisher, never()).publish(any(), any(), any())
    }

    // ── PA5 — Admin bypasses jurisdiction check → 202 ────────────────────

    @Test
    fun `PA5 admin creates property in any jurisdiction without JWT claim - 202`() {
        whenever(publisher.publish(any(), any(), any())).thenReturn(0L)

        mvc.perform(
            post("/properties")
                .with(jwt()
                    .authorities(SimpleGrantedAuthority("ROLE_exchange-admin"))
                    .jwt { it.subject("sub-admin") }
                    // No subsidiary_jurisdiction claim — admin doesn't need it
                )
                .contentType(MediaType.APPLICATION_JSON)
                .content(validBody)
        ).andExpect(status().isAccepted)

        verify(publisher).publish(any(), any(), any())
    }
}
