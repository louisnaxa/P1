package com.exchange.gateway

import com.exchange.common.EngineCommand
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.invocation.InvocationOnMock
import org.mockito.kotlin.whenever
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest
import org.springframework.boot.test.mock.mockito.MockBean
import org.springframework.context.annotation.Import
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.security.core.authority.SimpleGrantedAuthority
import org.springframework.security.oauth2.jwt.JwtDecoder
import org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.jwt
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.status
import org.springframework.web.server.ResponseStatusException

@WebMvcTest(OrderController::class, MarketDataController::class, AdminController::class)
@Import(SecurityConfig::class)
class SecurityTest {

    @Autowired
    private lateinit var mvc: MockMvc

    @MockBean
    private lateinit var publisher: CommandPublisher

    @MockBean
    private lateinit var userService: UserService

    @MockBean
    private lateinit var orderBookConsumer: OrderBookConsumer

    @MockBean
    private lateinit var tradeStreamConsumer: TradeStreamConsumer

    @MockBean
    private lateinit var candleAggregator: CandleAggregator

    @MockBean
    private lateinit var jwtDecoder: JwtDecoder

    @Test
    fun `POST orders without token returns 401`() {
        mvc.perform(post("/orders")
            .contentType(MediaType.APPLICATION_JSON)
            .content("""{"symbolId":1,"side":"BID","price":100,"quantity":5}"""))
            .andExpect(status().isUnauthorized)
    }

    @Test
    fun `DELETE orders without token returns 401`() {
        mvc.perform(delete("/orders/0?symbolId=1"))
            .andExpect(status().isUnauthorized)
    }

    @Test
    fun `POST admin credit without token returns 401`() {
        mvc.perform(post("/admin/credit")
            .contentType(MediaType.APPLICATION_JSON)
            .content("""{"uid":1,"currency":0,"amount":1000}"""))
            .andExpect(status().isUnauthorized)
    }

    @Test
    fun `POST admin credit with user token (no admin role) returns 403`() {
        mvc.perform(post("/admin/credit")
            .with(jwt().authorities(SimpleGrantedAuthority("ROLE_user")))
            .contentType(MediaType.APPLICATION_JSON)
            .content("""{"uid":1,"currency":0,"amount":1000}"""))
            .andExpect(status().isForbidden)
    }

    @Test
    fun `POST orders with unknown sub returns 403`() {
        whenever(userService.resolveUid(any())).thenThrow(
            ResponseStatusException(HttpStatus.FORBIDDEN, "Authenticated user has no registered account")
        )
        mvc.perform(post("/orders")
            .with(jwt().jwt { it.subject("unknown-sub") })
            .contentType(MediaType.APPLICATION_JSON)
            .content("""{"symbolId":1,"side":"BID","price":100,"quantity":5}"""))
            .andExpect(status().isForbidden)
    }

    @Test
    fun `GET orderbook is public - no token required`() {
        // Returns 404 (no cached data) rather than 401/403 — proves the endpoint is public.
        mvc.perform(get("/orderbook/1"))
            .andExpect(status().isNotFound)
    }

    @Test
    fun `POST admin account-status without token returns 401`() {
        mvc.perform(post("/admin/account-status")
            .contentType(MediaType.APPLICATION_JSON)
            .content("""{"uid":1,"status":"SUSPENDED"}"""))
            .andExpect(status().isUnauthorized)
    }

    @Test
    fun `POST admin account-status with user token (no admin role) returns 403`() {
        mvc.perform(post("/admin/account-status")
            .with(jwt().authorities(SimpleGrantedAuthority("ROLE_user")))
            .contentType(MediaType.APPLICATION_JSON)
            .content("""{"uid":1,"status":"SUSPENDED"}"""))
            .andExpect(status().isForbidden)
    }

    @Test
    fun `Alice token - command uid comes from token not from request body`() {
        whenever(userService.resolveUid("alice-sub")).thenReturn(1001L)
        var publishedUid: Long? = null
        whenever(publisher.publish(any(), any(), any())).thenAnswer { inv: InvocationOnMock ->
            publishedUid = (inv.arguments[2] as EngineCommand).uid
            0L
        }

        mvc.perform(post("/orders")
            .with(jwt().jwt { it.subject("alice-sub") })
            .contentType(MediaType.APPLICATION_JSON)
            .content("""{"symbolId":1,"side":"BID","price":100,"quantity":5}"""))
            .andExpect(status().isAccepted)

        // uid=1001L comes from UserService.resolveUid("alice-sub"), not from the request body
        // (PlaceOrderRequest has no uid field — a client cannot inject any uid).
        assertThat(publishedUid).isEqualTo(1001L)
    }
}
