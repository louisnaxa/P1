package com.exchange.gateway

import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.whenever
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest
import org.springframework.boot.test.mock.mockito.MockBean
import org.springframework.context.annotation.Import
import org.springframework.http.MediaType
import org.springframework.security.oauth2.jwt.JwtDecoder
import org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.jwt
import org.springframework.test.context.TestPropertySource
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.status

/**
 * Verifies per-user rate limiting on POST /orders.
 *
 * Rate set to 1.0/s. Guava SmoothBursty starts with storedPermits=0 and
 * nextFreeTicketMicros=0: the first request is always served immediately
 * (nextFreeTicketMicros=0 is in the past), then nextFreeTicketMicros is
 * set to now+1s. A second request sent within milliseconds finds
 * nextFreeTicketMicros still in the future → tryAcquire(timeout=0) returns
 * false → 429. Deterministic: no sleeping required, the 1-second gap between
 * two back-to-back MockMvc calls is never bridged in practice.
 *
 * Unique sub values per test prevent cross-test bucket state (the filter's
 * ConcurrentHashMap persists within the shared @WebMvcTest context).
 */
@WebMvcTest(OrderController::class)
@Import(SecurityConfig::class)
@TestPropertySource(properties = ["exchange.rate-limit.orders-per-second=1.0"])
class RateLimitTest {

    @Autowired
    private lateinit var mvc: MockMvc

    @MockBean
    private lateinit var publisher: CommandPublisher

    @MockBean
    private lateinit var userService: UserService

    @MockBean
    private lateinit var transferGuard: TransferGuard

    @MockBean
    private lateinit var jwtDecoder: JwtDecoder

    private val orderJson = """{"symbolId":1,"side":"BID","price":100,"quantity":5}"""

    @BeforeEach
    fun setUp() {
        whenever(userService.resolveUid(any())).thenReturn(42L)
        whenever(publisher.publish(any(), any(), any())).thenReturn(0L)
    }

    @Test
    fun `first request is accepted`() {
        mvc.perform(post("/orders")
            .with(jwt().jwt { it.subject("within-sub") })
            .contentType(MediaType.APPLICATION_JSON)
            .content(orderJson))
            .andExpect(status().isAccepted)
    }

    @Test
    fun `second rapid request returns 429`() {
        // First request consumes the free initial permit
        mvc.perform(post("/orders")
            .with(jwt().jwt { it.subject("over-sub") })
            .contentType(MediaType.APPLICATION_JSON)
            .content(orderJson))
            .andExpect(status().isAccepted)

        // Second request arrives before the 1-second refill → rejected
        mvc.perform(post("/orders")
            .with(jwt().jwt { it.subject("over-sub") })
            .contentType(MediaType.APPLICATION_JSON)
            .content(orderJson))
            .andExpect(status().isTooManyRequests)
    }

    @Test
    fun `rate limit is per user - another user keeps their own quota`() {
        // alice-iso exhausts her quota
        mvc.perform(post("/orders")
            .with(jwt().jwt { it.subject("alice-iso-sub") })
            .contentType(MediaType.APPLICATION_JSON)
            .content(orderJson))
            .andExpect(status().isAccepted)
        mvc.perform(post("/orders")
            .with(jwt().jwt { it.subject("alice-iso-sub") })
            .contentType(MediaType.APPLICATION_JSON)
            .content(orderJson))
            .andExpect(status().isTooManyRequests)

        // bob-iso has his own fresh bucket — first permit available immediately
        mvc.perform(post("/orders")
            .with(jwt().jwt { it.subject("bob-iso-sub") })
            .contentType(MediaType.APPLICATION_JSON)
            .content(orderJson))
            .andExpect(status().isAccepted)
    }
}
