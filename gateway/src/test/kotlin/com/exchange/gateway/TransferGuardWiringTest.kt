package com.exchange.gateway

import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.doThrow
import org.mockito.kotlin.whenever
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest
import org.springframework.boot.test.mock.mockito.MockBean
import org.springframework.context.annotation.Import
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.security.oauth2.jwt.JwtDecoder
import org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.jwt
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.status
import org.springframework.web.server.ResponseStatusException

/**
 * Isolated @WebMvcTest slice for R7: proves TransferGuard is wired into
 * OrderController.placeOrder() and that a guard rejection propagates as 403.
 *
 * Kept separate from SecurityTest to avoid Mockito stub leakage between tests
 * (JUnit 5 execution order differs across JVM distributions).
 */
@WebMvcTest(OrderController::class)
@Import(SecurityConfig::class)
class TransferGuardWiringTest {

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

    @Test
    fun `R7 TransferGuard rejection returns 403 - proves guard is wired in placeOrder`() {
        whenever(userService.resolveUid(any())).thenReturn(1001L)
        doThrow(ResponseStatusException(HttpStatus.FORBIDDEN, "Access denied by TransferGuard"))
            .whenever(transferGuard).checkOrderAccess(any(), any())

        mvc.perform(post("/orders")
            .with(jwt().jwt { it.subject("test-sub") })
            .contentType(MediaType.APPLICATION_JSON)
            .content("""{"symbolId":1,"side":"BID","price":100,"quantity":5}"""))
            .andExpect(status().isForbidden)
    }
}
