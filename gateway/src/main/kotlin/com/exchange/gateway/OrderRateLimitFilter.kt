package com.exchange.gateway

import com.google.common.util.concurrent.RateLimiter
import jakarta.servlet.FilterChain
import jakarta.servlet.http.HttpServletRequest
import jakarta.servlet.http.HttpServletResponse
import org.springframework.security.core.context.SecurityContextHolder
import org.springframework.web.filter.OncePerRequestFilter
import java.util.concurrent.ConcurrentHashMap

/**
 * Rejects write-order requests when a user exceeds their per-second quota.
 * Applied inside the Spring Security filter chain, after AuthorizationFilter,
 * so authentication.name is always available.
 *
 * Applied to: POST /orders and DELETE /orders/{orderId}
 * Not applied to: GET market data (public read), POST /admin/credit (privileged)
 *
 * TD-10: in-memory only — quota multiplies by instance count in multi-node
 * deployments. Replace with Redis-based distributed rate limiting before
 * horizontal scale. Map also grows unbounded (no TTL eviction).
 */
class OrderRateLimitFilter(private val permitsPerSecond: Double) : OncePerRequestFilter() {

    @Suppress("UnstableApiUsage")
    private val limiters = ConcurrentHashMap<String, RateLimiter>()

    override fun doFilterInternal(
        request: HttpServletRequest,
        response: HttpServletResponse,
        chain: FilterChain
    ) {
        if (!isOrderWrite(request)) {
            chain.doFilter(request, response)
            return
        }
        val userId = SecurityContextHolder.getContext().authentication?.name
            ?: run { chain.doFilter(request, response); return }

        @Suppress("UnstableApiUsage")
        val limiter = limiters.computeIfAbsent(userId) { RateLimiter.create(permitsPerSecond) }

        @Suppress("UnstableApiUsage")
        if (!limiter.tryAcquire()) {
            response.status = 429
            return
        }
        chain.doFilter(request, response)
    }

    private fun isOrderWrite(request: HttpServletRequest): Boolean =
        (request.method == "POST" && request.requestURI == "/orders") ||
        (request.method == "DELETE" && request.requestURI.startsWith("/orders/"))
}
