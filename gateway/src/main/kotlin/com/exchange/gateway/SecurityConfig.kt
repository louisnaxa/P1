package com.exchange.gateway

import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.http.HttpMethod
import org.springframework.security.web.access.intercept.AuthorizationFilter
import org.springframework.security.config.annotation.web.builders.HttpSecurity
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity
import org.springframework.security.config.http.SessionCreationPolicy
import org.springframework.security.core.authority.SimpleGrantedAuthority
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationConverter
import org.springframework.security.web.SecurityFilterChain

@Configuration
@EnableWebSecurity
class SecurityConfig(
    @Value("\${exchange.rate-limit.orders-per-second:10.0}") private val ordersPerSecond: Double
) {

    @Bean
    fun securityFilterChain(http: HttpSecurity): SecurityFilterChain {
        http
            .csrf { it.disable() }
            .sessionManagement { it.sessionCreationPolicy(SessionCreationPolicy.STATELESS) }
            .authorizeHttpRequests { auth ->
                // Market data is public — any client (browser, charting lib) can read it.
                auth.requestMatchers(HttpMethod.GET,
                    "/orderbook/**", "/ticker/**", "/candles/**").permitAll()
                // STOMP/WebSocket endpoint: public because it only broadcasts market data.
                auth.requestMatchers("/ws/**").permitAll()
                // Admin credit is the highest-risk endpoint: requires a dedicated admin role.
                // A valid user JWT without exchange-admin does NOT grant access (403, not 401).
                auth.requestMatchers("/admin/credit", "/admin/account-status").hasRole("exchange-admin")
                // Property creation: requires subsidiary role (filiale) or admin.
                // Jurisdiction coherence (JWT claim ↔ body) is further enforced in PropertyController.
                auth.requestMatchers(HttpMethod.POST, "/properties").hasAnyRole("subsidiary", "exchange-admin")
                // Everything else (POST /orders, DELETE /orders/**) requires authentication.
                auth.anyRequest().authenticated()
            }
            .oauth2ResourceServer { oauth2 ->
                oauth2.jwt { jwt -> jwt.jwtAuthenticationConverter(jwtAuthenticationConverter()) }
            }
            .addFilterAfter(OrderRateLimitFilter(ordersPerSecond), AuthorizationFilter::class.java)
        return http.build()
    }

    /**
     * Reads Keycloak realm roles from the "realm_access.roles" JWT claim and maps
     * them to Spring Security ROLE_ authorities.
     *
     * Keycloak does not put roles in the top-level "scope" claim (which the default
     * JwtGrantedAuthoritiesConverter reads), so we extract them manually here.
     */
    @Bean
    fun jwtAuthenticationConverter(): JwtAuthenticationConverter {
        val converter = JwtAuthenticationConverter()
        converter.setJwtGrantedAuthoritiesConverter { jwt ->
            @Suppress("UNCHECKED_CAST")
            val roles = (jwt.getClaim<Map<String, Any>>("realm_access")
                ?.get("roles") as? List<String>)
                ?: emptyList()
            roles.map { SimpleGrantedAuthority("ROLE_$it") }
        }
        return converter
    }
}
