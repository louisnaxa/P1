# Technical Debt

## TD-1 — Full replay from offset 0 on engine restart

**Location**: `EngineCommandConsumer` — seeks to beginning on every partition assignment, no offset commit.

Acceptable for M1 while the `commands` topic is short. Restart cost is O(n) in journal length.
Will require snapshots (exchange-core snapshot API + snapshot offset tracking) before the journal grows large.

**Planned milestone**: M2 / snapshot support.

---

## TD-2 — Silent tradeId collision when matchIndex >= 65 536

**Location**: `MatchingEngineService.kt:107`
```kotlin
val tradeId = (offset shl 16) or matchIndex.toLong()
```

If a single PLACE_ORDER generates ≥ 65 536 fills, `matchIndex` overflows into the offset bits and two
trades receive the same `tradeId`. The bug is silent: no exception, wrong transfers in TigerBeetle.

Fix: one guard line before the assignment:
```kotlin
require(matchIndex < 65_536) { "matchIndex overflow: $matchIndex fills on offset $offset" }
```

**Planned milestone**: next M1 cleanup pass.

---

## TD-3 — Integration tests require seccomp=unconfined (io_uring)

**Location**: `TigerBeetleContainer.kt` — both the format and server containers are started with
`seccomp=unconfined` because Docker ≥ 25 blocks `io_uring` syscalls by default and TigerBeetle
requires it.

This makes `SettlementChaosIntegrationTest` fragile on:
- CI runners with hardened seccomp profiles (GKE autopilot, some CircleCI configs)
- Container-in-container environments where the outer container also restricts syscalls

Mitigation options when the need arises:
1. Custom seccomp profile that allows only `io_uring_*` syscalls (minimal blast radius)
2. Switch to a TigerBeetle build that does not use `io_uring` (not currently available)
3. Run chaos integration tests on a dedicated EC2/VM runner, not shared Kubernetes nodes

**Planned milestone**: when a hardened CI runner is adopted.

---

## TD-4 — Two trade legs not atomically linked in TigerBeetle

**Location**: `SettlementService.kt:89-109`

Both legs (quote and base) are submitted in a single `createTransfers(batch)` call but without
`TransferFlags.LINKED`.  TigerBeetle processes them independently: if leg 1 fails (e.g. a future
balance constraint on a locked account), leg 0 is already committed and the trade is half-settled.

In M1 this cannot happen (user accounts have no upper-bound constraint and both legs always fit).
In M2 (locked accounts, real margin checks) this becomes a correctness risk.

Fix for M2: set `TransferFlags.LINKED` on leg 0 so that a leg 1 failure rolls back leg 0:
```kotlin
batch.setFlags(TransferFlags.LINKED)   // on leg 0 only; leg 1 keeps flags = 0
```

**Planned milestone**: M2 / locked accounts.

---

## TD-5 — No idempotency on incoming HTTP requests (POST /orders)

**Location**: `gateway/src/main/kotlin/com/exchange/gateway/OrderController.kt`

With Option A (orderId = Kafka offset), the client does not know its orderId before the POST is
confirmed.  A network retry sends a second PLACE_ORDER command that lands at a different offset
and becomes a distinct order — the engine sees two separate orders.

Fix: add an `Idempotency-Key` header to POST /orders; the gateway stores (key → orderId) durably
(Redis or Postgres) and returns the cached response on duplicate keys without re-publishing.

**Planned milestone**: M3 / authentication and rate limiting.
