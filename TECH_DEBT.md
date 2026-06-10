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

## TD-4 — ~~Two trade legs not atomically linked in TigerBeetle~~ ✓ CLOSED

**Fixed**: `SettlementService.kt` — `TransferFlags.LINKED` set on leg 0.
`ensureAccount` now creates both available and locked accounts.
Chaos 4 test re-enabled and rewritten to verify LINKED atomicity against real TigerBeetle.

---

## TD-5 — No idempotency on incoming HTTP requests (POST /orders)

**Location**: `gateway/src/main/kotlin/com/exchange/gateway/OrderController.kt`

With Option A (orderId = Kafka offset), the client does not know its orderId before the POST is
confirmed.  A network retry sends a second PLACE_ORDER command that lands at a different offset
and becomes a distinct order — the engine sees two separate orders.

Fix: add an `Idempotency-Key` header to POST /orders; the gateway stores (key → orderId) durably
(Redis or Postgres) and returns the cached response on duplicate keys without re-publishing.

**Planned milestone**: M3 / authentication and rate limiting.

---

## TD-6 — Tests E2E multi-services manquants

**Location**: tests — couverture actuelle en deux moitiés disjointes :
- `OrderLifecycleTest` (gateway) : HTTP → Kafka
- `MatchingEngineTest` (engine) : Kafka → moteur

Les deux se rejoignent par confiance sur un offset partagé, pas par un test qui traverse
les deux services en même temps.

Fix : quand l'infra de test multi-services existe (Testcontainers Compose ou équivalent),
écrire un test qui démarre gateway + engine ensemble, poste un ordre via HTTP, observe
le trade ou l'annulation dans la réponse WebSocket ou le topic trades.

**Planned milestone**: M5 ou quand la complexité inter-services le justifie.

---

## TD-9 — Pas de rejeu automatique d'un batch LINKED rejeté

**Location**: `SettlementService.settleTrade` — `log.error` quand errors non-EXISTS.

TigerBeetle 0.16.11 consomme définitivement les transferId d'un batch LINKED rejeté
(`CreateTransferResult.IdAlreadyFailed` au retry). Si `settleTrade` est appelé et
qu'une jambe échoue (compte absent, contrainte TB), LINKED rollback les deux jambes
atomiquement — aucun fonds ne bouge — mais le trade reste non réglé. Rejouer avec
les mêmes `tradeId` → mêmes transferId → silencieusement rejeté encore.

**Prévention à la source** : `TradeConsumer.onTrade` appelle `ensureAccount` pour
les 4 comptes participants avant `settleTrade` (gap-2 prevention). Élimine le cas
"compte absent" en trafic normal.

**Exposition résiduelle** : toute autre erreur TB sur une jambe (contrainte métier,
indisponibilité partielle) laisse le trade non réglé sans chemin de retry automatique.

**Remédiation manuelle** : identifier le trade dans les logs (niveau ERROR), corriger
la cause, ré-émettre via `POST /admin/credit` pour les montants concernés.

**Fix automatisé** : nécessite un compteur de retry dans le schéma de transferId +
un lookup TB avant retry. Reporté — le cas est rare après gap-2 prevention.

**Planned milestone**: M3+ si le cas devient runtime régulier.

---

## TD-7 — Test replay-gate pilote onCommand directement, pas le listener réel

**Location**: `EngineReplayGateTest.EngineCommandConsumerReplayTest`

Le test appelle `consumer.onCommand(cr)` manuellement après avoir construit le
`ConsumerRecord` en mémoire. Il n'exerce pas le vrai chemin
`@KafkaListener → onPartitionsAssigned → seekToBeginning → détection de fin de replay`
en conditions Spring Kafka réelles. En particulier, le seek appliqué par le callback
réel (pas `NoOpSeekCallback`) n'est jamais déclenché.

Fix : test d'intégration complet démarrant un `KafkaMessageListenerContainer` réel
avec un topic pré-peuplé, vérifiant que la détection de fin de replay se déclenche
sans pilotage manuel.

**Planned milestone**: M3 ou lors de l'adoption d'une infrastructure de test multi-services.

---

## TD-10 — Rate limiting en mémoire locale, non distribué + map sans éviction

**Location**: `OrderRateLimitFilter` — `ConcurrentHashMap<String, RateLimiter>`

**Problème 1 — multi-instance** : chaque instance gateway a sa propre map de `RateLimiter`.
En production multi-instance (derrière un load balancer), un utilisateur obtient son quota ×
le nombre d'instances. Pour 10 req/s et 3 instances, le quota réel est 30 req/s. Le fix est un
rate limiting distribué via Redis (déjà dans la pile), par exemple avec un compteur INCR + TTL
ou une sliding window via Lua. Acceptable M3 (mono-instance), non-acceptable à partir du
moment où le gateway scale horizontalement.

**Problème 2 — fuite mémoire lente** : la map grandit à chaque nouvel utilisateur et n'est
jamais nettoyée. Un utilisateur qui s'authentifie une seule fois occupe un `RateLimiter` pour
toujours. En production longue durée avec beaucoup d'utilisateurs, cela représente une
accumulation lente. Fix : Guava `Cache` avec TTL d'éviction (ex. 1h d'inactivité) ou Redis
(résout les deux problèmes à la fois).

**Planned milestone**: avant scale horizontal du gateway (Redis rate limiting remplace les deux).

---

## TD-11 — Single hot wallet MPC, pas de séparation hot/cold

**Location**: `AccountIds.external(ledgerId)` — un seul compte système externe par currency/ledger.

M4 opère avec un seul wallet MPC qui reçoit tous les dépôts on-chain. Il n'existe pas de compte
TigerBeetle `external-hot` / `external-cold` — `SYSTEM_EXTERNAL_USER = 0L` est unique par ledger.

Conséquences en production multi-instance :
- Tout le collatéral en stablecoin réside dans un seul wallet on-chain (hot wallet).
- Exposition maximale en cas de clé MPC compromise.
- La règle de sécurité standard (>80 % des fonds en cold storage) n'est pas respectée.

Fix : introduire deux adresses on-chain (hot + cold), une logique de sweep automatique
(hot→cold quand hot > seuil), et deux comptes TigerBeetle distincts ou un seul compte
with réconciliation hors-bande. N'affecte pas les soldes virtuels utilisateurs.

**Planned milestone**: M6 / infrastructure custody avancée.

---

## TD-13 — Retrait : signature/diffusion/confirmations MPC et résolution trou de nonce

**Scope** : tout ce qui se passe après `WithdrawalSigner.sign()` dans la production réelle.

**Reporté à l'intégration MPC — hors CI** :

- **Signature réelle** : `WithdrawalSigner` production délègue au prestataire MPC ; `raw_tx` persisté avant diffusion (crash-3a : rediffuser depuis DB sans re-signer).
- **Diffusion on-chain** : envoi du `raw_tx` au nœud Ethereum ; `tx_hash` déjà en DB avant l'appel réseau.
- **Confirmations on-chain** : le watcher scrute les blocs ; dès N confirmations sur `tx_hash` → `confirmBroadcast()` (idempotent via TB Exists). État BROADCAST→CONFIRMED piloté par la chaîne, pas par le signer.
- **Résolution trou de nonce** : si un nonce N reste non miné au-delà du TTL, émettre une transaction de remplacement (`gasPrice` élevé, `to=self`, `value=0`) sur le même nonce pour débloquer N+1, N+2, etc. Logique de cancellation dans le sub-lot MPC suivant.
- **Gestion des erreurs on-chain** : transaction dropped / replaced / nonce trop ancien → VOID ou retry selon la politique. Non couvert par les tests CI actuels.

**Ce qui est prouvé en CI et ne sera PAS retouché** : verrou TB PENDING, machine d'état LOCKED→BROADCAST→CONFIRMED|VOID, conservation des montants, nonce durable + ordre ASC, crash-1/2/3b.

**Planned milestone**: intégration MPC + audit custody.

---

## TD-14 — Limite 24 bits sur ledgerId (propertyLedgerId)

**Location**: `AccountIds.kt` — `encode(userId, ledgerId, accountType)` :
```kotlin
(userId shl 32) or (ledgerId.toLong() shl 8) or accountType.toLong()
```

`ledgerId` occupe les bits 8–31 (24 bits). Valeur max : 16 777 215.
Les ledger IDs de propriétés actuels sont de petits entiers — sûrs. Si la plateforme atteint
plusieurs millions de biens, la limite sera franchie. Débordement silencieux : pas d'exception,
de mauvais accountIds dans TigerBeetle.

Correction si nécessaire : étendre le schéma d'ID (nouveau cluster TigerBeetle ou migration
d'accountId). N'affecte pas les soldes utilisateurs (la partie haute des 128 bits TB est libre).

**Jalon prévu** : avant d'atteindre ~10 M de biens.

---

## TD-12 — ~~Test isolé layer-2 manquant : même commande à deux offsets Kafka différents~~ ✓ CLOSED

**Fixed**: `settlement:AdjustBalanceIdempotencyTest` — D1 + D2 prouvent la protection contre
le double-crédit sans outillage blockchain :
1. Même `onChainRef`, offset Kafka 5 → TigerBeetle crédité une fois (500).
2. Même `onChainRef`, offset Kafka 17 → SHA-256 transferId identique → TigerBeetle no-op → solde inchangé (500).

Layer 1 (Postgres UNIQUE) et Layer 2 (SHA-256 transferId) sont prouvés indépendants.
