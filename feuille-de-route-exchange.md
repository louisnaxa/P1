# Feuille de route — Plateforme d'échange

Document de travail vivant. À tenir à jour et committer au fil des portes franchies.
**Règle : on garde ce fichier au niveau « jalon + condition de validation ». Le détail va ailleurs
(principes → `CLAUDE.md`, dette → `TECH_DEBT.md`, implémentation → le code).**

> **État au 10 juin 2026 :** M0–M4 socle, **brique B1 (statut des détenteurs)** et **brique B2 (gestion par bien) franchis et prouvés** (CI verte). Jalon courant : **brique B3 — contrôle au transfert**.

---

## Virage produit

Le noyau exchange (M0–M4) est la fondation d'une infrastructure d'échange pour un parc immobilier réel et localisé — Abu Dhabi au départ. Stratégie actée : base régulée unique, deux classes de détenteurs (résident agréé / étranger sans droit économique). M4 signature/diffusion MPC et M5 (front React) sont repoussés APRÈS le cœur tokenisation — décision : construire d'abord le levier démontrable. Voir `SPEC-PRODUIT.md` pour le détail du cadrage produit.

---

## Phase produit — briques

Ordre de construction décidé. Aucune case cochée : rien n'est encore prouvé.

- [x] **Brique 1 — Statut des détenteurs** *(money-path)* — prouvé en CI : modèle Postgres (UNVERIFIED/FOREIGN_SPECULATIVE/CITIZEN_APPROVED/SUSPENDED + juridiction + audit trail), contraintes CHECK rejetant les cas invalides prouvées par bypass du guard applicatif (`AccountStatusIntegrationTest`, job `status`)
- [x] **Brique 2 — Gestion par bien** *(money-path)* — prouvé en CI : tables `properties`/`symbols`/`property_holders`, `SymbolRepository` remplace le TODO hardcodé, `PropertyService` émet 100% des tokens sur création (TB réel), 4 tests P1–P4 (`PropertyIntegrationTest`, job `property`)
- [ ] **Brique 3 — Contrôle au transfert** *(money-path, rigueur maximale, tests de refus)*
- [ ] **Brique 3 — Contrôle au transfert** *(money-path, rigueur maximale, tests de refus)*
- [ ] **Brique 4 — Droits économiques** (distribution loyers / seuil 100 %)

Voir `CARTE-BRIQUES-PRODUIT.md` pour la carte détaillée.

---

## Le projet en une phrase

Une plateforme d'échange centralisée où les utilisateurs tradent des actifs que je crée moi-même (ils n'existent que dans ma base, pas sur une blockchain). La seule valeur réelle qui entre et sort est du stablecoin (USDT/USDC), détenu en custody.

Conséquence à ne jamais oublier : techniquement, c'est plus simple qu'un Binance classique (presque pas de blockchain). **Juridiquement, ce n'est pas plus simple** — dès que de l'argent réel entre et sort, c'est une activité financière régulée, peu importe le mot « virtuel ».

---

## Deux chantiers en parallèle

### Chantier juridique (le délai le plus long, hors de mon contrôle) — LE VRAI CHEMIN CRITIQUE

- [ ] Prendre rendez-vous avec un avocat en droit financier français/européen
- [ ] Faire classer juridiquement les actifs créés (MiCA / licence CASP ? ou MiFID II / monnaie électronique ?)
- [ ] Identifier le régime d'autorisation applicable et constituer le dossier
- [ ] Suivre l'examen (l'AMF peut prendre ~4 mois après un dossier complet)

Ce chantier tourne en fond pendant tout le développement. S'il démarre tard, il retarde le lancement d'un an.
**Tant qu'il n'est pas lancé, il prime sur l'avancement technique.**

### Chantier technique

Voir la feuille de route des jalons plus bas.

---

## La pile technique (décidée)

- **Moteur de matching + carnet d'ordres** : exchange-core (JVM)
- **Registre des soldes** : TigerBeetle (partie double)
- **Bus d'événements** : Kafka
- **Services** : Kotlin + Spring Boot
- **Authentification** : Keycloak
- **Custody stablecoin** : web3j, clés confiées à un prestataire MPC
- **Données** : PostgreSQL, Redis, TimescaleDB
- **Frontend** : React + TradingView Lightweight Charts
- **Infra** : Docker / Kubernetes + Prometheus / Grafana
- **Non open source** : KYC (Sumsub / Onfido), surveillance AML (Chainalysis)

---

## Feuille de route — jalons

Principe : chaque jalon est **prouvé**, pas seulement « codé ».

### M0 — Le moteur tourne — ✅ FRANCHI

- [x] Un test envoie des ordres dans exchange-core et obtient les bons trades
- [x] Toute l'infra démarre avec une seule commande (`docker compose up`)
- [x] CI verte

### M1 — Le settlement est correct — ✅ FRANCHI ET PROUVÉ

- [x] Le cycle ordre → matching → règlement fonctionne pour une paire (command-sourced)
- [x] Tests chaos verts en CI contre **vrai** TigerBeetle + Kafka : double livraison, crash moteur + replay, crash settlement avant commit
- [x] Réconciliation au niveau solde (engine reconstruit depuis le log = TigerBeetle), sans heuristique de timing, ne se déclenche qu'après fin de replay confirmée
- [x] Jamais d'écart entre solde moteur et solde registre

Dettes inscrites (voir `TECH_DEBT.md`) : full-replay depuis offset 0 (→ snapshots plus tard) ; garde sur débordement de bits `tradeId` ; flag `LINKED` des deux jambes (M2) ; fragilité seccomp/io_uring des tests.

### M2 — Cœur tradable en argent fictif — ✅ FRANCHI ET PROUVÉ

- [x] API REST (placer / annuler) + WebSocket (carnet, trades, ticker)
- [x] Bougies (candles) enregistrées dans TimescaleDB
- [x] Endpoint admin qui crédite un solde (dépôts encore simulés)
- [x] Comptes verrouillés + flag `LINKED` sur les deux jambes (clôt TD-4)
- [x] Session de trading multi-comptes en ligne de commande, sans dérive de solde

### M3 — Sécurisé ✅ FRANCHI ET PROUVÉ

- [x] Keycloak branché comme resource server OAuth2 : `jwk-set-uri` pointé sur le realm Keycloak, `JwtAuthenticationConverter` lit `realm_access.roles` — `SecurityFilterChain` : market data GET public, `/orders` et `DELETE /orders/**` exigent un JWT valide, `/admin/credit` exige `ROLE_exchange-admin` (403 avec token user, pas 401)
- [x] Requêtes non authentifiées rejetées : uid non falsifiable — `UserService.resolveUid()` lit `keycloak_sub → internal_uid` dans la table `users` ; sub inconnu → 403 immédiat, jamais de fallback ; le body de la requête n'a pas de champ uid (injection côté client structurellement impossible) — 6 tests dans `SecurityTest` : 401 sans token (3 endpoints), 403 token sans rôle admin, 403 sub inconnu, market data public, test Alice (uid vient du token)
- [x] Rate limiting par utilisateur : `OrderRateLimitFilter` (Guava `RateLimiter`, après `AuthorizationFilter`, 10 req/s par défaut) — TD-10 ouvert pour le distribué Redis avant scale horizontal

### M4 — Custody réelle (2e zone à haut risque)

- [x] Dépôt stablecoin : idempotence layer-1 (Postgres UNIQUE tx_hash+log_index) + layer-2 (SHA-256 transferId TB) — prouvée en CI sans outillage blockchain (AdjustBalanceIdempotencyTest D1+D2)
- [x] Retrait — verrou + machine d'état (LOCKED→BROADCAST→CONFIRMED|VOID) : TB PENDING avant DB row, anti-double-broadcast structural, conservation prouvée — CI vert (W1–W7)
- [x] Retrait — nonce : réservation durable avant sign(), ordre diffusion nonce ASC obligatoire, reprise crash-3b (même nonce réutilisé) — CI vert (N8–N9)
- [ ] Retrait — signature/diffusion MPC réelle : `WithdrawalSigner` production, raw_tx on-chain, watcher confirmations (BROADCAST→CONFIRMED on-chain) — à valider en intégration MPC, hors CI
- [ ] Retrait — résolution trou de nonce (transaction d'annulation si nonce bloqué) — MPC sub-lot suivant
- [ ] Gestion des clés revue par quelqu'un de compétent
- [x] Un événement rejoué ne crédite jamais deux fois — prouvé en layer-2 idempotency (AdjustBalanceIdempotencyTest D1+D2)

### M5 — Utilisable

- [ ] Interface React : graphique, saisie d'ordres, soldes, historique
- [ ] Tourne sur l'API stabilisée

### M6 — Conforme et durci

- [ ] KYC/AML réel : bloque les comptes non vérifiés, screene les retraits
- [ ] Exercice de reprise après panne réellement répété
- [ ] Test de charge à la cible de volume
- [ ] Runbook d'incident écrit
- [ ] Revue de sécurité de la custody et du settlement

### M7 — Lancement pilote (porte juridique, pas technique)

- [ ] Autorisation légale obtenue
- [ ] M6 complet
- [ ] Ouverture à un petit groupe fermé, avec limites de retrait

---

## Les trois endroits où le projet se gagne

(Détail des invariants : voir `CLAUDE.md`.)

1. **Le settlement** (frontière moteur ↔ registre) — ✅ fondation posée et prouvée en M1+M2.
2. **La custody** (M4) — là où les exchanges meurent. Clés MPC, audit, aucun code non compris ligne à ligne.
3. **La conformité** — conditionne le droit d'ouvrir, pas une formalité de fin.

---

## Jalon courant : M4 — Custody réelle

Ce qui est prouvé en CI :
- [x] Dépôt : idempotence layer-1 (Postgres UNIQUE `tx_hash+log_index`) + layer-2 (SHA-256 transferId TB) — `AdjustBalanceIdempotencyTest` D1+D2
- [x] Retrait verrou + machine d'état (LOCKED→BROADCAST→CONFIRMED|VOID) — `WithdrawalChaosTest` W1–W7
- [x] Retrait nonce : réservation durable avant sign(), ordre ASC obligatoire, reprise crash-3b — N8–N9

Ce qui reste (hors CI — intégration MPC + audit, voir TD-13) :
- [ ] Signature/diffusion MPC réelle (`WithdrawalSigner` production, `raw_tx` on-chain)
- [ ] Confirmations on-chain (watcher N blocs → BROADCAST→CONFIRMED piloté par la chaîne)
- [ ] Résolution trou de nonce (transaction d'annulation si nonce bloqué)
- [ ] Gestion des clés revue par quelqu'un de compétent

Réflexe de supervision sur chaque fonction touchant un solde : « contre quoi ça tourne, qu'est-ce que ça prouve ? ».

---

## Rappel honnête

M0+M1 franchis = la fondation est étanche, mais ce n'est que ~30 % du chemin. Les 70 % restants (custody,
conformité, durcissement) concentrent le risque et ne se démontrent pas dans une démo. Ne pas se précipiter,
faire auditer custody + settlement, et traiter le juridique comme le vrai chemin critique.