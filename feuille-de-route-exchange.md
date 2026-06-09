# Feuille de route — Plateforme d'échange

Document de travail vivant. À tenir à jour et committer au fil des portes franchies.
**Règle : on garde ce fichier au niveau « jalon + condition de validation ». Le détail va ailleurs
(principes → `CLAUDE.md`, dette → `TECH_DEBT.md`, implémentation → le code).**

> **État au 9 juin 2026 :** M0 et M1 franchis et **prouvés** (CI verte, tests chaos réels). Jalon courant : **M2**.

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

### M2 — Cœur tradable en argent fictif — ⏳ EN COURS

- [ ] API REST (placer / annuler) + WebSocket (carnet, trades, ticker)
- [ ] Bougies (candles) enregistrées dans TimescaleDB
- [x] Endpoint admin qui crédite un solde (dépôts encore simulés)
- [x] Comptes verrouillés + flag `LINKED` sur les deux jambes (clôt TD-4)
- [ ] Session de trading multi-comptes en ligne de commande, sans dérive de solde

### M3 — Sécurisé

- [ ] Keycloak branché, clés d'API émises
- [ ] Requêtes non authentifiées rejetées
- [ ] Rate limiting par utilisateur

### M4 — Custody réelle (2e zone à haut risque)

- [ ] Dépôt stablecoin avec N confirmations → crédite le registre **puis** le moteur
- [ ] Retrait : débite-et-verrouille **puis** diffuse on-chain
- [ ] Un événement rejoué ne crédite jamais deux fois
- [ ] Gestion des clés revue par quelqu'un de compétent

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

1. **Le settlement** (frontière moteur ↔ registre) — ✅ fondation posée et prouvée en M1.
2. **La custody** (M4) — là où les exchanges meurent. Clés MPC, audit, aucun code non compris ligne à ligne.
3. **La conformité** — conditionne le droit d'ouvrir, pas une formalité de fin.

---

## Jalon courant : M2 — tâches concrètes

- [ ] `gateway` : API REST placer/annuler qui écrit dans le topic `commands` (jamais d'appel direct au moteur)
- [ ] WebSocket : diffusion du carnet (L2), des trades et du ticker
- [ ] Agrégation des bougies (OHLCV) dans TimescaleDB
- [x] Endpoint admin de crédit (utilitaire, **pas** la logique de dépôt M4)
- [x] Comptes verrouillés + flag `LINKED` (clôt TD-4 ; chaos 4 réécrit et vert en CI)
- [ ] Test de bout en bout : session multi-comptes en ligne de commande, réconciliation sans écart

Réflexe de supervision sur chaque fonction touchant un solde : « contre quoi ça tourne, qu'est-ce que ça prouve ? ».

---

## Rappel honnête

M0+M1 franchis = la fondation est étanche, mais ce n'est que ~30 % du chemin. Les 70 % restants (custody,
conformité, durcissement) concentrent le risque et ne se démontrent pas dans une démo. Ne pas se précipiter,
faire auditer custody + settlement, et traiter le juridique comme le vrai chemin critique.