# Feuille de route — Plateforme d'échange

Document de travail à garder et cocher au fil de l'eau.

---

## Le projet en une phrase

Une plateforme d'échange centralisée où les utilisateurs tradent des actifs que je crée moi-même (ils n'existent que dans ma base, pas sur une blockchain). La seule valeur réelle qui entre et sort est du stablecoin (USDT/USDC), détenu en custody.

Conséquence à ne jamais oublier : techniquement, c'est plus simple qu'un Binance classique (presque pas de blockchain). **Juridiquement, ce n'est pas plus simple** — dès que de l'argent réel entre et sort, c'est une activité financière régulée, peu importe le mot « virtuel ».

---

## Deux chantiers en parallèle dès le jour 1

### Chantier juridique (le délai le plus long, hors de mon contrôle)

- [ ] Prendre rendez-vous avec un avocat en droit financier français/européen — **cette semaine**
- [ ] Faire classer juridiquement les actifs créés (MiCA / licence CASP ? ou MiFID II / monnaie électronique ?)
- [ ] Identifier le régime d'autorisation applicable et constituer le dossier
- [ ] Suivre l'examen (l'AMF peut prendre ~4 mois après un dossier complet)

Ce chantier tourne en fond pendant tout le développement. S'il démarre tard, il retarde le lancement d'un an.

### Chantier technique

Voir la feuille de route des jalons plus bas.

---

## La pile technique (décidée)

Tout repose sur la JVM pour que le cœur sensible partage un seul langage sûr — c'est aussi le terrain où l'aide de l'IA est la plus fiable.

- **Moteur de matching + carnet d'ordres** : exchange-core (choisi plutôt que viabtc, qui est en C)
- **Registre des soldes** : TigerBeetle (comptabilité en partie double)
- **Bus d'événements** : Kafka (ou Redpanda)
- **Services** : Kotlin + Spring Boot
- **Authentification** : Keycloak
- **Custody stablecoin** : web3j, clés confiées à un prestataire MPC
- **Données** : PostgreSQL (utilisateurs, ordres), Redis (cache), TimescaleDB (données de marché)
- **Frontend** : React + TradingView Lightweight Charts
- **Infra** : Docker / Kubernetes + Prometheus / Grafana
- **Non open source (pas d'équivalent sérieux)** : KYC (Sumsub / Onfido), surveillance AML (Chainalysis)

---

## Feuille de route — jalons

Principe : chaque jalon est **prouvé**, pas seulement « codé ». On ne passe au suivant que quand la porte est franchie.

### M0 — Le moteur tourne (≈ semaine 1)

- [ ] Un test envoie des ordres dans exchange-core et obtient les bons trades
- [ ] Toute l'infra démarre avec une seule commande (`docker compose up`)
- [ ] CI verte

### M1 — Le settlement est correct (≈ semaine 4) — porte qui porte tout le reste

- [ ] Le cycle ordre → matching → règlement fonctionne pour une paire
- [ ] Les tests « chaos » passent : couper le service en plein milieu, livrer deux fois un événement, rejouer depuis un ancien point
- [ ] Jamais d'écart entre le solde du moteur et celui du registre

**On ne construit rien d'autre avant que cette porte soit verte.**

### M2 — Cœur tradable en argent fictif (≈ semaine 8)

- [ ] API REST (placer / annuler) + WebSocket (carnet, trades, ticker)
- [ ] Bougies (candles) enregistrées dans TimescaleDB
- [ ] Endpoint admin qui crédite un solde (dépôts encore simulés)
- [ ] Session de trading multi-comptes en ligne de commande, sans dérive de solde

### M3 — Sécurisé (≈ semaine 9)

- [ ] Keycloak branché, clés d'API émises
- [ ] Les requêtes non authentifiées sont rejetées
- [ ] Limites de débit (rate limiting) par utilisateur

### M4 — Custody réelle (≈ semaine 13)

- [ ] Dépôt stablecoin avec N confirmations → crédite le registre **puis** le moteur
- [ ] Retrait : débite-et-verrouille **puis** diffuse on-chain
- [ ] Un événement rejoué ne crédite jamais deux fois
- [ ] Gestion des clés revue par quelqu'un de compétent

### M5 — Utilisable (≈ semaine 16)

- [ ] Interface React : graphique, saisie d'ordres, soldes, historique
- [ ] Tourne sur l'API désormais stabilisée

### M6 — Conforme et durci (en continu après M5)

- [ ] KYC/AML réel : bloque les comptes non vérifiés, screene les retraits
- [ ] Exercice de reprise après panne (snapshot + replay) réellement répété
- [ ] Test de charge atteignant la cible de volume
- [ ] Runbook d'incident écrit
- [ ] Revue de sécurité de la custody et du settlement

### M7 — Lancement pilote (porte juridique, pas technique)

- [ ] Autorisation légale obtenue
- [ ] M6 complet
- [ ] Ouverture à un petit groupe fermé, avec limites de retrait

---

## Les trois endroits où le projet se gagne

Le reste est de la plomberie rapide. Ces trois points méritent une attention manuelle — pas du code généré à la chaîne.

### 1. Le settlement (frontière moteur ↔ registre)

Danger : le moteur garde les soldes en mémoire, TigerBeetle de façon durable ; s'ils divergent en silence, on perd de l'argent sans le savoir.

Invariants de protection :
- Un seul journal ordonné fait foi
- Chaque transfert a un identifiant dérivé du numéro de séquence de l'événement → un doublon est ignoré (idempotence)
- On applique dans l'ordre, on valide l'offset après l'écriture
- La reprise après panne = simplement rejouer
- Un job vérifie en continu que solde moteur = solde registre
- **Les tests « chaos » s'écrivent avant le code**

### 2. La custody

C'est là que les exchanges meurent vraiment — pas à cause du moteur, mais des erreurs de clés, des bugs de retrait, des écarts de réconciliation. Surface la plus attaquée d'internet.
- Clés confiées à un prestataire MPC
- Cette partie est auditée
- Aucun code que je ne comprends pas ligne à ligne

### 3. La conformité

KYC/AML et autorisation ne sont pas une formalité de fin : ils conditionnent le droit d'ouvrir.
- Crochets de conformité prévus dès le départ (vérification, surveillance, ségrégation des fonds), pas en rustine

---

## Cette semaine, concrètement

- [ ] Créer le monorepo Gradle multi-module en Kotlin (modules `engine`, `settlement`, `common`, `gateway` réservé)
- [ ] Écrire le `docker-compose.yml` qui démarre Kafka, Postgres, Redis, TigerBeetle et Keycloak en une commande
- [ ] Faire tourner le service `engine` sur Spring Boot avec exchange-core et faire passer le plus petit test de matching réel (= M0)
- [ ] Mettre la CI en place (GitHub Actions) et la garder verte dès le premier commit
- [ ] Figer sur papier le modèle de comptes TigerBeetle (disponible / verrouillé par utilisateur et par actif, décomposition d'un trade en transferts, frais)
- [ ] Prendre rendez-vous avec l'avocat

Les quatre premières tâches sont de la plomberie que l'IA gère bien → avancer vite.
La cinquième est celle où il faut ralentir et réfléchir soi-même.
La sixième tourne en parallèle de tout le reste.

---

## Rappel honnête

Atteindre M2 (un exchange qui matche des ordres en argent fictif) est possible en quelques semaines, surtout avec l'IA — et donnera l'impression d'être à 80 %. Ce sera en réalité ~30 %.

Les 70 % restants — correction du settlement, custody, conformité — ne se démontrent pas dans une démo et concentrent l'essentiel du temps et du risque.

Ce n'est pas une raison d'abandonner. C'est une raison de ne pas se précipiter, de faire auditer les deux ou trois points sensibles par quelqu'un d'expérimenté, et de traiter le juridique comme le vrai chemin critique. Le projet est faisable. Il n'est pas facile — et c'est en le sachant qu'on le réussit.

---

## Modèle de comptes TigerBeetle (rappel de référence)

- Un **registre (ledger) par actif** : USDT a le sien, chaque actif créé a le sien. Un transfert ne traverse jamais deux registres.
- Par utilisateur et par actif : un compte **disponible** + un compte **verrouillé**.
- Comptes système par actif : un compte **externe** (monde extérieur : dépôts / retraits) + un compte **frais**.
- Partie double : chaque transfert débite un compte et en crédite un autre du **même montant**. Un trade avec frais = plusieurs transferts.
- Montants en **entiers** (plus petite unité), jamais de nombres à virgule.

Exemple d'un trade (Alice achète à Bob, frais 1 %) :
- Registre USDT : 5000 quittent « Alice · verrouillé » → 4950 vers « Bob · disponible » + 50 vers « Frais »
- Registre FOO : 1000 quittent « Bob · verrouillé » → 990 vers « Alice · disponible » + 10 vers « Frais »
- Vérification : chaque registre reste équilibré (4950 + 50 = 5000 ; 990 + 10 = 1000)

(`FOO` = nom d'exemple, à remplacer par le nom réel de l'actif. Convention de paire : `BASE/QUOTE`, ex. `MONACTIF/USDT`.)
