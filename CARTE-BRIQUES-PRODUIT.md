# Carte des briques produit — Plateforme de tokenisation immobilière

> Vue de navigation, PAS spécification détaillée. Donne le « quoi » et l'ordre de dépendance,
> pas le « comment ». Chaque brique se spécifie en détail juste avant d'être codée (cadrage
> ancré dans le code réel), se code, se prouve, puis devient la fondation de la suivante.
> Mise à jour à chaque brique franchie.

---

## Principe d'ensemble

Deux strates qu'on ne mélange jamais :

- **Infrastructure** (M0–M4) : settlement, custody, sécurité. Fondation stable, prouvée jusqu'au
  retrait (custody dépôt + retrait verrou/état/nonce). Ne se refait pas — s'étend.
- **Produit tokenisation** : ce qui crée la valeur réelle (biens, statuts, droits, marché). Se
  construit PAR-DESSUS l'infrastructure, brique par brique.

Règle de rigueur (vaut pour toute la phase) :
- **Money-path** (flux d'argent, contrôle d'accès aux fonds, droits économiques, prix, émission) =
  rigueur maximale, tests réels contre vraie infra, on prouve le REFUS autant que le passage.
- **Habillage** (UI, lecture, présentation, confort) = niveau preuve de concept, on va vite.

Objectif stratégique : construire un produit **démontrable** (qu'un régulateur/investisseur peut voir
et manipuler), pas le produit final exhaustif. La validation juridique du montage et le choix du
prestataire MPC restent les vrais chemins critiques, hors de cette carte technique.

---

## Le modèle produit en une phrase

Un carnet d'ordres commun où deux classes de détenteurs échangent des fractions de biens
immobiliers réels : des **étrangers spéculatifs** (sans droit aux loyers ni à l'acquisition —
pure exposition au prix, fournissent la liquidité) et des **citoyens agréés** d'une juridiction
(droit aux loyers sur les biens de leur juridiction, droit d'acquérir le bien à 100 % des tokens).
Les citoyens accèdent via l'API d'une filiale régulée (point de contrôle) ; les étrangers en direct.
Stratégie : base régulée unique (Abu Dhabi/ADGM), parc immobilier réel et localisé.

---

# PHASE 1 — Le cœur de contrôle  ✅ FRANCHIE ET PROUVÉE

Les trois briques fondatrices. Toutes prouvées en CI contre vrai PostgreSQL + vrai TigerBeetle.

### Brique 1 — Statut des détenteurs  ✅ PROUVÉE
Chaque compte porte un statut (`UNVERIFIED` / `FOREIGN_SPECULATIVE` / `CITIZEN_APPROVED` /
`SUSPENDED`), une juridiction (agréés seulement), une trace d'audit. Dans Postgres, lu au contrôle,
jamais dans l'accountId TB ni le journal Kafka (mutable → casserait l'idempotence).
**Preuve** : AccountStatusIntegrationTest (vrai PostgreSQL, tests de rejet des contraintes CHECK), job `status`.

### Brique 2 — Gestion par bien  ✅ PROUVÉE
Chaque bien = un actif distinct (ledger TB), nombre fixe de tokens, cap table, juridiction attachée.
Tables `properties`, `symbols`, `property_holders`. Émission : 100 % au compte propriétaire dédié
(SYSTEM_PROPERTY_OWNER_USER). Stablecoin = ledger partagé unique. TB fait foi ; property_holders est
une projection (voir TD-15). Limite ledgerId 24 bits = 16,7M biens (TD-14).
**Preuve** : tests P1–P4 (émission, conservation, fidélité projection, juridiction), job `property`.

### Brique 3 — Contrôle au transfert  ✅ PROUVÉE
`TransferGuard` refuse/autorise selon statut × juridiction du bien, AVANT publication Kafka.
UNVERIFIED/SUSPENDED → rejet ; FOREIGN_SPECULATIVE → autorisé partout ; CITIZEN_APPROVED(J) →
autorisé sur biens de J seulement. Fail-closed : symbole inconnu ou non qualifiable → rejet.
**Preuve** : R1–R6 (vrai PostgreSQL) + R7 (guard prouvé câblé dans placeOrder), job `transfer`.

**◄ À la fin de la phase 1 : le cœur de contrôle existe.** Le moteur fait la bonne chose. Mais ce
n'est pas encore démontrable par un tiers — il manque le marché, les prix, l'accès, l'interface.

---

# PHASE 2 — Le produit démontrable

Ce qui transforme le cœur de contrôle en produit qu'un tiers peut voir et manipuler.
Ordre indicatif ; chaque brique cadrée juste avant d'être codée.

### Brique 4 — Droits économiques (loyers)  ✅ PROUVÉE
Loyers aux CITIZEN_APPROVED au prorata entre agréés (compaction). Lit TB en direct — pas
`property_holders` (TD-15 non bloquante ici). Reste non divisible → reste dans le pool.
Idempotence : `distributionKey` par distribution → mêmes TB transferIds → TB Exists sur retry.
**Preuve** : L1 (montant exact), L2 (étranger=0), L3 (mauvaise juridiction=0, défense en profondeur),
L4 (conservation), L5 (idempotence), L6 (reste), job `rent`. TD-16 : bornes rentTransferId inscrites.
**Seuil 100 % → droit d'acquisition** : reporté à B6 (dépend du buy-out TWAP pour avoir le prix réel).
**Dépend de** : phase 1 complète.

### Brique 5 — Marché primaire / prévente (money-path)  ⚠ DRAPEAU JURIDIQUE
Mécanique d'émission et de distribution initiale des tokens d'un bien (du compte propriétaire vers
les souscripteurs).
**Mécanique** (neutre) : peut être cadrée et construite — comment un bien passe de « créé » à
« tokens distribuables », souscription, attribution.
**Adressage** (NON neutre) : QUI peut souscrire et COMMENT on collecte les fonds du public reste
conditionné à la clarification réglementaire (ADGM). La mécanique se construit ; le ciblage de
collecte s'active sous cadre régulé. Ne pas opérer une collecte publique non autorisée.

### Brique 6 — Rachat par buy-out sur prix TWAP (money-path)
Le mécanisme qui donne le « vrai prix » du bien : prix de marché continu (TWAP), ancré par la
possibilité de rachat total (buy-out). Différenciateur majeur.
**À détailler par le porteur produit avant cadrage** — calcul exact du TWAP, déclenchement et
exécution du buy-out (mouvements tokens + stablecoin). Money-path strict.

### Brique 7 — API filiale : création de biens + présentation  ✅ PROUVÉE
- **Création de tokens pour un bien** (money-path) : `POST /properties` gateway — rôle `subsidiary`
  ou `exchange-admin` (filtre Spring Security) + `subsidiary_jurisdiction` JWT↔body (pattern
  uid-from-token de B3) + validations bornes → 403/400 si refus, RIEN publié. Publié sur
  `property_commands` → `PropertyCommandConsumer` → `PropertyService.createProperty()`.
  Seul chemin production : gateway gardé → topic → consumer. Aucune porte dérobée.
- **Présentation** (habillage) : `GET /properties/{propertyLedgerId}` lit Postgres. `property_metadata`
  table séparée, jamais lue par settlement/engine.
**Preuve** : `PropertyApiWiringTest` PA1–PA5 (`verify(publisher, never())` prouve l'absence de
publication sur refus), `PropertyIntegrationTest` P5 (idempotence re-livraison → DB UNIQUE →
pas de double émission), jobs `build` + `property`.

### Brique 8 — Portefeuilles utilisateurs (lecture, preuve de concept)
Modèle « façon Binance » : l'utilisateur dépose du stablecoin → solde virtuel dans le système →
le portefeuille AFFICHE ce qu'il détient (stablecoin + fractions de biens). C'est de la LECTURE des
soldes TigerBeetle, pas de la custody. La custody réelle des fonds (clés MPC) reste séparée (M4-MPC,
bloquée sur le choix prestataire).

### Brique 9 — Front-end minimum viable (habillage)
Interface qui rend tout le reste visible et manipulable : liste des biens, passage d'ordres, carnet,
portefeuille, prix. Vient EN DERNIER — s'appuie sur toutes les briques précédentes. Preuve de
concept : suffisant pour démontrer, pas durci pour le grand public.

**◄ À la fin de la phase 2 : le produit est démontrable.** Un tiers peut voir un parc de biens,
des prix réels, passer des ordres selon son statut, voir son portefeuille. C'est le vrai levier.

---

## Hors de cette carte (dépendances externes, pas du code à cadrer ici)

- **M4 signature/diffusion MPC** : custody réelle des fonds. Bloquée sur le choix du prestataire MPC
  (lié au régime juridique). Sans elle, dépôt/retrait stablecoin restent en intégration, pas en prod.
- **Retrait stablecoin — contrôle de statut** : reporté de B3, cadrage dédié avec lecture custody-watcher.
- **Dossier régulateur ADGM** : qualification du token, périmètre de licence, non-sollicitation,
  adaptations fiscales. LE vrai chemin critique. À avancer en parallèle du code, délai le plus long.
- **Validation juridique de la prévente** : conditionne l'adressage de la brique 5.

---

## Dettes liées au produit (voir TECH_DEBT.md)
- **TD-14** : limite ledgerId 24 bits (16,7M biens) — non bloquante.
- **TD-15** : property_holders diverge de TB dès le 1er trade. DOIT être résolue AVANT que la brique 4
  distribue des loyers depuis cette table. Remédiations écrites : syncHolder avant ack + reconstruction.

---

## Comment on avance (méthode, inchangée)

1. Pour chaque brique : cadrage juste-à-temps (l'agent localise le code réel, inventaire des chemins
   touchés, on décide l'architecture, on borne) → code → preuve en CI → on inscrit comme franchie.
2. On ne spécifie pas les briques futures en détail à l'avance (hypothèses = fiction). La carte donne
   le cap ; le détail vient au moment de coder.
3. Une brique à la fois. Finie et prouvée avant la suivante.
4. Money-path : rigueur maximale, prouver le refus, vraie infra. Reste : preuve de concept, vite.
5. Cadrage : toujours exiger l'inventaire des chemins existants touchés + le test qui échoue si
   l'invariant est violé (leçon B2 : un défaut vit à la frontière entre le neuf et l'ancien).