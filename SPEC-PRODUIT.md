# Spécification produit — Infrastructure d'échange pour parc immobilier tokenisé (stratégie Abu Dhabi)

> Version du produit à construire, alignée sur la stratégie : base régulée unique (Abu Dhabi/ADGM),
> parc immobilier réel et localisé, deux classes de détenteurs, conçue pour être présentable à un régulateur.
> Ce document fige le QUOI. Le COMMENT (architecture, code) se cadre brique par brique au moment de coder.
> Toute affirmation de viabilité juridique reste une hypothèse à valider auprès de l'ADGM / d'un avocat —
> ce document décrit le produit, pas sa qualification légale.
> Carte de navigation des briques : voir `CARTE-BRIQUES-PRODUIT.md`.

---

## 1. Ce que le produit EST

Une infrastructure d'échange centralisée qui permet de tokeniser des biens immobiliers d'un parc
réel et localisé (Abu Dhabi au départ), et d'en échanger les fractions sur un carnet d'ordres commun,
entre deux classes de détenteurs aux droits différenciés.

Ce n'est pas un exchange crypto généraliste. C'est l'infrastructure de marché d'un parc immobilier
précis — pensée pour durer comme la couche d'échange de référence de ce parc.

## 2. Les deux classes de détenteurs

- **Résident agréé** (citoyen/résident de la juridiction, vérifié) : droits économiques complets sur
  les biens de sa juridiction — perception des loyers (concentrés entre agréés), droit d'acquisition
  à 100 % des tokens d'un bien, possibilité de résidence-contre-loyer.
- **Investisseur étranger** : exposition au prix du bien, sans droit économique de propriété, sans
  loyer, sans acquisition. Fournit la liquidité du marché secondaire.

Le **statut** d'un compte pilote ce qu'il peut détenir, recevoir et déclencher. C'est la donnée
fondatrice du produit (brique 1, prouvée).

## 3. Les capacités du produit (le QUOI fonctionnel)

### 3.1 — Cœur de contrôle (phase 1 — ✅ PROUVÉ)

- **Statut des détenteurs** : résident agréé / étranger / non vérifié / suspendu, + juridiction,
  + trace d'audit. Lu au moment du contrôle, jamais dans le journal immuable. *(Brique 1 ✅)*
- **Gestion par bien** : chaque bien = un actif distinct, nombre fixe de tokens, cap table par bien,
  juridiction attachée au bien. *(Brique 2 ✅)*
- **Contrôle au transfert** : autoriser/refuser un transfert selon le statut du détenteur et la
  juridiction du bien, AVANT que l'événement devienne durable. Refus prouvé. *(Brique 3 ✅)*
- **Carnet d'ordres commun** : les deux classes tradent sur le même carnet (liquidité partagée),
  l'accès et les droits étant filtrés en amont. *(Infrastructure M2 + contrôle B3)*

À la fin de la phase 1, le moteur fait la bonne chose — mais ce n'est pas encore démontrable par un
tiers. Il manque le marché, les prix, l'accès filiale, le portefeuille et l'interface (phase 2).

### 3.2 — Le produit démontrable (phase 2 — à construire)

Ce qui transforme le cœur de contrôle en produit qu'un tiers peut voir et manipuler :

- **Droits économiques** *(money-path)* : loyers aux agréés au prorata ENTRE agréés (compaction) ;
  seuil 100 % → droit d'acquisition. Dépend de TD-15 résolue avant toute distribution réelle.
- **Marché primaire / prévente** *(money-path, ⚠ drapeau juridique)* : mécanique d'émission et de
  distribution initiale des tokens. La mécanique est neutre et constructible ; l'ADRESSAGE (qui
  souscrit, comment on collecte les fonds du public) reste conditionné au cadre régulé.
- **Rachat par buy-out sur prix TWAP** *(money-path)* : prix de marché continu ancré par la
  possibilité de rachat total. Donne le « vrai prix » du bien. Différenciateur majeur. À détailler
  par le porteur produit avant cadrage (calcul TWAP, déclenchement/exécution du buy-out).
- **API filiale** : création de tokens pour un bien *(money-path)* + présentation du bien
  (métadonnées, photos, documents — habillage, preuve de concept).
- **Portefeuilles utilisateurs** *(lecture)* : modèle « façon Binance » — dépôt stablecoin → solde
  virtuel → affichage de ce que l'utilisateur détient (stablecoin + fractions). Pas de custody ici.
- **Front-end minimum viable** *(habillage)* : rend tout visible et manipulable. Vient en dernier.

### 3.3 — Fonctionnalités avancées (plus tard — chacune sous son propre cadre régulé)

Chacune ajoute une activité régulée distincte → à activer une par une quand le cadre le permet :
- **Résidence-contre-loyer** : un vendeur qui cède une fraction continue d'y résider en payant un
  loyer proportionnel à la part cédée. Besoin universel, proche du « home equity release » régulé.
- **Hypothèque des tokens** : emprunter contre des tokens détenus (= activité de prêt, régulée,
  risque systémique → tard, avec soin).

## 4. Ce qui distingue ce produit (proposition de valeur, triée)

**Tient sans dépendre d'aucune friction réglementaire (durable) :**
- Liquidité continue là où l'immobilier et les REIT sont illiquides.
- Découverte de prix réelle par le marché (vs estimation opaque).
- Résidence-contre-loyer (libérer du capital sans déménager).
- Acquisition fractionnée progressive jusqu'à 100 %.

**Dépend d'un cadre régulé favorable (à activer avec le régulateur) :**
- Exposition à l'immobilier d'une juridiction pour des étrangers sans droit de propriété
  (le moat « capital étranger entrant » — légal là où le régulateur receveur l'autorise).

## 5. Contraintes de conception propres à la stratégie

- **Présentable à un régulateur** : traçabilité de bout en bout, contrôle des droits explicite et
  vérifiable, auditabilité (qui a fait quoi, quand, sous quel statut) = fonctions de premier plan,
  pas de l'habillage. C'est ce que l'ADGM regardera.
- **Lien au bien réel** : le token reflète un bien réel détenu/géré par la structure locale. La
  cohérence entre l'état on-plateforme et l'état réel (propriété, statut du bien) doit être traçable.
- **Money-path = rigueur maximale** : statut, contrôle au transfert, loyers, émission, prix, settlement.
  Le reste (UI, présentation, lecture) = niveau preuve de concept, on va vite.

## 6. Ce que ce document NE tranche PAS (et qui ne nous appartient pas)

- La qualification juridique de l'instrument (actif virtuel / dérivé / titre) → ADGM + avocat.
- Le périmètre de licence (qui peut-on accueillir : résidents, qualifiés, étrangers) → ADGM.
- La tenabilité de la posture de non-sollicitation vis-à-vis des pays tiers → avocat transfrontalier.
- L'adressage de la prévente (qui souscrit, collecte publique) → conditionné au cadre régulé.
- Les adaptations fiscales (double mutation, prix prévente=rachat) → négociation régulateur.

Ces points sont des PRÉCONDITIONS au déploiement réel, pas à la construction du produit.
On construit le produit ; on déploie quand ces réponses sont obtenues.

## 7. Fondation déjà acquise (sur quoi ça se construit)

Le produit s'appuie sur l'infrastructure prouvée M0–M4 : settlement command-sourced et idempotent
(M1), carnet + comptes verrouillés (M2), identité non falsifiable + sécurité (M3), custody dépôt +
retrait verrou/état/nonce (M4). La custody réelle des fonds (signature/diffusion MPC) reste bloquée
sur le choix du prestataire MPC. La phase produit ÉTEND cette base, ne la refait pas.

## 8. Ordre de construction

**Phase 1 — cœur de contrôle (✅ prouvé)** : statut → gestion par bien → contrôle au transfert.
À la fin : le moteur fait la bonne chose, mais pas encore démontrable par un tiers.

**Phase 2 — produit démontrable (à construire)** : droits économiques → marché primaire/prévente →
rachat buy-out TWAP → API filiale → portefeuilles → front MVP.
À la fin : un tiers peut voir le parc, des prix réels, passer des ordres selon son statut, voir son
portefeuille. C'est le vrai levier régulateur/investisseur.

**En parallèle, hors code (vrai chemin critique)** : dossier régulateur ADGM, choix prestataire MPC.

Méthode inchangée : une brique à la fois, cadrage juste-à-temps ancré dans le code réel (avec
inventaire des chemins existants touchés), codée, prouvée en CI, puis inscrite. Un jalon n'est
franchi que prouvé.