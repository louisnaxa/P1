# Carte des briques produit — Plateforme de tokenisation immobilière

> Vue de navigation, PAS spécification détaillée. Donne le « quoi » et l'ordre de dépendance,
> pas le « comment ». Chaque brique se spécifie en détail juste avant d'être codée (cadrage
> ancré dans le code réel), se code, se prouve, puis devient la fondation de la suivante.
> À inscrire dans le repo une fois validée. Mise à jour à chaque brique franchie.

---

## Principe d'ensemble

Deux strates qu'on ne mélange jamais :

- **Infrastructure** (M0–M4) : settlement, custody, sécurité. Fondation stable, déjà prouvée
  jusqu'au retrait. Ne se refait pas — s'étend.
- **Produit tokenisation** : ce qui crée la valeur réelle (biens, statuts, droits différenciés).
  Se construit PAR-DESSUS l'infrastructure, brique par brique.

Règle de rigueur (vaut pour toute la phase) :
- **Money-path** (flux d'argent, contrôle d'accès aux fonds, droits économiques) = rigueur maximale,
  tests réels, on prouve le REFUS autant que le passage.
- **Habillage** (UI, confort, cas rares) = niveau preuve de concept, on va vite.

Objectif stratégique : construire le **levier démontrable** au plus vite (un cœur qui prouve la
disruption), pas le produit final exhaustif. La validation juridique du montage et le choix du
prestataire MPC restent les vrais chemins critiques, hors de cette carte technique.

---

## Le modèle produit en une phrase

Un carnet d'ordres commun où deux classes de détenteurs échangent des fractions de biens
immobiliers réels : des **étrangers spéculatifs** (sans droit aux loyers ni à l'acquisition —
pure exposition au prix, fournissent la liquidité) et des **citoyens agréés** d'une juridiction
(droit aux loyers sur les biens de leur juridiction, droit d'acquérir le bien à 100 % des tokens).
Les citoyens accèdent via l'API d'une filiale régulée (point de contrôle) ; les étrangers
accèdent en direct à l'exchange offshore.

---

## Les briques, dans l'ordre de dépendance

Chaque brique présuppose la précédente. On les fait une à une, finie-prouvée avant la suivante.

### Brique 1 — Statut des détenteurs  ✅ FRANCHIE ET PROUVÉE (CI verte, job status)

**Quoi** : chaque compte porte un statut (`UNVERIFIED` → `FOREIGN_SPECULATIVE` /
`CITIZEN_APPROVED` → `SUSPENDED`), une juridiction (pour les agréés seulement), et une trace
d'audit (quand/qui a changé le statut).

**Dépend de** : l'identité non falsifiable de M3 (table `users`, uid résolu depuis le token). On étend, on ne refait pas.

**Où vit la donnée** : Postgres (table `users`). JAMAIS dans l'accountId TigerBeetle ni dans le
journal Kafka — le statut est mutable, ça casserait l'idempotence. Lu au moment du contrôle.

**Rigueur** : money-path (le statut conditionne l'accès aux fonds).

**Décidé** : KYC ultra-léger pour le spéculatif (exclusion US + AML de base), frictionless pour
préserver la liquidité internationale.

**Ce lot ne fait PAS** : le contrôle au transfert (c'est la brique 3). Il pose seulement le
modèle, la lecture, et la transition de statut.

### Brique 2 — Gestion par bien  ◄ EN COURS

**Quoi** : chaque bien immobilier devient un actif distinct dans le registre, avec un nombre fixe
de tokens et une cap table (qui détient quelle fraction). Une juridiction est attachée à chaque bien.

**Dépend de** : le modèle de comptes/ledger TigerBeetle existant. Point à éprouver au cadrage :
comment le modèle actuel porte de NOMBREUX actifs, et comment se structurent les paires d'échange
sur le carnet commun.

**Rigueur** : money-path (la cap table par bien est de la propriété d'actifs).

**Question ouverte à trancher au cadrage** (produit, pas technique) : un bien = un ledger ?
combien de biens le modèle supporte-t-il ? les tokens d'un bien se tradent contre quoi (stablecoin) ?

### Brique 3 — Contrôle au transfert

**Quoi** : refuser ou autoriser un transfert selon le statut du détenteur ET la juridiction du
bien. Un étranger ne peut pas recevoir un droit réservé aux citoyens ; un citoyen ne peut toucher
que les biens de sa juridiction ; un compte SUSPENDED/UNVERIFIED est bloqué.

**Dépend de** : brique 1 (statut) + brique 2 (biens et leur juridiction). C'est la combinaison des deux.

**Où s'applique le contrôle** : AVANT que l'événement devienne durable — gateway avant publication
Kafka pour les ordres, service avant la première écriture TB pour les retraits. JAMAIS dans le
consumer/engine (qui rejouent le journal et casseraient l'idempotence). Principe identique à
« valider l'offset après l'écriture durable » du CLAUDE.md.

**Rigueur** : money-path maximal. C'est LE cœur démontrable — on prouve le refus dans chaque cas.

**Jalon de démonstration** : à la fin de cette brique, le levier existe — un carnet commun où deux
classes tradent des fractions de biens réels avec des droits différenciés, vérifiés, infalsifiables.

### Brique 4 — Droits économiques (loyers + seuil 100 %)

**Quoi** :
- **Loyers** : versés uniquement aux détenteurs agréés, au prorata de leur part ENTRE AGRÉÉS
  (pas de la part totale). Mécanique de « compaction » : plus les étrangers détiennent, plus le
  rendement par token des agréés monte.
- **Seuil 100 %** : détecter qu'un détenteur agréé atteint 100 % des tokens d'un bien et
  déclencher son droit contractuel d'acquisition.

**Dépend de** : les trois briques précédentes (statut, biens, contrôle).

**Rigueur** : money-path (distribution d'argent réel + déclenchement d'un droit).

**Note** : moins urgent pour démontrer le potentiel — peut venir après que le cœur (1+2+3) tourne.

---

## Ce qui N'EST PAS dans cette carte (volontairement)

- Le front / l'UI : habillage, niveau preuve de concept, après le cœur.
- M4 signature/diffusion MPC : repoussé jusqu'au choix du prestataire (dépend de la juridiction régulée).
- La structure juridique des biens (SPV, etc.) : à clarifier avec un avocat — conditionne la brique 2.
- La validation juridique du montage token : chemin critique hors-code, prime sur tout déploiement réel.

---

## Comment on avance (rappel de méthode)

1. Pour chaque brique : cadrage juste-à-temps (l'agent localise le code réel, on décide
   l'architecture, on borne) → code → preuve en CI → on inscrit la brique comme franchie.
2. On ne spécifie PAS les briques futures en détail à l'avance (hypothèses non validées = fiction).
   Cette carte donne le cap ; le détail vient au moment de coder, ancré dans le code réel.
3. Une brique à la fois. Finie et prouvée avant la suivante.
4. Money-path : rigueur maximale, prouver le refus. Reste : preuve de concept, vite.# Carte des briques produit — Tokenisation immobilière

Fichier réservé. Contenu à venir.
