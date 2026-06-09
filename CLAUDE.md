# CLAUDE.md — Règles permanentes du projet

Ce fichier est lu automatiquement à chaque session. Il contient les règles **non négociables**.
Si une demande entre en conflit avec une règle ci-dessous, signale-le au lieu de l'ignorer.

---

## Le projet

Plateforme d'échange centralisée. Les utilisateurs tradent des actifs créés en interne (off-chain,
seulement dans notre base). La seule valeur réelle qui entre/sort est du stablecoin (USDT/USDC) en custody.

Conséquence permanente : « virtuel » ne veut pas dire « non régulé ». Dès qu'il y a de l'argent réel,
c'est une activité financière régulée.

---

## Règle d'or : prouver, pas prétendre

Un jalon n'est **jamais** « fait » parce que le code compile ou que les tests passent.
Il est fait quand sa **condition de validation est prouvée** dans un environnement fidèle.

- « Build vert » ≠ « jalon franchi ».
- Sur tout ce qui touche un solde, la bonne question n'est pas « est-ce que les tests passent ? »
  mais **« contre quoi tournent-ils, et qu'est-ce qu'ils prouvent ? »**.
- Si tu te surprends à simplifier pour faire passer une commande au vert, arrête-toi et signale-le.

---

## Invariants de correction monétaire (ne jamais violer)

1. **Un seul journal ordonné fait foi.** Le topic Kafka `commands` (une partition par paire) est la
   source de vérité. Aucune commande n'entre dans le moteur sans passer d'abord par ce journal.

2. **Identifiants déterministes, dérivés du journal — jamais de compteur local ni d'aléatoire :**
   - `tradeId = (commandOffset << 16) | matchIndex`
   - `transferId = (tradeId << 4) | legIndex`
   Un compteur local (`AtomicLong`) ou un UUID casse l'idempotence : interdit sur ces chemins.

3. **Idempotence par rejet de doublon TigerBeetle.** Rejouer un événement réécrit le même `transferId`,
   que TigerBeetle rejette. C'est ce qui rend la reprise sûre.

4. **Ordre strict : valider l'offset APRÈS l'écriture durable**, jamais avant. (Engine : publier le trade
   avant d'avancer l'offset `commands`. Settlement : ack Kafka après l'écriture TigerBeetle.)

5. **Montants en entiers** (plus petite unité). Jamais de virgule flottante sur de l'argent.

6. **Modèle de comptes** (voir `docs/`) : un registre par actif ; comptes disponible + verrouillé par
   (utilisateur, actif) ; comptes système externe + frais. Un transfert ne traverse jamais deux registres.

---

## Discipline de test

- **Tests contre la vraie infra.** Les tests d'intégration / chaos tournent contre un **vrai** TigerBeetle
  et un **vrai** Kafka (Testcontainers), jamais contre des maquettes.
- **Aucune maquette qui recopie la logique de production.** Un `Fake*` qui duplique `SettlementService`
  dérive et teste du code mort. Le test doit exercer le vrai code.
- **Tests chaos avant le code** sur les chemins critiques. Les trois de référence : double livraison,
  crash en plein lot, replay depuis un offset ancien. Assertion finale toujours : solde moteur = solde
  TigerBeetle, au centime près.
- Un garde-fou (réconciliation) doit aussi être jugé sur « peut-il se déclencher à tort ? », pas seulement
  « détecte-t-il un vrai écart ? ». Ne jamais déclencher sur un replay incomplet.

---

## Frontière des jalons (ne pas franchir en avance)

- **Appliquer une commande déjà présente dans le journal** (ex. un `adjustBalance`) de façon déterministe
  = jalon courant. Autorisé.
- **Faire entrer de la valeur depuis l'extérieur** (écouteur on-chain, hash de transaction, confirmations,
  stablecoin) = **M4 (custody)**. Si tu touches à la chaîne, arrête : ce n'est pas le bon jalon.
- La custody est l'endroit où les exchanges meurent. Aucun code custody généré sans revue ligne à ligne.

---

## Dette technique

Toute simplification consciente va dans `TECH_DEBT.md` (localisation exacte, raison, jalon de traitement).
Une dette assumée et écrite est saine ; une dette oubliée est un piège. On ne règle une dette que quand
elle commence à faire mal ou avant le lancement réel.

---

## Sécurité des accès

- **Aucun secret en clair.** Jamais de token (`ghp_...`) passé en ligne de commande ni committé.
  Utiliser `gh auth login` (keyring) ou un secret de dépôt côté CI.
- Si un secret a été exposé, il est compromis : le révoquer et le régénérer, ne pas le réutiliser.

---

## Quand tu es bloqué

Si tu tournes plus de quelques minutes sans produire de sortie : arrête, rétrécis le problème,
et expose le constat factuel (ce qui est observé vs attendu) avant de tenter une correction.
Ne t'acharne pas sur un test qui ne conditionne pas le jalon courant.


---

## Pièges déjà rencontrés (règles issues d'erreurs réelles)

### P-1 — Tests Docker / Testcontainers : jamais en local sur macOS

Ne jamais lancer les tests d'intégration (chaos, Testcontainers : TigerBeetle, Kafka) en local
sur macOS / Docker Desktop. Le socket de Docker Desktop n'est pas négociable proprement par
Testcontainers — c'est un puits de temps déjà rencontré deux fois (essais de `docker.sock`,
`docker.raw.sock`, `.testcontainers.properties`, désactivation du daemon Gradle… tout échoue).

Règle :
- **En local, on se limite à la compilation** (`./gradlew :module:compileTestKotlin`).
- **Les tests d'intégration font foi uniquement en CI** (GitHub Actions, `ubuntu-latest`,
  Docker standard) — c'est l'environnement de référence décidé pour ces tests.
- Pour vérifier un test d'intégration : **committer, pousser, lire le résultat du job dans
  l'onglet Actions**. Jamais bricoler le socket Docker local.
- Si un contournement de socket local est tenté, c'est une régression de méthode : arrête-toi.

### P-2 — Ne pas raisonner en boucle sur un problème non reproduit

Si un bug est soupçonné mais pas reproduit, ne pas enchaîner les hypothèses en boucle.
Écrire le test qui tranche, l'exécuter, et conclure sur le résultat — pas sur le raisonnement.

Règle :
- Sur un doute, la première action est **un test qui donne un résultat binaire**, pas une analyse.
- Si le problème n'est pas reproduit après **deux essais**, s'arrêter et signaler le constat
  factuel (observé vs attendu) au lieu de continuer à chercher.
- Un soupçon n'est pas un bug : ne pas corriger un comportement qu'aucun test ne met en défaut.