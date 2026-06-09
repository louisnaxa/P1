# Withdrawal — état d'avancement

## Ce qui est prouvé en CI (`WithdrawalChaosTest` — TB + Postgres, mock signer, pas de MPC)

| Test | Invariant prouvé |
|------|-----------------|
| W1 | Lock réserve les fonds : TB PENDING bloque un second débit (`ExceedsCredits`) |
| W2 | `initiate()` est idempotent : même `withdrawalId` → no-op TB et DB |
| W3 | `broadcastPending()` appelle le signer exactement une fois par ligne LOCKED |
| W4 | `confirmBroadcast()` finalise le débit TB : montant exact (`externalAfter - externalBefore == AMOUNT`) + conservation zéro-somme |
| W5 | Reprise crash-1 : ligne LOCKED sans tx est naturellement reprise par `broadcastPending()` au redémarrage |
| W6 | Reprise crash-2 : ligne BROADCAST confirmée sans jamais appeler le signer (anti-double-diffusion structurel, pas conditionnel) |
| W7 | `voidWithdrawal()` restitue les fonds exactement : `getSpendableBalance` restauré à AMOUNT, `getBalance` inchangé |
| N8 | Nonces assignés et signer appelé dans l'ordre strictement croissant des nonces (contrainte dure Ethereum : nonce N+1 bloqué on-chain tant que N n'est pas miné) |
| N9 | Reprise crash-3b via vrai chemin : `broadcastPending()` avec signer qui throw commit le nonce en DB ; au retry, le MÊME nonce est réutilisé — NEXTVAL n'est pas rappelé |

## Ce qui reste (hors CI — à valider en intégration MPC + audit)

- **Signature MPC réelle** : `WithdrawalSigner` production → prestataire MPC ; `raw_tx` persisté avant diffusion
- **Diffusion on-chain** : envoi `raw_tx` au nœud Ethereum
- **Confirmations on-chain** : watcher N-blocs → `confirmBroadcast()` (BROADCAST→CONFIRMED piloté par la chaîne)
- **Résolution trou de nonce** : transaction d'annulation si nonce N bloqué au-delà du TTL (gasPrice élevé, to=self, value=0)
- **Erreurs on-chain** : transaction dropped / replaced / nonce expiré → VOID ou retry

Tout ce qui est au-dessus reste derrière l'interface `WithdrawalSigner` — la frontière MPC est nette.

---

## Deux règles TigerBeetle 0.16.x à ne jamais redécouvrir

Ces deux comportements ne sont pas documentés clairement et ont causé des échecs silencieux en CI (W4 retournait le bon code mais ne bougeait pas les fonds).

### Règle 1 — Comptes explicites obligatoires sur POST_PENDING et VOID_PENDING

Sur un transfert résolvant (`POST_PENDING_TRANSFER` ou `VOID_PENDING_TRANSFER`), TigerBeetle **ne hérite pas** des comptes du transfert PENDING correspondant. Passer `debitAccountId = 0` ou `creditAccountId = 0` poste vers/depuis le compte 0 — TB retourne SUCCESS sans erreur, aucun fonds ne bouge sur les bons comptes.

```kotlin
// OBLIGATOIRE — même si c'est les mêmes comptes que le PENDING
batch.setDebitAccountId(AccountIds.available(userId, ledgerId))
batch.setCreditAccountId(AccountIds.external(ledgerId))
```

### Règle 2 — Montant explicite obligatoire sur POST_PENDING (asymétrie avec VOID)

Sur `POST_PENDING_TRANSFER`, `amount = 0` poste **zéro**, pas le montant du PENDING. Il faut passer le montant explicitement.

Sur `VOID_PENDING_TRANSFER`, `amount = 0` annule **la totalité** du PENDING — comportement correct, pas besoin de passer le montant.

```kotlin
// POST_PENDING : passer le montant explicitement
batch.setAmount(amount)   // 0 poste zéro, pas le montant pending

// VOID_PENDING : amount = 0 est correct (annule tout)
// ne pas appeler setAmount() ou passer 0
```

Ces deux règles sont documentées dans `SettlementService.kt` (`postPendingWithdrawal` et `voidPendingWithdrawal`).
