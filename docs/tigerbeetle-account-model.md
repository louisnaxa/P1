# TigerBeetle Account Model

Design document — M0 deliverable.

---

## Principles

- One **ledger per asset** (USDT = ledger 1, FOO = ledger 2, BAR = ledger 3, ...).
- A transfer never crosses ledgers.
- Per user per asset: **2 accounts** (available + locked).
- Per asset: **2 system accounts** (external + fees).
- All amounts are **integers** in the smallest unit (e.g. 1 USDT = 1_000_000 units with 6 decimals).
- Double-entry: every transfer debits one account and credits another for the same amount.

---

## Account ID Scheme

Account IDs are 128-bit. We encode them deterministically:

```
account_id = (user_id << 32) | (ledger_id << 8) | account_type
```

| account_type | meaning   |
|--------------|-----------|
| 0x01         | available |
| 0x02         | locked    |

System accounts use reserved user IDs:

| user_id | meaning  |
|---------|----------|
| 0       | external (deposits/withdrawals) |
| 1       | fees     |

Example: Alice (user_id=1000), USDT (ledger=1):
- Available: `(1000 << 32) | (1 << 8) | 0x01` = `0x000003E8_00000101`
- Locked:    `(1000 << 32) | (1 << 8) | 0x02` = `0x000003E8_00000102`

---

## Account Types per Asset

For each asset (ledger L):

| Account          | Owner   | Ledger | Flags                       |
|------------------|---------|--------|-----------------------------|
| User · available | user N  | L      | debits_must_not_exceed_credits |
| User · locked    | user N  | L      | debits_must_not_exceed_credits |
| External         | system  | L      | (none — can go negative)    |
| Fees             | system  | L      | debits_must_not_exceed_credits |

`debits_must_not_exceed_credits` prevents a user from spending more than they have.

---

## Transfer Flows

### 1. Deposit (USDT enters the platform)

User deposits 100 USDT:

| # | Debit            | Credit             | Amount  | Note              |
|---|------------------|--------------------|---------|-------------------|
| 1 | External · USDT  | Alice · available  | 100     | On-chain confirmed |

### 2. Place Order (lock funds)

Alice places a BID for 10 FOO at price 50 USDT (total 500 USDT):

| # | Debit              | Credit           | Amount | Note       |
|---|--------------------|------------------|--------|------------|
| 1 | Alice · available  | Alice · locked   | 500    | USDT locked |

### 3. Trade Settlement (the critical path)

Alice buys 10 FOO from Bob at 50 USDT. Fee: 0.1% on each side.

**USDT ledger (quote):**

| # | Debit            | Credit             | Amount | Note              |
|---|------------------|--------------------|--------|-------------------|
| 1 | Alice · locked   | Bob · available    | 499.50 | Net to seller     |
| 2 | Alice · locked   | Fees               | 0.50   | Taker fee (0.1%)  |

**FOO ledger (base):**

| # | Debit            | Credit             | Amount | Note              |
|---|------------------|--------------------|--------|-------------------|
| 3 | Bob · locked     | Alice · available  | 9.99   | Net to buyer      |
| 4 | Bob · locked     | Fees               | 0.01   | Maker fee (0.1%)  |

**Unlock remainder** (if partial fill, return unlocked portion):

| # | Debit            | Credit             | Amount | Note              |
|---|------------------|--------------------|--------|-------------------|
| 5 | Alice · locked   | Alice · available  | (remainder) | Unlock unused |
| 6 | Bob · locked     | Bob · available    | (remainder) | Unlock unused |

### 4. Cancel Order (unlock funds)

| # | Debit          | Credit             | Amount | Note      |
|---|----------------|--------------------|--------|-----------|
| 1 | User · locked  | User · available   | full   | Unlock    |

### 5. Withdrawal (USDT leaves the platform)

| # | Debit              | Credit           | Amount | Note              |
|---|--------------------|------------------|--------|-------------------|
| 1 | Alice · available  | External · USDT  | 100    | On-chain broadcast |

---

## Idempotency

Each transfer uses a deterministic ID derived from the event sequence number:

```
transfer_id = hash(event_source, sequence_number, sub_index)
```

TigerBeetle rejects duplicate transfer IDs natively. Replaying the same event is a no-op.

---

## Reconciliation Invariant

For every asset (ledger), at all times:

```
sum(all credits) == sum(all debits)
```

A background job compares exchange-core in-memory balances against TigerBeetle balances. Any divergence triggers an alert and halts trading for that pair.
