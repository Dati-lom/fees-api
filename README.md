# Fees API

A Fees API built with [Encore](https://encore.dev) (Go) and [Temporal](https://temporal.io).
A bill is opened at the start of a fee period, line items (fees and refunds) accrue on it,
and at the end of the period, or earlier on request, it is closed: the total is frozen
and charged. Bills are single-currency; GEL and USD are supported out of the box, and more
currencies can be added at runtime.

## Running it

**Prerequisites:** Go 1.26+, Docker, and the [Encore CLI](https://encore.dev/docs/install).

1. **Start Temporal** (dev server + Web UI at http://localhost:8233):

   ```bash
   docker compose up -d
   ```

   Postgres is not in `docker-compose.yml`; Encore provisions it itself.

2. **Create the local secrets file.** Every endpoint requires a service token, and
   `.secrets.local.cue` is gitignored, so a fresh clone gets `401` on every request
   until this file exists:

   ```bash
   echo 'ServiceToken: "local-dev-token"' > .secrets.local.cue
   ```

3. **Run the app:**

   ```bash
   encore run
   ```

   The API listens on http://localhost:4000. Encore's dev dashboard (traces, API docs)
   is at http://localhost:9400.

**Tests:**

```bash
encore test ./...
```

The workflow tests use Temporal's test environment with a mocked clock, so a
month-long bill period runs in milliseconds.

## API

All requests need `Authorization: Bearer <ServiceToken>`. Amounts are integers in **minor
units** (tetri / cents): `1250` GEL means 12.50 GEL. Errors are returned as
`{"code": "...", "message": "..."}`.

The examples assume:

```bash
API=http://localhost:4000
AUTH="Authorization: Bearer local-dev-token"
```

### Endpoints

| Method | Path | Description |
|---|---|---|
| `POST` | `/bills` | Open a bill for a period (needs `Idempotency-Key`) |
| `POST` | `/bills/:id/line-items` | Add a fee or refund to an open bill (needs `Idempotency-Key`) |
| `POST` | `/bills/:id/close` | Close a bill early; returns the frozen total and its line items |
| `POST` | `/bills/:id/retry-charge` | Retry charging a bill whose charge failed |
| `GET` | `/bills/:id` | Get a bill with its line items |
| `GET` | `/bills` | List bills (`account_id`, `status`, `limit` query params) |
| `GET` | `/currencies` | List supported currencies |

### Open a bill

```bash
curl -X POST $API/bills -H "$AUTH" -H "Idempotency-Key: open-acc-42-2026-10" \
  -d '{
    "account_id": "acc-42",
    "currency": "GEL",
    "period_start": "2026-10-01T00:00:00Z",
    "period_end": "2026-11-01T00:00:00Z"
  }'
```

This returns the bill with `"status": "OPEN"`, `"total": null` and an `id`. The bill closes by
itself at `period_end`.

### Add line items

```bash
BILL=<id from above>

# A fee
curl -X POST $API/bills/$BILL/line-items -H "$AUTH" -H "Idempotency-Key: wire-fee-981" \
  -d '{"description": "Outgoing wire fee", "amount": {"amount": 1250, "currency": "GEL"}}'

# A refund (negative amount)
curl -X POST $API/bills/$BILL/line-items -H "$AUTH" -H "Idempotency-Key: wire-fee-981-refund" \
  -d '{"description": "Wire fee refund", "amount": {"amount": -250, "currency": "GEL"}}'
```

The call returns once the item is stored.

### Close a bill early

```bash
curl -X POST $API/bills/$BILL/close -H "$AUTH"
```

The response shows `"status": "CLOSING"`, the `total` being charged (`1000` GEL here) and every
line item it covers. The charge runs in the background: poll `GET /bills/:id` until the status
is `CLOSED` or `CHARGE_FAILED`. Closing an already closing or closed bill just returns it.

### Read

```bash
curl $API/bills/$BILL -H "$AUTH"
curl "$API/bills?account_id=acc-42&status=OPEN&limit=20" -H "$AUTH"
curl $API/currencies -H "$AUTH"
```

### Retry a failed charge

```bash
curl -X POST $API/bills/$BILL/retry-charge -H "$AUTH"
```

### Status codes

| Code | When |
|---|---|
| `400 invalid_argument` | Bad input, unsupported currency, item currency ≠ bill currency, `Idempotency-Key` reused with a different body |
| `400 failed_precondition` | A refund would make the bill's total negative |
| `401 unauthenticated` | Missing or wrong service token |
| `404 not_found` | No such bill |
| `409 aborted` | The bill is no longer open (adding items), or isn't `CHARGE_FAILED` (retry) |
| `503 unavailable` | Temporal didn't answer in time; retry with the same `Idempotency-Key` |

### Idempotency

`POST /bills` and `POST /bills/:id/line-items` require an `Idempotency-Key` header. Retrying
with the same key and body returns the original result and never creates a second bill or
item. Reusing a key with a *different* body is rejected with `400` instead of silently
returning the old result.

### Trying out payment failures

There's no real payment provider; a mock charger decides by `account_id` prefix:

- `decline-…`: every charge is declined, so the bill ends `CHARGE_FAILED`.
- `flaky-…`: the first 2 attempts fail with a transient error and Temporal retries them.
  Watch it in the Temporal UI.

### Adding a currency

The `currencies` table is the source of truth. No code change or restart is needed:

```sql
INSERT INTO currencies (code, name, minor_units) VALUES ('EUR', 'Euro', 2);
```

(`encore db shell fees --write` opens a shell.)

## Design

### Why Temporal

Each bill is one long-running Temporal workflow (the *entity workflow* pattern), started
when the bill is opened and finished when it is charged. Temporal solves three problems here:

- **Race conditions.** The workflow is the only writer for its bill and handles one message
  at a time, so an item added at the same moment as a close can't be lost or charged
  twice. Line items and close are Temporal *Updates*: a validator rejects invalid requests
  (closed bill, wrong currency) before they're recorded, and the caller gets a synchronous
  answer.
- **Long-running timers.** "Close at the end of the period" is a durable timer inside the
  workflow. No cron jobs, and no polling a table for bills that are due.
- **Reliability.** Database writes and the charge are activities that Temporal retries
  with backoff. A crash or deploy mid-close resumes where it stopped, and the charge uses a
  provider idempotency key, so a retry never takes the money twice.

Postgres holds a durable copy of every bill, written by activities, and serves reads.
The database enforces the same rules as the code: the status values, a total that exists
exactly when the bill isn't `OPEN`, items in the bill's currency, and a total that's
never negative. Before a total is charged, it's checked against the sum of the stored
line items.

### Bill state machine

```
             close / period end       charge ok (or total is 0)
   OPEN ─────────────────────────▶ CLOSING ─────────────────────▶ CLOSED
                                    ▲    │
                       retry-charge │    │ declined / retries exhausted
                                    │    ▼
                                CHARGE_FAILED
```

- **OPEN**: accepts line items.
- **CLOSING**: no more items. The workflow waits for item writes still in progress, then
  freezes the total and charges it. Retrying from `CHARGE_FAILED` charges the same frozen
  total again.
- **CLOSED**: charged. The workflow ends and its ID can never be reused, so a late retry
  can't reopen the bill.

### Money

Amounts are `int64` minor units, never floats. Every amount carries its currency, and adding
amounts in different currencies is an error. A bill has one currency, and items in another
are rejected (no FX). A refund that would take the total below zero is rejected, so a bill
never ends with a credit that would then have to be tracked or paid out.

### Database transactions

The workflow is already the single writer for its bill, but writes can still overlap. For
example, an activity that timed out may still be running while Temporal starts its retry.
So the store doesn't rely on the workflow alone and keeps the data consistent on its own.
All of this is in `fees/store`.

#### Transactions

Every write that checks state and then changes it runs in one transaction, so the check
and the change can't be split by another writer. For example, adding a line item reads
the bill's status and inserts the item. Closing sums the stored items, compares the sum
with the workflow's total, and updates the bill.

Each write is also safe to repeat, because Temporal retries activities:

- Inserts use `ON CONFLICT DO NOTHING`.
- Status changes are conditional updates (`UPDATE … WHERE status = 'CLOSING'`).
- If the bill is already in the target state, the repeat counts as success.

#### SERIALIZABLE for writes

Writes run at `SERIALIZABLE`. The case that needs it is adding a line item at the same
time as a close:

- Adding an item **reads `bills`** (is it `OPEN`?) and **writes `line_items`**.
- Closing **reads `line_items`** (the sum) and **writes `bills`** (`CLOSING` + total).

Neither transaction writes a row the other one writes. Under `READ COMMITTED` or
`REPEATABLE READ` both can commit, which is a *write skew*: the item lands on a bill whose
total was frozen without it. `SERIALIZABLE` makes Postgres abort one of the two.

Aborted transactions (serialization failure `40001`, deadlock `40P01`) are retried up to 3
times in the store. If they still fail, the error goes back to Temporal, which retries the
whole activity with backoff.

#### REPEATABLE READ for reads

`GET /bills/:id` reads the bill and then its line items in two queries. They run in one
`REPEATABLE READ, READ ONLY` transaction, so both queries see the same snapshot. Without
it, a close committed between the two queries could return a total that doesn't match the
items listed. A read-only snapshot can't cause write skew, so `SERIALIZABLE` isn't needed
there; it would only add aborts.
