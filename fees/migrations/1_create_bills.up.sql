-- Money is stored as BIGINT minor units (tetri / cents), never NUMERIC/FLOAT,
-- matching money.Money. Status and currency use TEXT + CHECK rather than
-- Postgres ENUMs so adding a value later is a simple constraint change.
--
-- Rows are never deleted: bills are financial records, so there is no
-- ON DELETE CASCADE.

CREATE TABLE bills (
    id           UUID        PRIMARY KEY,
    account_id   TEXT        NOT NULL,
    currency     TEXT        NOT NULL CHECK (currency IN ('GEL', 'USD')),
    status       TEXT        NOT NULL CHECK (status IN ('OPEN', 'CLOSING', 'CLOSED', 'CHARGE_FAILED')),
    period_start TIMESTAMPTZ NOT NULL,
    period_end   TIMESTAMPTZ NOT NULL,
    -- Frozen total in minor units of `currency`; NULL while the bill is OPEN.
    total_amount BIGINT,
    created_at   TIMESTAMPTZ NOT NULL,
    closed_at    TIMESTAMPTZ,

    CHECK (period_end > period_start),
    -- Mirrors the domain state machine so bad writes fail at the DB too.
    CHECK ((status = 'OPEN') = (total_amount IS NULL)),
    CHECK ((status = 'CLOSED') = (closed_at IS NOT NULL)),

    -- Target for line_items' composite foreign key below.
    UNIQUE (id, currency)
);

CREATE INDEX bills_account_id_idx ON bills (account_id, period_start DESC);

CREATE TABLE line_items (
    id              UUID        PRIMARY KEY,
    bill_id         UUID        NOT NULL,
    description     TEXT        NOT NULL,
    -- Positive = fee, negative = refund/credit.
    amount          BIGINT      NOT NULL CHECK (amount <> 0),
    currency        TEXT        NOT NULL,
    idempotency_key TEXT        NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL,

    -- Composite FK: an item's currency must equal its bill's currency.
    FOREIGN KEY (bill_id, currency) REFERENCES bills (id, currency),
    -- A retried request with the same key can't add the item twice.
    -- Also serves as the index for "items of bill X".
    UNIQUE (bill_id, idempotency_key)
);
