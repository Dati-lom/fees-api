-- Money is stored as BIGINT minor units (tetri / cents), never NUMERIC/FLOAT,
-- matching money.Money. Status uses TEXT + CHECK rather than a Postgres ENUM
-- so adding a value later is a simple constraint change.
--
-- Rows are never deleted: bills are financial records, so there is no
-- ON DELETE CASCADE.

-- Supported currencies (ISO 4217). This table is the source of truth: insert
-- a row to support a new currency, no code change or restart needed.
CREATE TABLE currencies (
    code        TEXT     PRIMARY KEY CHECK (code ~ '^[A-Z]{3}$'),
    name        TEXT     NOT NULL,
    -- Decimal places of the minor unit: 2 means 1250 = 12.50.
    minor_units SMALLINT NOT NULL CHECK (minor_units BETWEEN 0 AND 4)
);

INSERT INTO currencies (code, name, minor_units) VALUES
    ('GEL', 'Georgian Lari',        2),
    ('USD', 'United States Dollar', 2);

CREATE TABLE bills (
    id           UUID        PRIMARY KEY,
    account_id   TEXT        NOT NULL,
    currency     TEXT        NOT NULL REFERENCES currencies (code),
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
    CHECK (total_amount >= 0),

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

    -- Composite FK: an item's currency must equal its bill's currency
    -- (and so is also guaranteed to exist in currencies).
    FOREIGN KEY (bill_id, currency) REFERENCES bills (id, currency),
    -- A retried request with the same key can't add the item twice.
    -- Also serves as the index for "items of bill X".
    UNIQUE (bill_id, idempotency_key)
);
