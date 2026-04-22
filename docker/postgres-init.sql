-- Transactions table: one row per product line in the Excel
CREATE TABLE IF NOT EXISTS transactions (
    id                        SERIAL PRIMARY KEY,
    distributor_id            TEXT NOT NULL,
    retailer_id               TEXT NOT NULL,
    sku_name                  TEXT,
    product_category_snapshot TEXT,
    secondary_transaction_id  TEXT UNIQUE NOT NULL,
    transaction_date          DATE NOT NULL,
    secondary_gross_value     NUMERIC(12, 2),
    secondary_tax_amount      NUMERIC(12, 2),
    secondary_net_value       NUMERIC(12, 2),
    reminder_sent_at          TIMESTAMPTZ,
    reminder_count            INTEGER DEFAULT 0,
    escalation_sent_at        TIMESTAMPTZ,
    warning_sent_at           TIMESTAMPTZ,
    created_at                TIMESTAMPTZ DEFAULT NOW()
);

-- Payment replies: one row per retailer reply received
CREATE TABLE IF NOT EXISTS payment_replies (
    id                    SERIAL PRIMARY KEY,
    transaction_id        TEXT REFERENCES transactions(secondary_transaction_id),
    retailer_id           TEXT NOT NULL,
    distributor_id        TEXT NOT NULL,
    reply_received_at     TIMESTAMPTZ NOT NULL,
    promised_payment_date DATE,
    promised_days         INTEGER,
    amount_confirmed      NUMERIC(12, 2),
    raw_reply             TEXT,
    remarks               JSONB,
    parse_source          TEXT,   -- 'duckling', 'dateparser', or 'unparseable'
    created_at            TIMESTAMPTZ DEFAULT NOW()
);

-- Indexes for common queries
CREATE INDEX IF NOT EXISTS idx_txn_distributor ON transactions(distributor_id);
CREATE INDEX IF NOT EXISTS idx_txn_retailer    ON transactions(retailer_id);
CREATE INDEX IF NOT EXISTS idx_txn_date        ON transactions(transaction_date);
CREATE INDEX IF NOT EXISTS idx_txn_reminder    ON transactions(reminder_sent_at);
CREATE INDEX IF NOT EXISTS idx_reply_txn       ON payment_replies(transaction_id);
