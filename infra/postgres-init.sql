CREATE TABLE IF NOT EXISTS payments (
    transaction_id TEXT PRIMARY KEY,
    card_id TEXT,
    amount NUMERIC,
    currency TEXT,
    merchant_id TEXT,
    country TEXT,
    device_id TEXT,
    auth_method TEXT,
    event_time TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS tps_metrics (
    id SERIAL PRIMARY KEY,
    ts TIMESTAMPTZ NOT NULL,
    country TEXT,
    auth_method TEXT,
    amount_bucket TEXT,
    tps INT NOT NULL
);
