CREATE TABLE IF NOT EXISTS dummy_init_marker (
    id SERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
