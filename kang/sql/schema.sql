-- select schema
SET search_path TO :schema;
--;;
CREATE TABLE facts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    key TEXT NOT NULL,
    value BYTEA NOT NULL,
    hash TEXT NOT NULL
);
--;;
ALTER TABLE facts ADD CONSTRAINT fact_uq UNIQUE (key, hash);
--;;
CREATE INDEX facts_key_idx ON facts(key);
--;;
CREATE INDEX facts_hash_idx ON facts(hash);
--;;
CREATE TABLE transactions (
    id UUID PRIMARY KEY,
    hash TEXT NOT NULL,
    business_time TIMESTAMPTZ NOT NULL,
    at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
--;;
ALTER TABLE transactions ADD CONSTRAINT business_hash_uq UNIQUE (hash, business_time);
--;;
CREATE INDEX transactions_business_time_idx ON transactions(business_time DESC);
--;;
CREATE INDEX transactions_tx_time_idx ON transactions(at DESC);
--;;
CREATE INDEX transactions_hash_idx ON transactions(hash);
--;;