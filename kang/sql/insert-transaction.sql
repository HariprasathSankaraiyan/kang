INSERT INTO {schema}.transactions (id, hash, business_time)
VALUES (%(tx_id)s, %(hash)s, %(business_time)s)
ON CONFLICT (hash, business_time) DO NOTHING;