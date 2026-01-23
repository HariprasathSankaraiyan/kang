INSERT INTO {schema}.facts (id, hash, key, value)
VALUES (gen_random_uuid(), %(hash)s, %(key)s, %(value)s)
ON CONFLICT (key, hash) DO NOTHING;
