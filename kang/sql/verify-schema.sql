SELECT COUNT(*)::INTEGER
FROM information_schema.tables 
WHERE table_schema = %(schema)s 
  AND table_name IN ('facts', 'transactions');