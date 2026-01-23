SELECT facts.value,
  transactions.business_time,
  transactions.at,
  transactions.id
FROM {schema}.transactions
JOIN {schema}.facts
  ON transactions.hash = facts.hash
WHERE facts.key = ANY(%(kang_ids)s)
  AND (%(upto)s IS NULL OR transactions.business_time <= %(upto)s)
ORDER BY facts.key, transactions.business_time;