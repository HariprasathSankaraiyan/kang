#!/usr/bin/env python3
"""cricket match scoring as a simple example

setup:
  create db kang_demo
  export DB_URL="postgresql://localhost/kang_demo"
  python example.py
"""

from kang import FactStore

store = FactStore(url="postgresql://localhost/kang_demo")

# match uuid: india vs australia, 2025-01-15
match = "cricket.match.550e8400-e29b-41d4-a716-446655440000"

# record score at 14:30
store.add_fact(
    {"kang_id": match, "runs": 87, "wickets": 2},
    business_time="2025-01-15T14:30:00"
)
print("recorded: 87/2 at 14:30")

# correction: runs should be 88 (wide ball missed)
store.add_fact(
    {"kang_id": match, "runs": 88},
    business_time="2025-01-15T14:30:00"
)
print("corrected: 88/2 at 14:30")

# wicket falls at 14:35
store.add_fact(
    {"kang_id": match, "wickets": 3},
    business_time="2025-01-15T14:35:00"
)
print("updated: wicket at 14:35")

# late data from 14:27
store.add_fact(
    {"kang_id": match, "batsman": "Kohli", "bowler": "Starc"},
    business_time="2025-01-15T14:27:30"
)
print("backfilled: ball data from 14:27")

print("\n--- all facts ---")
for fact in store.get_facts(kang_id=match):
    print(f"{fact.get('at')}: {fact}")

print("\n--- state at 14:30 ---")
past = store.as_of(match, "2025-01-15T14:30:00")
print(f"runs: {past.get('runs')}, wickets: {past.get('wickets')}")

print("\n--- current state ---")
current = store.rollup(match)
print(f"runs: {current.get('runs')}, wickets: {current.get('wickets')}")
print(f"batsman: {current.get('batsman')}, bowler: {current.get('bowler')}")
