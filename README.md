# Kang - Bitemporal Fact Storage

<img src="images/kang.jpg" alt="Kang" width="200"/>

Python library for bitemporal fact storage with audit trail, **inspired by [verter](https://github.com/tolitius/verter)**.

> **named after kang the conqueror and the TVA (time variance authority) from marvel's multiverse saga.**  
> just like the TVA monitors and maintains the sacred timeline, kang tracks when facts were true in reality (business time) while keeping an audit trail of when you recorded them.
> 
> ***he who remains… remembers everything…***

---

## table of contents

- [the problem](#the-problem)
  - [example: cricket match scoring](#example-cricket-match-scoring)
- [installation](#installation)
  - [1. install package](#1-install-package)
  - [2. initialize schema](#2-initialize-schema)
  - [3. use in code](#3-use-in-code)
- [schema](#schema)
- [quick start](#quick-start)
- [core concepts](#core-concepts)
  - [granular facts](#granular-facts)
  - [business time and transaction time](#business-time-and-transaction-time)
  - [the `at` field](#the-at-field)
  - [deduplication behavior](#deduplication-behavior)
- [methods](#methods)
  - [`add_fact(fact, business_time=None)`](#add_factfact-business_timenone)
  - [`add_facts(facts, business_time=None)`](#add_factsfacts-business_timenone)
  - [`get_facts(kang_id, upto=None, with_tx=False)`](#get_factskang_id-uptonone-with_txfalse)
  - [`rollup(kang_id, with_nils=False)`](#rollupkang_id-with_nilsfalse)
  - [`as_of(kang_id, time, with_nils=False)`](#as_ofkang_id-time-with_nilsfalse)
- [advanced usage](#advanced-usage)
  - [transaction metadata (`with_tx`)](#transaction-metadata-with_tx)
  - [nil values (`with_nils`)](#nil-values-with_nils)
  - [time range filtering (`upto`)](#time-range-filtering-upto)
- [database connection](#database-connection)
- [error handling](#error-handling)
- [dependencies](#dependencies)
- [extending to other databases](#extending-to-other-databases)

---

## the problem

tracking changing data over time is hard. your system faces several challenges:

- **late-arriving data**: information arrives after the fact
- **corrections**: you need to fix past mistakes without losing history
- **audit requirements**: you must prove what the data looked like at any point in time
- **time travel queries**: "what was the state at 2pm yesterday?"

without bitemporal tracking, you lose either history or query simplicity:

| approach | what you lose |
|----------|---------|
| **update in place** | history of corrections |
| **version columns** | simple queries for "state at time t" |
| **event logs** | easy reconstruction of current state |

### example: cricket match scoring

imagine tracking a live cricket match:
- ball-by-ball details arrive late (drs reviews take 30+ seconds)
- scoring official corrects runs from 87 to 88 (wide ball was missed)
- you need to answer: "what was the score at 14:30?"

**the solution:**

kang tracks **when facts were true** (business time) while keeping an audit trail of when you recorded them (transaction time):

- ✅ **query on business time**: "what was the score at 14:30?" (time travel queries)
- ✅ **audit with transaction time**: "when did we record this score?" (compliance/debugging)
- ✅ backfill historical data without losing chronology
- ✅ correct past mistakes while preserving original values
- ✅ store granular changes (only what changed, not full snapshots)

---

## installation

### 1. install package

```bash
cd kang
pip install -e .
```

### 2. initialize schema

the schema file uses `:schema` placeholder. replace it with your actual schema name:

```bash
# edit sql/schema.sql: change :schema to public (or your schema name)
# then execute it in your database
```

### 3. use in code

```python
from kang import FactStore

# default: uses 'public' schema
store = FactStore(url="postgresql://user:pass@localhost/mydb")

# custom schema (must match what you initialized)
store = FactStore(url="postgresql://user:pass@localhost/mydb", schema="my_app_schema")
```

> **note**: if the schema is not initialized, you'll get a `SchemaNotInitializedError`

---
## schema

kang uses two tables:

```
     facts table                    transactions table
┌─────────────────────┐          ┌──────────────────────┐
│ id      (uuid)      │          │ id         (uuid)    │
│ key     (text)      │          │ hash       (text) ───┼──┐
│ value   (bytea)     │          │ business_time        │  │
│ hash    (text)      │◄─────────┼──────────────────────┘  │
└─────────────────────┘          │ at      (tx_time)    │  │
  stores unique facts            └──────────────────────┘  │
                                   records when facts      │
                                   were true               │
                                                           │
                                   join: facts.hash = transactions.hash
```

**why two tables?**

- **facts**: stores each unique payload once (deduplication by hash)
- **transactions**: tracks when each fact was true (business_time) and when recorded (at)
- **efficiency**: same fact at multiple times → stored once in facts, referenced multiple times in transactions
- **corrections**: different facts at same business_time → multiple fact rows, each linked to same business_time
- **audit trail**: transaction time (`at`) shows when each fact was recorded

see `kang/sql/schema.sql` for complete table definitions and indexes.

## quick start

track a cricket match with corrections, late data, and time-travel queries:

```python
from kang import FactStore

store = FactStore(url="postgresql://user:pass@localhost/mydb")

# for your application that maintains a match table:
# | id                                   | team1     | team2     | date       | venue |
# |--------------------------------------|-----------|-----------|------------|-------|
# | 550e8400-e29b-41d4-a716-446655440000 | india     | australia | 2025-01-15 | mcg   |
#
# this is how you track history in kang using kang_id (uuid with prefix):
match_id = "cricket.match.550e8400-e29b-41d4-a716-446655440000"

# 1. record initial score at 14:30
tx_id = store.add_fact(
    {"kang_id": match_id, "runs": 87, "wickets": 2},
    business_time="2025-01-15T14:30:00"
)
# returns: '1471b44c-f83e-11f0-8b9c-bafd80b8a6a7'

# 2. correction: wide ball was missed, runs should be 88
#    creates a different fact at the same business time
tx_id = store.add_fact(
    {"kang_id": match_id, "runs": 88},
    business_time="2025-01-15T14:30:00"
)
# returns: '152c1422-f83e-11f0-8b9c-bafd80b8a6a7'

# 3. wicket falls at 14:35
tx_id = store.add_fact(
    {"kang_id": match_id, "wickets": 3},
    business_time="2025-01-15T14:35:00"
)
# returns: '15d81eac-f83e-11f0-8b9c-bafd80b8a6a7'

# 4. backfill: ball-by-ball data from 14:27 arrives late
tx_id = store.add_fact(
    {"kang_id": match_id, "batsman": "Kohli", "bowler": "Starc"},
    business_time="2025-01-15T14:27:30"
)
# returns: '168c01f6-f83e-11f0-8b9c-bafd80b8a6a7'

# get all facts (ordered by business time)
facts = store.get_facts(kang_id=match_id)
# returns:
# [
#   {
#     'kang_id': 'cricket.match.550e8400-e29b-41d4-a716-446655440000',
#     'batsman': 'Kohli',
#     'bowler': 'Starc',
#     'at': '2025-01-15T14:27:30+00:00'
#   },
#   {
#     'kang_id': 'cricket.match.550e8400-e29b-41d4-a716-446655440000',
#     'runs': 87,
#     'wickets': 2,
#     'at': '2025-01-15T14:30:00+00:00'
#   },
#   {
#     'kang_id': 'cricket.match.550e8400-e29b-41d4-a716-446655440000',
#     'runs': 88,
#     'at': '2025-01-15T14:30:00+00:00'
#   },
#   {
#     'kang_id': 'cricket.match.550e8400-e29b-41d4-a716-446655440000',
#     'wickets': 3,
#     'at': '2025-01-15T14:35:00+00:00'
#   }
# ]

# get facts up to 14:30
facts = store.get_facts(kang_id=match_id, upto="2025-01-15T14:30:00")
# returns: first 3 facts (up to and including 14:30)

# get current state (latest values for all attributes)
current_state = store.rollup(match_id)
# returns:
# {
#   'kang_id': 'cricket.match.550e8400-e29b-41d4-a716-446655440000',
#   'runs': 88,
#   'wickets': 3,
#   'batsman': 'Kohli',
#   'bowler': 'Starc',
#   'at': '2025-01-15T14:35:00+00:00'
# }

# time travel: what was the state at 14:30?
state_at_14_30 = store.as_of(match_id, "2025-01-15T14:30:00")
# returns:
# {
#   'kang_id': 'cricket.match.550e8400-e29b-41d4-a716-446655440000',
#   'runs': 88,
#   'wickets': 2,
#   'batsman': 'Kohli',
#   'bowler': 'Starc',
#   'at': '2025-01-15T14:30:00+00:00'
# }
```

---
## core concepts

### granular facts

facts in kang are **granular**—you only record the specific attributes that changed, not full snapshots.

```python
# ❌ Don't do this (full snapshot)
store.add_fact(
    {"kang_id": match_id, "runs": 88, "wickets": 2, "overs": 15, "team": "IND"},
    business_time="2025-01-15T14:30:00"
)

# ✅ Do this (only what changed)
store.add_fact(
    {"kang_id": match_id, "runs": 88},  # Only runs changed
    business_time="2025-01-15T14:30:00"
)
```

**why?**

1. **storage efficiency**: only changed values are stored
2. **semantic clarity**: the fact clearly indicates "runs changed to 88"
3. **rollup works**: `rollup()` merges all facts, latest value wins per attribute

when you call `rollup()` or `as_of()`, kang automatically merges all granular facts to give you the complete state.

---

### business time and transaction time

kang **stores** both times but **queries only on business time**:

- **business time**: when the fact was true in reality
- **transaction time**: when you recorded it in the database (stored for audit trail)

#### scenario 1: live scoring

at 14:30, score is 87/2, recorded immediately:

```python
from kang import FactStore
store = FactStore("postgresql://user:pass@localhost/mydb")

store.add_fact(
    {"kang_id": "cricket.match.550e8400-e29b-41d4-a716-446655440000", "runs": 87, "wickets": 2},
    business_time="2025-01-15T14:30:00"
)
# business_time = 2025-01-15T14:30:00 (when it happened)
# tx_time      = 2025-01-15T14:30:05 (when we recorded it)
```

#### scenario 2: late correction

at 14:35, realize the 14:30 score was actually 88 (wide ball missed):

```python
store.add_fact(
    {"kang_id": "cricket.match.550e8400-e29b-41d4-a716-446655440000", "runs": 88},
    business_time="2025-01-15T14:30:00"  # Still 14:30 (when it was true)
)
# business_time = 2025-01-15T14:30:00 (when it was true)
# tx_time      = 2025-01-15T14:35:12 (when we corrected it)
```

#### scenario 3: backfilling

at 15:00, add ball-by-ball data from 14:27 that arrived late:

```python
store.add_fact(
    {"kang_id": "cricket.match.550e8400-e29b-41d4-a716-446655440000", "batsman": "Kohli", "bowler": "Starc"},
    business_time="2025-01-15T14:27:30"  # Historical time
)
# business_time = 2025-01-15T14:27:30 (when the ball was bowled)
# tx_time      = 2025-01-15T15:00:00 (when we received the data)
```

> **note**: business time can be in the past (backfilling) or present (live updates). transaction time is always now().

---

### the `at` field

each fact returned by kang includes an **`at`** field showing its business time:

```python
facts = store.get_facts(kang_id="cricket.match.550e8400-e29b-41d4-a716-446655440000")
# [
#   {'kang_id': '...', 'runs': 87, 'wickets': 2, 'at': '2025-01-15T14:30:00'},
#   {'kang_id': '...', 'runs': 88, 'at': '2025-01-15T14:30:00'},
#   ...
# ]
```

- the `at` field **always equals business_time**
- when you don't provide `business_time`, kang uses the current time (transaction time) as the business time
- so `at` tells you "when this fact was true in reality"

#### viewing transaction metadata

to see **when facts were recorded** (audit trail), use `with_tx=True`:

```python
facts = store.get_facts(kang_id="cricket.match.550e8400-e29b-41d4-a716-446655440000", with_tx=True)
# [
#   {
#     'kang_id': '...',
#     'runs': 87,
#     'wickets': 2,
#     'at': '2025-01-15T14:30:00',       # When it was true
#     'tx_time': '2025-01-15T14:30:05',  # When we recorded it
#     'tx_id': '5f62d4c0-...'            # Transaction ID
#   },
#   {
#     'kang_id': '...',
#     'runs': 88,
#     'at': '2025-01-15T14:30:00',       # When it was true
#     'tx_time': '2025-01-15T14:35:12',  # When we corrected it
#     'tx_id': '8a93f5d1-...'
#   }
# ]
```

---

### deduplication behavior

kang prevents storing **identical facts** at the same business time:

```python
# First time: stores the fact
store.add_fact(
    {"kang_id": "cricket.match.550e8400-e29b-41d4-a716-446655440000", "runs": 87},
    business_time="2025-01-15T14:30:00"
)
# Returns: [<transaction_id>]

# Second time: exact same fact at same business time
result = store.add_fact(
    {"kang_id": "cricket.match.550e8400-e29b-41d4-a716-446655440000", "runs": 87},
    business_time="2025-01-15T14:30:00"
)
# Returns: {"noop": "fact already exists at this business time"}
```

**different facts at the same business time are allowed**:

```python
# Different fact (runs=88 instead of runs=87) at same business time
store.add_fact(
    {"kang_id": "cricket.match.550e8400-e29b-41d4-a716-446655440000", "runs": 88},
    business_time="2025-01-15T14:30:00"
)
# Returns: [<transaction_id>] - stored successfully
```

> **note**: this is the correction scenario—the original score of 87 and corrected score of 88 both exist at 14:30.

---

## methods

### `add_fact(fact, business_time=None)`

records a single fact.

**parameters:**
- `fact` (dict): Must include `kang_id` field for identity tracking
- `business_time` (str, optional): ISO 8601 timestamp. Defaults to current time if not provided

**returns:** transaction uuid or `{"noop": "message"}` if fact already exists at that business time

**example:**
```python
# record live data
tx_id = store.add_fact(
    {"kang_id": "cricket.match.550e8400-e29b-41d4-a716-446655440000", "runs": 92},
    business_time="2025-01-15T15:00:00"
)
# returns: '1a2b3c4d-f83e-11f0-8b9c-bafd80b8a6a7'

# record without business_time (uses current time)
tx_id = store.add_fact(
    {"kang_id": "cricket.match.550e8400-e29b-41d4-a716-446655440000", "wickets": 4}
)
# returns: '2b3c4d5e-f83e-11f0-8b9c-bafd80b8a6a7'

# duplicate fact returns noop
result = store.add_fact(
    {"kang_id": "cricket.match.550e8400-e29b-41d4-a716-446655440000", "runs": 92},
    business_time="2025-01-15T15:00:00"
)
# returns: {'noop': 'no changes to identities were detected...'}
```

---

### `add_facts(facts, business_time=None)`

records multiple facts in a single transaction.

**parameters:**
- `facts` (list[dict]): List of facts, each must include `kang_id`
- `business_time` (str, optional): ISO 8601 timestamp applied to all facts

**returns:** list of transaction uuids or `{"noop": "message"}`

**example:**
```python
# record multiple match updates together
tx_ids = store.add_facts([
    {"kang_id": "cricket.match.550e8400-e29b-41d4-a716-446655440000", "runs": 92, "wickets": 3},
    {"kang_id": "cricket.match.550e8400-e29b-41d4-a716-446655440000", "run_rate": 6.5}
], business_time="2025-01-15T15:00:00")
# returns: ['3c4d5e6f-f83e-11f0-8b9c-bafd80b8a6a7', '4d5e6f7a-f83e-11f0-8b9c-bafd80b8a6a7']
```

---

### `get_facts(kang_id, upto=None, with_tx=False)`

retrieves facts for a specific identity.

**parameters:**
- `kang_id` (str): Identity to retrieve facts for
- `upto` (str, optional): ISO 8601 timestamp. Only returns facts with `business_time <= upto`
- `with_tx` (bool): Include transaction metadata (`tx_time`, `tx_id`)

**returns:** list of facts ordered by business time

**example:**
```python
# get all facts
facts = store.get_facts(kang_id="cricket.match.550e8400-e29b-41d4-a716-446655440000")
# returns:
# [
#   {'kang_id': '...', 'batsman': 'Kohli', 'bowler': 'Starc', 'at': '2025-01-15T14:27:30+00:00'},
#   {'kang_id': '...', 'runs': 87, 'wickets': 2, 'at': '2025-01-15T14:30:00+00:00'},
#   {'kang_id': '...', 'runs': 88, 'at': '2025-01-15T14:30:00+00:00'},
#   {'kang_id': '...', 'wickets': 3, 'at': '2025-01-15T14:35:00+00:00'}
# ]

# get facts up to specific time
facts = store.get_facts(
    kang_id="cricket.match.550e8400-e29b-41d4-a716-446655440000",
    upto="2025-01-15T14:30:00"
)
# returns: first 3 facts (business_time <= 14:30)

# include audit metadata
facts = store.get_facts(
    kang_id="cricket.match.550e8400-e29b-41d4-a716-446655440000",
    with_tx=True
)
# returns: facts with 'tx_time' and 'tx_id' fields
# [
#   {
#     'kang_id': '...',
#     'batsman': 'Kohli',
#     'bowler': 'Starc',
#     'at': '2025-01-15T14:27:30+00:00',
#     'tx_time': '2025-01-20T14:59:56.123456+00:00',
#     'tx_id': '168c01f6-f83e-11f0-8b9c-bafd80b8a6a7'
#   },
#   ...
# ]
```

---

### `rollup(kang_id, with_nils=False)`

computes the current state by merging all facts. latest value wins for each attribute.

**parameters:**
- `kang_id` (str): Identity to rollup
- `with_nils` (bool): Include attributes set to `None`

**returns:** dictionary with latest values for all attributes

**example:**
```python
# get current match state
current = store.rollup("cricket.match.550e8400-e29b-41d4-a716-446655440000")
# returns:
# {
#   'kang_id': 'cricket.match.550e8400-e29b-41d4-a716-446655440000',
#   'runs': 88,
#   'wickets': 3,
#   'batsman': 'Kohli',
#   'bowler': 'Starc',
#   'at': '2025-01-15T14:35:00+00:00'
# }

# with None values
store.add_fact({"kang_id": "cricket.match.550e8400-e29b-41d4-a716-446655440000", "rain_delay": None})
current = store.rollup("cricket.match.550e8400-e29b-41d4-a716-446655440000")
# returns: {'kang_id': '...', 'runs': 88, 'wickets': 3, ...}  # rain_delay excluded

current = store.rollup("cricket.match.550e8400-e29b-41d4-a716-446655440000", with_nils=True)
# returns: {'kang_id': '...', 'runs': 88, 'wickets': 3, 'rain_delay': None, ...}
```

---

### `as_of(kang_id, time, with_nils=False)`

time-travel query: reconstructs state at a specific business time.

**parameters:**
- `kang_id` (str): Identity to query
- `time` (str): ISO 8601 timestamp
- `with_nils` (bool): Include attributes set to `None`

**returns:** dictionary with state at that time

**example:**
```python
# what was the match state at 14:30 on jan 15?
state = store.as_of(
    "cricket.match.550e8400-e29b-41d4-a716-446655440000",
    "2025-01-15T14:30:00"
)
# returns:
# {
#   'kang_id': 'cricket.match.550e8400-e29b-41d4-a716-446655440000',
#   'runs': 88,
#   'wickets': 2,
#   'batsman': 'Kohli',
#   'bowler': 'Starc',
#   'at': '2025-01-15T14:30:00+00:00'
# }
# (note: wickets=2, not 3, because the wicket fell at 14:35)
```

---

## advanced usage

### transaction metadata (`with_tx`)

by default, `get_facts()` only returns business data. use `with_tx=True` to see the audit trail:

```python
# default: business data only
facts = store.get_facts(kang_id="cricket.match.550e8400-e29b-41d4-a716-446655440000")
# returns:
# [
#   {'kang_id': '...', 'runs': 87, 'wickets': 2, 'at': '2025-01-15T14:30:00+00:00'}
# ]

# with audit trail
facts = store.get_facts(kang_id="cricket.match.550e8400-e29b-41d4-a716-446655440000", with_tx=True)
# returns:
# [
#   {
#     'kang_id': '...',
#     'runs': 87,
#     'wickets': 2,
#     'at': '2025-01-15T14:30:00+00:00',       # when it was true (business time)
#     'tx_time': '2025-01-20T14:59:56+00:00',  # when we recorded it (transaction time)
#     'tx_id': '1471b44c-f83e-11f0-8b9c-bafd80b8a6a7'
#   }
# ]
```

**use cases:**
- **audit compliance**: "when did we record this score?"
- **correction tracking**: "why are there multiple facts at 14:30?" → check tx_time to see one was recorded at 14:30:05 and the correction at 14:35:12
- **late data detection**: compare `at` vs `tx_time` to find backfilled data

---

### nil values (`with_nils`)

control whether `None` values appear in rollup/as_of results:

```python
match_id = "cricket.match.550e8400-e29b-41d4-a716-446655440000"

# record some facts
store.add_fact({"kang_id": match_id, "runs": 87})
store.add_fact({"kang_id": match_id, "rain_delay": "20 minutes"})

# later: rain delay ends, remove the attribute
store.add_fact({"kang_id": match_id, "rain_delay": None})

# default: excludes None values
current = store.rollup(match_id)
# returns: {'kang_id': '...', 'runs': 87, 'at': '...'}  # no rain_delay

# include None values
current = store.rollup(match_id, with_nils=True)
# returns: {'kang_id': '...', 'runs': 87, 'rain_delay': None, 'at': '...'}
```

**use `with_nils=True` to:**
- distinguish "never existed" from "was explicitly deleted"
- preserve schema awareness (all possible fields visible)
- debug missing data issues

---

### time range filtering (`upto`)

query facts within a business time range:

```python
match_id = "cricket.match.550e8400-e29b-41d4-a716-446655440000"

# get facts up to a specific time
facts = store.get_facts(kang_id=match_id, upto="2025-01-15T14:30:00")
# returns: facts where business_time <= 14:30
# [
#   {'kang_id': '...', 'batsman': 'Kohli', 'bowler': 'Starc', 'at': '2025-01-15T14:27:30+00:00'},
#   {'kang_id': '...', 'runs': 87, 'wickets': 2, 'at': '2025-01-15T14:30:00+00:00'},
#   {'kang_id': '...', 'runs': 88, 'at': '2025-01-15T14:30:00+00:00'}
# ]
# (excludes wicket update at 14:35)

# combine with as_of for time-travel
state = store.as_of(match_id, "2025-01-15T14:30:00")
# internally uses get_facts(upto="2025-01-15T14:30:00") then merges
```

**use cases:**
- **historical snapshots**: "what facts existed at end of day?"
- **incremental processing**: "give me facts since last sync"
- **debugging**: "which facts contributed to this state?"

## database connection

FactStore accepts either a database URL or a connection pool:

- **url**: postgresql connection string
- **pool**: psycopg2 ThreadedConnectionPool instance
- **schema**: database schema name (default: "public")

---
## error handling

```python
from kang import FactStore, SchemaNotInitializedError

try:
    store = FactStore("postgresql://user:pass@localhost/mydb")
except SchemaNotInitializedError:
    print("schema not found. edit sql/schema.sql and run: psql -d mydb -f sql/schema.sql")

# validation error
try:
    store.add_fact({"runs": 87})  # missing kang_id
except ValueError as e:
    print(e)  # "fact must contain a 'kang_id'"
```

---

## dependencies

- psycopg2-binary >= 2.9

---

## extending to other databases

to add support for sqlite, mysql, etc.:

1. **create database-specific schema**: adapt `sql/schema.sql`
2. **update `_verify_schema()`**: change table existence check
3. **update `add_facts()`**: adjust conflict handling for upsert behavior

---