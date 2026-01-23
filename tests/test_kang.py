"""integration tests - requires schema initialised and DB_URL, DB_SCHEMA set"""

import os
import pytest
from uuid import uuid4
from kang import FactStore, SchemaNotInitializedError


@pytest.fixture
def db_url():
    """get database url from env"""
    url = os.environ.get("DB_URL")
    if not url:
        pytest.skip("DB_URL not set")
    return url


@pytest.fixture
def schema():
    """get schema from env or default to public"""
    return os.environ.get("DB_SCHEMA", "public")


@pytest.fixture
def store(db_url, schema):
    """create factstore instance"""
    return FactStore(url=db_url, schema=schema)


@pytest.fixture
def unique_id():
    """generate unique test id suffix to ensure test idempotency"""
    return str(uuid4())[:8]


def test_schema_not_initialized():
    """error if tables missing"""
    bad_url = "postgresql://user:pass@localhost/nonexistent"
    with pytest.raises((SchemaNotInitializedError, Exception)):
        FactStore(url=bad_url)


def test_single_fact(store, unique_id):
    """add and retrieve fact"""
    kang_id = f"cricket.match.test_001_{unique_id}"
    tx_id = store.add_fact(
        {"kang_id": kang_id, "runs": 87, "wickets": 2},
        business_time="2025-01-15T14:30:00"
    )
    assert tx_id is not None
    
    facts = store.get_facts(kang_id=kang_id)
    assert len(facts) >= 1
    assert facts[0]["runs"] == 87


def test_multiple_facts(store, unique_id):
    """add multiple facts"""
    kang_id_002 = f"cricket.match.test_002_{unique_id}"
    kang_id_003 = f"cricket.match.test_003_{unique_id}"
    tx_ids = store.add_facts([
        {"kang_id": kang_id_002, "runs": 105, "wickets": 3},
        {"kang_id": kang_id_003, "runs": 120, "wickets": 4}
    ], business_time="2025-01-20T15:00:00")
    
    assert len(tx_ids) == 2
    
    facts = store.get_facts(kang_id=kang_id_002)
    assert any(f["runs"] == 105 for f in facts)


def test_get_facts_with_time_filter(store, unique_id):
    """retrieve facts with time filter"""
    kang_id = f"cricket.match.test_004_{unique_id}"
    store.add_fact(
        {"kang_id": kang_id, "runs": 87},
        business_time="2025-01-15T14:30:00"
    )
    store.add_fact(
        {"kang_id": kang_id, "runs": 120},
        business_time="2025-01-15T15:00:00"
    )
    
    # get facts up to specific time
    facts = store.get_facts(
        kang_id=kang_id,
        upto="2025-01-15T14:30:00"
    )
    assert any(f["runs"] == 87 for f in facts)
    assert not any(f["runs"] == 120 for f in facts)


def test_no_business_time(store, unique_id):
    """facts without business time use tx time"""
    kang_id = f"system.event.test_005_{unique_id}"
    store.add_fact({"kang_id": kang_id, "event": "startup"})
    
    facts = store.get_facts(kang_id=kang_id)
    assert any(f.get("event") == "startup" for f in facts)


def test_correction_same_business_time(store, unique_id):
    """corrections use same business time, different transaction time"""
    kang_id = f"cricket.match.test_006_{unique_id}"
    # original score
    store.add_fact(
        {"kang_id": kang_id, "runs": 87},
        business_time="2025-01-15T14:30:00"
    )
    
    # correction
    store.add_fact(
        {"kang_id": kang_id, "runs": 88, "note": "corrected"},
        business_time="2025-01-15T14:30:00"
    )
    
    # both facts exist
    facts = store.get_facts(
        kang_id=kang_id,
        upto="2025-01-15T14:30:00"
    )
    assert len(facts) == 2


def test_backfill_late_data(store, unique_id):
    """backfill late data"""
    kang_id = f"cricket.match.test_007_{unique_id}"
    # current data
    store.add_fact(
        {"kang_id": kang_id, "runs": 120},
        business_time="2025-01-15T15:00:00"
    )
    
    # late data
    store.add_fact(
        {"kang_id": kang_id, "runs": 87},
        business_time="2025-01-15T14:30:00"
    )
    
    facts = store.get_facts(kang_id=kang_id)
    assert len(facts) >= 2


def test_rollup(store, unique_id):
    """rollup merges all facts to get current state"""
    kang_id = f"cricket.match.test_008_{unique_id}"
    store.add_fact(
        {"kang_id": kang_id, "runs": 50, "wickets": 1},
        business_time="2025-01-15T14:00:00"
    )
    store.add_fact(
        {"kang_id": kang_id, "runs": 87},
        business_time="2025-01-15T14:30:00"
    )
    store.add_fact(
        {"kang_id": kang_id, "wickets": 3},
        business_time="2025-01-15T15:00:00"
    )
    
    state = store.rollup(kang_id)
    assert state["runs"] == 87
    assert state["wickets"] == 3


def test_as_of(store, unique_id):
    """state at specific time"""
    kang_id = f"cricket.match.test_009_{unique_id}"
    store.add_fact(
        {"kang_id": kang_id, "runs": 50, "wickets": 1},
        business_time="2025-01-15T14:00:00"
    )
    store.add_fact(
        {"kang_id": kang_id, "runs": 87},
        business_time="2025-01-15T14:30:00"
    )
    store.add_fact(
        {"kang_id": kang_id, "wickets": 3},
        business_time="2025-01-15T15:00:00"
    )
    
    state = store.as_of(kang_id, "2025-01-15T14:30:00")
    assert state["runs"] == 87
    assert state["wickets"] == 1  # before update


def test_with_tx_metadata(store, unique_id):
    """include transaction metadata in results"""
    kang_id = f"cricket.match.test_010_{unique_id}"
    store.add_fact(
        {"kang_id": kang_id, "runs": 100},
        business_time="2025-01-15T14:00:00"
    )
    
    facts = store.get_facts(kang_id=kang_id, with_tx=True)
    assert "tx_at" in facts[0]
    assert "tx_id" in facts[0]
    assert "at" in facts[0]
    
    facts_no_tx = store.get_facts(kang_id=kang_id, with_tx=False)
    assert "tx_at" not in facts_no_tx[0]
    assert "at" in facts_no_tx[0]


def test_noop_duplicate_fact(store, unique_id):
    """duplicate fact returns noop - verifies (hash, business_time) deduplication"""
    kang_id = f"cricket.match.test_011_{unique_id}"
    
    # first insert should succeed
    result1 = store.add_fact(
        {"kang_id": kang_id, "runs": 100},
        business_time="2025-01-15T14:00:00"
    )
    assert isinstance(result1, str)  # tx_id UUID string
    
    # exact duplicate should return noop (same hash + business_time)
    result2 = store.add_fact(
        {"kang_id": kang_id, "runs": 100},
        business_time="2025-01-15T14:00:00"
    )
    assert isinstance(result2, dict)
    assert "noop" in result2
    
    # verify only one fact exists
    facts = store.get_facts(kang_id=kang_id)
    assert len(facts) == 1
    assert facts[0]["runs"] == 100


def test_missing_kang_id(store):
    """missing kang_id raises error"""
    with pytest.raises(ValueError, match="kang_id"):
        store.add_fact({"runs": 100})
    
    with pytest.raises(ValueError, match="kang_id"):
        store.add_fact({"kang_id": "", "runs": 100})


def test_deduplication_constraints(store, unique_id):
    """verify deduplication by (key, hash) and (hash, business_time) constraints"""
    kang_id = f"cricket.match.test_dedup_{unique_id}"
    
    # scenario 1: same fact, same time → deduplicated (noop)
    tx1 = store.add_fact(
        {"kang_id": kang_id, "runs": 100},
        business_time="2025-01-15T14:00:00"
    )
    assert isinstance(tx1, str)  # success
    
    result = store.add_fact(
        {"kang_id": kang_id, "runs": 100},
        business_time="2025-01-15T14:00:00"
    )
    assert isinstance(result, dict) and "noop" in result  # deduplicated
    
    # scenario 2: same fact, different time == both stored
    tx2 = store.add_fact(
        {"kang_id": kang_id, "runs": 100},
        business_time="2025-01-15T15:00:00"
    )
    assert isinstance(tx2, str)  # success, different business_time
    
    # scenario 3: different fact, same time → both stored (correction)
    tx3 = store.add_fact(
        {"kang_id": kang_id, "runs": 101},
        business_time="2025-01-15T14:00:00"
    )
    assert isinstance(tx3, str)  # success, different hash
    
    # verify all facts stored correctly
    facts = store.get_facts(kang_id=kang_id)
    assert len(facts) == 3  # 3 unique (hash, business_time) combinations
    
    # verify facts at 14:00 (should have both runs=100 and runs=101)
    facts_at_14 = [f for f in facts if f["at"] == "2025-01-15T14:00:00+00:00"]
    assert len(facts_at_14) == 2
    runs_values = {f["runs"] for f in facts_at_14}
    assert runs_values == {100, 101}


@pytest.fixture
def multi_data(store, unique_id):
    """shared data for multi-id tests"""
    kang_ids = [
        f"cricket.match.multi_001_{unique_id}",
        f"cricket.match.multi_002_{unique_id}",
        f"cricket.match.multi_003_{unique_id}"
    ]
    # setup facts for multiple entities
    store.add_facts([
        {"kang_id": kang_ids[0], "runs": 50, "wickets": 1},
        {"kang_id": kang_ids[1], "runs": 150, "wickets": 3},
        {"kang_id": kang_ids[2], "runs": 200, "wickets": 5}
    ], business_time="2025-01-15T14:00:00")
    
    # updates at later time
    store.add_facts([
        {"kang_id": kang_ids[0], "runs": 100},
        {"kang_id": kang_ids[1], "runs": 180}
    ], business_time="2025-01-15T15:00:00")
    
    return kang_ids


def test_get_facts_for_multiple_kang_ids(store, multi_data):
    """get facts for multiple ids"""
    facts = store.get_facts_for_many(kang_ids=multi_data[:2])
    assert len(facts) >= 2
    kang_ids = {f["kang_id"] for f in facts}
    assert multi_data[0] in kang_ids
    assert multi_data[1] in kang_ids
    assert multi_data[2] not in kang_ids


def test_rollup_for_multiple_kang_ids(store, multi_data):
    """rollup multiple ids returns nested dict"""
    states = store.rollup_for_many(kang_ids=multi_data[:2])
    assert multi_data[0] in states
    assert multi_data[1] in states
    assert states[multi_data[0]]["runs"] == 100  # updated value
    assert states[multi_data[0]]["wickets"] == 1  # original
    assert states[multi_data[1]]["runs"] == 180  # updated value
    
    # verify single rollup returns flat dict (not nested)
    single_state = store.rollup(kang_id=multi_data[0])
    assert single_state["runs"] == 100
    assert multi_data[0] not in single_state  # kang_id not a key in result


def test_as_of_for_multiple_kang_ids(store, multi_data):
    """as_of multiple ids returns nested dict"""
    states = store.as_of_for_many(
        kang_ids=multi_data[:2],
        time="2025-01-15T14:00:00"
    )
    assert multi_data[0] in states
    assert multi_data[1] in states
    assert states[multi_data[0]]["runs"] == 50  # before update
    assert states[multi_data[1]]["runs"] == 150  # before update
