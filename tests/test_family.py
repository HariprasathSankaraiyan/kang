"""integration tests for FamilySupport - requires schema initialised and DB_URL, DB_SCHEMA set"""

import os
import pytest
import psycopg2
from uuid import uuid4
from kang import FactStore, SchemaNotInitializedError
from kang.family import FamilySupport


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
def db_connection(db_url):
    """provide a database connection for raw SQL operations"""
    conn = psycopg2.connect(db_url)
    yield conn
    conn.close()


@pytest.fixture
def store(db_url, schema):
    """create factstore instance"""
    return FactStore(url=db_url, schema=schema)


@pytest.fixture
def unique_id(request):
    """generate unique test id suffix to ensure test idempotency"""
    uid = str(uuid4())[:8]
    request.node._unique_id = uid
    return uid


@pytest.fixture
def family(store):
    """create familysupport instance"""
    return FamilySupport(store)


@pytest.fixture(autouse=True)
def cleanup_test_data(db_connection, request):
    """cleanup test data after each test completes
    
    Kang stores immutable facts, so we can't truly delete them.
    Instead, we delete rows by matching the test's unique_id pattern.
    """
    yield
    
    unique_id = getattr(request.node, '_unique_id', None)
    if not unique_id:
        return
    
    cur = db_connection.cursor()
    
    like_pattern = f'%{unique_id}%'
    
    cur.execute(
        "SELECT id::text, hash FROM facts WHERE key LIKE %s",
        (like_pattern,)
    )
    rows = cur.fetchall()
    
    if rows:
        fact_ids = [row[0] for row in rows]
        tx_hashes = [row[1] for row in rows]
        
        cur.execute("DELETE FROM facts WHERE id::text = ANY(%s)", (fact_ids,))
        cur.execute("DELETE FROM transactions WHERE hash = ANY(%s)", (tx_hashes,))
    
    db_connection.commit()
    cur.close()


# --- validation ---

def test_add_child_missing_kang_id(family, unique_id):
    """child fact without kang_id raises error"""
    with pytest.raises(ValueError, match="kang_id"):
        family.add_child(
            parent_id=f"team.squad_{unique_id}",
            child_type="player",
            child_facts={"name": "Alice"}
        )


def test_add_child_empty_kang_id(family, unique_id):
    """child fact with empty kang_id raises error"""
    with pytest.raises(ValueError, match="kang_id"):
        family.add_child(
            parent_id=f"team.squad_{unique_id}",
            child_type="player",
            child_facts={"kang_id": "  ", "name": "Alice"}
        )


def test_add_children_one_invalid(family, unique_id):
    """add_children raises if any child is invalid"""
    with pytest.raises(ValueError, match="kang_id"):
        family.add_children(
            parent_id=f"team.squad_{unique_id}",
            child_type="player",
            children_facts=[
                {"kang_id": f"player.alice_{unique_id}", "name": "Alice"},
                {"name": "no_id_here"}
            ]
        )


# --- add_child ---

def test_add_child_returns_tx_id(family, unique_id):
    """add_child returns a transaction id string on success"""
    child_id = f"player.alice_{unique_id}"
    tx_id = family.add_child(
        parent_id=f"team.squad_{unique_id}",
        child_type="player",
        child_facts={"kang_id": child_id, "name": "Alice"},
        business_time="2025-01-15T10:00:00"
    )
    assert isinstance(tx_id, str)


def test_add_child_duplicate_is_noop(family, unique_id):
    """adding exact duplicate child returns noop"""
    child_id = f"player.bob_{unique_id}"
    parent_id = f"team.squad_{unique_id}"
    child_facts = {"kang_id": child_id, "name": "Bob"}
    bt = "2025-01-15T10:00:00"

    result1 = family.add_child(parent_id, "player", child_facts, bt)
    assert isinstance(result1, str)

    result2 = family.add_child(parent_id, "player", child_facts, bt)
    assert isinstance(result2, dict)
    assert "noop" in result2


# --- add_children ---

def test_add_children_returns_tx_ids(family, unique_id):
    """add_children returns list of tx ids"""
    parent_id = f"team.squad_{unique_id}"
    tx_ids = family.add_children(
        parent_id=parent_id,
        child_type="player",
        children_facts=[
            {"kang_id": f"player.alice_{unique_id}", "name": "Alice"},
            {"kang_id": f"player.bob_{unique_id}", "name": "Bob"},
        ],
        business_time="2025-01-15T10:00:00"
    )
    assert isinstance(tx_ids, list)
    assert len(tx_ids) > 0


# --- find_children ---

def test_find_children_returns_active(family, unique_id):
    """find_children returns currently active children"""
    parent_id = f"team.squad_{unique_id}"
    child_id = f"player.carol_{unique_id}"

    family.add_child(parent_id, "player", {"kang_id": child_id, "name": "Carol"})

    children = family.find_children(parent_id, "player")
    assert child_id in children
    assert children[child_id]["name"] == "Carol"


def test_find_children_empty_for_unknown_parent(family, unique_id):
    """find_children returns empty dict for unknown parent"""
    children = family.find_children(f"team.nobody_{unique_id}", "player")
    assert children == {}


def test_find_children_excludes_deleted_by_default(family, unique_id):
    """find_children hides deleted children by default"""
    parent_id = f"team.squad_{unique_id}"
    child_id = f"player.dave_{unique_id}"

    family.add_child(
        parent_id, "player",
        {"kang_id": child_id, "name": "Dave"},
        business_time="2025-01-15T10:00:00"
    )
    family.remove_child(
        parent_id, "player", child_id,
        business_time="2025-01-15T11:00:00"
    )

    children = family.find_children(parent_id, "player")
    assert child_id not in children


def test_find_children_includes_deleted_when_requested(family, unique_id):
    """find_children includes deleted children when with_deleted=True"""
    parent_id = f"team.squad_{unique_id}"
    child_id = f"player.eve_{unique_id}"

    family.add_child(
        parent_id, "player",
        {"kang_id": child_id, "name": "Eve"},
        business_time="2025-01-15T10:00:00"
    )
    family.remove_child(
        parent_id, "player", child_id,
        business_time="2025-01-15T11:00:00"
    )

    children = family.find_children(parent_id, "player", with_deleted=True)
    assert child_id in children
    assert children[child_id].get("deleted") is True


# --- remove_child ---

def test_remove_child_returns_tx_id(family, unique_id):
    """remove_child returns a tx id string"""
    parent_id = f"team.squad_{unique_id}"
    child_id = f"player.frank_{unique_id}"

    family.add_child(
        parent_id, "player",
        {"kang_id": child_id, "name": "Frank"},
        business_time="2025-01-15T10:00:00"
    )
    result = family.remove_child(
        parent_id, "player", child_id,
        business_time="2025-01-15T11:00:00"
    )
    assert isinstance(result, str)


def test_remove_child_marks_as_deleted(family, store, unique_id):
    """removed child has deleted=True in its own fact"""
    parent_id = f"team.squad_{unique_id}"
    child_id = f"player.grace_{unique_id}"

    family.add_child(
        parent_id, "player",
        {"kang_id": child_id, "name": "Grace"},
        business_time="2025-01-15T10:00:00"
    )
    family.remove_child(
        parent_id, "player", child_id,
        business_time="2025-01-15T11:00:00"
    )

    state = store.rollup(child_id)
    assert state.get("deleted") is True


def test_remove_then_readd_child(family, unique_id):
    """child removed then re-added appears in find_children"""
    parent_id = f"team.squad_{unique_id}"
    child_id = f"player.heidi_{unique_id}"

    family.add_child(
        parent_id, "player",
        {"kang_id": child_id, "name": "Heidi"},
        business_time="2025-01-15T10:00:00"
    )
    family.remove_child(
        parent_id, "player", child_id,
        business_time="2025-01-15T11:00:00"
    )
    family.add_child(
        parent_id, "player",
        {"kang_id": child_id, "name": "Heidi"},
        business_time="2025-01-15T12:00:00"
    )

    children = family.find_children(parent_id, "player")
    assert child_id in children


# --- find_children_at ---

def test_find_children_at_past_time(family, unique_id):
    """find_children_at returns children as they existed at given time"""
    parent_id = f"team.squad_{unique_id}"
    child_a = f"player.ivan_{unique_id}"
    child_b = f"player.judy_{unique_id}"

    family.add_child(
        parent_id, "player",
        {"kang_id": child_a, "name": "Ivan"},
        business_time="2025-01-15T10:00:00"
    )
    family.add_child(
        parent_id, "player",
        {"kang_id": child_b, "name": "Judy"},
        business_time="2025-01-15T12:00:00"
    )

    children = family.find_childen_at(parent_id, "player", "2025-01-15T11:00:00")
    assert child_a in children
    assert child_b not in children


def test_find_children_at_after_removal(family, unique_id):
    """find_children_at shows child before removal but not after"""
    parent_id = f"team.squad_{unique_id}"
    child_id = f"player.kyle_{unique_id}"

    family.add_child(
        parent_id, "player",
        {"kang_id": child_id, "name": "Kyle"},
        business_time="2025-01-15T10:00:00"
    )
    family.remove_child(
        parent_id, "player", child_id,
        business_time="2025-01-15T12:00:00"
    )

    before = family.find_childen_at(parent_id, "player", "2025-01-15T11:00:00")
    after = family.find_childen_at(parent_id, "player", "2025-01-15T13:00:00")

    assert child_id in before
    assert child_id not in after


# --- get_children_diff ---

def test_get_children_diff_added(family, unique_id):
    """diff shows newly added children"""
    parent_id = f"team.squad_{unique_id}"
    child_a = f"player.lena_{unique_id}"
    child_b = f"player.mike_{unique_id}"

    family.add_child(
        parent_id, "player",
        {"kang_id": child_a, "name": "Lena"},
        business_time="2025-01-15T10:00:00"
    )
    family.add_child(
        parent_id, "player",
        {"kang_id": child_b, "name": "Mike"},
        business_time="2025-01-15T12:00:00"
    )

    diff = family.get_children_diff(
        parent_id, "player",
        from_time="2025-01-15T11:00:00",
        to_time="2025-01-15T13:00:00"
    )

    assert child_b in diff["added"]
    assert child_a in diff["retained"]
    assert diff["deleted"] == {}


def test_get_children_diff_deleted(family, unique_id):
    """diff shows removed children as deleted"""
    parent_id = f"team.squad_{unique_id}"
    child_id = f"player.nina_{unique_id}"

    family.add_child(
        parent_id, "player",
        {"kang_id": child_id, "name": "Nina"},
        business_time="2025-01-15T10:00:00"
    )
    family.remove_child(
        parent_id, "player", child_id,
        business_time="2025-01-15T12:00:00"
    )

    diff = family.get_children_diff(
        parent_id, "player",
        from_time="2025-01-15T11:00:00",
        to_time="2025-01-15T13:00:00"
    )

    assert child_id in diff["deleted"]
    assert diff["added"] == {}
    assert diff["retained"] == {}


def test_get_children_diff_no_from_time(family, unique_id):
    """diff with no from_time returns all current children as added"""
    parent_id = f"team.squad_{unique_id}"
    child_id = f"player.oliver_{unique_id}"

    family.add_child(
        parent_id, "player",
        {"kang_id": child_id, "name": "Oliver"},
        business_time="2025-01-15T10:00:00"
    )

    diff = family.get_children_diff(parent_id, "player")

    assert child_id in diff["added"]
    assert diff["deleted"] == {}
    assert diff["retained"] == {}


# --- get_child_history ---

def test_get_child_history_add_and_remove(family, unique_id):
    """history contains add and remove events in order"""
    parent_id = f"team.squad_{unique_id}"
    child_id = f"player.pam_{unique_id}"

    family.add_child(
        parent_id, "player",
        {"kang_id": child_id, "name": "Pam"},
        business_time="2025-01-15T10:00:00"
    )
    family.remove_child(
        parent_id, "player", child_id,
        business_time="2025-01-15T11:00:00"
    )

    history = family.get_child_history(parent_id, "player", child_id)

    assert len(history) == 2
    assert history[0]["state"] == {"state": "active"}
    assert history[1]["state"] is None
    assert history[0]["business_time"] < history[1]["business_time"]


def test_get_child_history_includes_tx_metadata(family, unique_id):
    """history entries include business_time, tx_time, tx_id"""
    parent_id = f"team.squad_{unique_id}"
    child_id = f"player.quinn_{unique_id}"

    family.add_child(
        parent_id, "player",
        {"kang_id": child_id, "name": "Quinn"},
        business_time="2025-01-15T10:00:00"
    )

    history = family.get_child_history(parent_id, "player", child_id)

    assert len(history) >= 1
    entry = history[0]
    assert "business_time" in entry
    assert "tx_time" in entry
    assert "tx_id" in entry


def test_get_child_history_empty_for_unrelated_child(family, unique_id):
    """history is empty when child never associated with parent"""
    parent_id = f"team.squad_{unique_id}"
    unrelated_child = f"player.stranger_{unique_id}"

    # add a different child so the family exists
    family.add_child(
        parent_id, "player",
        {"kang_id": f"player.real_{unique_id}", "name": "Real"},
        business_time="2025-01-15T10:00:00"
    )

    history = family.get_child_history(parent_id, "player", unrelated_child)
    assert history == []


def test_get_child_history_readd_shows_full_trail(family, unique_id):
    """re-adding a child shows full add/remove/add sequence"""
    parent_id = f"team.squad_{unique_id}"
    child_id = f"player.rosa_{unique_id}"

    family.add_child(
        parent_id, "player",
        {"kang_id": child_id, "name": "Rosa"},
        business_time="2025-01-15T10:00:00"
    )
    family.remove_child(
        parent_id, "player", child_id,
        business_time="2025-01-15T11:00:00"
    )
    family.add_child(
        parent_id, "player",
        {"kang_id": child_id, "name": "Rosa"},
        business_time="2025-01-15T12:00:00"
    )

    history = family.get_child_history(parent_id, "player", child_id)

    assert len(history) == 3
    states = [h["state"] for h in history]
    assert states[0] == {"state": "active"}
    assert states[1] is None
    assert states[2] == {"state": "active"}


# --- child type isolation ---

def test_different_child_types_are_isolated(family, unique_id):
    """children of different types do not appear in each other's families"""
    parent_id = f"org.company_{unique_id}"
    employee_id = f"employee.alice_{unique_id}"
    department_id = f"department.eng_{unique_id}"

    family.add_child(
        parent_id, "employee",
        {"kang_id": employee_id, "name": "Alice"}
    )
    family.add_child(
        parent_id, "department",
        {"kang_id": department_id, "name": "Engineering"}
    )

    employees = family.find_children(parent_id, "employee")
    departments = family.find_children(parent_id, "department")

    assert employee_id in employees
    assert department_id not in employees
    assert department_id in departments
    assert employee_id not in departments