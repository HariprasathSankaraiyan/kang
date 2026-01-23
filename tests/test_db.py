"""test connection modes - requires schema initialised and DB_URL, DB_SCHEMA set"""

import os
import pytest
from uuid import uuid4
from psycopg2.pool import ThreadedConnectionPool
from urllib.parse import urlparse

from kang import FactStore


@pytest.fixture
def db_url():
    """get db url from env"""
    url = os.environ.get("DB_URL")
    if not url:
        pytest.skip("DB_URL not set")
    return url


@pytest.fixture
def schema():
    """get schema from env or default to public"""
    return os.environ.get("DB_SCHEMA", "public")


@pytest.fixture
def db_pool(db_url):
    """create connection pool"""
    parsed = urlparse(db_url)
    
    pool = ThreadedConnectionPool(
        1, 5,
        dbname=parsed.path[1:],
        user=parsed.username,
        password=parsed.password,
        host=parsed.hostname,
        port=parsed.port or 5432
    )
    
    yield pool
    
    pool.closeall()


@pytest.fixture
def unique_id():
    """generate unique test id suffix to ensure test idempotency"""
    return str(uuid4())[:8]


def test_url_mode(db_url, schema, unique_id):
    """connection via url"""
    store = FactStore(url=db_url, schema=schema)
    kang_id = f"test.db.url_mode_{unique_id}"
    
    tx_id = store.add_fact({
        "kang_id": kang_id,
        "value": 100
    })
    
    assert tx_id is not None
    
    facts = store.get_facts(kang_id=kang_id)
    assert len(facts) >= 1
    assert facts[0]["value"] == 100


def test_pool_mode(db_pool, schema, unique_id):
    """connection via pool"""
    store = FactStore(pool=db_pool, schema=schema)
    kang_id = f"test.db.pool_mode_{unique_id}"
    
    tx_id = store.add_fact({
        "kang_id": kang_id,
        "value": 200
    })
    
    assert tx_id is not None
    
    facts = store.get_facts(kang_id=kang_id)
    assert len(facts) >= 1
    assert facts[0]["value"] == 200
