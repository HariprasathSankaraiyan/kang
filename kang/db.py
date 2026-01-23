"""database connection and query loader"""

from contextlib import contextmanager
from pathlib import Path
from typing import Dict, Optional

from psycopg2.pool import ThreadedConnectionPool

# sql queries directory and preloaded queries
SQL_DIR = Path(__file__).parent / 'sql'

# load all sql queries at module initialization (exclude schema.sql)
QUERIES: Dict[str, str] = {}
for sql_file in SQL_DIR.glob('*.sql'):
    query_name = sql_file.stem
    if query_name != 'schema':
        QUERIES[query_name] = sql_file.read_text().strip()


class SchemaNotInitializedError(Exception):
    """tables missing in database"""
    pass


def get_query(query_name: str, schema: str) -> str:
    """get query and format schema placeholder"""
    if query_name not in QUERIES:
        raise ValueError(f"unknown query: {query_name}")
    return QUERIES[query_name].format(schema=schema)


class DatabaseConnection:
    """connection pool manager"""
    
    def __init__(
        self,
        url: Optional[str] = None,
        pool: Optional[ThreadedConnectionPool] = None
    ):
        if url is not None and pool is not None:
            raise ValueError("provide url or pool, not both")
        
        if url is None and pool is None:
            raise ValueError("must provide url or pool")
        
        if url is not None:
            self.pool = ThreadedConnectionPool(1, 10, url)
            self._owns_pool = True
        else:
            self.pool = pool
            self._owns_pool = False
    
    @contextmanager
    def get_connection(self):
        """get connection from pool"""
        conn = self.pool.getconn()
        try:
            yield conn
        finally:
            self.pool.putconn(conn)
    
    def close(self):
        """close pool if we own it"""
        if self._owns_pool:
            self.pool.closeall()
