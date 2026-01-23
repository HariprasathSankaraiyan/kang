"""bitemporal fact storage"""

from collections import ChainMap, defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from uuid import uuid1

from psycopg2.extras import RealDictCursor

from .db import DatabaseConnection, get_query, SchemaNotInitializedError
from .serialization import compress, decompress, hash_fact


def _validate_fact(fact: Dict[str, Any]) -> None:
    """ensure fact has kang_id"""
    kang_id = fact.get("kang_id")
    if not kang_id or (isinstance(kang_id, str) and not kang_id.strip()):
        raise ValueError(f"fact must have 'kang_id': {fact}")


def _get_effective_business_time(business_time: Optional[str]) -> datetime:
    """return business_time or default to now"""
    return business_time or datetime.now(timezone.utc)


def _insert_fact_and_transaction(cursor, schema: str, fact: Dict[str, Any], business_time: datetime) -> tuple[str, int]:
    """insert fact and transaction, returns (tx_id, rowcount)"""
    fact_hash = hash_fact(fact)
    compressed = compress(fact)
    tx_id = str(uuid1())
    
    cursor.execute(
        get_query('insert-fact', schema),
        {
            "hash": fact_hash,
            "key": fact["kang_id"],
            "value": compressed
        }
    )
    
    cursor.execute(
        get_query('insert-transaction', schema),
        {
            "tx_id": tx_id,
            "hash": fact_hash,
            "business_time": business_time
        }
    )
    
    return tx_id, cursor.rowcount


class FactStore:
    """bitemporal fact store (business + transaction time)"""
    
    def __init__(
        self,
        url: Optional[str] = None,
        pool: Optional["ThreadedConnectionPool"] = None,
        schema: str = "public"
    ):
        self.db = DatabaseConnection(url=url, pool=pool)
        self.schema = schema
        self._schema_verified = False
        self._verify_schema()
    
    def _verify_schema(self):
        """verify tables exist - cached per app instance"""
        if self._schema_verified:
            return
        
        with self.db.get_connection() as conn:
            conn.autocommit = True
            with conn.cursor() as cursor:
                sql = get_query('verify-schema', self.schema)
                cursor.execute(sql, {"schema": self.schema})
                result = cursor.fetchone()
                if not result or result[0] != 2:
                    raise SchemaNotInitializedError(
                        f"fact tables missing in '{self.schema}'"
                    )
                
                self._schema_verified = True
    
    def _fetch_facts(
        self,
        kang_ids: List[str],
        upto: Optional[str] = None,
        with_tx: bool = False
    ) -> List[Dict[str, Any]]:
        """fetch facts from database"""
        params = {"kang_ids": kang_ids, "upto": upto}
        query = get_query('get-facts', self.schema)
        
        with self.db.get_connection() as conn:
            conn.autocommit = True
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, params)
                rows = cursor.fetchall()
                
                facts = []
                for row in rows:
                    fact = decompress(row['value'])
                    fact['at'] = row['business_time'].isoformat()

                    if with_tx:
                        fact['tx_at'] = row['at'].isoformat()
                        fact['tx_id'] = row['id']
                    
                    facts.append(fact)
                
                return facts
    
    def _merge_facts(
        self,
        facts: List[Dict[str, Any]],
        with_nils: bool = False
    ) -> Dict[str, Any]:
        """merge facts into single state dict"""
        if not facts:
            return {}
        
        merged = dict(ChainMap(*reversed(facts)))
        
        if not with_nils:
            merged = {k: v for k, v in merged.items() if v is not None}
        
        return merged
    
    def _merge_facts_by_id(
        self,
        facts: List[Dict[str, Any]],
        with_nils: bool = False
    ) -> Dict[str, Dict[str, Any]]:
        """merge facts by kang_id"""
        kang_id_vs_facts = defaultdict(dict)
        for fact in facts:
            kang_id_vs_facts[fact.get('kang_id')].update(fact)
        
        if not with_nils:
            return {
                kang_id: {k: v for k, v in state.items() if v is not None}
                for kang_id, state in kang_id_vs_facts.items()
            }
        
        return dict(kang_id_vs_facts)
    
    # Write operations
    
    def add_fact(self, fact: Dict[str, Any], business_time: Optional[str] = None):
        """add single fact, returns tx id or noop dict"""
        result = self.add_facts([fact], business_time)
        if isinstance(result, dict) and "noop" in result:
            return result
        return result[0]
    
    def add_facts(self, facts: List[Dict[str, Any]], business_time: Optional[str] = None) -> List[str]:
        """add multiple facts in single tx, returns list of tx ids or noop dict"""
        tx_ids = []
        effective_business_time = _get_effective_business_time(business_time)
        
        with self.db.get_connection() as conn:
            conn.autocommit = False
            try:
                changes_recorded = False
                
                with conn.cursor() as cursor:
                    for fact in facts:
                        _validate_fact(fact)
                        tx_id, rowcount = _insert_fact_and_transaction(
                            cursor, self.schema, fact, effective_business_time
                        )
                        
                        if rowcount > 0:
                            changes_recorded = True
                            tx_ids.append(tx_id)
                
                if not changes_recorded:
                    conn.rollback()
                    return {"noop": f"no changes, facts not added: {facts}"}
                
                conn.commit()
                return tx_ids
                
            except Exception:
                conn.rollback()
                raise
    
    # Read operations
    
    def get_facts(
        self,
        kang_id: str,
        upto: Optional[str] = None,
        with_tx: bool = False
    ) -> List[Dict[str, Any]]:
        """get facts for single identity"""
        if not kang_id:
            raise ValueError("kang_id is required")
        
        return self._fetch_facts(kang_ids=[kang_id], upto=upto, with_tx=with_tx)
    
    def get_facts_for_many(
        self,
        kang_ids: List[str],
        upto: Optional[str] = None,
        with_tx: bool = False
    ) -> List[Dict[str, Any]]:
        """get facts for multiple identities"""
        if not kang_ids:
            raise ValueError("kang_ids is required and must be non-empty")
        
        return self._fetch_facts(kang_ids=kang_ids, upto=upto, with_tx=with_tx)
    
    # Merge operations
    
    def rollup(
        self,
        kang_id: str,
        with_nils: bool = False
    ) -> Dict[str, Any]:
        """merge all facts to get current state"""
        facts = self.get_facts(kang_id=kang_id)
        return self._merge_facts(facts, with_nils=with_nils)
    
    def rollup_for_many(
        self,
        kang_ids: List[str],
        with_nils: bool = False
    ) -> Dict[str, Dict[str, Any]]:
        """merge facts for multiple identities, returns dict[kang_id, state]"""
        facts = self.get_facts_for_many(kang_ids=kang_ids)
        return self._merge_facts_by_id(facts, with_nils=with_nils)
    
    def as_of(
        self,
        kang_id: str,
        time: str,
        with_nils: bool = False
    ) -> Dict[str, Any]:
        """get state at specific time"""
        facts = self.get_facts(kang_id=kang_id, upto=time)
        return self._merge_facts(facts, with_nils=with_nils)
    
    def as_of_for_many(
        self,
        kang_ids: List[str],
        time: str,
        with_nils: bool = False
    ) -> Dict[str, Dict[str, Any]]:
        """get state at time for multiple identities, returns dict[kang_id, state]"""
        facts = self.get_facts_for_many(kang_ids=kang_ids, upto=time)
        return self._merge_facts_by_id(facts, with_nils=with_nils)
