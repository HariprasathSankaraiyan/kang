"""fact serialization, compression, and hashing utilities"""

import hashlib
import pickle
import gzip
from typing import Any, Dict


def compress(fact: Dict[str, Any]) -> bytes:
    """serialize and compress a fact"""
    pickled = pickle.dumps(fact)
    return gzip.compress(pickled, compresslevel=6)


def decompress(data: bytes) -> Dict[str, Any]:
    """decompress and deserialize a fact"""
    decompressed = gzip.decompress(data)
    return pickle.loads(decompressed)


def hash_fact(fact: Dict[str, Any]) -> str:
    """generate sha256 hash of a fact"""
    fact_bytes = pickle.dumps(sorted(fact.items()))
    return hashlib.sha256(fact_bytes).hexdigest()
