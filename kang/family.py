from typing import Any, Dict, List, Optional

def _validate_child_fact(child_fact: Dict[str, Any]) -> None:
    """ensure child_facts is non-empty and each child has kang_id"""
    kang_id = child_fact.get("kang_id")
    if not kang_id or (isinstance(kang_id, str) and not kang_id.strip()):
        raise ValueError(
        f"child_fact must include a non-empty 'kang_id': {child_fact}")

def _build_family_id(parent_id: str, child_type: str) -> str:
    """create family identifier from parent-id and child type"""
    return f"{parent_id}.{child_type}"

def _build_child_relationship_fact(family_id:str, child_id: str, active: bool = True) -> Dict[str, Any]:
    """build relationship fact for child"""
    return {
        "kang_id": family_id,
        child_id: {"state":"active"} if active else None
    }

def _build_child_deletion_fact(child_id: str) -> Dict[str, Any]:
    """build fact when a child is deleted"""
    return {
        "kang_id" : child_id,
        "deleted" : True
    }

class FamilySupport:
    """manage parent-child relationships"""

    def __init__(self, fact_store):
        self.store = fact_store

    # Write operations

    def add_child(
        self,
        parent_id:  str,
        child_type: str,
        child_facts: Dict[str, Any],
        business_time: Optional[str] = None
    ):
        """add single child, returns tx id or noop dict"""
        result = self.add_children(parent_id, child_type, [child_facts], business_time)
        if isinstance(result, dict) and "noop" in result:
            return result
        return result[0]

    def add_children(
        self,
        parent_id: str,
        child_type: str,
        children_facts: List[Dict[str, Any]],
        business_time: Optional[str] = None
    ) -> List[str]:
        """add multiple children in single tx, returns a list of tx ids or noop dict"""

        #validate all children
        for child_fact in children_facts:
            _validate_child_fact(child_fact)

        # build facts
        family_id = _build_family_id(parent_id, child_type)
        all_facts= []

        for child_fact in children_facts:
            child_id = child_fact["kang_id"]
            all_facts.append(child_fact)
            all_facts.append(_build_child_relationship_fact(family_id, child_id))

        return self.store.add_facts(all_facts, business_time)


    def remove_child(
        self,
        parent_id: str,
        child_type: str,
        child_id: str,
        business_time: Optional[str] = None
        ):
        """ soft delete child from parent's family, return tx id or noop dict"""

        family_id = _build_family_id(parent_id, child_type)

        child_facts = [
            _build_child_relationship_fact(family_id, child_id, active=False),
            _build_child_deletion_fact(child_id)
        ]

        result = self.store.add_facts(child_facts, business_time)
        if isinstance(result, dict) and "noop" in result:
            return result
        return result[-1]

    def _extract_children(
        self,
        family_state: Dict[str, Any],
        timestamp: Optional[str] = None,
        with_deleted: bool = False,
    ) -> Dict[str, Dict[str, Any]]:
        """extract children from family state, optionally including deleted"""
        children = {}

        for key,value in family_state.items():
            if key in ["kang_id", "at"]:
                continue

            is_deleted = value is None

            if with_deleted or not is_deleted:
                child_state = (
                    self.store.as_of(key, timestamp)
                    if timestamp
                    else self.store.rollup(key)
                )

                if is_deleted and with_deleted:
                    child_state["deleted"] = True

                children[key] = child_state

        return children


    # Read operations

    def find_children(
        self,
        parent_id: str,
        child_type: str,
        with_deleted: bool = False
    ) -> Dict[str, Dict[str, Any]]:
        """find current children of specific type"""

        family_id = _build_family_id(parent_id, child_type)
        family_state = self.store.rollup(family_id, with_nils=True)

        if not family_state:
            return {}

        return self._extract_children(family_state, with_deleted=with_deleted)

    def find_childen_at(
        self,
        parent_id: str,
        child_type: str,
        timestamp: str,
        with_deleted: bool = False
        ) -> Dict[str, Dict[str, Any]]:
        """find children as they existed at specific time"""

        family_id = _build_family_id(parent_id, child_type)
        family_state = self.store.as_of(family_id, timestamp, with_nils=True)

        if not family_state:
            return {}

        return self._extract_children(family_state, timestamp=timestamp, with_deleted=with_deleted)

    def get_children_diff(
        self,
        parent_id: str,
        child_type: str,
        from_time: Optional[str] = None,
        to_time: Optional[str]= None
    ) -> Dict[str, Dict[str, Dict[str, Any]]]:
        """compare children at two time points"""

        from_children = (
            self.find_childen_at(parent_id, child_type, from_time)
            if from_time
            else {}
        )

        to_children = (
            self.find_childen_at(parent_id, child_type, to_time)
            if to_time
            else self.find_children(parent_id, child_type)
        )

        from_ids = set(from_children.keys())
        to_ids = set(to_children.keys())

        added_ids = to_ids - from_ids
        deleted_ids = from_ids - to_ids
        retained_ids = from_ids & to_ids

        return {
            "added": {child_id: to_children[child_id] for child_id in added_ids},
            "deleted": {child_id: from_children[child_id] for child_id in deleted_ids},
            "retained": {child_id: to_children[child_id] for child_id in retained_ids}
        }

    def get_child_history(
        self,
        parent_id: str,
        child_type: str,
        child_id: str
    ) -> List[Dict[str, Any]]:
        """get complete audit trail for child's relationship with parent"""

        family_id = _build_family_id(parent_id, child_type)
        facts = self.store.get_facts(family_id, with_tx=True)

        child_facts = [
            fact for fact in facts
            if child_id in fact and child_id != "kang_id"
        ]

        history = [
            {
                "business_time": fact["at"],
                "tx_time": fact["tx_at"],
                "tx_id": fact["tx_id"],
                "state": fact.get(child_id)
            }
            for fact in child_facts
        ]

        history.sort(key=lambda x: (x["business_time"], x["tx_time"]))

        return history
