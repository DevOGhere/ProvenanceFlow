"""
Helpers for navigating W3C PROV-JSON documents.

PROV-JSON serialises typed literals as {"$": value, "type": "..."}.
All helpers in this module handle that unwrapping transparently.
"""
from __future__ import annotations


def unwrap(v) -> object:
    """PROV-JSON typed literals arrive as {'$': value, 'type': '...'}. Unwrap them."""
    return v['$'] if isinstance(v, dict) and '$' in v else v


def get_ingestion_entity(doc: dict) -> tuple[str, dict]:
    """Return (entity_id, attrs) for the raw dataset_ entity in a PROV doc."""
    for eid, attrs in doc.get('entity', {}).items():
        if 'dataset_' in eid:
            return eid, attrs
    return '', {}


def get_validation_activity(doc: dict) -> tuple[str, dict]:
    """Return (activity_id, attrs) for the validate_ activity in a PROV doc."""
    for aid, attrs in doc.get('activity', {}).items():
        if 'validate_' in aid:
            return aid, attrs
    return '', {}


def get_validated_entity(doc: dict) -> tuple[str, dict]:
    """Return (entity_id, attrs) for the validated_ output entity in a PROV doc."""
    for eid, attrs in doc.get('entity', {}).items():
        if 'validated_' in eid:
            return eid, attrs
    return '', {}
