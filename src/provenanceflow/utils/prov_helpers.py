"""
Helpers for navigating W3C PROV-JSON documents.

PROV-JSON serialises typed literals as {"$": value, "type": "..."}.
All helpers in this module handle that unwrapping transparently.

Two run shapes exist:
  - Full pipeline runs: validate_ activities, validated_ output entities
  - @track decorator runs: transform_ activities, transformed_ output entities
All helpers handle both shapes.
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
    """Return (activity_id, attrs) for the processing activity in a PROV doc.

    Checks for validate_ activities first (full pipeline runs), then falls
    back to transform_ activities (@track decorator runs).
    """
    for aid, attrs in doc.get('activity', {}).items():
        if 'validate_' in aid:
            return aid, attrs
    for aid, attrs in doc.get('activity', {}).items():
        if 'transform_' in aid:
            return aid, attrs
    return '', {}


def get_validated_entity(doc: dict) -> tuple[str, dict]:
    """Return (entity_id, attrs) for the output entity in a PROV doc.

    Checks for validated_ entities first (full pipeline runs), then falls
    back to transformed_ entities (@track decorator runs).
    """
    for eid, attrs in doc.get('entity', {}).items():
        if 'validated_' in eid:
            return eid, attrs
    for eid, attrs in doc.get('entity', {}).items():
        if 'transformed_' in eid:
            return eid, attrs
    return '', {}


def get_activity_kind(attrs: dict) -> str:
    """Return 'validation' or 'transformation' based on activity attributes."""
    if 'pf:rules_applied' in attrs:
        return 'validation'
    if 'pf:function_name' in attrs:
        return 'transformation'
    return 'unknown'
