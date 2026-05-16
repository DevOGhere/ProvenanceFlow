"""
Core data contracts for the ProvenanceFlow validation layer.

ValidationResult is the single output type produced by every rule function
and consumed by Validator, ProvenanceTracker, and the reporting layer.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


MONTHLY_COLS = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun",
    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
]


@dataclass
class ValidationResult:
    """Outcome of a single rule applied to a row or DataFrame.

    Fields:
        passed:     Always False for results stored here (passing rows produce nothing).
        rule_name:  Identifier matching the rule's name= argument or function name.
        severity:   'warning' or 'hard_rejection'. Hard rejections are excluded
                    from the clean DataFrame; warnings are logged but kept.
        row_index:  DataFrame integer index of the offending row, or None for
                    dataset-level rules (temporal_continuity, baseline_integrity).
        value:      The raw value that triggered the failure, if applicable.
        reason:     Human-readable explanation, stored in PROV lineage metadata.
    """
    passed: bool
    rule_name: str
    severity: str           # 'hard_rejection' | 'warning'
    row_index: Optional[int]
    value: Optional[float]
    reason: str
