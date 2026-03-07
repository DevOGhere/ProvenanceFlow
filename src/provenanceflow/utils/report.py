"""
Reproducibility report generator for ProvenanceFlow.

Produces a human-readable Markdown document from a stored provenance run,
suitable for attaching to a paper, email, or supplementary material.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone

from .prov_helpers import (
    unwrap,
    get_ingestion_entity,
    get_validation_activity,
    get_validated_entity,
)


def render_report(run_id: str, store) -> str:
    """Generate a Markdown reproducibility report for a stored provenance run.

    Args:
        run_id: The run identifier (UUID string).
        store:  A ProvenanceStore instance.

    Returns:
        A Markdown string with dataset metadata, validation summary, and
        instructions for reproducing the run. Embeds the full W3C PROV-JSON
        as a fenced code block.

    Raises:
        ValueError: If the run_id is not found in the store.
    """
    doc = store.get(run_id)
    if not doc:
        raise ValueError(f"Run '{run_id}' not found in provenance store.")

    _, ing = get_ingestion_entity(doc)
    _, val = get_validation_activity(doc)
    _, out = get_validated_entity(doc)

    # ── Extract key fields ────────────────────────────────────────────────────
    source_url    = ing.get("fair:source_url", "—")
    title         = ing.get("dc:title", "—")
    fmt           = ing.get("dc:format", "—")
    license_      = ing.get("dc:license", "—")
    fair_id       = unwrap(ing.get("fair:identifier", "—"))
    ingest_ts     = ing.get("pf:ingest_timestamp", "—")
    checksum      = ing.get("pf:checksum_sha256", "—")
    row_count     = unwrap(ing.get("pf:row_count", "—"))

    rows_in       = unwrap(val.get("pf:rows_in", "—"))
    rows_passed   = unwrap(val.get("pf:rows_passed", "—"))
    rows_rejected = unwrap(val.get("pf:rows_rejected", "—"))
    rate          = unwrap(val.get("pf:rejection_rate", None))
    rules         = val.get("pf:rules_applied", "—")

    rate_pct = f"{float(rate)*100:.2f}%" if rate is not None else "—"

    out_id    = unwrap(out.get("fair:identifier", "—")) if out else "—"
    out_count = unwrap(out.get("pf:row_count", "—")) if out else "—"

    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    lines: list[str] = [
        f"# Reproducibility Report",
        f"",
        f"**Run ID:** `{run_id}`  ",
        f"**Generated:** {generated_at}  ",
        f"**ProvenanceFlow version:** 1.0.0",
        f"",
        f"---",
        f"",
        f"## Dataset",
        f"",
        f"| Field | Value |",
        f"|---|---|",
        f"| Title | {title} |",
        f"| Source URL | {source_url} |",
        f"| Format | {fmt} |",
        f"| License | {license_} |",
        f"| FAIR Identifier | `{fair_id}` |",
        f"| Ingested at | {ingest_ts} |",
        f"| Row count (raw) | {row_count:,} |" if isinstance(row_count, int)
            else f"| Row count (raw) | {row_count} |",
        f"| SHA-256 | `{checksum}` |",
        f"",
        f"---",
        f"",
        f"## Validation Summary",
        f"",
        f"| Metric | Value |",
        f"|---|---|",
        f"| Rows in | {rows_in:,} |" if isinstance(rows_in, int)
            else f"| Rows in | {rows_in} |",
        f"| Rows passed | {rows_passed:,} |" if isinstance(rows_passed, int)
            else f"| Rows passed | {rows_passed} |",
        f"| Rows rejected | {rows_rejected:,} |" if isinstance(rows_rejected, int)
            else f"| Rows rejected | {rows_rejected} |",
        f"| Rejection rate | {rate_pct} |",
        f"| Rules applied | `{rules}` |",
        f"",
    ]

    # ── Rejections-by-rule table ──────────────────────────────────────────────
    rejected_rows_data = store.get_rejections(run_id)

    if rejected_rows_data:
        rule_counts: dict[str, int] = {}
        for rec in rejected_rows_data:
            rule_counts[rec["rule"]] = rule_counts.get(rec["rule"], 0) + 1

        lines += [
            f"## Rejected Rows by Rule",
            f"",
            f"| Rule | Rejected Rows |",
            f"|---|---:|",
        ]
        for rule_name, count in sorted(rule_counts.items()):
            lines.append(f"| `{rule_name}` | {count} |")
        lines.append("")

        sample_count = min(20, len(rejected_rows_data))
        lines += [
            f"### Sample Rejected Rows (first {sample_count})",
            f"",
            f"| # | Rule | Message |",
            f"|---|---|---|",
        ]
        for rec in rejected_rows_data[:20]:
            msg = rec["message"].replace("|", "\\|")
            lines.append(f"| {rec['row_index']} | `{rec['rule']}` | {msg} |")
        lines.append("")

    if out:
        lines += [
            f"## Validated Output",
            f"",
            f"| Field | Value |",
            f"|---|---|",
            f"| FAIR Identifier | `{out_id}` |",
            f"| Row count | {out_count:,} |" if isinstance(out_count, int)
                else f"| Row count | {out_count} |",
            f"",
        ]

    lines += [
        f"---",
        f"",
        f"## How to Reproduce This Run",
        f"",
        f"```bash",
        f"pip install provenanceflow",
        f"provenanceflow run --url \"{source_url}\"",
        f"```",
        f"",
        f"Or with a local CSV file:",
        f"",
        f"```bash",
        f"provenanceflow run --file path/to/gistemp.csv",
        f"```",
        f"",
        f"---",
        f"",
        f"## Full W3C PROV-JSON",
        f"",
        f"```json",
        json.dumps(doc, indent=2),
        f"```",
    ]

    return "\n".join(lines)
