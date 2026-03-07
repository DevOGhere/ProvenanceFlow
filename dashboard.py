"""
ProvenanceFlow Dashboard
Run: streamlit run dashboard.py

Four tabs:
  🏠 Overview     — aggregate metrics + rejection-rate trend
  🔍 Run Detail   — PROV metadata, validation stats, export
  🔗 PROV Graph   — interactive (pyvis) or static (pydot) lineage graph
  ▶  Run Pipeline — trigger a pipeline run without the CLI
"""
from __future__ import annotations

import ast
import io
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import streamlit as st

from src.provenanceflow.provenance.store import ProvenanceStore
from src.provenanceflow.provenance.query import get_run
from src.provenanceflow.utils.prov_helpers import (
    unwrap as _unwrap,
    get_ingestion_entity as _get_ingestion_entity_full,
    get_validation_activity as _get_validation_activity_full,
    get_validated_entity as _get_validated_entity_full,
)
from src.provenanceflow.utils.report import render_report

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="ProvenanceFlow",
    page_icon="🔬",
    layout="wide",
    initial_sidebar_state="expanded",
)

DB_PATH = "provenance_store/lineage.db"


# ── Data helpers ──────────────────────────────────────────────────────────────

def _parse_dict_attr(v) -> dict:
    """Activity attributes like rejections_by_rule are stored as str(dict)."""
    try:
        return ast.literal_eval(str(v)) if v else {}
    except Exception:
        return {}


def _get_ingestion_entity(doc: dict) -> dict:
    _, attrs = _get_ingestion_entity_full(doc)
    return attrs


def _get_validation_activity(doc: dict) -> dict:
    _, attrs = _get_validation_activity_full(doc)
    return attrs


def _get_validated_entity(doc: dict) -> dict:
    _, attrs = _get_validated_entity_full(doc)
    return attrs


@st.cache_resource
def _get_store() -> ProvenanceStore:
    return ProvenanceStore(db_path=DB_PATH)


def _enrich_runs(store: ProvenanceStore) -> list[dict]:
    """Augment bare run metadata with key metrics from PROV-JSON."""
    enriched = []
    for run_meta in store.list_runs():
        doc = store.get(run_meta["run_id"])
        if not doc:
            continue
        ing = _get_ingestion_entity(doc)
        val = _get_validation_activity(doc)
        enriched.append({
            "run_id":         run_meta["run_id"],
            "created_at":     run_meta["created_at"],
            "rows_in":        _unwrap(val.get("pf:rows_in", "—")),
            "rows_passed":    _unwrap(val.get("pf:rows_passed", "—")),
            "rows_rejected":  _unwrap(val.get("pf:rows_rejected", "—")),
            "rejection_rate": _unwrap(val.get("pf:rejection_rate", None)),
            "source_url":     ing.get("fair:source_url", "—"),
        })
    return enriched


# ── PROV graph builders ───────────────────────────────────────────────────────

def _build_prov_graph_interactive(doc: dict) -> str | None:
    """Render the PROV graph as a pyvis HTML string (for st.components.html)."""
    try:
        from pyvis.network import Network
    except ImportError:
        return None

    net = Network(
        height="480px", width="100%", directed=True,
        bgcolor="#fafafa", font_color="#333333",
    )
    net.set_options("""{
        "edges": {"arrows": {"to": {"enabled": true}}},
        "physics": {"stabilization": {"iterations": 150}}
    }""")

    def _short(name: str) -> str:
        return name.replace("pf:", "").replace("prov:", "")

    for eid in doc.get("entity", {}):
        color = "#4A90D9" if "dataset_" in eid else "#27AE60"
        net.add_node(eid, label=_short(eid), color=color, shape="box",
                     title=f"Entity: {eid}", font={"color": "white"})

    for aid in doc.get("activity", {}):
        net.add_node(aid, label=_short(aid), color="#E67E22", shape="ellipse",
                     title=f"Activity: {aid}", font={"color": "white"})

    for agid in doc.get("agent", {}):
        net.add_node(agid, label=_short(agid), color="#95A5A6", shape="diamond",
                     title=f"Agent: {agid}", font={"color": "white"})

    def _add_edges(rel_key, tail_k, head_k, label):
        for rel in doc.get(rel_key, {}).values():
            if isinstance(rel, dict):
                tail = rel.get(tail_k)
                head = rel.get(head_k)
                if tail and head:
                    net.add_edge(tail, head, label=label, font={"size": 9})

    _add_edges("wasGeneratedBy",    "prov:activity",        "prov:entity",       "wasGeneratedBy")
    _add_edges("used",              "prov:activity",        "prov:entity",       "used")
    _add_edges("wasDerivedFrom",    "prov:generatedEntity", "prov:usedEntity",   "wasDerivedFrom")
    _add_edges("wasAssociatedWith", "prov:activity",        "prov:agent",        "wasAssociatedWith")

    try:
        return net.generate_html()
    except Exception:
        return None


def _build_prov_graph_static(doc: dict) -> bytes | None:
    """Render the PROV graph as PNG bytes via pydot (fallback)."""
    try:
        import pydot
    except ImportError:
        return None

    graph = pydot.Dot("provenance", graph_type="digraph", rankdir="LR",
                      fontname="Helvetica", bgcolor="#fafafa")
    graph.set_graph_defaults(pad="0.5", nodesep="0.5", ranksep="0.8")

    def _short(name: str) -> str:
        return name.replace("pf:", "").replace("prov:", "")

    for eid in doc.get("entity", {}):
        color = "#4A90D9" if "dataset_" in eid else "#27AE60"
        graph.add_node(pydot.Node(
            f'"{eid}"', label=f'"{_short(eid)}"', shape="box",
            style="filled", fillcolor=color, fontcolor="white",
            fontname="Helvetica", fontsize="11",
        ))

    for aid in doc.get("activity", {}):
        graph.add_node(pydot.Node(
            f'"{aid}"', label=f'"{_short(aid)}"', shape="ellipse",
            style="filled", fillcolor="#E67E22", fontcolor="white",
            fontname="Helvetica", fontsize="11",
        ))

    for agid in doc.get("agent", {}):
        graph.add_node(pydot.Node(
            f'"{agid}"', label=f'"{_short(agid)}"', shape="diamond",
            style="filled", fillcolor="#95A5A6", fontcolor="white",
            fontname="Helvetica", fontsize="11",
        ))

    def _add_edges(rel_key, tail_k, head_k, label, color="#555555"):
        for rel in doc.get(rel_key, {}).values():
            if isinstance(rel, dict):
                tail = rel.get(tail_k)
                head = rel.get(head_k)
                if tail and head:
                    graph.add_edge(pydot.Edge(
                        f'"{tail}"', f'"{head}"',
                        label=f'"{label}"', fontsize="9",
                        fontcolor=color, color=color, fontname="Helvetica",
                    ))

    _add_edges("wasGeneratedBy",    "prov:activity",        "prov:entity",       "wasGeneratedBy",   "#27AE60")
    _add_edges("used",              "prov:activity",        "prov:entity",       "used",             "#4A90D9")
    _add_edges("wasDerivedFrom",    "prov:generatedEntity", "prov:usedEntity",   "wasDerivedFrom",   "#8E44AD")
    _add_edges("wasAssociatedWith", "prov:activity",        "prov:agent",        "wasAssociatedWith","#95A5A6")

    try:
        return graph.create_png()
    except Exception:
        return None


# ── Sidebar ───────────────────────────────────────────────────────────────────

def _sidebar(run_ids: list[str]) -> str | None:
    with st.sidebar:
        st.markdown("## 🔬 ProvenanceFlow")
        st.markdown(
            "*FAIR-compliant data lineage tracker*  \n"
            "W3C PROV · Dublin Core · Schema.org"
        )
        st.divider()

        selected_run = None
        if run_ids:
            selected_run = st.selectbox(
                "Select run (for Detail & Graph tabs)",
                run_ids,
                format_func=lambda x: x[:16] + "…",
            )

        st.divider()
        st.caption("Standards")
        st.markdown(
            "- [W3C PROV-DM](https://www.w3.org/TR/prov-dm/)\n"
            "- [FAIR Principles](https://doi.org/10.1038/sdata.2016.18)\n"
            "- [Dublin Core](https://www.dublincore.org/specifications/dublin-core/dcmi-terms/)\n"
            "- [Schema.org](https://schema.org/Dataset)"
        )
    return selected_run


# ── Page: Overview ────────────────────────────────────────────────────────────

def _page_overview(runs: list[dict]):
    st.header("Pipeline Runs — Overview")

    if not runs:
        st.info(
            "No runs yet. Switch to the **▶ Run Pipeline** tab to create your first run, "
            "or run `provenanceflow run` from the terminal."
        )
        return

    # Metric cards
    total_runs = len(runs)
    latest_rate = runs[0].get("rejection_rate")
    total_rows_in = sum(r["rows_in"] for r in runs if isinstance(r.get("rows_in"), (int, float)))
    total_rows_passed = sum(r["rows_passed"] for r in runs if isinstance(r.get("rows_passed"), (int, float)))

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Runs", total_runs)
    c2.metric(
        "Latest Rejection Rate",
        f"{float(latest_rate)*100:.2f} %" if latest_rate is not None else "—",
    )
    c3.metric("Total Rows Processed", f"{total_rows_in:,}" if total_rows_in else "—")
    c4.metric("Total Rows Passed", f"{total_rows_passed:,}" if total_rows_passed else "—")

    # Rejection rate trend chart (requires ≥2 runs with numeric rates)
    trend_data = [
        {"Run": r["created_at"], "Rejection Rate (%)": float(r["rejection_rate"]) * 100}
        for r in reversed(runs)
        if r.get("rejection_rate") is not None
    ]
    if len(trend_data) >= 2:
        st.divider()
        st.markdown("**Rejection Rate Over Time**")
        df_trend = pd.DataFrame(trend_data).set_index("Run")
        st.line_chart(df_trend)

    st.divider()

    # Runs table
    df = pd.DataFrame(runs)
    if "rejection_rate" in df.columns:
        df["rejection_rate"] = df["rejection_rate"].apply(
            lambda x: f"{float(x)*100:.2f} %" if x is not None else "—"
        )
    df.columns = [c.replace("_", " ").title() for c in df.columns]
    st.dataframe(df, use_container_width=True, hide_index=True)

    # Compare Runs panel
    run_ids = [r["run_id"] for r in runs]
    if len(run_ids) >= 2:
        st.divider()
        with st.expander("🔄 Compare Two Runs", expanded=False):
            col_a, col_b = st.columns(2)
            with col_a:
                run_a = st.selectbox("Run A (baseline)", run_ids, key="cmp_a",
                                     format_func=lambda x: x[:20] + "…")
            with col_b:
                run_b = st.selectbox("Run B (compare to)", run_ids,
                                     index=1, key="cmp_b",
                                     format_func=lambda x: x[:20] + "…")
            if st.button("Compare", disabled=(run_a == run_b)):
                from src.provenanceflow.provenance.compare import compare_runs
                diff = compare_runs(run_a, run_b, store)
                cols = st.columns(4)
                cols[0].metric("Same Dataset", "✅ Yes" if diff.same_dataset else "❌ No")
                cols[1].metric("Same Rules",   "✅ Yes" if diff.same_rules   else "❌ No")
                cols[2].metric("Δ Rows Passed", f"{diff.delta_rows_passed:+,}")
                cols[3].metric("Δ Rejection Rate",
                               f"{diff.delta_rejection_rate * 100:+.2f}%")
                if diff.delta_rejection_rate < 0:
                    st.success(f"✅ {diff.summary}")
                elif diff.delta_rejection_rate > 0:
                    st.error(f"⚠️ {diff.summary}")
                else:
                    st.info(f"ℹ️ {diff.summary}")


# ── Page: Run Detail ──────────────────────────────────────────────────────────

def _page_run_detail(store: ProvenanceStore, run_id: str):
    doc = get_run(store, run_id)
    if not doc:
        st.error(f"Run `{run_id}` not found.")
        return

    ing = _get_ingestion_entity(doc)
    val = _get_validation_activity(doc)
    out = _get_validated_entity(doc)

    st.header(f"Run Detail — `{run_id}`")

    # ── Export ─────────────────────────────────────────────────────────────
    with st.expander("📤 Export", expanded=False):
        col_a, col_b = st.columns(2)
        with col_a:
            prov_json_bytes = json.dumps(doc, indent=2).encode("utf-8")
            st.download_button(
                label="⬇ Download PROV-JSON",
                data=prov_json_bytes,
                file_name=f"prov_{run_id[:8]}.json",
                mime="application/json",
            )
        with col_b:
            try:
                report_md = render_report(run_id, store)
                st.download_button(
                    label="⬇ Reproducibility Report (.md)",
                    data=report_md.encode("utf-8"),
                    file_name=f"report_{run_id[:8]}.md",
                    mime="text/markdown",
                )
            except Exception as exc:
                st.caption(f"Report unavailable: {exc}")

    # ── Source Dataset ─────────────────────────────────────────────────────
    with st.expander("📦 Source Dataset", expanded=True):
        col1, col2 = st.columns(2)
        with col1:
            st.markdown(f"**Title:** {ing.get('dc:title', '—')}")
            st.markdown(f"**Source URL:** {ing.get('fair:source_url', '—')}")
            st.markdown(f"**Format:** {ing.get('dc:format', '—')}")
            st.markdown(f"**License:** {ing.get('dc:license', '—')}")
        with col2:
            st.markdown(f"**FAIR Identifier:** `{ing.get('fair:identifier', '—')}`")
            st.markdown(f"**Ingested:** {ing.get('pf:ingest_timestamp', '—')}")
            rc = _unwrap(ing.get("pf:row_count", "—"))
            st.markdown(f"**Row Count:** {rc:,}" if isinstance(rc, int) else f"**Row Count:** {rc}")
            st.markdown(f"**SHA-256:** `{ing.get('pf:checksum_sha256', '—')[:24]}…`")

    # ── FAIR + DC / Schema.org Metadata ───────────────────────────────────
    with st.expander("🏷️ FAIR + Dublin Core / Schema.org Metadata"):
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("**Dublin Core**")
            for key in ("dc:title", "dc:identifier", "dc:source", "dc:format",
                        "dc:created", "dc:type", "dc:license"):
                if key in ing:
                    st.markdown(f"- `{key}`: {ing[key]}")
        with col2:
            st.markdown("**Schema.org**")
            for key in ("schema:name", "schema:url", "schema:encodingFormat"):
                if key in ing:
                    st.markdown(f"- `{key}`: {ing[key]}")

    # ── Validation Summary ─────────────────────────────────────────────────
    st.subheader("Validation Summary")

    rows_in  = _unwrap(val.get("pf:rows_in", 0)) or 0
    rows_out = _unwrap(val.get("pf:rows_passed", 0)) or 0
    rows_rej = _unwrap(val.get("pf:rows_rejected", 0)) or 0
    rate     = _unwrap(val.get("pf:rejection_rate", None))

    c1, c2, c3 = st.columns(3)
    c1.metric("Rows In",       f"{rows_in:,}" if isinstance(rows_in, int) else rows_in)
    c2.metric("Rows Passed",   f"{rows_out:,}" if isinstance(rows_out, int) else rows_out)
    c3.metric("Rows Rejected", f"{rows_rej:,}" if isinstance(rows_rej, int) else rows_rej,
              delta=f"-{rows_rej}" if isinstance(rows_rej, int) and rows_rej > 0 else None,
              delta_color="inverse")

    if rate is not None:
        pct = float(rate) * 100
        msg = f"Rejection rate: **{pct:.2f} %**"
        if pct > 5:
            st.error(msg)
        else:
            st.success(msg)

    # Rules applied pills
    rules_raw = val.get("pf:rules_applied", "")
    rules = [r.strip() for r in rules_raw.split(",") if r.strip()]
    if rules:
        st.markdown("**Rules Applied:**")
        cols = st.columns(min(len(rules), 5))
        for i, rule in enumerate(rules):
            cols[i % len(cols)].markdown(
                f'<span style="background:#4A90D9;color:white;padding:3px 8px;'
                f'border-radius:12px;font-size:0.8em">{rule}</span>',
                unsafe_allow_html=True,
            )

    # Bar charts
    rejections = _parse_dict_attr(val.get("pf:rejections_by_rule"))
    warnings   = _parse_dict_attr(val.get("pf:warnings_by_rule"))

    col1, col2 = st.columns(2)
    with col1:
        st.markdown("**Hard Rejections by Rule**")
        if rejections:
            st.bar_chart(pd.Series(rejections))
        else:
            st.caption("None")
    with col2:
        st.markdown("**Warnings by Rule**")
        if warnings:
            st.bar_chart(pd.Series(warnings))
        else:
            st.caption("None")

    # Validated output
    if out:
        with st.expander("✅ Validated Output Entity"):
            for key in ("dc:title", "fair:identifier", "dc:isVersionOf"):
                if key in out:
                    st.markdown(f"- `{key}`: {out[key]}")
            vc = _unwrap(out.get("pf:row_count", "—"))
            st.markdown(f"- `pf:row_count`: {vc:,}" if isinstance(vc, int) else f"- `pf:row_count`: {vc}")

    # Raw PROV-JSON
    with st.expander("📄 Raw W3C PROV-JSON"):
        st.json(doc)


# ── Page: PROV Graph ──────────────────────────────────────────────────────────

def _page_prov_graph(store: ProvenanceStore, run_id: str):
    doc = get_run(store, run_id)
    if not doc:
        st.error(f"Run `{run_id}` not found.")
        return

    st.header(f"PROV Lineage Graph — `{run_id}`")
    st.caption(
        "🟦 Raw dataset entity  ·  🟩 Validated dataset entity  ·  "
        "🟠 Activity  ·  ⬜ Agent"
    )

    # Try interactive pyvis first, fall back to static pydot PNG
    html = _build_prov_graph_interactive(doc)
    if html:
        import streamlit.components.v1 as components
        components.html(html, height=500, scrolling=False)
    else:
        png = _build_prov_graph_static(doc)
        if png:
            st.image(png, caption="W3C PROV entity-activity-agent graph",
                     use_container_width=True)
        else:
            st.warning(
                "Graph rendering unavailable. Install `graphviz` (`apt install graphviz` "
                "or `brew install graphviz`) or `pip install pyvis` for an interactive graph."
            )
            st.markdown("**Raw PROV relationships:**")
            for rel in ("wasGeneratedBy", "used", "wasDerivedFrom", "wasAssociatedWith"):
                if rel in doc:
                    st.markdown(f"**{rel}**: {list(doc[rel].keys())}")


# ── Page: Run Pipeline ────────────────────────────────────────────────────────

def _page_run_pipeline():
    st.header("▶ Run Pipeline")
    st.markdown(
        "Trigger a new provenance pipeline run directly from the browser — "
        "no CLI or terminal knowledge needed."
    )

    source_choice = st.radio(
        "Data source",
        ["NASA GISTEMP (default URL)", "Upload local CSV"],
        horizontal=True,
    )

    uploaded_file = None
    if source_choice == "Upload local CSV":
        uploaded_file = st.file_uploader(
            "Upload a GISTEMP-format CSV",
            type=["csv"],
            help="Must follow NASA GISTEMP format: Year column + Jan–Dec monthly columns.",
        )

    run_disabled = source_choice == "Upload local CSV" and uploaded_file is None
    if st.button("▶ Run Now", type="primary", disabled=run_disabled):
        _execute_pipeline(source_choice, uploaded_file)

    # Show last result if stored in session state
    if "last_pipeline_result" in st.session_state:
        res = st.session_state["last_pipeline_result"]
        v = res["validation"]
        st.success(f"✅ Last run complete — `{res['run_id']}`")
        cols = st.columns(4)
        cols[0].metric("Rows In",       f"{v['rows_in']:,}")
        cols[1].metric("Rows Passed",   f"{v['rows_passed']:,}")
        cols[2].metric("Rows Rejected", f"{v['rows_rejected']:,}")
        cols[3].metric("Rejection Rate", f"{v['rejection_rate']*100:.2f}%")
        st.info(
            "Switch to the **🔍 Run Detail** tab, "
            "select this run in the sidebar, to view full provenance."
        )


def _execute_pipeline(source_choice: str, uploaded_file):
    """Run the pipeline and store the summary in session state."""
    from src.provenanceflow.pipeline.runner import run_pipeline
    from src.provenanceflow.ingestion import NASAGISTEMPSource, LocalCSVSource
    from src.provenanceflow.config import get_settings

    settings = get_settings()

    try:
        if source_choice == "Upload local CSV" and uploaded_file is not None:
            upload_dir = Path("data/raw")
            upload_dir.mkdir(parents=True, exist_ok=True)
            ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
            tmp_path = upload_dir / f"uploaded_{ts}.csv"
            tmp_path.write_bytes(uploaded_file.getvalue())
            source = LocalCSVSource(path=tmp_path)
        else:
            source = NASAGISTEMPSource(
                url=str(settings.gistemp_url),
                output_dir=str(settings.raw_data_path),
            )

        with st.spinner("Running pipeline… fetching, validating, recording provenance…"):
            result = run_pipeline(source=source, db_path=DB_PATH)

        st.session_state["last_pipeline_result"] = {
            "run_id": result.run_id,
            "validation": {
                "rows_in":        result.validation.rows_in,
                "rows_passed":    result.validation.rows_passed,
                "rows_rejected":  result.validation.rows_rejected,
                "rejection_rate": result.validation.rejection_rate,
            },
        }
        st.rerun()

    except Exception as exc:
        st.error(f"Pipeline failed: {exc}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    store = _get_store()
    runs = _enrich_runs(store)
    run_ids = [r["run_id"] for r in runs]

    selected_run = _sidebar(run_ids)

    tab_overview, tab_detail, tab_graph, tab_run = st.tabs([
        "🏠 Overview",
        "🔍 Run Detail",
        "🔗 PROV Graph",
        "▶ Run Pipeline",
    ])

    with tab_overview:
        _page_overview(runs)

    with tab_detail:
        if run_ids:
            _page_run_detail(store, selected_run or run_ids[0])
        else:
            st.info(
                "No runs yet. Switch to the **▶ Run Pipeline** tab to create your first run."
            )

    with tab_graph:
        if run_ids:
            _page_prov_graph(store, selected_run or run_ids[0])
        else:
            st.info("No runs yet.")

    with tab_run:
        _page_run_pipeline()


if __name__ == "__main__":
    main()
