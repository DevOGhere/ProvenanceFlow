"""
ProvenanceFlow Dashboard
Run: streamlit run dashboard.py
Requires: python demo.py first (to populate the provenance store).
"""
import ast
import io
import json

import pandas as pd
import streamlit as st

from src.provenanceflow.provenance.store import ProvenanceStore
from src.provenanceflow.provenance.query import (
    get_run, list_runs, get_entities, get_activities,
)

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title='ProvenanceFlow',
    page_icon='🔬',
    layout='wide',
    initial_sidebar_state='expanded',
)

DB_PATH = 'provenance_store/lineage.db'

# ── Data helpers ──────────────────────────────────────────────────────────────

def _unwrap(v):
    """PROV-JSON stores typed literals as {'$': value, 'type': '...'}."""
    return v['$'] if isinstance(v, dict) and '$' in v else v


def _parse_dict_attr(v) -> dict:
    """Activity attributes like rejections_by_rule are stored as str(dict)."""
    try:
        return ast.literal_eval(str(v)) if v else {}
    except Exception:
        return {}


def _get_ingestion_entity(doc: dict) -> dict:
    for eid, attrs in doc.get('entity', {}).items():
        if 'dataset_' in eid:
            return attrs
    return {}


def _get_validation_activity(doc: dict) -> dict:
    for aid, attrs in doc.get('activity', {}).items():
        if 'validate_' in aid:
            return attrs
    return {}


def _get_validated_entity(doc: dict) -> dict:
    for eid, attrs in doc.get('entity', {}).items():
        if 'validated_' in eid:
            return attrs
    return {}


@st.cache_resource
def _get_store() -> ProvenanceStore:
    return ProvenanceStore(db_path=DB_PATH)


def _enrich_runs(store: ProvenanceStore) -> list[dict]:
    """Augment bare run metadata with key metrics from PROV-JSON."""
    enriched = []
    for run_meta in store.list_runs():
        doc = store.get(run_meta['run_id'])
        if not doc:
            continue
        ing = _get_ingestion_entity(doc)
        val = _get_validation_activity(doc)
        enriched.append({
            'run_id':         run_meta['run_id'],
            'created_at':     run_meta['created_at'],
            'rows_in':        _unwrap(val.get('pf:rows_in', '—')),
            'rows_passed':    _unwrap(val.get('pf:rows_passed', '—')),
            'rows_rejected':  _unwrap(val.get('pf:rows_rejected', '—')),
            'rejection_rate': _unwrap(val.get('pf:rejection_rate', None)),
            'source_url':     ing.get('fair:source_url', '—'),
        })
    return enriched


# ── PROV graph builder ────────────────────────────────────────────────────────

def _build_prov_graph(doc: dict) -> bytes | None:
    """Render the PROV entity-activity-agent graph as PNG bytes via pydot."""
    try:
        import pydot
    except ImportError:
        return None

    graph = pydot.Dot('provenance', graph_type='digraph', rankdir='LR',
                      fontname='Helvetica', bgcolor='#fafafa')
    graph.set_graph_defaults(pad='0.5', nodesep='0.5', ranksep='0.8')

    def _short(name: str) -> str:
        return name.replace('pf:', '').replace('prov:', '')

    node_map: dict[str, pydot.Node] = {}

    # Entities — boxes
    for eid in doc.get('entity', {}):
        label = _short(eid)
        color = '#4A90D9' if 'dataset_' in eid else '#27AE60'
        n = pydot.Node(
            f'"{eid}"', label=f'"{label}"', shape='box',
            style='filled', fillcolor=color, fontcolor='white',
            fontname='Helvetica', fontsize='11',
        )
        graph.add_node(n)
        node_map[eid] = n

    # Activities — ellipses
    for aid in doc.get('activity', {}):
        label = _short(aid)
        n = pydot.Node(
            f'"{aid}"', label=f'"{label}"', shape='ellipse',
            style='filled', fillcolor='#E67E22', fontcolor='white',
            fontname='Helvetica', fontsize='11',
        )
        graph.add_node(n)
        node_map[aid] = n

    # Agents — diamonds
    for agid in doc.get('agent', {}):
        label = _short(agid)
        n = pydot.Node(
            f'"{agid}"', label=f'"{label}"', shape='diamond',
            style='filled', fillcolor='#95A5A6', fontcolor='white',
            fontname='Helvetica', fontsize='11',
        )
        graph.add_node(n)
        node_map[agid] = n

    def _add_edges(relation_key: str, tail_key: str, head_key: str, label: str,
                   color: str = '#555555'):
        for rel in doc.get(relation_key, {}).values():
            if not isinstance(rel, dict):
                continue
            tail = rel.get(tail_key)
            head = rel.get(head_key)
            if tail and head:
                graph.add_edge(pydot.Edge(
                    f'"{tail}"', f'"{head}"',
                    label=f'"{label}"', fontsize='9', fontcolor=color,
                    color=color, fontname='Helvetica',
                ))

    _add_edges('wasGeneratedBy',   'prov:entity',   'prov:activity', 'wasGeneratedBy', '#27AE60')
    _add_edges('used',             'prov:activity',  'prov:entity',  'used',            '#4A90D9')
    _add_edges('wasDerivedFrom',   'prov:generatedEntity', 'prov:usedEntity', 'wasDerivedFrom', '#8E44AD')
    _add_edges('wasAssociatedWith','prov:activity',  'prov:agent',   'wasAssociatedWith','#95A5A6')

    try:
        png = graph.create_png()
        return png
    except Exception:
        return None


# ── Sidebar ───────────────────────────────────────────────────────────────────

def _sidebar(run_ids: list[str]) -> tuple[str, str | None]:
    with st.sidebar:
        st.markdown('## 🔬 ProvenanceFlow')
        st.markdown(
            '*FAIR-compliant data lineage tracker*  \n'
            'W3C PROV · Dublin Core · Schema.org'
        )
        st.divider()
        page = st.radio(
            'Navigate',
            ['Overview', 'Run Detail', 'PROV Graph'],
            label_visibility='collapsed',
        )
        st.divider()
        selected_run = None
        if run_ids and page in ('Run Detail', 'PROV Graph'):
            selected_run = st.selectbox('Select run', run_ids)
        st.divider()
        st.caption('Standards')
        st.markdown(
            '- [W3C PROV-DM](https://www.w3.org/TR/prov-dm/)\n'
            '- [FAIR Principles](https://doi.org/10.1038/sdata.2016.18)\n'
            '- [Dublin Core](https://www.dublincore.org/specifications/dublin-core/dcmi-terms/)\n'
            '- [Schema.org](https://schema.org/Dataset)'
        )
    return page, selected_run


# ── Page: Overview ────────────────────────────────────────────────────────────

def _page_overview(runs: list[dict]):
    st.header('Pipeline Runs — Overview')

    if not runs:
        st.info('No runs yet. Run `python demo.py` first to populate the provenance store.')
        return

    # Metric cards
    total_runs = len(runs)
    latest_rate = runs[0].get('rejection_rate')
    total_rows_in = sum(r['rows_in'] for r in runs if isinstance(r.get('rows_in'), (int, float)))
    total_rows_passed = sum(r['rows_passed'] for r in runs if isinstance(r.get('rows_passed'), (int, float)))

    c1, c2, c3, c4 = st.columns(4)
    c1.metric('Total Runs', total_runs)
    c2.metric(
        'Latest Rejection Rate',
        f"{float(latest_rate)*100:.2f} %" if latest_rate is not None else '—',
    )
    c3.metric('Total Rows Processed', f'{total_rows_in:,}' if total_rows_in else '—')
    c4.metric('Total Rows Passed', f'{total_rows_passed:,}' if total_rows_passed else '—')

    st.divider()

    # Runs table
    df = pd.DataFrame(runs)
    if 'rejection_rate' in df.columns:
        df['rejection_rate'] = df['rejection_rate'].apply(
            lambda x: f'{float(x)*100:.2f} %' if x is not None else '—'
        )
    df.columns = [c.replace('_', ' ').title() for c in df.columns]
    st.dataframe(df, use_container_width=True, hide_index=True)


# ── Page: Run Detail ──────────────────────────────────────────────────────────

def _page_run_detail(store: ProvenanceStore, run_id: str):
    doc = get_run(store, run_id)
    if not doc:
        st.error(f'Run `{run_id}` not found.')
        return

    ing = _get_ingestion_entity(doc)
    val = _get_validation_activity(doc)
    out = _get_validated_entity(doc)

    st.header(f'Run Detail — `{run_id}`')

    # ── Source Dataset ─────────────────────────────────────────────────────
    with st.expander('📦 Source Dataset', expanded=True):
        col1, col2 = st.columns(2)
        with col1:
            st.markdown(f"**Title:** {ing.get('dc:title', '—')}")
            st.markdown(f"**Source URL:** {ing.get('fair:source_url', '—')}")
            st.markdown(f"**Format:** {ing.get('dc:format', '—')}")
            st.markdown(f"**License:** {ing.get('dc:license', '—')}")
        with col2:
            st.markdown(f"**FAIR Identifier:** `{ing.get('fair:identifier', '—')}`")
            st.markdown(f"**Ingested:** {ing.get('pf:ingest_timestamp', '—')}")
            rc = _unwrap(ing.get('pf:row_count', '—'))
            st.markdown(f"**Row Count:** {rc:,}" if isinstance(rc, int) else f"**Row Count:** {rc}")
            st.markdown(f"**SHA-256:** `{ing.get('pf:checksum_sha256', '—')[:24]}…`")

    # ── FAIR + DC / Schema.org Metadata ───────────────────────────────────
    with st.expander('🏷️ FAIR + Dublin Core / Schema.org Metadata'):
        col1, col2 = st.columns(2)
        with col1:
            st.markdown('**Dublin Core**')
            for key in ('dc:title', 'dc:identifier', 'dc:source', 'dc:format',
                        'dc:created', 'dc:type', 'dc:license'):
                if key in ing:
                    st.markdown(f'- `{key}`: {ing[key]}')
        with col2:
            st.markdown('**Schema.org**')
            for key in ('schema:name', 'schema:url', 'schema:encodingFormat'):
                if key in ing:
                    st.markdown(f'- `{key}`: {ing[key]}')

    # ── Validation Summary ─────────────────────────────────────────────────
    st.subheader('Validation Summary')

    rows_in  = _unwrap(val.get('pf:rows_in', 0)) or 0
    rows_out = _unwrap(val.get('pf:rows_passed', 0)) or 0
    rows_rej = _unwrap(val.get('pf:rows_rejected', 0)) or 0
    rate     = _unwrap(val.get('pf:rejection_rate', None))

    c1, c2, c3 = st.columns(3)
    c1.metric('Rows In',      f'{rows_in:,}' if isinstance(rows_in, int) else rows_in)
    c2.metric('Rows Passed',  f'{rows_out:,}' if isinstance(rows_out, int) else rows_out, delta=None)
    c3.metric('Rows Rejected', f'{rows_rej:,}' if isinstance(rows_rej, int) else rows_rej,
              delta=f'-{rows_rej}' if isinstance(rows_rej, int) and rows_rej > 0 else None,
              delta_color='inverse')

    if rate is not None:
        pct = float(rate) * 100
        msg = f'Rejection rate: **{pct:.2f} %**'
        if pct > 5:
            st.error(msg)
        else:
            st.success(msg)

    # Rules applied pills
    rules_raw = val.get('pf:rules_applied', '')
    if rules_raw:
        st.markdown('**Rules Applied:**')
        cols = st.columns(min(len(rules_raw.split(',')), 5))
        for i, rule in enumerate(rules_raw.split(',')):
            cols[i % len(cols)].markdown(
                f'<span style="background:#4A90D9;color:white;padding:3px 8px;'
                f'border-radius:12px;font-size:0.8em">{rule.strip()}</span>',
                unsafe_allow_html=True,
            )

    # Bar charts
    rejections = _parse_dict_attr(val.get('pf:rejections_by_rule'))
    warnings   = _parse_dict_attr(val.get('pf:warnings_by_rule'))

    col1, col2 = st.columns(2)
    with col1:
        st.markdown('**Hard Rejections by Rule**')
        if rejections:
            st.bar_chart(pd.Series(rejections))
        else:
            st.caption('None')
    with col2:
        st.markdown('**Warnings by Rule**')
        if warnings:
            st.bar_chart(pd.Series(warnings))
        else:
            st.caption('None')

    # Validated output
    if out:
        with st.expander('✅ Validated Output Entity'):
            for key in ('dc:title', 'fair:identifier', 'dc:isVersionOf'):
                if key in out:
                    st.markdown(f'- `{key}`: {out[key]}')
            vc = _unwrap(out.get('pf:row_count', '—'))
            st.markdown(f'- `pf:row_count`: {vc:,}' if isinstance(vc, int) else f'- `pf:row_count`: {vc}')

    # Raw PROV-JSON
    with st.expander('📄 Raw W3C PROV-JSON'):
        st.json(doc)


# ── Page: PROV Graph ──────────────────────────────────────────────────────────

def _page_prov_graph(store: ProvenanceStore, run_id: str):
    doc = get_run(store, run_id)
    if not doc:
        st.error(f'Run `{run_id}` not found.')
        return

    st.header(f'PROV Lineage Graph — `{run_id}`')

    png = _build_prov_graph(doc)
    if png:
        st.image(png, caption='W3C PROV entity-activity-agent graph', use_container_width=True)
        st.caption(
            '🟦 Raw dataset entity  ·  🟩 Validated dataset entity  ·  '
            '🟠 Activity  ·  ⬜ Agent'
        )
    else:
        st.warning(
            'Graph rendering unavailable. Ensure `graphviz` is installed on your system '
            '(`apt install graphviz` or `brew install graphviz`).'
        )
        st.markdown('Raw PROV relationships:')
        for rel in ('wasGeneratedBy', 'used', 'wasDerivedFrom', 'wasAssociatedWith'):
            if rel in doc:
                st.markdown(f'**{rel}**: {list(doc[rel].keys())}')


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    store = _get_store()
    runs = _enrich_runs(store)
    run_ids = [r['run_id'] for r in runs]

    page, selected_run = _sidebar(run_ids)

    if page == 'Overview':
        _page_overview(runs)
    elif page == 'Run Detail':
        if not run_ids:
            st.info('No runs yet. Run `python demo.py` first.')
        else:
            _page_run_detail(store, selected_run or run_ids[0])
    elif page == 'PROV Graph':
        if not run_ids:
            st.info('No runs yet. Run `python demo.py` first.')
        else:
            _page_prov_graph(store, selected_run or run_ids[0])


if __name__ == '__main__':
    main()
