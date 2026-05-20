"""
ProvenanceFlow Dashboard
Run: streamlit run dashboard.py
"""
import ast

import pandas as pd
import streamlit as st

from src.provenanceflow.provenance.store import ProvenanceStore
from src.provenanceflow.provenance.query import get_run, list_runs
from src.provenanceflow.provenance.compare import compare_runs

st.set_page_config(
    page_title='ProvenanceFlow',
    page_icon='🔬',
    layout='wide',
    initial_sidebar_state='expanded',
)

DB_PATH = 'provenance_store/lineage.db'

SEVERITY_LABELS = {
    'hard_rejection': 'Hard Rejection',
    'warning': 'Warning',
}

RULE_DESCRIPTIONS = {
    'null_check':           'Missing monthly temperature values',
    'range_check':          'Annual mean outside [-3.0, +3.0]°C',
    'completeness_check':   'More than 3 monthly values missing',
    'temporal_continuity':  'Gap in the year sequence',
    'baseline_integrity':   'Incomplete 1951–1980 anomaly baseline',
    'row_null_rate':        'Too many null values in this row',
    'column_completeness':  'Column has high null rate',
}


# ── PROV-JSON navigation helpers ───────────────────────────────────────────────

def _unwrap(v):
    return v['$'] if isinstance(v, dict) and '$' in v else v


def _parse_dict_attr(v) -> dict:
    try:
        return ast.literal_eval(str(v)) if v else {}
    except Exception:
        return {}


def _get_ingestion_entity(doc: dict) -> dict:
    for eid, attrs in doc.get('entity', {}).items():
        if 'dataset_' in eid:
            return attrs
    return {}


def _get_processing_activity(doc: dict) -> tuple[dict, str]:
    """Return (attrs, kind) — kind is 'validation' or 'transformation'."""
    for aid, attrs in doc.get('activity', {}).items():
        if 'validate_' in aid:
            return attrs, 'validation'
    for aid, attrs in doc.get('activity', {}).items():
        if 'transform_' in aid:
            return attrs, 'transformation'
    return {}, 'unknown'


def _get_output_entity(doc: dict) -> dict:
    for eid, attrs in doc.get('entity', {}).items():
        if 'validated_' in eid:
            return attrs
    for eid, attrs in doc.get('entity', {}).items():
        if 'transformed_' in eid:
            return attrs
    return {}


def _short_id(name: str) -> str:
    for prefix in ('pf:', 'prov:', 'fair:', 'dc:', 'schema:'):
        name = name.replace(prefix, '')
    return name


# ── Store + run enrichment ─────────────────────────────────────────────────────

@st.cache_resource
def _get_store() -> ProvenanceStore:
    return ProvenanceStore(db_path=DB_PATH)


def _enrich_runs(store: ProvenanceStore) -> list[dict]:
    enriched = []
    for run_meta in store.list_runs():
        doc = store.get(run_meta['run_id'])
        if not doc:
            continue
        ing = _get_ingestion_entity(doc)
        val, kind = _get_processing_activity(doc)

        row = {
            'run_id':     run_meta['run_id'],
            'created_at': run_meta['created_at'],
            'type':       '🔬 Pipeline' if kind == 'validation' else '⚡ @track',
            'source':     (ing.get('dc:title') or ing.get('fair:source_url') or '—'),
        }

        if kind == 'validation':
            row['rows_in']     = _unwrap(val.get('pf:rows_in', '—'))
            row['rows_passed'] = _unwrap(val.get('pf:rows_passed', '—'))
            rate               = _unwrap(val.get('pf:rejection_rate', None))
            row['rejection_%'] = f'{float(rate)*100:.2f}' if rate is not None else '—'
        else:
            row['rows_in']     = _unwrap(val.get('pf:rows_in', '—'))
            row['rows_passed'] = _unwrap(val.get('pf:rows_out', '—'))
            row['rejection_%'] = '—'
            fn = val.get('pf:function_name')
            if fn:
                row['source'] = fn

        enriched.append(row)
    return enriched


# ── Lineage graph builder ──────────────────────────────────────────────────────

def _build_lineage_graph(doc: dict):
    try:
        import pydot
    except ImportError:
        return None

    graph = pydot.Dot('provenance', graph_type='digraph', rankdir='LR',
                      fontname='Helvetica', bgcolor='transparent')
    graph.set_graph_defaults(pad='0.5', nodesep='0.6', ranksep='0.9')

    for eid in doc.get('entity', {}):
        color = '#4F8BF9' if 'dataset_' in eid else '#27AE60'
        graph.add_node(pydot.Node(
            f'"{eid}"', label=f'"{_short_id(eid)}"', shape='box',
            style='filled,rounded', fillcolor=color,
            fontcolor='white', fontname='Helvetica', fontsize='11',
        ))

    for aid in doc.get('activity', {}):
        color = '#E67E22' if 'validate_' in aid else '#9B59B6'
        graph.add_node(pydot.Node(
            f'"{aid}"', label=f'"{_short_id(aid)}"', shape='ellipse',
            style='filled', fillcolor=color,
            fontcolor='white', fontname='Helvetica', fontsize='11',
        ))

    for agid in doc.get('agent', {}):
        graph.add_node(pydot.Node(
            f'"{agid}"', label=f'"{_short_id(agid)}"', shape='diamond',
            style='filled', fillcolor='#7F8C8D',
            fontcolor='white', fontname='Helvetica', fontsize='11',
        ))

    def _add_edges(rel_key, tail_key, head_key, label, color='#555555'):
        for rel in doc.get(rel_key, {}).values():
            if not isinstance(rel, dict):
                continue
            tail, head = rel.get(tail_key), rel.get(head_key)
            if tail and head:
                graph.add_edge(pydot.Edge(
                    f'"{tail}"', f'"{head}"',
                    label=f'"{label}"', fontsize='9',
                    fontcolor=color, color=color, fontname='Helvetica',
                ))

    _add_edges('wasGeneratedBy',    'prov:activity',        'prov:entity',     'wasGeneratedBy',    '#27AE60')
    _add_edges('used',              'prov:activity',        'prov:entity',     'used',              '#4F8BF9')
    _add_edges('wasDerivedFrom',    'prov:generatedEntity', 'prov:usedEntity', 'wasDerivedFrom',    '#8E44AD')
    _add_edges('wasAssociatedWith', 'prov:activity',        'prov:agent',      'wasAssociatedWith', '#7F8C8D')

    try:
        return graph.create_png()
    except Exception:
        return None


def _lineage_fallback_text(doc: dict) -> str:
    """Human-readable lineage summary when graphviz is unavailable."""
    lines = []
    entities   = {eid: attrs for eid, attrs in doc.get('entity', {}).items()}
    activities = {aid: attrs for aid, attrs in doc.get('activity', {}).items()}

    def label(key):
        attrs = entities.get(key) or activities.get(key) or {}
        return attrs.get('prov:label') or _short_id(key)

    for rel in doc.get('wasGeneratedBy', {}).values():
        if isinstance(rel, dict):
            e, a = rel.get('prov:entity'), rel.get('prov:activity')
            if e and a:
                lines.append(f'**{label(e)}** was generated by **{label(a)}**')

    for rel in doc.get('used', {}).values():
        if isinstance(rel, dict):
            a, e = rel.get('prov:activity'), rel.get('prov:entity')
            if a and e:
                lines.append(f'**{label(a)}** used **{label(e)}** as input')

    for rel in doc.get('wasDerivedFrom', {}).values():
        if isinstance(rel, dict):
            out = rel.get('prov:generatedEntity')
            inp = rel.get('prov:usedEntity')
            if out and inp:
                lines.append(f'**{label(out)}** was derived from **{label(inp)}**')

    return '  \n'.join(lines) if lines else 'No lineage relationships found in this record.'


# ── Sidebar ────────────────────────────────────────────────────────────────────

def _sidebar(run_ids: list[str]) -> tuple[str, str | None, str | None]:
    with st.sidebar:
        st.markdown('## 🔬 ProvenanceFlow')
        st.markdown(
            'Shows you **exactly what happened to your data** in each pipeline run — '
            'what was filtered, which rule triggered it, and how results changed over time.'
        )
        st.divider()

        page = st.radio(
            'Navigate',
            ['Overview', 'Run Detail', 'Lineage Graph', 'Compare Runs'],
            label_visibility='collapsed',
            captions=[
                'All runs at a glance',
                'What happened in one run',
                'Visual flow from input to output',
                'How two runs differ',
            ],
        )
        st.divider()

        selected_run  = None
        compare_run_b = None

        if run_ids:
            if page in ('Run Detail', 'Lineage Graph'):
                selected_run = st.selectbox('Select run', run_ids)
            elif page == 'Compare Runs':
                st.caption('**Baseline run**')
                selected_run  = st.selectbox('Run A', run_ids, key='run_a')
                st.caption('**Run to compare**')
                compare_run_b = st.selectbox('Run B', run_ids, key='run_b',
                                             index=min(1, len(run_ids) - 1))

        st.divider()
        st.caption('Standards')
        st.markdown(
            '- [W3C PROV](https://www.w3.org/TR/prov-dm/)\n'
            '- [FAIR Principles](https://doi.org/10.1038/sdata.2016.18)\n'
            '- [RO-Crate](https://www.researchobject.org/ro-crate/)\n'
            '- [Dublin Core](https://www.dublincore.org/specifications/dublin-core/dcmi-terms/)'
        )

    return page, selected_run, compare_run_b


# ── Page: Overview ────────────────────────────────────────────────────────────

def _page_overview(runs: list[dict]):
    st.header('Pipeline Runs')
    st.caption('Each row is one execution of your data pipeline. Select a run in the sidebar to inspect it.')

    if not runs:
        st.info(
            'No runs recorded yet.  \n'
            'Run `provenanceflow run` in your terminal, or `python demo.py` to create your first provenance record.'
        )
        return

    pipeline_runs = [r for r in runs if '🔬' in r['type']]
    track_runs    = [r for r in runs if '⚡' in r['type']]
    valid_rates   = [float(r['rejection_%']) for r in runs if r.get('rejection_%') not in ('—', None)]
    latest_rate   = valid_rates[0] if valid_rates else None

    c1, c2, c3, c4 = st.columns(4)
    c1.metric('Total Runs',     len(runs))
    c2.metric('Pipeline Runs',  len(pipeline_runs),
              help='Full ingest → validate → track pipeline runs')
    c3.metric('@track Runs',    len(track_runs),
              help='Runs captured by the @track decorator on individual functions')
    c4.metric('Latest Rejection %',
              f'{latest_rate:.2f} %' if latest_rate is not None else '—',
              help='Percentage of input rows rejected in the most recent pipeline run')

    st.divider()
    df = pd.DataFrame(runs)
    df.columns = [c.replace('_', ' ').title() for c in df.columns]
    st.dataframe(df, use_container_width=True, hide_index=True)


# ── Page: Run Detail ──────────────────────────────────────────────────────────

def _page_run_detail(store: ProvenanceStore, run_id: str):
    doc = get_run(store, run_id)
    if not doc:
        st.error(f'Run `{run_id}` not found. Select a different run in the sidebar.')
        return

    ing = _get_ingestion_entity(doc)
    val, kind = _get_processing_activity(doc)
    out = _get_output_entity(doc)

    badge = '🔬 Pipeline Run' if kind == 'validation' else '⚡ @track Decorator Run'
    st.header(badge)
    st.caption(f'`{run_id}`')

    # ── Source ─────────────────────────────────────────────────────────────
    with st.expander('📦 Input Dataset', expanded=True):
        col1, col2 = st.columns(2)
        with col1:
            st.markdown(f"**Name:** {ing.get('dc:title', '—')}")
            st.markdown(f"**Source:** {ing.get('fair:source_url', '—')}")
            st.markdown(f"**Format:** {ing.get('dc:format', '—')}")
        with col2:
            st.markdown(f"**Run identifier:** `{ing.get('fair:identifier', '—')}`")
            st.markdown(f"**Recorded at:** {ing.get('pf:ingest_timestamp', '—')}")
            rc = _unwrap(ing.get('pf:row_count', '—'))
            st.markdown(f"**Rows:** {rc:,}" if isinstance(rc, int) else f"**Rows:** {rc}")
            chk = ing.get('pf:checksum_sha256', '')
            if chk:
                st.markdown(f"**Fingerprint (SHA-256):** `{chk[:24]}…`",
                            help='A unique fingerprint of the input file — if this matches another run, both used identical input data')

    # ── Processing ─────────────────────────────────────────────────────────
    if kind == 'validation':
        st.subheader('Validation Results')

        rows_in  = _unwrap(val.get('pf:rows_in',       0)) or 0
        rows_out = _unwrap(val.get('pf:rows_passed',    0)) or 0
        rows_rej = _unwrap(val.get('pf:rows_rejected',  0)) or 0
        rate     = _unwrap(val.get('pf:rejection_rate', None))

        c1, c2, c3 = st.columns(3)
        c1.metric('Rows In',       f'{rows_in:,}'  if isinstance(rows_in,  int) else rows_in)
        c2.metric('Rows Passed',   f'{rows_out:,}' if isinstance(rows_out, int) else rows_out)
        c3.metric('Rows Rejected', f'{rows_rej:,}' if isinstance(rows_rej, int) else rows_rej,
                  delta=f'-{rows_rej}' if isinstance(rows_rej, int) and rows_rej > 0 else None,
                  delta_color='inverse',
                  help='Rows removed because they failed a Hard Rejection rule')

        if rate is not None:
            pct = float(rate) * 100
            msg = f'**{pct:.2f}%** of input rows were rejected'
            st.error(msg) if pct > 5 else (st.warning(msg) if pct > 0 else st.success('All rows passed — 0% rejection rate'))

        # Rules applied
        rules_raw = val.get('pf:rules_applied', '')
        rules = [r.strip() for r in rules_raw.split(',') if r.strip()]
        if rules:
            st.markdown('**Rules checked:**')
            for r in rules:
                desc = RULE_DESCRIPTIONS.get(r, '')
                st.markdown(f'- `{r}`{"  — " + desc if desc else ""}')

        # Rejection charts
        rejections = _parse_dict_attr(val.get('pf:rejections_by_rule'))
        warnings   = _parse_dict_attr(val.get('pf:warnings_by_rule'))

        if rejections or warnings:
            col1, col2 = st.columns(2)
            with col1:
                st.markdown('**Hard Rejections by Rule** *(row removed)*')
                st.bar_chart(pd.Series(rejections)) if rejections else st.caption('None')
            with col2:
                st.markdown('**Warnings by Rule** *(row kept, flagged)*')
                st.bar_chart(pd.Series(warnings)) if warnings else st.caption('None')

        # Rejected rows table
        rejected_rows = store.get_rejections(run_id)
        if rejected_rows:
            with st.expander(f'🔍 Why were rows rejected? ({len(rejected_rows)} rows)'):
                rdf = pd.DataFrame(rejected_rows)[['row_index', 'rule', 'severity', 'message']]
                rdf['severity'] = rdf['severity'].map(SEVERITY_LABELS).fillna(rdf['severity'])
                rdf.columns = ['Row #', 'Rule', 'Severity', 'Rejection Reason']
                st.dataframe(rdf, use_container_width=True, hide_index=True)

    else:
        # @track decorator run
        st.subheader('Transformation Result')
        fn       = val.get('pf:function_name', '—')
        rows_in  = _unwrap(val.get('pf:rows_in',  '—'))
        rows_out = _unwrap(val.get('pf:rows_out', '—'))
        chk_in   = val.get('pf:checksum_in',  '')
        chk_out  = val.get('pf:checksum_out', '')

        st.markdown(f'**Function:** `{fn}`')
        c1, c2, c3 = st.columns(3)
        c1.metric('Rows In',    f'{rows_in:,}'  if isinstance(rows_in,  int) else rows_in)
        c2.metric('Rows Out',   f'{rows_out:,}' if isinstance(rows_out, int) else rows_out)
        if isinstance(rows_in, int) and isinstance(rows_out, int):
            dropped = rows_in - rows_out
            c3.metric('Rows Dropped', dropped,
                      delta=f'-{dropped}' if dropped > 0 else None,
                      delta_color='inverse')

        if chk_in:
            same = (chk_in == chk_out)
            st.markdown(f'**Input fingerprint:** `{str(chk_in)[:24]}…`')
            if chk_out:
                st.markdown(f'**Output fingerprint:** `{str(chk_out)[:24]}…`')
                if same:
                    st.warning('Input and output fingerprints match — function returned the data unchanged.')

    # Output entity
    if out:
        with st.expander('✅ Output Dataset'):
            vc = _unwrap(out.get('pf:row_count', '—'))
            st.markdown(f'**Rows:** {vc:,}' if isinstance(vc, int) else f'**Rows:** {vc}')
            st.markdown(f"**Identifier:** `{out.get('fair:identifier', '—')}`")

    with st.expander('📄 Raw Provenance Record (W3C PROV-JSON)'):
        st.json(doc)


# ── Page: Lineage Graph ───────────────────────────────────────────────────────

def _page_lineage_graph(store: ProvenanceStore, run_id: str):
    doc = get_run(store, run_id)
    if not doc:
        st.error(f'Run `{run_id}` not found. Select a different run in the sidebar.')
        return

    st.header('Lineage Graph')
    st.caption(f'Visual W3C PROV entity-activity-agent graph for `{run_id}`')

    png = _build_lineage_graph(doc)
    if png:
        st.image(png, use_container_width=True)
        st.caption(
            '🟦 Raw input dataset  ·  🟩 Validated/transformed output  ·  '
            '🟠 Validation activity  ·  🟣 Transform activity  ·  🔘 Agent (ProvenanceFlow)'
        )
    else:
        st.warning('Graph image unavailable — graphviz binary not found on this system.')
        st.markdown('**Lineage summary for this run:**')
        st.markdown(_lineage_fallback_text(doc))


# ── Page: Compare Runs ────────────────────────────────────────────────────────

def _page_compare(store: ProvenanceStore, run_id_a: str, run_id_b: str):
    st.header('Compare Runs')

    if run_id_a == run_id_b:
        st.warning('Select two different runs to compare.')
        return

    try:
        diff = compare_runs(run_id_a, run_id_b, store)
    except ValueError as e:
        st.error(str(e))
        return

    # Banner
    delta = diff.delta_rejection_rate
    if delta < 0:
        st.success(f'✅ {diff.summary}')
    elif delta > 0:
        st.error(f'❌ {diff.summary}')
    else:
        st.info(f'➡️ {diff.summary}')

    st.divider()
    col1, col2 = st.columns(2)

    with col1:
        st.markdown(f'**Run A** (baseline)  \n`{run_id_a}`')
        st.metric('Rows Passed',    diff.rows_passed_a)
        st.metric('Rejection Rate', f'{diff.rejection_rate_a*100:.2f} %')
        if diff.source_url_a != '—':
            st.caption(f'Source: {diff.source_url_a}')

    with col2:
        st.markdown(f'**Run B** (comparison)  \n`{run_id_b}`')
        # delta_color='normal': more rows passed = green up = good
        st.metric('Rows Passed',    diff.rows_passed_b,
                  delta=diff.delta_rows_passed,
                  delta_color='normal')
        # delta_color='inverse': higher rejection rate = red even though positive = bad
        st.metric('Rejection Rate', f'{diff.rejection_rate_b*100:.2f} %',
                  delta=f'{diff.delta_rejection_rate*100:+.2f} %',
                  delta_color='inverse')
        if diff.source_url_b != '—':
            st.caption(f'Source: {diff.source_url_b}')

    st.divider()

    # Dataset + rules status
    col1, col2 = st.columns(2)
    with col1:
        if diff.same_dataset:
            st.success('✅ Same input dataset (identical SHA-256 fingerprint)',
                       icon=None)
            st.caption('Any difference in output is due to rule changes, not different input data.')
        else:
            st.warning('⚠️ Different input datasets')
            st.caption('Output differences may reflect different input data, not just rule changes.')
    with col2:
        if diff.same_rules:
            st.success('✅ Same validation rules applied')
        else:
            st.warning('⚠️ Different rules applied between runs')

    # Per-rule rejection breakdown
    st.divider()
    st.subheader('Rejection Breakdown by Rule')

    doc_a = store.get(run_id_a)
    doc_b = store.get(run_id_b)
    val_a, _ = _get_processing_activity(doc_a or {})
    val_b, _ = _get_processing_activity(doc_b or {})
    rej_a = _parse_dict_attr(val_a.get('pf:rejections_by_rule'))
    rej_b = _parse_dict_attr(val_b.get('pf:rejections_by_rule'))
    warn_a = _parse_dict_attr(val_a.get('pf:warnings_by_rule'))
    warn_b = _parse_dict_attr(val_b.get('pf:warnings_by_rule'))

    all_rules = sorted(set(rej_a) | set(rej_b) | set(warn_a) | set(warn_b))

    if all_rules:
        rows = []
        for r in all_rules:
            ra, rb = rej_a.get(r, 0), rej_b.get(r, 0)
            wa, wb = warn_a.get(r, 0), warn_b.get(r, 0)
            rows.append({
                'Rule':               r,
                'Rejections A':       ra,
                'Rejections B':       rb,
                'Rejection Δ':        rb - ra,
                'Warnings A':         wa,
                'Warnings B':         wb,
            })
        df = pd.DataFrame(rows)
        st.dataframe(df, use_container_width=True, hide_index=True)
    else:
        st.caption('No rejections or warnings in either run.')


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    store   = _get_store()
    runs    = _enrich_runs(store)
    run_ids = [r['run_id'] for r in runs]

    page, selected_run, compare_run_b = _sidebar(run_ids)

    if page == 'Overview':
        _page_overview(runs)

    elif page == 'Run Detail':
        if not run_ids:
            st.info('No runs yet. Run `provenanceflow run` or `python demo.py` first.')
        else:
            _page_run_detail(store, selected_run or run_ids[0])

    elif page == 'Lineage Graph':
        if not run_ids:
            st.info('No runs yet. Run `provenanceflow run` or `python demo.py` first.')
        else:
            _page_lineage_graph(store, selected_run or run_ids[0])

    elif page == 'Compare Runs':
        if len(run_ids) < 2:
            st.info('Need at least 2 runs to compare. Run the pipeline more than once.')
        else:
            _page_compare(store, selected_run or run_ids[0], compare_run_b or run_ids[1])


if __name__ == '__main__':
    main()
