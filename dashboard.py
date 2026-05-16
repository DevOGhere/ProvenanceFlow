"""
ProvenanceFlow Dashboard
Run: streamlit run dashboard.py
"""
import ast
import json

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
    """Return (attrs, kind) for the main processing activity.

    kind is 'validation' (full pipeline) or 'transformation' (@track decorator).
    """
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
            'source':     (ing.get('fair:source_url') or ing.get('dc:title') or '—'),
        }

        if kind == 'validation':
            row['rows_in']       = _unwrap(val.get('pf:rows_in', '—'))
            row['rows_passed']   = _unwrap(val.get('pf:rows_passed', '—'))
            rate                 = _unwrap(val.get('pf:rejection_rate', None))
            row['rejection_%']   = f'{float(rate)*100:.2f}' if rate is not None else '—'
        else:
            rows_in  = _unwrap(val.get('pf:rows_in', '—'))
            rows_out = _unwrap(val.get('pf:rows_out', '—'))
            fn       = val.get('pf:function_name', '—')
            row['rows_in']     = rows_in
            row['rows_passed'] = rows_out
            row['rejection_%'] = '—'
            row['source']      = fn

        enriched.append(row)
    return enriched


# ── PROV graph ────────────────────────────────────────────────────────────────

def _build_prov_graph(doc: dict):
    try:
        import pydot
    except ImportError:
        return None

    graph = pydot.Dot('provenance', graph_type='digraph', rankdir='LR',
                      fontname='Helvetica', bgcolor='transparent')
    graph.set_graph_defaults(pad='0.5', nodesep='0.6', ranksep='0.9')

    def _short(name: str) -> str:
        for prefix in ('pf:', 'prov:', 'fair:', 'dc:', 'schema:'):
            name = name.replace(prefix, '')
        return name

    node_map: dict[str, object] = {}

    for eid in doc.get('entity', {}):
        color = '#4F8BF9' if 'dataset_' in eid else '#27AE60'
        n = pydot.Node(f'"{eid}"', label=f'"{_short(eid)}"', shape='box',
                       style='filled,rounded', fillcolor=color,
                       fontcolor='white', fontname='Helvetica', fontsize='11')
        graph.add_node(n)
        node_map[eid] = n

    for aid in doc.get('activity', {}):
        color = '#E67E22' if 'validate_' in aid else '#9B59B6'
        n = pydot.Node(f'"{aid}"', label=f'"{_short(aid)}"', shape='ellipse',
                       style='filled', fillcolor=color,
                       fontcolor='white', fontname='Helvetica', fontsize='11')
        graph.add_node(n)
        node_map[aid] = n

    for agid in doc.get('agent', {}):
        n = pydot.Node(f'"{agid}"', label=f'"{_short(agid)}"', shape='diamond',
                       style='filled', fillcolor='#7F8C8D',
                       fontcolor='white', fontname='Helvetica', fontsize='11')
        graph.add_node(n)
        node_map[agid] = n

    def _add_edges(rel_key, tail_key, head_key, label, color='#555555'):
        for rel in doc.get(rel_key, {}).values():
            if not isinstance(rel, dict):
                continue
            tail, head = rel.get(tail_key), rel.get(head_key)
            if tail and head:
                graph.add_edge(pydot.Edge(
                    f'"{tail}"', f'"{head}"',
                    label=f'"{label}"', fontsize='9', fontcolor=color,
                    color=color, fontname='Helvetica',
                ))

    _add_edges('wasGeneratedBy',    'prov:activity',        'prov:entity',     'wasGeneratedBy',    '#27AE60')
    _add_edges('used',              'prov:activity',        'prov:entity',     'used',              '#4F8BF9')
    _add_edges('wasDerivedFrom',    'prov:generatedEntity', 'prov:usedEntity', 'wasDerivedFrom',    '#8E44AD')
    _add_edges('wasAssociatedWith', 'prov:activity',        'prov:agent',      'wasAssociatedWith', '#7F8C8D')

    try:
        return graph.create_png()
    except Exception:
        return None


# ── Sidebar ────────────────────────────────────────────────────────────────────

def _sidebar(run_ids: list[str]) -> tuple[str, str | None, str | None]:
    with st.sidebar:
        st.markdown('## 🔬 ProvenanceFlow')
        st.markdown(
            '*W3C PROV-native lineage for pandas pipelines.*  \n'
            'Row-level rejection rationale, FAIR-aligned metadata.'
        )
        st.divider()

        page = st.radio('Navigate', ['Overview', 'Run Detail', 'PROV Graph', 'Compare Runs'],
                        label_visibility='collapsed')
        st.divider()

        selected_run = None
        compare_run_b = None

        if run_ids:
            if page in ('Run Detail', 'PROV Graph'):
                selected_run = st.selectbox('Select run', run_ids)
            elif page == 'Compare Runs':
                st.markdown('**Run A (baseline)**')
                selected_run = st.selectbox('Run A', run_ids, key='run_a')
                st.markdown('**Run B (compare)**')
                compare_run_b = st.selectbox('Run B', run_ids, key='run_b',
                                             index=min(1, len(run_ids) - 1))

        st.divider()
        st.caption('Standards')
        st.markdown(
            '- [W3C PROV-DM](https://www.w3.org/TR/prov-dm/)\n'
            '- [FAIR Principles](https://doi.org/10.1038/sdata.2016.18)\n'
            '- [RO-Crate](https://www.researchobject.org/ro-crate/)\n'
            '- [Dublin Core](https://www.dublincore.org/specifications/dublin-core/dcmi-terms/)'
        )

    return page, selected_run, compare_run_b


# ── Page: Overview ────────────────────────────────────────────────────────────

def _page_overview(runs: list[dict]):
    st.header('Pipeline Runs — Overview')

    if not runs:
        st.info(
            'No runs yet.  \n'
            'Run `provenanceflow run` (CLI) or `python demo.py` to create your first provenance record.'
        )
        return

    pipeline_runs = [r for r in runs if '🔬' in r['type']]
    track_runs    = [r for r in runs if '⚡' in r['type']]

    c1, c2, c3, c4 = st.columns(4)
    c1.metric('Total Runs', len(runs))
    c2.metric('Pipeline Runs', len(pipeline_runs))
    c3.metric('@track Runs', len(track_runs))

    latest_rate = next(
        (r['rejection_%'] for r in runs if r.get('rejection_%') != '—'), None
    )
    c4.metric('Latest Rejection %', f'{latest_rate} %' if latest_rate else '—')

    st.divider()

    df = pd.DataFrame(runs)
    df.columns = [c.replace('_', ' ').title() for c in df.columns]
    st.dataframe(df, use_container_width=True, hide_index=True)


# ── Page: Run Detail ──────────────────────────────────────────────────────────

def _page_run_detail(store: ProvenanceStore, run_id: str):
    doc = get_run(store, run_id)
    if not doc:
        st.error(f'Run `{run_id}` not found.')
        return

    ing = _get_ingestion_entity(doc)
    val, kind = _get_processing_activity(doc)
    out = _get_output_entity(doc)

    badge = '🔬 Pipeline Run' if kind == 'validation' else '⚡ @track Decorator Run'
    st.header(f'{badge}')
    st.caption(f'`{run_id}`')

    # ── Source ─────────────────────────────────────────────────────────────
    with st.expander('📦 Source Dataset', expanded=True):
        col1, col2 = st.columns(2)
        with col1:
            st.markdown(f"**Title:** {ing.get('dc:title', '—')}")
            st.markdown(f"**Source:** {ing.get('fair:source_url', '—')}")
            st.markdown(f"**Format:** {ing.get('dc:format', '—')}")
        with col2:
            st.markdown(f"**FAIR Identifier:** `{ing.get('fair:identifier', '—')}`")
            st.markdown(f"**Ingested:** {ing.get('pf:ingest_timestamp', '—')}")
            rc = _unwrap(ing.get('pf:row_count', '—'))
            st.markdown(f"**Row Count:** {rc:,}" if isinstance(rc, int) else f"**Row Count:** {rc}")
            chk = ing.get('pf:checksum_sha256', '')
            if chk:
                st.markdown(f"**SHA-256:** `{chk[:24]}…`")

    # ── Processing ─────────────────────────────────────────────────────────
    if kind == 'validation':
        st.subheader('Validation Summary')
        rows_in  = _unwrap(val.get('pf:rows_in', 0)) or 0
        rows_out = _unwrap(val.get('pf:rows_passed', 0)) or 0
        rows_rej = _unwrap(val.get('pf:rows_rejected', 0)) or 0
        rate     = _unwrap(val.get('pf:rejection_rate', None))

        c1, c2, c3 = st.columns(3)
        c1.metric('Rows In',       f'{rows_in:,}' if isinstance(rows_in, int) else rows_in)
        c2.metric('Rows Passed',   f'{rows_out:,}' if isinstance(rows_out, int) else rows_out)
        c3.metric('Rows Rejected', f'{rows_rej:,}' if isinstance(rows_rej, int) else rows_rej,
                  delta=f'-{rows_rej}' if isinstance(rows_rej, int) and rows_rej > 0 else None,
                  delta_color='inverse')

        if rate is not None:
            pct = float(rate) * 100
            msg = f'Rejection rate: **{pct:.2f} %**'
            st.error(msg) if pct > 5 else st.success(msg)

        rules_raw = val.get('pf:rules_applied', '')
        rules = [r.strip() for r in rules_raw.split(',') if r.strip()]
        if rules:
            st.markdown('**Rules Applied:**  ' + '  '.join(
                f'`{r}`' for r in rules
            ))

        rejections = _parse_dict_attr(val.get('pf:rejections_by_rule'))
        warnings   = _parse_dict_attr(val.get('pf:warnings_by_rule'))
        col1, col2 = st.columns(2)
        with col1:
            st.markdown('**Hard Rejections by Rule**')
            st.bar_chart(pd.Series(rejections)) if rejections else st.caption('None')
        with col2:
            st.markdown('**Warnings by Rule**')
            st.bar_chart(pd.Series(warnings)) if warnings else st.caption('None')

        # Rejected rows detail
        rejected_rows = store.get_rejections(run_id)
        if rejected_rows:
            with st.expander(f'🔍 Rejected Rows ({len(rejected_rows)} total)'):
                rdf = pd.DataFrame(rejected_rows)[['row_index', 'rule', 'severity', 'message']]
                st.dataframe(rdf, use_container_width=True, hide_index=True)

    else:
        st.subheader('Transformation Summary')
        fn       = val.get('pf:function_name', '—')
        rows_in  = _unwrap(val.get('pf:rows_in', '—'))
        rows_out = _unwrap(val.get('pf:rows_out', '—'))
        chk_in   = val.get('pf:checksum_in', '—')
        chk_out  = val.get('pf:checksum_out', '—')

        st.markdown(f'**Function:** `{fn}`')
        c1, c2 = st.columns(2)
        c1.metric('Rows In',  f'{rows_in:,}'  if isinstance(rows_in, int)  else rows_in)
        c2.metric('Rows Out', f'{rows_out:,}' if isinstance(rows_out, int) else rows_out)
        if chk_in and chk_in != '—':
            st.markdown(f'**Input SHA-256:** `{str(chk_in)[:24]}…`')
        if chk_out and chk_out != '—':
            st.markdown(f'**Output SHA-256:** `{str(chk_out)[:24]}…`')
        same = (chk_in == chk_out and chk_in not in ('—', '', None))
        if same:
            st.warning('Input and output checksums match — function may have returned unchanged data.')

    # Output entity
    if out:
        with st.expander('✅ Output Entity'):
            vc = _unwrap(out.get('pf:row_count', '—'))
            st.markdown(f'**Row Count:** {vc:,}' if isinstance(vc, int) else f'**Row Count:** {vc}')
            st.markdown(f"**FAIR Identifier:** `{out.get('fair:identifier', '—')}`")

    # Raw PROV-JSON
    with st.expander('📄 Raw W3C PROV-JSON'):
        st.json(doc)


# ── Page: PROV Graph ──────────────────────────────────────────────────────────

def _page_prov_graph(store: ProvenanceStore, run_id: str):
    doc = get_run(store, run_id)
    if not doc:
        st.error(f'Run `{run_id}` not found.')
        return

    st.header(f'PROV Lineage Graph')
    st.caption(f'`{run_id}`')

    png = _build_prov_graph(doc)
    if png:
        st.image(png, use_container_width=True)
        st.caption('🟦 Raw dataset  ·  🟩 Validated output  ·  🟠 Validation activity  ·  🟣 Transform activity  ·  ⬜ Agent')
    else:
        st.warning('Graph rendering unavailable — ensure `graphviz` is installed.')
        st.markdown('**PROV relationships in this run:**')
        for rel in ('wasGeneratedBy', 'used', 'wasDerivedFrom', 'wasAssociatedWith'):
            if rel in doc:
                st.markdown(f'**{rel}**: {list(doc[rel].keys())}')


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

    # Summary banner
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
        st.metric('Rows Passed',   diff.rows_passed_a)
        st.metric('Rejection Rate', f'{diff.rejection_rate_a*100:.2f} %')
        st.caption(f'Source: {diff.source_url_a}')

    with col2:
        st.markdown(f'**Run B** (comparison)  \n`{run_id_b}`')
        st.metric('Rows Passed', diff.rows_passed_b,
                  delta=diff.delta_rows_passed,
                  delta_color='normal' if diff.delta_rows_passed >= 0 else 'inverse')
        st.metric('Rejection Rate', f'{diff.rejection_rate_b*100:.2f} %',
                  delta=f'{diff.delta_rejection_rate*100:+.2f} %',
                  delta_color='inverse')
        st.caption(f'Source: {diff.source_url_b}')

    st.divider()
    same_ds  = '✅ Same input dataset (SHA-256 match)' if diff.same_dataset  else '⚠️ Different input datasets'
    same_rules = '✅ Same rules applied'               if diff.same_rules    else '⚠️ Different rules applied'
    st.markdown(f'{same_ds}  \n{same_rules}')


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    store = _get_store()
    runs  = _enrich_runs(store)
    run_ids = [r['run_id'] for r in runs]

    page, selected_run, compare_run_b = _sidebar(run_ids)

    if page == 'Overview':
        _page_overview(runs)

    elif page == 'Run Detail':
        if not run_ids:
            st.info('No runs yet. Run `provenanceflow run` or `python demo.py` first.')
        else:
            _page_run_detail(store, selected_run or run_ids[0])

    elif page == 'PROV Graph':
        if not run_ids:
            st.info('No runs yet. Run `provenanceflow run` or `python demo.py` first.')
        else:
            _page_prov_graph(store, selected_run or run_ids[0])

    elif page == 'Compare Runs':
        if len(run_ids) < 2:
            st.info('Need at least 2 runs to compare. Run the pipeline more than once.')
        else:
            _page_compare(store, selected_run or run_ids[0], compare_run_b or run_ids[1])


if __name__ == '__main__':
    main()
