"""
ProvenanceFlow CLI

After `pip install -e .` (or `pip install provenanceflow`):

    provenanceflow run                         # NASA GISTEMP (default)
    provenanceflow run --file data/my.csv      # local GISTEMP-format CSV
    provenanceflow run --url URL --dir DIR     # custom URL + output directory
    provenanceflow runs list                   # all stored runs
    provenanceflow runs show <run_id>          # detail for one run
    provenanceflow dashboard                   # launch Streamlit UI
    provenanceflow serve                       # launch FastAPI REST API
"""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import click

from .config import get_settings, configure_logging
from .provenance.store import ProvenanceStore


@click.group()
def main():
    """ProvenanceFlow — FAIR-compliant data lineage tracking."""


# ── run ───────────────────────────────────────────────────────────────────────

@main.command()
@click.option('--file', 'csv_file', default=None, type=click.Path(exists=True),
              help='Path to a local GISTEMP-format CSV (no download needed).')
@click.option('--url', default=None,
              help='URL to download from. Defaults to NASA GISTEMP v4.')
@click.option('--dir', 'output_dir', default=None, type=click.Path(),
              help='Directory to save downloaded file. Defaults to settings.raw_data_path.')
@click.option('--db', 'db_path', default=None, type=click.Path(),
              help='Path to provenance SQLite DB. Defaults to settings.prov_db_path.')
def run(csv_file, url, output_dir, db_path):
    """Run the full ingestion → validation → provenance pipeline."""
    configure_logging()
    from .ingestion.nasa_gistemp import NASAGISTEMPSource
    from .ingestion.local_csv import LocalCSVSource
    from .pipeline.runner import run_pipeline

    settings = get_settings()
    resolved_db = db_path or str(settings.prov_db_path)

    if csv_file:
        source = LocalCSVSource(path=Path(csv_file))
    else:
        resolved_url = url or settings.gistemp_url
        resolved_dir = Path(output_dir) if output_dir else settings.raw_data_path
        source = NASAGISTEMPSource(url=resolved_url, output_dir=resolved_dir)

    result = run_pipeline(source=source, db_path=resolved_db)

    click.echo(f"Run ID   : {result.run_id}")
    click.echo(f"Rows in  : {result.validation.rows_in:,}")
    click.echo(f"Passed   : {result.validation.rows_passed:,}")
    click.echo(f"Rejected : {result.validation.rows_rejected:,} "
               f"({result.validation.rejection_rate:.1%})")
    click.echo(f"DB       : {resolved_db}")


# ── runs ──────────────────────────────────────────────────────────────────────

@main.group()
def runs():
    """Query stored provenance runs."""


@runs.command('list')
@click.option('--db', 'db_path', default=None, type=click.Path(),
              help='Path to provenance SQLite DB.')
@click.option('--limit', default=20, show_default=True,
              help='Maximum number of runs to display.')
def runs_list(db_path, limit):
    """List all stored pipeline runs."""
    settings = get_settings()
    store = ProvenanceStore(db_path=db_path or str(settings.prov_db_path))
    all_runs = store.list_runs()
    if not all_runs:
        click.echo('No runs found.')
        return
    for run in all_runs[:limit]:
        click.echo(f"{run['run_id']}  |  {run['created_at']}")
    if len(all_runs) > limit:
        click.echo(f"… and {len(all_runs) - limit} more (use --limit to show more)")


@runs.command('show')
@click.argument('run_id')
@click.option('--db', 'db_path', default=None, type=click.Path(),
              help='Path to provenance SQLite DB.')
@click.option('--json', 'as_json', is_flag=True, default=False,
              help='Output full PROV-JSON instead of summary.')
def runs_show(run_id, db_path, as_json):
    """Show detail for a specific run."""
    settings = get_settings()
    store = ProvenanceStore(db_path=db_path or str(settings.prov_db_path))
    doc = store.get(run_id)
    if doc is None:
        click.echo(f"Run '{run_id}' not found.", err=True)
        sys.exit(1)

    if as_json:
        click.echo(json.dumps(doc, indent=2))
        return

    # Human-readable summary
    from .utils.prov_helpers import unwrap, get_ingestion_entity, get_validation_activity
    _, ing = get_ingestion_entity(doc)
    _, val = get_validation_activity(doc)

    click.echo(f"Run ID       : {run_id}")
    click.echo(f"Source URL   : {ing.get('fair:source_url', '—')}")
    click.echo(f"Row count    : {unwrap(ing.get('pf:row_count', '—'))}")
    click.echo(f"Checksum     : {ing.get('pf:checksum_sha256', '—')[:24]}…")
    click.echo(f"Rows in      : {unwrap(val.get('pf:rows_in', '—'))}")
    click.echo(f"Rows passed  : {unwrap(val.get('pf:rows_passed', '—'))}")
    click.echo(f"Rejection %  : {float(unwrap(val.get('pf:rejection_rate', 0))) * 100:.2f} %")
    click.echo(f"Rules        : {val.get('pf:rules_applied', '—')}")


# ── dashboard ─────────────────────────────────────────────────────────────────

@main.command()
@click.option('--port', default=8501, show_default=True)
def dashboard(port):
    """Launch the Streamlit provenance dashboard."""
    dashboard_path = Path(__file__).resolve().parent.parent.parent / 'dashboard.py'
    if not dashboard_path.exists():
        click.echo(f"dashboard.py not found at {dashboard_path}", err=True)
        sys.exit(1)
    subprocess.run(
        [sys.executable, '-m', 'streamlit', 'run', str(dashboard_path),
         '--server.port', str(port)],
        check=True,
    )


# ── serve ─────────────────────────────────────────────────────────────────────

@main.command()
@click.option('--host', default=None, help='API host. Defaults to settings.api_host.')
@click.option('--port', default=None, type=int,
              help='API port. Defaults to settings.api_port.')
def serve(host, port):
    """Launch the FastAPI REST API."""
    configure_logging()
    settings = get_settings()
    resolved_host = host or settings.api_host
    resolved_port = port or settings.api_port
    subprocess.run(
        [sys.executable, '-m', 'uvicorn',
         'provenanceflow.api.app:app',
         '--host', resolved_host,
         '--port', str(resolved_port)],
        check=True,
    )
