"""Tests for the provenanceflow CLI."""
import textwrap
import pytest
from click.testing import CliRunner

from src.provenanceflow.cli import main

FIXTURE_CSV = textwrap.dedent("""\
    Global-mean monthly, seasonal, and annual means, 1880-present
    Year,Jan,Feb,Mar,Apr,May,Jun,Jul,Aug,Sep,Oct,Nov,Dec,J-D,D-N,DJF,MAM,JJA,SON
    1880,-.15,-.21,-.16,-.10,-.11,-.22,-.18,-.26,-.20,-.24,-.19,-.17,-.18,****,****,-.12,-.22,-.21
    1881,-.19,-.14,  .02,  .05,  .06,-.20,  .00,-.03,-.14,-.22,-.17,-.07,-.08,-.09,-.17,  .04,-.08,-.18
    1882,-.28,  .05,  .02, -.17,-.13,-.24,-.28,-.14,-.20,-.33,-.27,-.35,-.20,-.20,-.09, -.09,-.22,-.27
""")


@pytest.fixture
def fixture_csv(tmp_path):
    p = tmp_path / "gistemp.csv"
    p.write_text(FIXTURE_CSV)
    return str(p)


@pytest.fixture
def tmp_db(tmp_path):
    return str(tmp_path / "lineage.db")


# ── provenanceflow --help ─────────────────────────────────────────────────────

def test_cli_help():
    runner = CliRunner()
    result = runner.invoke(main, ['--help'])
    assert result.exit_code == 0
    assert 'provenanceflow' in result.output.lower()


# ── provenanceflow run ────────────────────────────────────────────────────────

def test_cli_run_with_local_file(fixture_csv, tmp_db):
    runner = CliRunner()
    result = runner.invoke(main, ['run', '--file', fixture_csv, '--db', tmp_db])
    assert result.exit_code == 0, result.output
    assert 'run_' in result.output
    assert 'Rows in' in result.output


def test_cli_run_shows_rejection_rate(fixture_csv, tmp_db):
    runner = CliRunner()
    result = runner.invoke(main, ['run', '--file', fixture_csv, '--db', tmp_db])
    assert result.exit_code == 0
    assert 'Rejected' in result.output


# ── provenanceflow runs list ──────────────────────────────────────────────────

def test_cli_runs_list_empty(tmp_db):
    runner = CliRunner()
    result = runner.invoke(main, ['runs', 'list', '--db', tmp_db])
    assert result.exit_code == 0
    assert 'No runs found' in result.output


def test_cli_runs_list_shows_run_after_run(fixture_csv, tmp_db):
    runner = CliRunner()
    runner.invoke(main, ['run', '--file', fixture_csv, '--db', tmp_db])
    result = runner.invoke(main, ['runs', 'list', '--db', tmp_db])
    assert result.exit_code == 0
    assert 'run_' in result.output


# ── provenanceflow runs show ──────────────────────────────────────────────────

def test_cli_runs_show_not_found(tmp_db):
    runner = CliRunner()
    result = runner.invoke(main, ['runs', 'show', 'bogus_run', '--db', tmp_db])
    assert result.exit_code != 0


def test_cli_runs_show_valid_run(fixture_csv, tmp_db):
    runner = CliRunner()
    run_result = runner.invoke(main, ['run', '--file', fixture_csv, '--db', tmp_db])
    # Extract run_id from output
    for line in run_result.output.splitlines():
        if 'Run ID' in line:
            run_id = line.split(':')[1].strip()
            break
    show_result = runner.invoke(main, ['runs', 'show', run_id, '--db', tmp_db])
    assert show_result.exit_code == 0
    assert 'Row count' in show_result.output


def test_cli_runs_show_json_flag(fixture_csv, tmp_db):
    runner = CliRunner()
    run_result = runner.invoke(main, ['run', '--file', fixture_csv, '--db', tmp_db])
    for line in run_result.output.splitlines():
        if 'Run ID' in line:
            run_id = line.split(':')[1].strip()
            break
    show_result = runner.invoke(main, ['runs', 'show', run_id, '--db', tmp_db, '--json'])
    assert show_result.exit_code == 0
    import json
    doc = json.loads(show_result.output)
    assert 'entity' in doc
