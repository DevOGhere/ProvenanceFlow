import sqlite3
import json
from pathlib import Path
from datetime import datetime


class ProvenanceStore:
    """
    Persists PROV-JSON documents to SQLite.
    SQLite chosen for portability and FAIR 'Accessible' compliance —
    readable without proprietary tools.
    """

    def __init__(self, db_path: str = 'provenance_store/lineage.db'):
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self._init_schema()

    def _init_schema(self):
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS provenance_runs (
                run_id      TEXT PRIMARY KEY,
                created_at  TEXT NOT NULL,
                prov_json   TEXT NOT NULL,
                summary     TEXT
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS entities (
                entity_id   TEXT NOT NULL,
                run_id      TEXT NOT NULL,
                label       TEXT,
                row_count   INTEGER,
                FOREIGN KEY (run_id) REFERENCES provenance_runs(run_id)
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS rejections (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id      TEXT NOT NULL,
                rule        TEXT,
                severity    TEXT,
                message     TEXT,
                row_index   INTEGER,
                row_data    TEXT
            )
        """)
        self.conn.commit()

    def save(self, run_id: str, prov_json: dict):
        created_at = datetime.utcnow().isoformat()
        self.conn.execute(
            "INSERT OR REPLACE INTO provenance_runs VALUES (?, ?, ?, ?)",
            (run_id, created_at, json.dumps(prov_json, indent=2), None)
        )
        # Populate entities table from PROV-JSON
        self.conn.execute("DELETE FROM entities WHERE run_id = ?", (run_id,))
        for entity_id, attrs in prov_json.get('entity', {}).items():
            label = attrs.get('prov:label')
            raw_rc = attrs.get('pf:row_count')
            # PROV-JSON serializes typed literals as {"$": value, "type": "..."}
            row_count = raw_rc['$'] if isinstance(raw_rc, dict) else raw_rc
            self.conn.execute(
                "INSERT INTO entities (entity_id, run_id, label, row_count) VALUES (?, ?, ?, ?)",
                (entity_id, run_id, label, row_count)
            )
        self.conn.commit()

    def get(self, run_id: str) -> dict | None:
        cursor = self.conn.execute(
            "SELECT prov_json FROM provenance_runs WHERE run_id = ?", (run_id,)
        )
        row = cursor.fetchone()
        return json.loads(row[0]) if row else None

    def list_runs(self) -> list[dict]:
        cursor = self.conn.execute(
            "SELECT run_id, created_at FROM provenance_runs ORDER BY created_at DESC"
        )
        return [{'run_id': r[0], 'created_at': r[1]} for r in cursor.fetchall()]

    def save_rejections(self, run_id: str, rejections: list[dict]) -> None:
        """Persist rejected rows for a run (idempotent — replaces on repeat call)."""
        self.conn.execute("DELETE FROM rejections WHERE run_id = ?", (run_id,))
        for r in rejections:
            self.conn.execute(
                "INSERT INTO rejections (run_id, rule, severity, message, row_index, row_data) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (run_id, r["rule"], r["severity"], r["message"], r["row_index"], r["row_data"])
            )
        self.conn.commit()

    def get_rejections(self, run_id: str) -> list[dict]:
        """Return all rejected rows for a run, or [] if none."""
        cursor = self.conn.execute(
            "SELECT rule, severity, message, row_index, row_data "
            "FROM rejections WHERE run_id = ? ORDER BY id",
            (run_id,)
        )
        return [
            {"rule": r[0], "severity": r[1], "message": r[2], "row_index": r[3], "row_data": r[4]}
            for r in cursor.fetchall()
        ]

    def query_by_date_range(self, start: str, end: str) -> list[dict]:
        """Return all runs whose created_at falls within [start, end] (ISO strings)."""
        cursor = self.conn.execute(
            "SELECT run_id, created_at FROM provenance_runs "
            "WHERE created_at >= ? AND created_at <= ? ORDER BY created_at DESC",
            (start, end)
        )
        return [{'run_id': r[0], 'created_at': r[1]} for r in cursor.fetchall()]
