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
            CREATE TABLE IF NOT EXISTS rejected_rows (
                run_id      TEXT NOT NULL,
                row_index   INTEGER,
                rule        TEXT NOT NULL,
                severity    TEXT NOT NULL,
                message     TEXT NOT NULL,
                row_data    TEXT NOT NULL,
                FOREIGN KEY (run_id) REFERENCES provenance_runs(run_id)
            )
        """)
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_rejected_rows_run_id ON rejected_rows(run_id)"
        )
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
        """Persist hard-rejected rows for a run.

        Args:
            run_id:     The run identifier (matches provenance_runs.run_id).
            rejections: List of dicts with keys: rule, severity, message,
                        row_index, row_data (JSON string).
        """
        self.conn.execute(
            "DELETE FROM rejected_rows WHERE run_id = ?", (run_id,)
        )
        self.conn.executemany(
            "INSERT INTO rejected_rows "
            "(run_id, row_index, rule, severity, message, row_data) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            [
                (
                    run_id,
                    r.get("row_index"),
                    r["rule"],
                    r["severity"],
                    r["message"],
                    r["row_data"],
                )
                for r in rejections
            ],
        )
        self.conn.commit()

    def get_rejections(self, run_id: str) -> list[dict]:
        """Retrieve all hard-rejected rows for a run.

        Returns:
            List of dicts with keys: row_index, rule, severity, message,
            row_data.  Empty list if run_id not found.
        """
        cursor = self.conn.execute(
            "SELECT row_index, rule, severity, message, row_data "
            "FROM rejected_rows WHERE run_id = ? ORDER BY row_index",
            (run_id,),
        )
        return [
            {
                "row_index": row[0],
                "rule":      row[1],
                "severity":  row[2],
                "message":   row[3],
                "row_data":  row[4],
            }
            for row in cursor.fetchall()
        ]

    def query_by_date_range(self, start: str, end: str) -> list[dict]:
        """Return all runs whose created_at falls within [start, end] (ISO strings)."""
        cursor = self.conn.execute(
            "SELECT run_id, created_at FROM provenance_runs "
            "WHERE created_at >= ? AND created_at <= ? ORDER BY created_at DESC",
            (start, end)
        )
        return [{'run_id': r[0], 'created_at': r[1]} for r in cursor.fetchall()]
