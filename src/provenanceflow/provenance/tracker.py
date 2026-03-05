import json
import uuid
from datetime import datetime

import prov.model as prov

from .store import ProvenanceStore
from ..utils.identifiers import generate_pid
from ..utils.checksums import sha256_file


class ProvenanceTracker:
    """
    Builds W3C PROV documents for pipeline runs.
    Each track_*() call adds assertions to the current document.
    Call finalize() to serialize and persist.
    """

    def __init__(self, run_id: str = None):
        self.run_id = run_id or f"run_{uuid.uuid4().hex[:12]}"
        self.doc = prov.ProvDocument()
        self.doc.set_default_namespace('http://provenanceflow.org/')
        self.doc.add_namespace('pf', 'http://provenanceflow.org/ns#')
        self.doc.add_namespace('fair', 'http://provenanceflow.org/fair#')
        self.doc.add_namespace('dc', 'http://purl.org/dc/terms/')
        self.doc.add_namespace('schema', 'https://schema.org/')
        self._pipeline_agent = self._register_agent()

    def _register_agent(self) -> prov.ProvAgent:
        return self.doc.agent(
            'pf:provenanceflow_v1',
            {
                'prov:type': 'prov:SoftwareAgent',
                'pf:version': '1.0.0',
                'pf:run_id': self.run_id,
            }
        )

    def track_ingestion(self, source_url: str, local_path: str,
                        row_count: int) -> prov.ProvEntity:
        """Record that a dataset was downloaded from source_url."""
        dataset_pid = generate_pid('dataset')
        ingest_ts = datetime.utcnow().isoformat()
        entity = self.doc.entity(
            f'pf:dataset_{dataset_pid}',
            {
                'prov:label': f'Raw dataset from {source_url}',
                'fair:identifier': dataset_pid,
                'fair:source_url': source_url,
                'pf:row_count': row_count,
                'pf:ingest_timestamp': ingest_ts,
                'pf:checksum_sha256': sha256_file(local_path),
                # Dublin Core terms — interoperable with EUDAT / Zenodo / RADAR registries
                'dc:title':      'NASA GISTEMP v4 Global Surface Temperature',
                'dc:identifier': dataset_pid,
                'dc:source':     source_url,
                'dc:format':     'text/csv',
                'dc:created':    ingest_ts,
                'dc:type':       'Dataset',
                'dc:license':    'https://data.giss.nasa.gov/gistemp/',
                # Schema.org vocabulary
                'schema:name':           'NASA GISTEMP v4',
                'schema:url':            source_url,
                'schema:encodingFormat': 'text/csv',
            }
        )
        activity = self.doc.activity(
            f'pf:ingest_{uuid.uuid4().hex[:8]}',
            startTime=datetime.utcnow(),
        )
        self.doc.wasGeneratedBy(entity, activity)
        self.doc.wasAssociatedWith(activity, self._pipeline_agent)
        return entity

    def track_validation(self, input_entity: prov.ProvEntity,
                         rows_in: int, rows_passed: int,
                         rejections: dict, warnings: dict,
                         rules_applied: list[str] | None = None) -> prov.ProvEntity:
        """Record validation step with full rejection/warning breakdown."""
        output_pid = generate_pid('validated')
        rows_rejected = rows_in - rows_passed

        validation_activity = self.doc.activity(
            f'pf:validate_{uuid.uuid4().hex[:8]}',
            startTime=datetime.utcnow(),
            other_attributes={
                'pf:rules_applied': ','.join(rules_applied) if rules_applied else '',
                'pf:rows_in': rows_in,
                'pf:rows_passed': rows_passed,
                'pf:rows_rejected': rows_rejected,
                'pf:rejections_by_rule': str(rejections),
                'pf:warnings_by_rule': str(warnings),
                'pf:rejection_rate': round(rows_rejected / rows_in, 4) if rows_in else 0.0,
            }
        )

        output_entity = self.doc.entity(
            f'pf:validated_{output_pid}',
            {
                'prov:label': 'Validated GISTEMP dataset',
                'fair:identifier': output_pid,
                'pf:row_count': rows_passed,
                # Dublin Core — links validated output back to raw source
                'dc:title':       'Validated NASA GISTEMP v4 dataset',
                'dc:identifier':  output_pid,
                'dc:type':        'Dataset',
                'dc:isVersionOf': str(input_entity.identifier),
            }
        )

        self.doc.used(validation_activity, input_entity)
        self.doc.wasGeneratedBy(output_entity, validation_activity)
        self.doc.wasDerivedFrom(output_entity, input_entity)
        self.doc.wasAssociatedWith(validation_activity, self._pipeline_agent)

        return output_entity

    def finalize(self, store: ProvenanceStore) -> str:
        """Serialize PROV document to JSON and persist to store."""
        prov_json = json.loads(self.doc.serialize(format='json'))
        store.save(self.run_id, prov_json)
        return self.run_id
