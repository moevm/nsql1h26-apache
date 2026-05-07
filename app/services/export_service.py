from __future__ import annotations

from datetime import datetime, timezone

from pymongo.database import Database

from app.repositories.cluster_runs import ClusterRunsRepository
from app.repositories.clusters import ClustersRepository
from app.repositories.logs import LogsRepository
from app.services.serialization import serialize_cluster, serialize_cluster_run, serialize_log_document


class ExportService:
    def __init__(self, db: Database) -> None:
        self.logs_repository = LogsRepository(db)
        self.runs_repository = ClusterRunsRepository(db)
        self.clusters_repository = ClustersRepository(db)

    def export_application(self) -> dict:
        return {
            "version": "0.8",
            "exported_at": datetime.now(timezone.utc).isoformat(),
            "logs": [
                serialize_log_document(document, omit_empty_normalized=True)
                for document in self.logs_repository.find_all_for_export()
            ],
            "cluster_runs": [
                serialize_cluster_run(document)
                for document in self.runs_repository.find_all_for_export()
            ],
            "clusters": [
                serialize_cluster(document)
                for document in self.clusters_repository.find_all_for_export()
            ],
        }
