# app/backends/bq_runner.py

import logging
from typing import Optional, List, Dict, Any

import pandas as pd
from google.cloud import bigquery

_log = logging.getLogger(__name__)


class BigQueryRunner:
    """
    Lightweight BigQuery executor.

    - Initializes a BigQuery client
    - Executes SQL and returns pandas DataFrames
    - Fetches table schema metadata
    """

    def __init__(
        self,
        project_id: Optional[str] = None,
        dataset_id: Optional[str] = "bigquery-public-data.thelook_ecommerce",
    ) -> None:
        """
        Args:
            project_id: GCP project identifier; if None, uses ADC.
            dataset_id: Dataset to target for table references.
        """
        _log.info("Bootstrapping BigQuery client...")
        try:
            self.client = bigquery.Client(project=project_id)
            self.dataset_id = dataset_id
            _log.info("BigQuery client ready. Dataset context: %s", self.dataset_id)
        except Exception as exc:
            _log.error("BigQuery client initialization failed: %s", exc)
            raise

    def execute_query(self, sql_query: str, job_config: bigquery.QueryJobConfig) -> pd.DataFrame:
        """
        Run a SQL statement and return results as a DataFrame.

        Args:
            sql_query: SQL text to execute.
            job_config: BigQuery QueryJobConfig with settings (e.g., dry_run, limits).

        Returns:
            pandas.DataFrame with the query results.
        """
        try:
            _log.info("Submitting query to BigQuery.")
            job = self.client.query(sql_query, job_config=job_config)
            frame = job.result().to_dataframe()
            _log.info("Query finished. Rows returned: %d", len(frame))
            return frame
        except Exception as exc:
            _log.error("Query execution error: %s", exc)
            raise

    def get_table_schema(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Retrieve column metadata for a table within the configured dataset.

        Args:
            table_name: Table identifier (e.g., 'orders') within dataset_id.

        Returns:
            List of dicts with keys: name, type, mode, description.
        """
        try:
            table_ref = f"{self.dataset_id}.{table_name}"
            table = self.client.get_table(table_ref)
            cols: List[Dict[str, Any]] = []
            for field in table.schema:
                cols.append(
                    {
                        "name": field.name,
                        "type": field.field_type,
                        "mode": field.mode,
                        "description": field.description or "",
                    }
                )
            _log.info("Schema fetched for table: %s", table_name)
            return cols
        except Exception as exc:
            _log.error("Failed to fetch schema for %s: %s", table_name, exc)
            raise
