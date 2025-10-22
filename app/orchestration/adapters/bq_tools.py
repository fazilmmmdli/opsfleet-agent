# app/orchestration/adapters/bq_tools.py

import json
import logging
import re
from typing import Optional

from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPICallError, BadRequest
from langchain_core.tools import tool

from app.boot.load_settings import AppConfigLoader
from app.backends.bq_runner import BigQueryRunner

_log = logging.getLogger(__name__)

# Shared runner singleton
_RUNNER: Optional[BigQueryRunner] = None

# Safety caps
SCAN_CAP_BYTES = 1024 * 1024 * 1024  
ROW_LIMIT_CAP = 1000


def _get_runner() -> BigQueryRunner:
    """
    Return a shared BigQueryRunner instance.
    Pulls project/dataset from settings; initializes once.
    """
    global _RUNNER
    if _RUNNER is not None:
        _log.info("Reusing BigQuery runner singleton.")
        return _RUNNER

    _log.info("Creating BigQuery runner singleton.")
    cfg = AppConfigLoader().get_config()
    bq_cfg = cfg.get("bigquery", {})

    project_id = bq_cfg.get("project_id")
    dataset_id = bq_cfg.get("dataset_id")

    if dataset_id is None or project_id is None:
        _log.error("BigQuery configuration missing project_id or dataset_id.")
        raise ValueError("BigQuery dataset_id and project_id must be provided in settings or CLI.")

    _RUNNER = BigQueryRunner(project_id=project_id, dataset_id=dataset_id)
    _log.info("BigQuery runner ready.")
    return _RUNNER


@tool
def run_sql_bq_tool(*, sql: str, top_n_rows: Optional[int] = 50) -> str:
    """
    Execute a BigQuery Standard SQL statement and return a text table of results.

    Constraints:
      - Read-only: must start with SELECT
      - No SELECT * (explicit columns required)
      - Must include a numeric LIMIT <= ROW_LIMIT_CAP
      - Dry-run first; refuse if bytes scanned would exceed SCAN_CAP_BYTES
    """
    _log.info("Received SQL for execution.")

    # --- Safety checks ---
    q = sql.lstrip().lower()
    if not q.startswith("select"):
        _log.warning("Rejected non-SELECT query.")
        return "ERROR: Only read-only SELECT statements are allowed."

    if re.search(r"select\s+\*\s+from", q):
        _log.warning("Rejected SELECT * usage.")
        return "ERROR: Avoid `SELECT *`. Specify the required columns explicitly."

    limit_match = re.search(r"limit\s+(\d+)", q)
    if limit_match:
        limit_value = int(limit_match.group(1))
        if limit_value > ROW_LIMIT_CAP:
            _log.warning("Limit exceeds allowed cap: %s", limit_value)
            return f"ERROR: LIMIT {limit_value} exceeds the maximum allowed {ROW_LIMIT_CAP}."
    else:
        _log.warning("Missing LIMIT clause.")
        return "ERROR: Query must include a numeric LIMIT clause."

    try:
        runner = _get_runner()

        # --- Dry run ---
        try:
            _log.info("Performing dry run...")
            dry_cfg = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
            dry_job = runner.client.query(sql, job_config=dry_cfg)

            if dry_job.total_bytes_processed > SCAN_CAP_BYTES:
                _log.warning(
                    "Dry run indicates excessive scan: %s bytes > %s cap",
                    dry_job.total_bytes_processed,
                    SCAN_CAP_BYTES,
                )
                return (
                    "ERROR: Query would scan "
                    f"{dry_job.total_bytes_processed} bytes, exceeding the cap of {SCAN_CAP_BYTES}."
                )
            _log.info("Dry run OK.")
        except (GoogleAPICallError, BadRequest) as exc: 
            _log.error("Dry run failed: %s", exc)
            return f"Dry run failed: {exc}"

        # --- Actual execution ---
        _log.info("Executing query against BigQuery.")
        run_cfg = bigquery.QueryJobConfig(dry_run=False, use_query_cache=True)
        df = runner.execute_query(sql, job_config=run_cfg)
        _log.info("Execution complete.")

        # ── Pretty print: thousands separators, 2 decimals for floats, trim long text ──
        preview = df if top_n_rows is None else df.head(top_n_rows)

        def _format_frame(frame):
            fmt_frame = frame.copy()

            # Trim long text columns to 40 chars
            for col in fmt_frame.select_dtypes(include=["object"]).columns:
                fmt_frame[col] = fmt_frame[col].astype(str).str.slice(0, 40)

            # Format integers with thousands separators
            for col in fmt_frame.select_dtypes(include=["int", "int64", "Int64"]).columns:
                fmt_frame[col] = fmt_frame[col].map(lambda x: f"{x:,}")

            # Format floats with thousands separators and 2 decimals
            for col in fmt_frame.select_dtypes(include=["float", "float64"]).columns:
                fmt_frame[col] = fmt_frame[col].map(lambda x: f"{x:,.2f}")

            return fmt_frame

        pretty = _format_frame(preview)
        return pretty.to_string(index=False)

    except Exception as exc:
        _log.error("Query execution failed: %s", exc)
        return f"ERROR: {exc}"


@tool
def inspect_bq_schema_tool(*, table_name: str) -> str:
    """
    Return the JSON schema for a table (e.g., 'orders', 'users') in the configured dataset.
    """
    _log.info("Describing schema for table: %s", table_name)
    try:
        runner = _get_runner()
        schema = runner.get_table_schema(table_name)
        _log.info("Schema retrieval successful.")
        return json.dumps(schema)
    except Exception as exc:
        _log.error("Failed to retrieve schema: %s", exc)
        return f"ERROR: {exc}"
