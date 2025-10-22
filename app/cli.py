# app/cli.py

import sys
import logging
import argparse
from typing import List, Dict, Any, Optional
import io 

from dotenv import load_dotenv

from app.backends.bq_runner import BigQueryRunner
from app.boot.load_settings import AppConfigLoader
from app.orchestration.run_once import run_chat_once


# ──────────────────────────────────────────────────────────────────────────────
# UI helpers 
# ──────────────────────────────────────────────────────────────────────────────

def _box(title: str) -> str:
    """Return a single-line title in a lightweight box."""
    pad = f"  {title.strip()}  "
    return f"┌{'─' * len(pad)}┐\n│{pad}│\n└{'─' * len(pad)}┘"

def _rule(text: str = "") -> str:
    """Horizontal rule with optional label."""
    label = f" {text.strip()} " if text else ""
    line = "─" * max(4, 72 - len(label))
    return f"{label}{line}"

def _print_welcome() -> None:
    print(_box("Opsfleet Data Copilot"))
    print("Type ':quit' to exit - Ask about sales, customers, products, trends.\n")

def _print_prompt_header() -> None:
    print(_rule(" ask "))

def _print_answer_header() -> None:
    print(_rule(" response "))

def _print_footer() -> None:
    print(_rule())


# ──────────────────────────────────────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────────────────────────────────────

def setup_logging(runtime_cfg: Dict[str, Any], *, verbose: bool = False, debug: bool = False) -> None:
    """
    Initialize application logging using config values and verbosity flags.

    Args:
        runtime_cfg: Merged configuration dictionary.
        verbose: If True, log INFO and above to console.
        debug: If True, log DEBUG and above to console (overrides verbose).
    """
    log_cfg = runtime_cfg.get("logging", {})
    fmt = log_cfg.get("format", "%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    logfile = log_cfg.get("file", "logs/app.log")
    base_level = log_cfg.get("level", "WARNING").upper()

    if debug:
        level = logging.DEBUG
    elif verbose:
        level = logging.INFO
    else:
        level = getattr(logging, base_level, logging.WARNING)

    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(level)

    formatter = logging.Formatter(fmt)

    if verbose or debug:
        ch = logging.StreamHandler()
        ch.setFormatter(formatter)
        root.addHandler(ch)

    fh = logging.FileHandler(logfile)
    fh.setFormatter(formatter)
    root.addHandler(fh)


# ──────────────────────────────────────────────────────────────────────────────
# BigQuery utilities
# ──────────────────────────────────────────────────────────────────────────────

def _print_bq_schema(table_name: str, fields: List[Dict[str, Any]]) -> None:
    """
    Pretty-print the schema for a BigQuery table.
    """
    logging.info("Rendering schema for table: %s", table_name)
    print(f"\n{_box(f' BigQuery Schema • {table_name} ')}")
    for f in fields:
        col = f.get("name", "")
        typ = f.get("type", "")
        mode = f.get("mode", "")
        desc = f.get("description", "") or ""
        line = f"• {col:<28} | {typ:<10} | {mode:<8}"
        print(line if not desc else f"{line} → {desc}")


def cmd_check_bq(app_cfg: Dict[str, Any], tables_csv: Optional[str]) -> int:
    """
    Validate BigQuery access and show table schemas.

    Returns:
        0 on success, 1 on failure.
    """
    bq_cfg = app_cfg.get("bigquery", {})
    try:
        runner = BigQueryRunner(
            project_id=bq_cfg.get("project_id"),
            dataset_id=bq_cfg.get("dataset_id"),
        )
        logging.info("Initialized BigQuery client.")
    except Exception as exc:
        logging.error("Failed to initialize BigQuery client: %s", exc)
        print(_box("BigQuery client initialization failed"))
        return 1

    # List tables
    try:
        logging.info("Listing tables in dataset: %s", runner.dataset_id)
        table_iter = runner.client.list_tables(runner.dataset_id)
        table_ids = sorted(t.table_id for t in table_iter)
        print(f"\n{_box(' BigQuery Dataset Tables ')}")
        for t in table_ids:
            print(f"• {t}")
    except Exception as exc:
        logging.error("Failed to list tables: %s", exc)
        print(_box("Could not list tables"))
        print("Hint: ensure dataset id is formatted as 'project.dataset'.")
        return 1

    # Describe selected tables (or defaults)
    targets = [
        t.strip()
        for t in (tables_csv or "orders,order_items,products,users").split(",")
        if t.strip()
    ]
    for tbl in targets:
        try:
            schema = runner.get_table_schema(tbl)
            _print_bq_schema(tbl, schema)
        except Exception as exc:
            logging.error("Failed to fetch schema for %s: %s", tbl, exc)
            print(_box(f"Skipped: {tbl}"))  # continue loop

    return 0


# ──────────────────────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────────────────────

def build_parser() -> argparse.ArgumentParser:
    """
    Build and return the CLI argument parser with new wording.
    """
    parser = argparse.ArgumentParser(description="Opsfleet Data Copilot (CLI)")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # check-bq
    p_check = subparsers.add_parser(
        "check-bq", help="Verify BigQuery connectivity and inspect schemas"
    )
    p_check.add_argument("--project", default=None, help="GCP project id (overrides settings file)")
    p_check.add_argument("--dataset", default=None, help="Dataset id 'project.dataset' (overrides settings file)")
    p_check.add_argument("--tables", default=None, help="Comma-separated table names to describe")
    p_check.add_argument("-v", "--verbose", action="store_true", help="Verbose logs to console")
    p_check.add_argument("--debug", action="store_true", help="Debug logs (most detailed)")

    # chat
    p_chat = subparsers.add_parser(
        "chat", help="Interactive analysis session with the Opsfleet agent"
    )
    p_chat.add_argument("--project", default=None, help="GCP project id (overrides settings file)")
    p_chat.add_argument("--dataset", default=None, help="Dataset id 'project.dataset' (overrides settings file)")
    p_chat.add_argument("--model", default=None, help="Gemini model name (overrides settings file)")
    p_chat.add_argument("-v", "--verbose", action="store_true", help="Verbose logs to console")
    p_chat.add_argument("--debug", action="store_true", help="Debug logs (most detailed)")

    return parser


def main() -> None:
    """
    Main entry point for the Opsfleet Data Copilot CLI.
    """
    parser = build_parser()
    args = parser.parse_args()

    # Merge config
    settings_loader = AppConfigLoader()
    cfg = settings_loader.merge_with_args(args)

    # Logging
    setup_logging(cfg, verbose=args.verbose, debug=args.debug)

    # Load .env (best-effort)
    try:
        load_dotenv()
        logging.info("Environment variables loaded from .env")
    except Exception as exc:
        logging.warning("Unable to load .env: %s", exc)

    if args.command == "check-bq":
        sys.exit(cmd_check_bq(cfg, args.tables))

    if args.command == "chat":
        agent_cfg = cfg.get("agent", {})
        _print_welcome()
        while True:
            try:
                _print_prompt_header()
                user_text = input(" - ").strip()
                _print_footer()
            except (EOFError, KeyboardInterrupt):
                print()
                break

            if user_text.lower() in {":quit", "exit", "quit"}:
                break

            try:
                # Capture any intermediate prints from the orchestration layer
                _captured = io.StringIO()
                _old_stdout = sys.stdout
                try:
                    sys.stdout = _captured
                    reply = run_chat_once(
                        question=user_text,
                        agent_config=agent_cfg,
                    )
                finally:
                    sys.stdout = _old_stdout  # restore stdout

                _print_answer_header()
                print(reply if isinstance(reply, str) else f"{reply}")
                _print_footer()
            except Exception as exc:
                logging.error("Chat execution error: %s", exc, exc_info=True)
                print(_box("Something went wrong"))
                print(f"Reason: {exc}")
                _print_footer()
                continue

        return

    parser.print_help()
    sys.exit(2)


if __name__ == "__main__":
    main()
