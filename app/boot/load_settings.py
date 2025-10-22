# app/boot/load_settings.py

import os
import yaml
import argparse
import logging
from typing import Dict, Any, Optional, TypedDict

_log = logging.getLogger(__name__)


class AppConfigLoader:
    """
    Singleton-style settings loader.

    - Loads the base YAML from settings/agent-settings.yaml
    - Exposes a copy via get_config()
    - Applies CLI arg overrides via merge_with_args()
    """
    _instance = None
    _config: Optional[Dict[str, Any]] = None

    def __new__(cls) -> "AppConfigLoader":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self) -> None:
        # Load once per process
        if self._config is None:
            _log.info("Initializing application settings.")
            self._load_from_yaml()

    # ──────────────────────────────────────────────────────────────────────────
    # Internal loading
    # ──────────────────────────────────────────────────────────────────────────
    def _load_from_yaml(self) -> None:
        """
        Resolve the settings path and parse the YAML file into memory.
        """
        # project_root = repo root (two levels up from app/boot/)
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
        settings_path = os.path.join(project_root, "settings", "agent-settings.yaml")

        try:
            with open(settings_path, "r", encoding="utf-8") as fh:
                self._config = yaml.safe_load(fh) or {}
                _log.info("Settings loaded from %s", settings_path)
        except FileNotFoundError:
            _log.warning("Settings file not found at %s. Using empty defaults.", settings_path)
            self._config = {}
        except Exception as exc:
            _log.error("Failed to load settings: %s", exc, exc_info=True)
            self._config = {}


    # ──────────────────────────────────────────────────────────────────────────
    # Public API
    # ──────────────────────────────────────────────────────────────────────────
    def get_config(self) -> Dict[str, Any]:
        """
        Return a shallow copy of the loaded settings.
        """
        _log.debug("Providing a copy of the loaded settings.")
        return dict(self._config or {})

    def merge_with_args(self, args: argparse.Namespace) -> Dict[str, Any]:
        """
        Merge CLI flags into the loaded configuration.
        CLI always takes precedence over YAML.

        Returns a new merged dict (does not mutate the internal cache).
        """
        _log.info("Merging CLI arguments into settings.")
        cfg = self.get_config()

        # BigQuery overrides
        bq_cfg = cfg.setdefault("bigquery", {})
        if getattr(args, "project", None) is not None:
            bq_cfg["project_id"] = args.project
        if getattr(args, "dataset", None) is not None:
            bq_cfg["dataset_id"] = args.dataset

        # Agent overrides (model selection)
        agent_cfg = cfg.setdefault("agent", {})
        if getattr(args, "model", None) is not None:
            agent_cfg["llm_model"] = args.model

        # Logging overrides
        log_cfg = cfg.setdefault("logging", {})
        # Verbose flag bumps level to DEBUG
        if getattr(args, "verbose", False) or getattr(args, "debug", False):
            log_cfg["level"] = "DEBUG"

        _log.info("Settings merge complete.")
        return cfg
