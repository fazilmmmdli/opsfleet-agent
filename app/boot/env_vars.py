# app/boot/env_vars.py

import os
import sys
import logging
from typing import Optional

_log = logging.getLogger(__name__)


class EnvConfig:
    """
    Environment variable accessor.

    - Ensures GOOGLE_API_KEY is present at process start.
    - Exposes `google_api_key` for consumers.
    """

    def __init__(self) -> None:
        api_key: Optional[str] = os.getenv("GOOGLE_API_KEY")
        if not api_key:
            _log.error("Required env var GOOGLE_API_KEY is not set.")
            sys.exit("ERROR: Missing GOOGLE_API_KEY. Set it in your environment or .env file.")
        self.google_api_key: str = api_key
        _log.info("GOOGLE_API_KEY detected and loaded.")
