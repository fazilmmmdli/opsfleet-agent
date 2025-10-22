# app/backends/model_gateway.py

import logging
from typing import Optional

from langchain_core.runnables import Runnable
from langchain_google_genai import ChatGoogleGenerativeAI

from app.boot.load_settings import AppConfigLoader
from app.boot.env_vars import EnvConfig

_log = logging.getLogger(__name__)


_SHARED_LLM: Optional[Runnable] = None


def _bootstrap_llm() -> Runnable:
    """
    Build the primary Gemini chat model with a fallback.
    Returns a Runnable that encapsulates the fallback chain.
    """
    _log.info("Creating Gemini client(s) with fallback configuration.")
    try:
        api_key: Optional[str] = EnvConfig().google_api_key
        if not api_key:
            _log.error("Missing GOOGLE_API_KEY in environment.")
            raise ValueError("GOOGLE_API_KEY is required to initialize the LLM backend.")

        cfg = AppConfigLoader().get_config()
        agent_cfg = cfg.get("agent", {})

        
        primary_model = agent_cfg.get("llm_model", "gemini-2.5-flash")
        fallback_model = agent_cfg.get("fallback_llm_model", "gemini-1.5-flash-8b")
        temperature = agent_cfg.get("temperature", 0.25)

        primary = ChatGoogleGenerativeAI(
            model=primary_model,
            temperature=temperature,
            api_key=api_key,
        )
        backup = ChatGoogleGenerativeAI(
            model=fallback_model,
            temperature=temperature,
            api_key=api_key,
        )

        _log.info("Gemini models initialized: primary=%s, fallback=%s", primary_model, fallback_model)
        return primary.with_fallbacks([backup])

    except Exception as exc:
        _log.error("Failed to set up LLM backend: %s", exc, exc_info=True)
        raise


def get_llm() -> Runnable:
    """
    Provide a process-wide shared LLM instance.
    Lazily initializes on first call, then reuses the same client.
    """
    global _SHARED_LLM
    if _SHARED_LLM is None:
        _log.info("Initializing shared LLM instance.")
        _SHARED_LLM = _bootstrap_llm()
    return _SHARED_LLM
