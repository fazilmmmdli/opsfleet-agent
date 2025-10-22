# app/orchestration/stages/stage_base.py

import logging
from abc import ABC, abstractmethod
from pathlib import Path

from app.backends.model_gateway import get_llm
from app.orchestration.session_state import AgentState

_log = logging.getLogger(__name__)


class BaseNode(ABC):
    """
    Abstract base for orchestration stages.

    - Grabs a shared LLM instance on init
    - Provides a resilient prompt loader
    """

    def __init__(self) -> None:
        _log.info("Stage bootstrap: acquiring shared LLM client.")
        self.llm = get_llm()

    def _load_prompt(self, template_name: str) -> str:
        """
        Load a prompt template by name from the packaged instructions folder,
        with a filesystem fallback.
        """
        import importlib.resources as pkg_resources

        try:
            # Primary: load from the packaged instructions module
            from app.orchestration import instructions as _instr_pkg

            _log.info("Loading prompt template: %s", template_name)
            with pkg_resources.files(_instr_pkg).joinpath(template_name).open(
                "r", encoding="utf-8"
            ) as fh:
                _log.info("Prompt template loaded: %s", template_name)
                return fh.read()

        except FileNotFoundError as exc:
            _log.error("Prompt not found in package: %s", template_name)
            raise exc

        except Exception as exc:
            # Fallback: compute the on-disk path relative to this file
            _log.warning("Pkg resource load failed (%s); trying filesystem fallback.", exc)
            fallback = Path(__file__).resolve().parents[1] / "instructions" / template_name
            try:
                _log.info("Fallback prompt path: %s", fallback)
                return fallback.read_text(encoding="utf-8")
            except Exception as fb_exc:
                _log.error("Fallback prompt load failed: %s", fb_exc)
                raise fb_exc

    @abstractmethod
    def __call__(self, state: AgentState) -> AgentState:
        """
        Process the agent state and return an updated state.
        Concrete stages must implement this.
        """
        raise NotImplementedError("Stages must implement __call__")
