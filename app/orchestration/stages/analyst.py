# app/orchestration/stages/analyst.py

import logging
from typing import Any

from langchain_core.messages import SystemMessage

from app.orchestration.stages.stage_base import BaseNode
from app.orchestration.session_state import AgentState
from app.orchestration.adapters.bq_tools import (
    run_sql_bq_tool,
    inspect_bq_schema_tool,
)

_log = logging.getLogger(__name__)


class AnalyzeNode(BaseNode):
    """
    Reasoning stage that prepares the system prompt and binds BigQuery tools.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        _log.info("Wiring tools into LLM for analyst stage.")
        self.llm_with_tools = self.llm.bind_tools(
            [
                run_sql_bq_tool,
                inspect_bq_schema_tool,
            ]
        )

    def __call__(self, state: AgentState) -> AgentState:
        """
        Invoke the LLM+tools on the current conversation state and return the new message.
        """
        _log.info("Analyst stage invoked with current state.")
        try:
            messages = state.get("messages", [])
            system_prompt = self._load_prompt("analysis.md")
            _log.info("System prompt loaded for analyst stage.")

            # Prepend the system message
            enriched = [SystemMessage(content=system_prompt)] + list(messages)

            _log.debug("Dispatching messages to LLM with tools.")
            response = self.llm_with_tools.invoke(enriched)

            _log.info("Analyst stage completed successfully.")
            return {"messages": [response]}
        except Exception as exc:
            _log.error("Analyst stage error: %s", exc, exc_info=True)
            raise
