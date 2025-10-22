# app/orchestration/run_once.py

import logging
from typing import Optional, Dict, Any

from langchain_core.messages import HumanMessage
from langgraph.errors import GraphRecursionError

from app.orchestration.build_flow import build_graph
from app.orchestration.session_state import AgentState

_log = logging.getLogger(__name__)

# Module-level singleton for the compiled graph
_GRAPH_SINGLETON: Optional[Any] = None


def _fmt_step(content: str) -> str:
    """Lightweight step divider for streamed graph messages."""
    bar = "─" * 72
    return f"\n{bar}\n{content}\n{bar}\n"


def _fmt_msg_preview(msg: Any) -> str:
    """
    Render a short preview of a message without relying on pretty_print.
    Supports text content; if structured, falls back to str().
    """
    role = getattr(msg, "type", None) or getattr(msg, "role", "assistant")
    text = getattr(msg, "content", "")
    # Guard against non-string content
    if not isinstance(text, str):
        try:
            text = str(text)
        except Exception:
            text = "<non-text content>"
    role_tag = "user" if role in {"human", "user"} else "agent"
    return f"{role_tag}\n{text}"


def get_graph() -> Any:
    """
    Retrieve or build the state graph (singleton).
    """
    global _GRAPH_SINGLETON
    if _GRAPH_SINGLETON is None:
        _log.info("Constructing orchestration graph (singleton).")
        _GRAPH_SINGLETON = build_graph()
    return _GRAPH_SINGLETON


def run_chat_once(question: str, agent_config: Dict[str, Any]) -> str:
    """
    Execute a single turn of the agent graph and return the final answer text.

    Args:
        question: User's input prompt.
        agent_config: Agent runtime configuration (e.g., max_iterations).

    Returns:
        Final assistant message content, or an explanatory error string.
    """
    _log.info("Starting single-turn graph execution.")
    graph = get_graph()

    initial_state: AgentState = {
        "messages": [HumanMessage(content=question)],
        "question": question,
    }

    max_iterations = agent_config.get("max_iterations", 5)
    recursion_limit = 2 * max_iterations + 1

    try:
        events = graph.stream(
            initial_state,
            config={
                "configurable": {
                    "thread_id": "opsfleet-session",
                },
                "recursion_limit": recursion_limit,
            },
            stream_mode="values",
        )

        _log.info("Streaming events from orchestration graph.")

        last_event: Optional[Dict[str, Any]] = None
        for ev in events:
            last_event = ev
            try:
                msgs = ev.get("messages") or []
                if msgs:
                    preview = _fmt_msg_preview(msgs[-1])
                    print(_fmt_step(preview))
            except Exception as exc:
                _log.error("Stream rendering error: %s", exc, exc_info=True)
                continue

        _log.info("Graph execution completed; delivering final response.")

        if last_event and last_event.get("messages"):
            return last_event["messages"][-1].content  # type: ignore[return-value]

        return "No response was produced by the agent."

    except GraphRecursionError:
        _log.warning("Recursion limit reached; halting execution.")
        return "Stopped: maximum reasoning depth reached for this request."
    except Exception as exc:
        _log.error("Unhandled error during graph execution: %s", exc, exc_info=True)
        return f"Error: {exc}"
