# app/orchestration/build_flow.py

import logging

from langgraph.graph import StateGraph
from langgraph.checkpoint.memory import MemorySaver
from langgraph.prebuilt import ToolNode, tools_condition as _tools_condition_base

from app.orchestration.session_state import AgentState
from app.orchestration.stages.analyst import AnalyzeNode
from app.orchestration.adapters.bq_tools import (
    run_sql_bq_tool,
    inspect_bq_schema_tool,
)

_log = logging.getLogger(__name__)


def build_graph() -> StateGraph:
    """
    Assemble and compile the orchestration graph.

    """
    _log.info("Composing orchestration graph ...")

    g = StateGraph(AgentState)

    # Main reasoning stage
    g.add_node("analyst", AnalyzeNode())

    # Tool hub
    toolset = [run_sql_bq_tool, inspect_bq_schema_tool]
    g.add_node("workbench", ToolNode(tools=toolset))

    # No-op finalize node
    def _finalize(state: AgentState) -> AgentState:
        return state

    g.add_node("finalize", _finalize)

    
    def _route_label(state: AgentState) -> str:
        outcome = _tools_condition_base(state)
        return "tools" if outcome == "tools" else "finish"

    # Conditional routing
    g.add_conditional_edges(
        "analyst",
        _route_label,
        {"tools": "workbench", "finish": "finalize"},
    )

    # Tools feed back into analyst
    g.add_edge("workbench", "analyst")

    # Finalize then end
    g.add_edge("finalize", "__end__")

    # Entry point
    g.set_entry_point("analyst")

    # Compile with in-memory checkpoints
    memory = MemorySaver()
    graph = g.compile(checkpointer=memory)


    _log.info("Orchestration compiled successfully.")
    return graph
