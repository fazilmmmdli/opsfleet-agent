# app/orchestration/session_state.py

from typing import TypedDict, Annotated, Optional, Dict, Any, List
from langgraph.graph.message import add_messages


class AgentState(TypedDict, total=False):
    """
    Conversation + runtime scratchpad for the orchestration graph.

    Keys:
        messages   : Rolling transcript carried by LangGraph (append-only).
        question   : The latest user question (string form for convenience).
        dataset_id : Active BigQuery dataset to target (e.g., 'project.dataset').
        project_id : Optional GCP project id (if provided/overridden).
        model_name : Identifier for the LLM chosen this run.
        summary    : Optional short-form result the agent may populate.
    """
    messages: Annotated[List[Dict[str, Any]], add_messages]
    question: str
    dataset_id: str
    project_id: Optional[str]
    model_name: str
    summary: Optional[str]
