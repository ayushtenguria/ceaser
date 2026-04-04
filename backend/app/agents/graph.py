"""LangGraph state-graph definition and entry-point for the analysis agent.

Graph topology:

    Entry -> Router -> (SQL Agent -> Validate SQL -> Execute SQL -> Repair SQL)  \
                    -> (Python Agent -> Validate Py -> Execute Py -> Repair Py)   > -> Respond
                    -> Analyst                                                   /
                    -> Respond directly
                         ^              ^
                         |              |
                         +--- retry ----+
"""

from __future__ import annotations

import json
import logging
from collections.abc import AsyncGenerator
from typing import Any

from langchain_core.language_models import BaseChatModel
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage
from langgraph.graph import END, StateGraph
from sqlalchemy.ext.asyncio import AsyncSession

from app.agents.analyst import run_analyst
from app.agents.decomposer import decompose_query
from app.agents.disambiguator import disambiguate
from app.agents.executor import execute_code, execute_sql
from app.agents.python_agent import generate_python
from app.agents.python_repair import repair_python
from app.agents.python_validator import validate_python
from app.agents.repair import repair_sql
from app.agents.router import route_query
from app.agents.sql_agent import generate_sql
from app.agents.state import AgentState
from app.agents.validator import validate_sql
from app.agents.verifier import verify_results

logger = logging.getLogger(__name__)

def _max_retries() -> int:
    try:
        from app.core.config import get_settings
        return get_settings().max_retries
    except Exception:
        return 3

_MAX_RETRIES = _max_retries()


_RESPONSE_SYSTEM_PROMPT = """\
You are Ceaser, a friendly and expert AI data analyst.

Summarise the analysis result for the user in clear, concise language.
Reference specific numbers / columns when available.

RESPONSE STRUCTURE (follow this order):
1. **Methodology** — If you computed a score, metric, or derived value, FIRST explain how it
   was calculated: "I calculated the buying behavior score by normalizing total spending and
   purchase frequency on a 0-1 scale, then averaging them with 50/50 weight."
2. **Key findings** — Present the top insights with EXACT numbers and percentages.
3. **Interpretation** — Explain what the numbers mean in business terms.

CRITICAL: EVERY claim must include a specific number. NEVER use vague words like "significant",
"substantial", "notable", "considerable", "a large portion", "many", "most".
Instead: "$248,000 (32% of total revenue)" or "1,247 of 5,000 customers (24.9%)".

IMPORTANT rules:
- If the query returned NULL values, empty results, or zero rows, tell the user clearly:
  "No data found for this query." Then explain WHY.
- If there was an error, explain in plain language and suggest a fix.
  Do NOT repeat the same error message twice. One clear sentence is enough.
- Never say "null" or "None" without explanation.
- If results look correct, present them with key insights and highlight notable patterns.
- NEVER generate SQL code, Python code, or code blocks in your response.
  If no data source is connected, tell the user:
  "Please select a database connection or attach a file using the paperclip button to start analyzing."
- Charts render automatically below your text. Do NOT say "a chart was generated",
  "here's the chart", or "a visualisation has been created". Just describe the data insights.
- For advice/strategy questions, provide data-driven suggestions.
- Keep responses concise — 2-4 sentences max for simple queries, up to 6 for complex analyses.
- NEVER say "you would need additional data" or "the data doesn't have X" if there ARE tables
  or DataFrames that contain the information. Always JOIN/merge across available data first.
- NEVER tell the user to look elsewhere for data. You ARE the analyst. Work with what's available.
- NEVER ask the user to "provide a list of industries" or "specify columns" when the data is
  already loaded. Look at the columns and values yourself and analyze them.
- If the user asks about industries, categories, regions — CHECK THE DATA FIRST.
  The columns and sample values are in the context. Use them.

{context}
"""


async def _respond(state: AgentState, llm: BaseChatModel) -> AgentState:
    """Generate a natural-language response summarising the analysis."""
    context_parts: list[str] = []
    if state.get("sql_query"):
        context_parts.append(f"SQL executed:\n{state['sql_query']}")
    if state.get("execution_result"):
        context_parts.append(f"Execution output:\n{state['execution_result']}")
    if state.get("table_data"):
        preview = json.dumps(state["table_data"], default=str)[:2000]
        context_parts.append(f"Table data (preview):\n{preview}")
    if state.get("error"):
        error = state["error"]
        technical_keywords = (
            "UNION", "SYNTAX", "COLUMN", "RELATION", "CAST", "TYPE",
            "TRACEBACK", "FILE \"", "LINE ", "VALUEERROR", "KEYERROR",
            "INDEXERROR", "TYPEERROR", "ATTRIBUTEERROR", "IMPORTERROR",
            "MODULENOTFOUNDERROR", "LENGTH MISMATCH", "SETATTR",
        )
        if any(kw in error.upper() for kw in technical_keywords):
            context_parts.append(
                "Note: There was a technical issue executing the analysis. "
                "Do NOT show any traceback or error details to the user. "
                "Instead, explain in simple terms that the analysis encountered "
                "an issue and suggest they try rephrasing their question or "
                "asking for a simpler analysis."
            )
        else:
            context_parts.append(f"Error:\n{error}")

    context = "\n\n".join(context_parts) if context_parts else ""

    messages = [
        SystemMessage(content=_RESPONSE_SYSTEM_PROMPT.format(context=context)),
        *state["messages"],
    ]

    response = await llm.ainvoke(messages)
    content: str = response.content  # type: ignore[assignment]

    return {
        **state,
        "messages": [AIMessage(content=content)],
    }


def _after_router(state: AgentState) -> str:
    return state["next_action"]


def _after_validate(state: AgentState) -> str:
    """After SQL validation: retry on error, otherwise proceed to execute."""
    if state.get("error") and state.get("retry_count", 0) < _MAX_RETRIES:
        return "sql_agent"
    return "sql_execute"


def _after_sql_execute(state: AgentState) -> str:
    """After SQL execution: try repair first, then retry sql_agent if repair fails."""
    if state.get("error") and state.get("retry_count", 0) < _MAX_RETRIES:
        retry = state.get("retry_count", 0)
        if retry == 0:
            return "repair_sql"
        return "sql_agent"
    return "verify_results"


def _after_verify(state: AgentState) -> str:
    """After result verification: retry on error, chain to Python for viz, or respond."""
    if state.get("error") and state.get("retry_count", 0) < _MAX_RETRIES:
        return "sql_agent"
    if state.get("next_action") == "sql_then_viz" and not state.get("error"):
        return "python_agent"
    return "respond"


def _after_validate_python(state: AgentState) -> str:
    """After Python validation: retry generation on error, otherwise execute."""
    if state.get("error") and state.get("retry_count", 0) < _MAX_RETRIES:
        return "python_agent"
    return "code_execute"


def _after_code_execute(state: AgentState) -> str:
    if state.get("error") and state.get("retry_count", 0) < _MAX_RETRIES:
        retry = state.get("retry_count", 0)
        if retry <= 1:
            return "repair_python"  # surgical fix on first errors
        return "python_agent"      # full regeneration after that
    return "respond"


def build_graph(llm: BaseChatModel, db: AsyncSession, llm_light: BaseChatModel | None = None) -> StateGraph:
    """Construct and return the compiled LangGraph agent.

    Two model tiers:
      llm       — heavy (gemini-3-flash): SQL, Python, analyst, respond, decomposer
      llm_light — light (gemini-3.1-flash-lite): router, verifier, repair, suggestions
    """
    _light = llm_light or llm

    async def router_node(state: AgentState) -> AgentState:
        return await route_query(state, _light)

    async def sql_agent_node(state: AgentState) -> AgentState:
        return await generate_sql(state, llm)

    async def python_agent_node(state: AgentState) -> AgentState:
        # Reset retry_count when entering Python from sql_then_viz so Python
        # gets its own retry budget independent of SQL retries.
        if state.get("next_action") == "sql_then_viz" and state.get("retry_count", 0) > 0:
            state = {**state, "retry_count": 0, "error": None}
        return await generate_python(state, llm)

    async def validate_node(state: AgentState) -> AgentState:
        return validate_sql(state)

    async def sql_execute_node(state: AgentState) -> AgentState:
        return await execute_sql(state, db)

    async def verify_node(state: AgentState) -> AgentState:
        return await verify_results(state, _light)

    async def validate_python_node(state: AgentState) -> AgentState:
        return validate_python(state)

    async def code_execute_node(state: AgentState) -> AgentState:
        return await execute_code(state)

    async def repair_python_node(state: AgentState) -> AgentState:
        return await repair_python(state, _light)

    async def disambiguator_node(state: AgentState) -> AgentState:
        return disambiguate(state)

    async def respond_node(state: AgentState) -> AgentState:
        return await _respond(state, llm)

    async def repair_node(state: AgentState) -> AgentState:
        return await repair_sql(state, _light)

    async def analyst_node(state: AgentState) -> AgentState:
        return await run_analyst(state, llm, db)

    graph = StateGraph(AgentState)

    graph.add_node("router", router_node)
    graph.add_node("sql_agent", sql_agent_node)
    graph.add_node("validate_sql", validate_node)
    graph.add_node("python_agent", python_agent_node)
    graph.add_node("validate_python", validate_python_node)
    graph.add_node("sql_execute", sql_execute_node)
    graph.add_node("verify_results", verify_node)
    graph.add_node("code_execute", code_execute_node)
    graph.add_node("repair_python", repair_python_node)
    graph.add_node("disambiguator", disambiguator_node)
    graph.add_node("respond", respond_node)
    graph.add_node("repair_sql", repair_node)
    graph.add_node("analyst", analyst_node)

    graph.set_entry_point("router")

    graph.add_conditional_edges(
        "router",
        _after_router,
        {
            "sql": "disambiguator",       # → disambiguator first, then sql_agent
            "python": "python_agent",
            "sql_then_viz": "disambiguator",  # → disambiguator first
            "analyze": "analyst",
            "respond": "respond",
            "error": "respond",
        },
    )

    # Disambiguator: if ambiguous → stop (END), else → sql_agent
    def _after_disambiguator(state: AgentState) -> str:
        if state.get("disambiguation"):
            return "respond"  # Pipeline stops — frontend shows disambiguation UI
        return "sql_agent"

    graph.add_conditional_edges(
        "disambiguator",
        _after_disambiguator,
        {"respond": "respond", "sql_agent": "sql_agent"},
    )

    graph.add_edge("analyst", "respond")

    graph.add_edge("sql_agent", "validate_sql")
    graph.add_conditional_edges(
        "validate_sql",
        _after_validate,
        {"sql_agent": "sql_agent", "sql_execute": "sql_execute"},
    )
    graph.add_conditional_edges(
        "sql_execute",
        _after_sql_execute,
        {"repair_sql": "repair_sql", "sql_agent": "sql_agent", "verify_results": "verify_results"},
    )
    graph.add_edge("repair_sql", "sql_execute")
    graph.add_conditional_edges(
        "verify_results",
        _after_verify,
        {"sql_agent": "sql_agent", "python_agent": "python_agent", "respond": "respond"},
    )

    graph.add_edge("python_agent", "validate_python")
    graph.add_conditional_edges(
        "validate_python",
        _after_validate_python,
        {"python_agent": "python_agent", "code_execute": "code_execute"},
    )
    graph.add_conditional_edges(
        "code_execute",
        _after_code_execute,
        {
            "repair_python": "repair_python",
            "python_agent": "python_agent",
            "respond": "respond",
        },
    )
    graph.add_edge("repair_python", "code_execute")

    graph.add_edge("respond", END)

    return graph


async def _run_cross_db_query(
    query: str,
    connection_ids: list[str],
    schema_context: str,
    llm: BaseChatModel,
    db: AsyncSession,
    file_ids: list[str] | None = None,
) -> AsyncGenerator[dict[str, Any], None]:
    """Run a cross-source query — databases + files, parallel execution, join results."""
    from app.agents.crossdb import (
        load_all_schemas, plan_cross_db_query, execute_parallel_queries, join_results,
    )

    source_label = "databases" if not file_ids else "sources"
    yield {"type": "status", "content": f"Loading schemas from all {source_label}..."}

    multi_schema = await load_all_schemas(connection_ids, db, file_ids=file_ids)
    available = multi_schema.get_available_connections()

    if not available:
        yield {"type": "error", "content": "No databases are reachable. Check your connections."}
        return

    yield {"type": "status", "content": f"Connected to {len(available)} databases. Planning query..."}

    plan = await plan_cross_db_query(query, multi_schema, llm)

    if not plan.queries:
        yield {"type": "error", "content": "Could not plan a query across your databases. Try rephrasing."}
        return

    if plan.is_single_db and len(plan.queries) == 1:
        yield {"type": "status", "content": f"Query targets {plan.queries[0].connection_name} only."}
        yield {"type": "sql", "content": plan.queries[0].sql}

    yield {"type": "status", "content": f"Executing {len(plan.queries)} queries in parallel..."}

    results = await execute_parallel_queries(plan, db)

    for alias, result in results.items():
        if result.success:
            yield {"type": "status", "content": f"✓ {result.connection_name}: {result.row_count} rows ({result.execution_ms}ms)"}
        else:
            yield {"type": "status", "content": f"✗ {result.connection_name}: {result.error}"}

    if len(plan.joins) > 0:
        yield {"type": "status", "content": "Joining results across databases..."}

    joined = join_results(results, plan)

    if joined.get("table_data"):
        yield {"type": "table", "content": joined["table_data"]}

    for warning in joined.get("warnings", []):
        yield {"type": "status", "content": f"⚠ {warning}"}

    table_preview = ""
    td = joined.get("table_data", {})
    if td.get("rows"):
        import json
        table_preview = json.dumps(td["rows"][:10], default=str)[:1000]

    response_prompt = (
        f"User asked: {query}\n\n"
        f"Query plan: {plan.explanation}\n"
        f"Results from {len(results)} databases:\n{joined.get('execution_summary', '')}\n"
        f"Final joined data ({td.get('total_rows', 0)} rows): {table_preview}"
    )

    from langchain_core.messages import SystemMessage as SM, HumanMessage as HM
    resp = await llm.ainvoke([
        SM(content="You are Ceaser, a data analyst. Summarize the cross-database query results. Reference specific numbers. Keep it concise."),
        HM(content=response_prompt),
    ])
    yield {"type": "text", "content": resp.content}


async def _run_single_query(
    compiled: Any,
    query: str,
    connection_id: str | None,
    file_id: str | None,
    schema_context: str,
    history_messages: list,
    timeout_seconds: int = 60,
    db: AsyncSession | None = None,
    llm: BaseChatModel | None = None,
) -> AsyncGenerator[dict[str, Any], None]:
    """Run a single query through the compiled graph and yield stream chunks."""

    # ── Verified query fast-path ────────────────────────────────────
    # If a matching verified query exists, skip the entire LLM pipeline
    # and execute the proven SQL directly.
    if db and connection_id:
        try:
            import uuid as _uuid
            from app.services.verified_queries import find_matching_verified_query
            verified = await find_matching_verified_query(
                db, query, _uuid.UUID(connection_id),
                org_id="",  # org_id is checked at the chat endpoint level
            )
            if verified:
                yield {"type": "status", "content": "Using a verified query pattern"}
                yield {"type": "sql", "content": verified.sql_template}
                yield {"type": "verified", "content": str(verified.id)}

                # Execute directly
                from app.agents.executor import execute_sql
                exec_state: AgentState = {
                    "messages": [],
                    "query": query,
                    "connection_id": connection_id,
                    "file_id": None,
                    "schema_context": "",
                    "sql_query": verified.sql_template,
                    "code_block": None,
                    "execution_result": None,
                    "table_data": None,
                    "plotly_figure": None,
                    "error": None,
                    "retry_count": 0,
                    "next_action": "",
                    "query_reasoning": f"Using verified query (used {verified.use_count} times)",
                    "confidence": "high",
                    "analysis_type": "",
                }
                result_state = await execute_sql(exec_state, db)

                if result_state.get("table_data"):
                    yield {"type": "table", "content": result_state["table_data"]}
                    yield {"type": "confidence", "content": "high"}
                    yield {"type": "reasoning", "content": f"Using a verified query pattern (used {verified.use_count} times successfully)"}

                if result_state.get("error"):
                    # Verified query failed — deactivate it and fall through to normal pipeline
                    verified.is_active = False
                    await db.flush()
                    logger.warning("Verified query failed, deactivating: %s", verified.id)
                    yield {"type": "status", "content": "Verified query outdated, regenerating..."}
                else:
                    # Generate response using LLM
                    if llm:
                        resp_state = await _respond(result_state, llm)
                        for msg in resp_state.get("messages", []):
                            if isinstance(msg, AIMessage):
                                yield {"type": "text", "content": msg.content}
                    else:
                        yield {"type": "text", "content": result_state.get("execution_result", "")}
                    await db.commit()
                    return
        except Exception as exc:
            logger.debug("Verified query lookup skipped: %s", exc)

    # ── Normal LangGraph pipeline ───────────────────────────────────
    messages = list(history_messages) + [HumanMessage(content=query)]

    initial_state: AgentState = {
        "messages": messages,
        "query": query,
        "connection_id": connection_id,
        "file_id": file_id,
        "schema_context": schema_context,
        "sql_query": None,
        "code_block": None,
        "execution_result": None,
        "table_data": None,
        "plotly_figure": None,
        "error": None,
        "retry_count": 0,
        "next_action": "",
        "query_reasoning": "",
        "confidence": "",
        "analysis_type": "",
        "disambiguation": None,
        "disambiguation_resolution": "",
    }

    import asyncio as _asyncio
    import time as _time

    final_state: AgentState | None = None
    start_time = _time.monotonic()

    async for event in compiled.astream(initial_state, stream_mode="updates"):
        if _time.monotonic() - start_time > timeout_seconds:
            yield {"type": "error", "content": "Analysis timed out. Try a simpler query."}
            return
        for node_name, node_state in event.items():
            logger.debug("Node '%s' completed.", node_name)

            if node_name == "router":
                action = node_state.get("next_action", "")
                yield {"type": "status", "content": f"Decided to use: {action}"}

            elif node_name == "disambiguator":
                if node_state.get("disambiguation"):
                    yield {"type": "disambiguation", "content": "", "data": node_state["disambiguation"]}
                    return  # Stop pipeline — waiting for user input

            elif node_name == "sql_agent":
                sql = node_state.get("sql_query")
                if sql:
                    yield {"type": "sql", "content": sql}
                reasoning = node_state.get("query_reasoning")
                if reasoning:
                    yield {"type": "reasoning", "content": reasoning}

            elif node_name == "validate_sql":
                if node_state.get("error"):
                    yield {"type": "status", "content": f"SQL validation issue: {node_state['error'][:100]}"}

            elif node_name == "sql_execute":
                if node_state.get("error"):
                    yield {"type": "status", "content": f"SQL error, retrying... ({node_state.get('retry_count', 0)}/{_MAX_RETRIES})"}
                elif node_state.get("table_data"):
                    yield {"type": "table", "content": node_state["table_data"]}

            elif node_name == "verify_results":
                if node_state.get("error"):
                    yield {"type": "status", "content": "Verifying results... re-trying for better accuracy"}
                confidence = node_state.get("confidence")
                if confidence:
                    yield {"type": "confidence", "content": confidence}

            elif node_name == "repair_sql":
                yield {"type": "status", "content": "Fixing query..."}

            elif node_name == "analyst":
                yield {"type": "status", "content": "Running deep analysis..."}
                if node_state.get("code_block"):
                    yield {"type": "code", "content": node_state["code_block"]}
                if node_state.get("plotly_figure"):
                    yield {"type": "plotly", "content": node_state["plotly_figure"]}
                if node_state.get("table_data"):
                    yield {"type": "table", "content": node_state["table_data"]}

            elif node_name == "python_agent":
                code = node_state.get("code_block")
                if code:
                    yield {"type": "code", "content": code}

            elif node_name == "validate_python":
                if node_state.get("error"):
                    yield {"type": "status", "content": f"Code validation issue, regenerating... ({node_state.get('retry_count', 0)}/{_MAX_RETRIES})"}

            elif node_name == "repair_python":
                yield {"type": "status", "content": "Fixing code..."}

            elif node_name == "code_execute":
                if node_state.get("error"):
                    yield {"type": "status", "content": f"Code error, retrying... ({node_state.get('retry_count', 0)}/{_MAX_RETRIES})"}
                else:
                    if node_state.get("plotly_figure"):
                        yield {"type": "plotly", "content": node_state["plotly_figure"]}
                    if node_state.get("table_data") and node_state["table_data"] != initial_state.get("table_data"):
                        yield {"type": "table", "content": node_state["table_data"]}

            elif node_name == "respond":
                msgs = node_state.get("messages", [])
                for msg in msgs:
                    if isinstance(msg, AIMessage):
                        yield {"type": "text", "content": msg.content}

            final_state = node_state

    if final_state and final_state.get("error"):
        error = final_state["error"]
        raw_traceback_markers = ("Traceback", "File \"", "  File ", "SyntaxError", "IndentationError", "ModuleNotFoundError")
        if any(marker in error for marker in raw_traceback_markers):
            yield {"type": "error", "content": "The analysis encountered a technical issue. Try rephrasing your question or asking for a simpler analysis."}
        else:
            yield {"type": "error", "content": error}


async def run_agent(
    *,
    query: str,
    connection_id: str | None,
    connection_ids: list[str] | None = None,
    file_id: str | None,
    schema_context: str,
    llm: BaseChatModel,
    llm_light: BaseChatModel | None = None,
    db: AsyncSession,
    history: list[dict[str, str]] | None = None,
) -> AsyncGenerator[dict[str, Any], None]:
    """Run the analysis agent, handling compound queries via decomposition.

    If the user asks multiple independent questions in one message, the
    decomposer splits them and each sub-query is executed separately.
    """
    _light = llm_light or llm
    graph = build_graph(llm, db, llm_light=_light)
    compiled = graph.compile()

    history_messages: list = []
    for msg in (history or []):
        if msg["role"] == "user":
            history_messages.append(HumanMessage(content=msg["content"]))
        else:
            history_messages.append(AIMessage(content=msg["content"]))

    yield {"type": "status", "content": "Analysing your question..."}

    if connection_ids and len(connection_ids) > 1:
        yield {"type": "status", "content": "Multi-database query mode..."}
        async for chunk in _run_cross_db_query(query, connection_ids, schema_context, llm, db):
            yield chunk
        try:
            from app.agents.suggestions import generate_follow_up_suggestions
            follow_ups = await generate_follow_up_suggestions(
                schema_context=schema_context,
                conversation_history=history or [],
                last_question=query,
                last_answer="",
                llm=_light,
            )
        except Exception:
            follow_ups = []
        yield {"type": "suggestions", "content": "", "data": follow_ups}
        return

    sub_queries = await decompose_query(query, _light)

    all_texts: list[str] = []

    if len(sub_queries) == 1:
        async for chunk in _run_single_query(
            compiled, query, connection_id, file_id, schema_context, history_messages,
            db=db, llm=llm,
        ):
            if chunk.get("type") == "text":
                all_texts.append(chunk.get("content", ""))
            yield chunk
    else:
        yield {"type": "status", "content": f"Breaking into {len(sub_queries)} parts..."}

        for i, sub_q in enumerate(sub_queries, 1):
            yield {"type": "status", "content": f"Part {i}/{len(sub_queries)}: {sub_q}"}

            sub_text = ""
            async for chunk in _run_single_query(
                compiled, sub_q, connection_id, file_id, schema_context, history_messages,
                db=db, llm=llm,
            ):
                if chunk["type"] in ("table", "plotly", "sql", "code", "chart"):
                    yield chunk
                elif chunk["type"] == "text":
                    sub_text = chunk["content"]
                elif chunk["type"] == "error":
                    sub_text = f"Error: {chunk['content']}"

            if sub_text:
                all_texts.append(f"**{sub_q}**\n{sub_text}")

        if all_texts:
            yield {"type": "text", "content": "\n\n---\n\n".join(all_texts)}

    try:
        from app.agents.suggestions import generate_follow_up_suggestions
        follow_ups = await generate_follow_up_suggestions(
            schema_context=schema_context,
            conversation_history=history or [],
            last_question=query,
            last_answer=all_texts[-1] if all_texts else "",
            llm=_light,
        )
    except Exception:
        follow_ups = []

    yield {"type": "suggestions", "content": "", "data": follow_ups}
