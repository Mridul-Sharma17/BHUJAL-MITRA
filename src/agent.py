from __future__ import annotations

import os
from typing import Any, Dict, List, Optional, Sequence, Tuple

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole, QueryEndpointResponse
from databricks.vector_search.client import VectorSearchClient
from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import DateType, NumericType, StringType, TimestampType

try:
    from langchain_core.prompts import ChatPromptTemplate
except ImportError:
    try:
        from langchain.prompts import ChatPromptTemplate
    except ImportError:
        ChatPromptTemplate = None


CATALOG = os.getenv("BHUJAL_CATALOG", "iitb")
SCHEMA = os.getenv("BHUJAL_SCHEMA", "bharat_bricks")

VECTOR_ENDPOINT_NAME = os.getenv("BHUJAL_VECTOR_ENDPOINT", "bhujal_mitra_endpoint")
VECTOR_INDEX_NAME = os.getenv(
    "BHUJAL_VECTOR_INDEX", f"{CATALOG}.{SCHEMA}.bhujal_mitra_policy_index"
)
FORECAST_TABLE = os.getenv(
    "BHUJAL_FORECAST_TABLE", f"{CATALOG}.{SCHEMA}.groundwater_prediction_gold"
)
LLM_ENDPOINT_NAME = os.getenv(
    "BHUJAL_LLM_ENDPOINT", "databricks-qwen3-next-80b-a3b-instruct"
)

SYSTEM_PROMPT = """
You are Bhujal Mitra, a groundwater policy and planning advisor for Maharashtra.
Use the provided policy chunks and forecast rows as your primary context.
If a context block is missing or incomplete, say so briefly and still provide safe, practical guidance.

Output format requirements:
1. Section title: English Advice
2. Section title: Marathi Advice
3. 4 to 6 concise bullet points in each section
4. Mention at least one policy source in both sections when available
""".strip()

USER_PROMPT_TEMPLATE = """
User query:
{user_query}

District:
{district_name}

Policy context (top retrieved chunks):
{policy_context}

Forecast context (30-day window):
{forecast_context}

Retrieval diagnostics:
{diagnostics}
""".strip()


def _normalize(name: str) -> str:
    return "".join(ch for ch in name.lower() if ch.isalnum())


def _find_column(columns: Sequence[str], candidates: Sequence[str]) -> Optional[str]:
    normalized_map = {_normalize(col): col for col in columns}
    for candidate in candidates:
        matched = normalized_map.get(_normalize(candidate))
        if matched:
            return matched

    candidate_tokens = [_normalize(c) for c in candidates]
    for col in columns:
        col_norm = _normalize(col)
        if any(token in col_norm for token in candidate_tokens):
            return col
    return None


def _get_spark() -> SparkSession:
    spark = SparkSession.getActiveSession()
    if spark is None:
        spark = SparkSession.builder.getOrCreate()
    return spark


def _parse_similarity_response(response: Dict[str, Any]) -> List[Dict[str, Any]]:
    manifest = response.get("manifest") or {}
    columns_meta = manifest.get("columns") or []
    column_names: List[str] = []

    for item in columns_meta:
        if isinstance(item, dict):
            column_names.append(str(item.get("name", "")))
        else:
            column_names.append(str(getattr(item, "name", "")))

    data_rows = (response.get("result") or {}).get("data_array") or []
    parsed_rows: List[Dict[str, Any]] = []
    for raw_row in data_rows:
        if isinstance(raw_row, dict):
            parsed_rows.append(raw_row)
            continue

        if isinstance(raw_row, list):
            row_dict: Dict[str, Any] = {}
            for idx, value in enumerate(raw_row):
                key = column_names[idx] if idx < len(column_names) and column_names[idx] else f"col_{idx}"
                row_dict[key] = value
            parsed_rows.append(row_dict)
    return parsed_rows


def _retrieve_policy_chunks(user_query: str, top_k: int = 3) -> List[Dict[str, Any]]:
    vector_client = VectorSearchClient()
    index = vector_client.get_index(
        endpoint_name=VECTOR_ENDPOINT_NAME,
        index_name=VECTOR_INDEX_NAME,
    )

    candidate_column_sets = [
        ["chunk_id", "source_name", "chunk_text", "path", "chunk_index"],
        ["chunk_id", "source_name", "chunk_text"],
        ["chunk_id", "chunk_text"],
        ["chunk_text"],
    ]

    last_error: Optional[Exception] = None
    for columns in candidate_column_sets:
        try:
            results = index.similarity_search(
                columns=columns,
                query_text=user_query,
                num_results=top_k,
            )
            rows = _parse_similarity_response(results)
            if rows:
                return rows[:top_k]
        except Exception as exc:  # pragma: no cover - service exceptions vary by runtime
            last_error = exc

    if last_error:
        raise RuntimeError(f"Vector search retrieval failed: {last_error}") from last_error
    return []


def _format_policy_context(policy_rows: List[Dict[str, Any]]) -> str:
    if not policy_rows:
        return "No policy chunks were retrieved from the vector index."

    lines: List[str] = []
    for idx, row in enumerate(policy_rows, start=1):
        source = (
            row.get("source_name")
            or row.get("path")
            or row.get("source")
            or row.get("chunk_id")
            or "unknown_source"
        )
        text = row.get("chunk_text") or row.get("text") or row.get("content") or ""
        text_str = " ".join(str(text).split())
        if len(text_str) > 420:
            text_str = text_str[:417] + "..."

        score = row.get("score")
        score_part = f" | score={float(score):.4f}" if isinstance(score, (int, float)) else ""
        lines.append(f"- [{idx}] source={source}{score_part} | excerpt={text_str}")

    return "\n".join(lines)


def _resolve_date_column(df: DataFrame) -> Optional[str]:
    explicit = _find_column(
        df.columns,
        ["forecast_date", "date", "datetime", "prediction_date", "ds"],
    )
    if explicit:
        return explicit

    for field in df.schema.fields:
        if isinstance(field.dataType, (DateType, TimestampType)):
            return field.name
    return None


def _resolve_district_column(df: DataFrame) -> Optional[str]:
    return _find_column(
        df.columns,
        ["district_name", "district", "district_id", "district_encoded"],
    )


def _resolve_prediction_columns(df: DataFrame, max_cols: int = 3) -> List[str]:
    preferred = [
        "predicted_groundwater_level",
        "prediction",
        "forecast_value",
        "forecast",
        "yhat",
        "groundwater_level_forecast",
        "groundwater_prediction",
    ]
    selected: List[str] = []
    for name in preferred:
        col = _find_column(df.columns, [name])
        if col and col not in selected:
            selected.append(col)
        if len(selected) >= max_cols:
            return selected

    blocked = {
        _normalize(name)
        for name in ["time_idx", "month", "day_of_year", "step_ahead_days", "date_ord"]
    }
    for field in df.schema.fields:
        if not isinstance(field.dataType, NumericType):
            continue
        if _normalize(field.name) in blocked:
            continue
        if field.name not in selected:
            selected.append(field.name)
        if len(selected) >= max_cols:
            break

    return selected


def _retrieve_forecast_rows(
    district_name: str,
    horizon_days: int = 30,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    spark = _get_spark()
    df = spark.table(FORECAST_TABLE)

    metadata: Dict[str, Any] = {
        "table": FORECAST_TABLE,
        "district_filter": None,
        "date_column": None,
        "prediction_columns": [],
    }

    district_col = _resolve_district_column(df)
    date_col = _resolve_date_column(df)
    prediction_cols = _resolve_prediction_columns(df)

    metadata["district_filter"] = district_col
    metadata["date_column"] = date_col
    metadata["prediction_columns"] = prediction_cols

    filtered_df = df
    district_lower = district_name.strip().lower()
    if district_col:
        filtered_df = filtered_df.filter(F.lower(F.trim(F.col(district_col))) == district_lower)
    else:
        # Fallback for schemas that do not carry a district column.
        string_cols = [
            field.name
            for field in df.schema.fields
            if isinstance(field.dataType, StringType)
            and any(token in field.name.lower() for token in ("station", "location", "taluka", "block", "site"))
        ]
        if string_cols:
            contains_expr = None
            for col_name in string_cols:
                term = F.lower(F.col(col_name)).contains(district_lower)
                contains_expr = term if contains_expr is None else (contains_expr | term)
            candidate_df = filtered_df.filter(contains_expr)
            if candidate_df.limit(1).count() > 0:
                filtered_df = candidate_df
                metadata["district_filter"] = ",".join(string_cols)
                metadata["district_filter_mode"] = "substring"

    if filtered_df.limit(1).count() == 0:
        metadata["note"] = "No forecast rows matched the district filter."
        return [], metadata

    if date_col:
        filtered_df = (
            filtered_df
            .withColumn("_forecast_date", F.to_date(F.col(date_col)))
            .filter(F.col("_forecast_date").isNotNull())
        )
        future_df = filtered_df.filter(
            (F.col("_forecast_date") >= F.current_date())
            & (F.col("_forecast_date") <= F.date_add(F.current_date(), horizon_days))
        )
        if future_df.limit(1).count() > 0:
            filtered_df = future_df
            metadata["date_window"] = "current_to_horizon"
        else:
            metadata["date_window"] = "latest_available"
    else:
        metadata["date_window"] = "unordered"

    if not prediction_cols:
        metadata["note"] = "Could not infer numeric prediction columns in forecast table."
        return [], metadata

    select_exprs = []
    if date_col:
        select_exprs.append(F.col("_forecast_date").alias("forecast_date"))
    if district_col:
        select_exprs.append(F.col(district_col).alias("district_name"))

    for col_name in prediction_cols:
        select_exprs.append(F.col(col_name))

    for optional_col in ["station_id", "step_ahead_days", "model_name", "generated_at"]:
        if optional_col in df.columns and optional_col not in prediction_cols and optional_col != district_col:
            select_exprs.append(F.col(optional_col))

    projected_df = filtered_df.select(*select_exprs)

    if date_col and metadata["date_window"] == "latest_available":
        records = projected_df.orderBy(F.col("forecast_date").desc()).limit(horizon_days).collect()
        records = list(reversed(records))
    elif date_col:
        records = projected_df.orderBy(F.col("forecast_date").asc()).limit(horizon_days).collect()
    else:
        records = projected_df.limit(horizon_days).collect()

    output = [row.asDict(recursive=True) for row in records]
    metadata["rows_returned"] = len(output)
    return output, metadata


def _format_forecast_context(
    district_name: str,
    forecast_rows: List[Dict[str, Any]],
    metadata: Dict[str, Any],
) -> str:
    if not forecast_rows:
        reason = metadata.get("note", "No rows were returned from the forecast table query.")
        return (
            f"No 30-day forecast rows found for district '{district_name}' in table {FORECAST_TABLE}. "
            f"Reason: {reason}"
        )

    pred_cols = metadata.get("prediction_columns", [])
    lines = [f"Forecast table: {metadata.get('table', FORECAST_TABLE)}"]
    if metadata.get("district_filter"):
        lines.append(f"Applied district filter on: {metadata['district_filter']}")
    if metadata.get("date_column"):
        lines.append(f"Date column used: {metadata['date_column']} ({metadata.get('date_window', 'unknown')})")

    for row in forecast_rows:
        date_value = row.get("forecast_date")
        date_text = str(date_value) if date_value is not None else "NA"

        metrics: List[str] = []
        for col_name in pred_cols:
            value = row.get(col_name)
            if value is None:
                continue
            if isinstance(value, float):
                metrics.append(f"{col_name}: {value:.3f}")
            else:
                metrics.append(f"{col_name}: {value}")

        station = row.get("station_id")
        station_part = f" | station_id: {station}" if station else ""
        lines.append(f"- {date_text} | {', '.join(metrics)}{station_part}")

    return "\n".join(lines)


def _build_databricks_messages(
    user_query: str,
    district_name: str,
    policy_context: str,
    forecast_context: str,
    diagnostics: str,
) -> List[ChatMessage]:
    if ChatPromptTemplate is not None:
        prompt_template = ChatPromptTemplate.from_messages(
            [
                ("system", SYSTEM_PROMPT),
                ("human", USER_PROMPT_TEMPLATE),
            ]
        )
        langchain_messages = prompt_template.format_messages(
            user_query=user_query,
            district_name=district_name,
            policy_context=policy_context,
            forecast_context=forecast_context,
            diagnostics=diagnostics,
        )

        dbx_messages: List[ChatMessage] = []
        for msg in langchain_messages:
            msg_type = getattr(msg, "type", "user").lower()
            if msg_type == "system":
                role = ChatMessageRole.SYSTEM
            elif msg_type in {"assistant", "ai"}:
                role = ChatMessageRole.ASSISTANT
            else:
                role = ChatMessageRole.USER
            dbx_messages.append(ChatMessage(role=role, content=str(msg.content)))
        return dbx_messages

    rendered_user_prompt = USER_PROMPT_TEMPLATE.format(
        user_query=user_query,
        district_name=district_name,
        policy_context=policy_context,
        forecast_context=forecast_context,
        diagnostics=diagnostics,
    )
    return [
        ChatMessage(role=ChatMessageRole.SYSTEM, content=SYSTEM_PROMPT),
        ChatMessage(role=ChatMessageRole.USER, content=rendered_user_prompt),
    ]


def _extract_llm_text(response: QueryEndpointResponse) -> str:
    if response.choices:
        for choice in response.choices:
            if choice.message and choice.message.content:
                return choice.message.content.strip()
            if choice.text:
                return choice.text.strip()

    for field_name in ["predictions", "outputs"]:
        values = getattr(response, field_name, None)
        if not values:
            continue

        first = values[0]
        if isinstance(first, str):
            return first.strip()
        if isinstance(first, dict):
            for key in ["text", "generated_text", "prediction", "content", "answer"]:
                value = first.get(key)
                if isinstance(value, str) and value.strip():
                    return value.strip()
            return str(first)
        return str(first)

    return ""


def _query_llm(messages: List[ChatMessage]) -> str:
    workspace_client = WorkspaceClient()
    response = workspace_client.serving_endpoints.query(
        name=LLM_ENDPOINT_NAME,
        messages=messages,
        temperature=0.2,
        max_tokens=700,
    )
    answer = _extract_llm_text(response)
    if not answer:
        raise RuntimeError("LLM endpoint returned an empty response payload.")
    return answer


def get_bhujal_advice(user_query: str, district_name: str) -> str:
    """
    Build a district-aware groundwater advisory using:
    1) Top-3 policy chunks from Databricks Vector Search
    2) Up to 30 rows from the district forecast table
    3) A Databricks LLM endpoint response in English + Marathi
    """
    query = (user_query or "").strip()
    district = (district_name or "").strip()

    if not query:
        raise ValueError("user_query cannot be empty.")
    if not district:
        raise ValueError("district_name cannot be empty.")

    diagnostics: List[str] = []

    try:
        policy_rows = _retrieve_policy_chunks(query, top_k=3)
    except Exception as exc:  # pragma: no cover - service exceptions vary by runtime
        policy_rows = []
        diagnostics.append(f"Policy retrieval warning: {exc}")

    try:
        forecast_rows, forecast_meta = _retrieve_forecast_rows(district, horizon_days=30)
    except Exception as exc:  # pragma: no cover - service exceptions vary by runtime
        forecast_rows, forecast_meta = [], {"note": str(exc)}
        diagnostics.append(f"Forecast retrieval warning: {exc}")

    policy_context = _format_policy_context(policy_rows)
    forecast_context = _format_forecast_context(district, forecast_rows, forecast_meta)
    diagnostics_text = "\n".join(f"- {item}" for item in diagnostics) if diagnostics else "- none"

    messages = _build_databricks_messages(
        user_query=query,
        district_name=district,
        policy_context=policy_context,
        forecast_context=forecast_context,
        diagnostics=diagnostics_text,
    )
    return _query_llm(messages)


__all__ = ["get_bhujal_advice"]


if __name__ == "__main__":
    sample = get_bhujal_advice(
        user_query="What should farmers do this month to reduce groundwater stress?",
        district_name="Pune",
    )
    print(sample)
