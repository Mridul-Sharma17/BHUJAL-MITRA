from __future__ import annotations

import csv
import os
import re
from collections import Counter
from datetime import date, datetime
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole, QueryEndpointResponse
from databricks.vector_search.client import VectorSearchClient
from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import DateType, NumericType, StringType, TimestampType

try:
    from pypdf import PdfReader
except ImportError:
    PdfReader = None

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
ADVISORY_LLM_ENDPOINT_NAME = os.getenv(
    "BHUJAL_ADVISORY_LLM_ENDPOINT",
    os.getenv("BHUJAL_LLM_ENDPOINT", "databricks-meta-llama-3-1-8b-instruct"),
)
ADVISORY_FALLBACK_ENDPOINT_NAME = os.getenv(
    "BHUJAL_ADVISORY_FALLBACK_ENDPOINT",
    "databricks-gemma-3-12b",
)
TRANSLATION_LLM_ENDPOINT_HINDI = os.getenv(
    "BHUJAL_TRANSLATION_LLM_ENDPOINT_HINDI",
    os.getenv("BHUJAL_TRANSLATION_LLM_ENDPOINT", "databricks-meta-llama-3-1-8b-instruct"),
)
TRANSLATION_LLM_ENDPOINT_MARATHI = os.getenv(
    "BHUJAL_TRANSLATION_LLM_ENDPOINT_MARATHI",
    "databricks-gemma-3-12b",
)
TRANSLATION_LLM_ENDPOINT_DEFAULT = os.getenv(
    "BHUJAL_TRANSLATION_LLM_ENDPOINT_DEFAULT",
    "databricks-meta-llama-3-1-8b-instruct",
)
TRANSLATION_FALLBACK_ENDPOINT_NAME = os.getenv(
    "BHUJAL_TRANSLATION_FALLBACK_ENDPOINT", "databricks-gemma-3-12b"
)
TRANSLATION_QUALITY_FALLBACK_ENDPOINT_NAME = os.getenv(
    "BHUJAL_TRANSLATION_QUALITY_FALLBACK_ENDPOINT",
    "databricks-qwen3-next-80b-a3b-instruct",
)
FORECAST_RETRIEVAL_MODE = os.getenv("BHUJAL_FORECAST_RETRIEVAL_MODE", "auto").strip().lower()
PROJECT_ROOT = Path(__file__).resolve().parent.parent
LOCAL_FORECAST_CSV = Path(
    os.getenv("BHUJAL_LOCAL_FORECAST_CSV", str(PROJECT_ROOT / "data" / "Maharashtra_Pune_Filtered.csv"))
)
LOCAL_POLICY_DIR = Path(os.getenv("BHUJAL_LOCAL_POLICY_DIR", str(PROJECT_ROOT / "data" / "policies")))
LOCAL_POLICY_SNIPPETS = {
    "ahmednagar_irrigation_plan_2017_gov_en.pdf": (
        "Ahmednagar irrigation planning emphasizes drought contingency, rotational irrigation, priority allocation,"
        " and groundwater extraction discipline under stress conditions."
    ),
    "ahmednagar_watershed_success_2015_niti_en.pdf": (
        "Watershed treatment, farm ponds, contour bunding, and recharge-first planning improve moisture retention"
        " and reduce borewell dependence."
    ),
    "pune_irrigation_plan_2017_gov_en.pdf": (
        "Prioritize micro-irrigation, rotation scheduling, canal-groundwater balancing, and crop-stage based"
        " irrigation planning to reduce groundwater stress in Pune district."
    ),
    "pune_disaster_management_plan_2025_gov_en.pdf": (
        "Use contingency water planning, farm-level drought preparedness, emergency irrigation prioritization,"
        " and village-level water allocation during stress periods."
    ),
    "pune_water_sustainability_2021_teri_en.pdf": (
        "Promote recharge structures, demand-side irrigation efficiency, aquifer monitoring, and participatory"
        " water budgeting for sustainable groundwater use."
    ),
    "maharashtra_dynamic_groundwater_2009_gsda_en.pdf": (
        "Groundwater blocks show seasonal depletion; regulations recommend recharge planning, extraction limits,"
        " and improved irrigation efficiency in stressed areas."
    ),
    "maharashtra_dynamic_groundwater_2012_gsda_en.pdf": (
        "Aquifer stress trends require careful well operation, recharge enhancement, and district-level"
        " groundwater governance."
    ),
    "maharashtra_water_quality_2018_mpcb_en.pdf": (
        "Irrigation water quality risks require periodic testing, dilution management, and crop selection aligned"
        " with salinity and contamination constraints."
    ),
}

SYSTEM_PROMPT = """
You are Bhujal Mitra, a groundwater policy and planning advisor for Maharashtra.
Use the provided policy chunks and forecast rows as your primary context.
If a context block is missing or incomplete, say so briefly and still provide safe, practical guidance.

Output format requirements:
1. Respond in only one language: {response_language}
2. Write in very simple language for small farmers (short sentences, practical words, no jargon).
3. Give a step-by-step plan with numbered actions, not short generic bullets.
4. Structure the answer in 3 time windows: first 24 hours, next 7 days, next 30 days.
5. For each action, explain: what to do, how to do it, and why it helps.
6. Mention at least 2 policy sources when available (never use maharashtra_water_policy.txt).
7. When forecast values are available, cite at least 2 forecast dates and numeric values.
8. Include specific quantities or schedules wherever possible (for example timing, frequency, rough amounts).
9. Do not include a second-language section or mixed-language output.
10. Be comprehensive; do not shorten the response to save tokens.
11. Each time window must include at least 3 numbered steps.
12. Each numbered step must include three explicit sub-lines: What to do, How to do, Why it helps.
13. Make the answer district-specific; do not give generic advice that could apply to any district.
14. Start with a short "District Situation" section that analyzes groundwater/rainfall trends using the provided forecast rows.
15. Add a "Policy Evidence" section with at least 3 concrete evidence bullets, each tied to a source and a specific example from context.
16. For every numbered action, include an evidence line that references policy and/or forecast evidence used for that step.
17. If district-specific policy chunks are missing, state that clearly and use Maharashtra-level evidence explicitly.
18. Never invent source names, dates, numeric values, talukas, or historical claims that are not in the provided context.
19. Avoid vague lines like "contact authorities" unless you explain what support is expected and why it matches district evidence.
20. Keep source names exactly as provided in context.
21. Use this exact section order:
    - District Situation
    - Policy Evidence (label bullets as P1, P2, P3...)
    - Forecast Evidence (label bullets as F1, F2, F3...)
    - First 24 Hours Plan
    - Next 7 Days Plan
    - Next 30 Days Plan
22. For each numbered action, add one extra line: Evidence used: [policy tags and/or forecast tags].
23. In Policy Evidence bullets, include the exact source filename and one short quoted phrase copied from the provided excerpt text.
24. In Forecast Evidence bullets, include date + metric name + numeric value exactly from context rows.
25. All recommendations must be agriculture and groundwater specific for farmers; do not include household or urban advice.
26. Every action must mention a district-grounded reason (for example low groundwater levels, weak rainfall pattern, recharge priority in district studies).
""".strip()

TRANSLATION_SYSTEM_PROMPT_TEMPLATE = """
You are an expert agricultural translator for Maharashtra farmers.
Translate the provided advisory into {target_language} while preserving all practical details.

Strict translation requirements:
1. Keep the same structure, section order, and numbered step order.
2. Do not omit, shorten, or summarize any step.
3. Preserve all numbers, quantities, dates, units, and schedules exactly.
4. Keep every policy reference and forecast reference intact.
5. Use simple {target_language} suitable for farmers.
6. Output only {target_language} text.
""".strip()

TRANSLATION_REPAIR_PROMPT_TEMPLATE = """
You are fixing a translation that became too short.
Rewrite the advisory in {target_language} so it preserves all details from the source advisory.

Strict repair requirements:
1. Keep all sections and numbered steps from the source.
2. Do not omit any instruction, quantity, date, unit, policy reference, or forecast value.
3. Use simple {target_language} for farmers.
4. Expand any shortened steps so the {target_language} version has similar depth to the source text.
5. Output only {target_language} text.
""".strip()

USER_PROMPT_TEMPLATE = """
User query:
{user_query}

District:
{district_name}

District profile note:
{district_profile_note}

Policy context (top retrieved chunks):
{policy_context}

Forecast context (30-day window):
{forecast_context}

Retrieval diagnostics:
{diagnostics}

Response language:
{response_language}

User preference for writing style:
Detailed, step-by-step, easy for farmers, and not generic.

Strict relevance instructions:
- Use district-specific details from the provided policy and forecast context.
- Include concrete examples from policy context (for example practice names, outcomes, or observed district patterns from excerpts).
- Include groundwater analysis from forecast values (for example low/high values, direction of change, and risk implication).
- Explicitly connect each recommendation to the cited policy/forecast evidence.
- If required evidence is unavailable, say what is missing and then provide the safest practical alternative.
- Avoid generic advice that is not directly actionable on farms.
""".strip()

SUPPORTED_RESPONSE_LANGUAGES = {
    "english": "English",
    "hindi": "Hindi",
    "marathi": "Marathi",
}

SUPPORTED_RESPONSE_LANGUAGE_ALIASES = {
    "en": "english",
    "hi": "hindi",
    "mr": "marathi",
}

DISTRICT_PROFILE_NOTES = {
    "pune": (
        "Pune district has mixed urban-agri water demand, canal + groundwater dependence, "
        "and recurrent summer stress in peri-urban blocks."
    ),
    "nashik": (
        "Nashik district has horticulture-heavy demand (including vineyards), variable rainfall, "
        "and irrigation reliability risks in dry spells."
    ),
    "ahmednagar": (
        "Ahmednagar district is drought-prone with chronic groundwater stress in several talukas; "
        "prioritize conservation and recharge-focused actions."
    ),
}

DISTRICT_SOURCE_KEYWORDS = {
    "pune": ("pune", "maharashtra"),
    "nashik": ("nashik", "maharashtra"),
    "ahmednagar": ("ahmednagar", "maharashtra"),
}

_MISSING_TRANSLATION_ENDPOINTS: set[str] = set()
_UNAVAILABLE_ADVISORY_ENDPOINTS: set[str] = set()


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


def _build_vector_client() -> VectorSearchClient:
    # Reuse Databricks SDK auth so Vector Search works in local runs and Databricks Apps.
    try:
        workspace_client = WorkspaceClient()
        host = getattr(workspace_client.config, "host", None)
        token = getattr(workspace_client.config, "token", None)
        client_id = getattr(workspace_client.config, "client_id", None)
        client_secret = getattr(workspace_client.config, "client_secret", None)

        if host and token:
            return VectorSearchClient(
                workspace_url=host,
                personal_access_token=token,
            )

        if host and client_id and client_secret:
            return VectorSearchClient(
                workspace_url=host,
                service_principal_client_id=client_id,
                service_principal_client_secret=client_secret,
            )
    except Exception:
        pass

    return VectorSearchClient()


def _query_policy_index_with_workspace_client(
    user_query: str,
    columns: Sequence[str],
    top_k: int,
) -> Dict[str, Any]:
    workspace_client = WorkspaceClient()
    path = f"/api/2.0/vector-search/indexes/{VECTOR_INDEX_NAME}/query"
    body = {
        "query_text": user_query,
        "num_results": top_k,
        "columns": list(columns),
    }
    return workspace_client.api_client.do("POST", path, body=body)


def _safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        text = str(value).strip()
        if not text:
            return None
        return float(text)
    except (TypeError, ValueError):
        return None


def _parse_date_value(value: Any) -> Optional[date]:
    if value is None:
        return None

    text = str(value).strip()
    if not text:
        return None

    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%d-%m-%Y", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue

    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).date()
    except ValueError:
        return None


@lru_cache(maxsize=1)
def _load_local_policy_documents() -> List[Dict[str, str]]:
    if not LOCAL_POLICY_DIR.exists() or not LOCAL_POLICY_DIR.is_dir():
        return []

    documents: List[Dict[str, str]] = []
    seen_names = set()

    for policy_path in sorted(LOCAL_POLICY_DIR.glob("*.pdf")):
        seen_names.add(policy_path.name)
        text = ""
        try:
            if PdfReader is None:
                text = LOCAL_POLICY_SNIPPETS.get(policy_path.name, "")
            else:
                reader = PdfReader(str(policy_path))
                page_texts: List[str] = []
                for page in reader.pages:
                    page_text = page.extract_text() or ""
                    if page_text:
                        page_texts.append(page_text)
                text = "\n".join(page_texts).strip()

        except Exception:
            text = LOCAL_POLICY_SNIPPETS.get(policy_path.name, "")

        if not text:
            continue

        documents.append(
            {
                "source_name": policy_path.name,
                "path": str(policy_path),
                "text": text,
            }
        )

    # Add snippet-backed virtual docs for known files not available in runtime storage.
    for source_name, snippet in LOCAL_POLICY_SNIPPETS.items():
        if source_name in seen_names:
            continue
        if not snippet.strip():
            continue
        documents.append(
            {
                "source_name": source_name,
                "path": f"virtual://{source_name}",
                "text": snippet,
            }
        )

    return documents


def _extract_excerpt_for_terms(text: str, terms: Sequence[str], max_len: int = 520) -> str:
    clean_text = " ".join((text or "").split())
    if not clean_text:
        return ""

    lower_text = clean_text.lower()
    best_pos = -1
    for term in terms:
        if not term:
            continue
        pos = lower_text.find(term)
        if pos >= 0 and (best_pos < 0 or pos < best_pos):
            best_pos = pos

    if best_pos < 0:
        return clean_text[:max_len]

    start = max(best_pos - 160, 0)
    end = min(start + max_len, len(clean_text))
    return clean_text[start:end]


def _retrieve_policy_chunks_local_files(
    user_query: str,
    district_name: str,
    top_k: int = 3,
) -> List[Dict[str, Any]]:
    docs = _load_local_policy_documents()
    if not docs:
        return []

    district_key = (district_name or "").strip().lower()
    terms = [tok.lower() for tok in re.findall(r"\w+", user_query or "") if len(tok) >= 2]
    scored_docs: List[Tuple[int, Dict[str, str]]] = []
    for doc in docs:
        if not _is_policy_source_allowed_for_district(doc.get("source_name", ""), district_name):
            continue

        text_lower = doc["text"].lower()
        score = 0
        for term in terms[:40]:
            score += text_lower.count(term)
        if score == 0:
            # Keep policy retrieval robust for mixed-language queries.
            if any(token in text_lower for token in ("groundwater", "irrigation", "water", "recharge", "crop")):
                score = 1
        if district_key and district_key in doc.get("source_name", "").lower():
            score += 100
        scored_docs.append((score, doc))

    if not scored_docs:
        return []

    scored_docs.sort(key=lambda item: item[0], reverse=True)
    output: List[Dict[str, Any]] = []
    for score, doc in scored_docs[:max(top_k, 1)]:
        excerpt = _extract_excerpt_for_terms(doc["text"], terms)
        if not excerpt:
            continue
        output.append(
            {
                "source_name": doc["source_name"],
                "path": doc["path"],
                "chunk_text": excerpt,
                "score": float(score),
            }
        )
    return output


def _retrieve_forecast_rows_local_csv(
    district_name: str,
    horizon_days: int = 30,
    fallback_reason: Optional[str] = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    metadata: Dict[str, Any] = {
        "table": str(LOCAL_FORECAST_CSV),
        "district_filter": "local_csv_no_district_column",
        "date_column": None,
        "prediction_columns": [],
        "source": "local_csv_fallback",
    }

    if fallback_reason:
        metadata["fallback_reason"] = fallback_reason

    if not LOCAL_FORECAST_CSV.exists():
        metadata["note"] = "Local forecast CSV fallback file is not available in this runtime."
        return [], metadata

    with LOCAL_FORECAST_CSV.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        fieldnames = list(reader.fieldnames or [])
        raw_rows = list(reader)

    if not raw_rows:
        metadata["note"] = "Local forecast CSV exists but has no rows."
        return [], metadata

    date_col = _find_column(fieldnames, ["forecast_date", "date", "datetime", "prediction_date", "ds"])
    if date_col is None:
        metadata["note"] = "Local forecast CSV does not contain a usable date column."
        return [], metadata

    preferred_predictions = [
        "predicted_groundwater_level",
        "groundwater_level_target",
        "prediction",
        "forecast_value",
        "forecast",
        "yhat",
        "rainfall",
        "t2m_avg",
    ]

    prediction_cols: List[str] = []
    for col in preferred_predictions:
        matched = _find_column(fieldnames, [col])
        if matched and matched not in prediction_cols:
            prediction_cols.append(matched)
        if len(prediction_cols) >= 3:
            break

    if not prediction_cols:
        metadata["note"] = "Local forecast CSV does not contain usable numeric prediction columns."
        return [], metadata

    parsed_rows: List[Dict[str, Any]] = []
    for row in raw_rows:
        parsed_date = _parse_date_value(row.get(date_col))
        if parsed_date is None:
            continue

        entry: Dict[str, Any] = {
            "forecast_date": parsed_date.isoformat(),
            "district_name": district_name,
        }

        for col in prediction_cols:
            numeric_value = _safe_float(row.get(col))
            if numeric_value is not None:
                entry[col] = round(numeric_value, 3)

        if len(entry) > 2:
            parsed_rows.append(entry)

    if not parsed_rows:
        metadata["note"] = "Local forecast CSV rows could not be parsed into forecast records."
        return [], metadata

    parsed_rows.sort(key=lambda item: str(item.get("forecast_date", "")))
    output = parsed_rows[-max(horizon_days, 1):]

    metadata["date_column"] = date_col
    metadata["prediction_columns"] = prediction_cols
    metadata["rows_returned"] = len(output)
    metadata["date_window"] = "latest_available"
    if district_name.strip().lower() != "pune":
        metadata["note"] = (
            "Using local CSV fallback. District-specific forecast rows are unavailable in this runtime, "
            "so latest generic trend rows were used."
        )
    else:
        metadata["note"] = "Using local CSV fallback forecast rows because Spark/UC access is unavailable."

    return output, metadata


def _resolve_response_language(response_language: str) -> str:
    requested = (response_language or "").strip().lower()
    requested = SUPPORTED_RESPONSE_LANGUAGE_ALIASES.get(requested, requested)
    if requested in SUPPORTED_RESPONSE_LANGUAGES:
        return SUPPORTED_RESPONSE_LANGUAGES[requested]
    if not requested:
        return SUPPORTED_RESPONSE_LANGUAGES["english"]
    allowed = ", ".join(SUPPORTED_RESPONSE_LANGUAGES.values())
    raise ValueError(f"Unsupported response language '{response_language}'. Allowed values: {allowed}")


def _resolve_district_profile_note(district_name: str) -> str:
    district_key = (district_name or "").strip().lower()
    return DISTRICT_PROFILE_NOTES.get(
        district_key,
        "No district-specific profile note found. Use available policy and forecast context carefully.",
    )


def _is_policy_source_allowed_for_district(source_name: str, district_name: str) -> bool:
    source = (source_name or "").strip().lower()
    if not source:
        return False
    if source.endswith(".txt") or source == "maharashtra_water_policy.txt":
        return False

    district_key = (district_name or "").strip().lower()
    allowed_tokens = DISTRICT_SOURCE_KEYWORDS.get(district_key)
    if not allowed_tokens:
        return True
    return any(token in source for token in allowed_tokens)


def _is_databricks_app_runtime() -> bool:
    app_runtime_env_vars = [
        "DATABRICKS_APP_NAME",
        "DATABRICKS_APP_ID",
        "DATABRICKS_APP_DEPLOYMENT_ID",
    ]
    return any(bool(os.getenv(var_name)) for var_name in app_runtime_env_vars)


def _resolve_forecast_retrieval_mode() -> str:
    mode = (FORECAST_RETRIEVAL_MODE or "auto").strip().lower()
    if mode not in {"auto", "spark", "local_csv"}:
        mode = "auto"

    if mode == "auto" and _is_databricks_app_runtime():
        return "local_csv"
    return mode


def _count_numbered_steps(text: str) -> int:
    return len(re.findall(r"(?m)^\s*\d+[.)]\s+", text or ""))


def _has_excessive_repetition(text: str) -> bool:
    normalized = " ".join((text or "").split())
    if not normalized:
        return False

    # Detect repeating multi-word phrase loops.
    repeating_phrase_pattern = r"((?:\S+\s+){2,12}\S+)(?:\s+\1){3,}"
    if re.search(repeating_phrase_pattern, normalized, flags=re.IGNORECASE):
        return True

    words = re.findall(r"\w+", normalized, flags=re.UNICODE)
    if len(words) >= 80:
        repeated_adjacent = sum(1 for idx in range(1, len(words)) if words[idx] == words[idx - 1])
        if repeated_adjacent / max(len(words), 1) > 0.06:
            return True

        line_counts = Counter(line.strip() for line in (text or "").splitlines() if line.strip())
        if line_counts and max(line_counts.values()) >= 4:
            return True

    return False


def _translation_quality_failed(source_text: str, translated_text: str) -> bool:
    source = (source_text or "").strip()
    translated = (translated_text or "").strip()

    if not translated:
        return True

    if source and source == translated:
        return True

    if _has_excessive_repetition(translated):
        return True

    source_len = len(source)
    translated_len = len(translated)
    if source_len >= 1200 and translated_len < int(source_len * 0.55):
        return True

    source_steps = _count_numbered_steps(source)
    translated_steps = _count_numbered_steps(translated)
    if source_steps >= 6 and translated_steps < max(3, int(source_steps * 0.55)):
        return True

    return False


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


def _retrieve_policy_chunks(
    user_query: str,
    district_name: str,
    top_k: int = 3,
) -> List[Dict[str, Any]]:
    fetch_k = max(top_k * 4, 12)
    district_key = (district_name or "").strip().lower()

    def _filter_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        district_rows: List[Dict[str, Any]] = []
        state_rows: List[Dict[str, Any]] = []
        for row in rows:
            source_name = str(row.get("source_name") or row.get("path") or "").strip().lower()
            if not _is_policy_source_allowed_for_district(source_name, district_name):
                continue
            if district_key and district_key in source_name:
                district_rows.append(row)
            else:
                state_rows.append(row)

        ordered_rows = district_rows + state_rows
        return ordered_rows[:top_k]

    candidate_column_sets = [
        ["chunk_id", "source_name", "chunk_text", "path", "chunk_index"],
        ["chunk_id", "source_name", "chunk_text"],
        ["chunk_id", "chunk_text"],
        ["chunk_text"],
    ]

    last_error: Optional[Exception] = None
    for columns in candidate_column_sets:
        try:
            results = _query_policy_index_with_workspace_client(user_query, columns, fetch_k)
            rows = _parse_similarity_response(results)
            if rows:
                filtered_rows = _filter_rows(rows)
                if filtered_rows:
                    return filtered_rows
        except Exception as exc:  # pragma: no cover - service exceptions vary by runtime
            last_error = exc

    # Fallback to the dedicated vector client for runtimes where the REST query path is restricted.
    try:
        vector_client = _build_vector_client()
        index = vector_client.get_index(
            endpoint_name=VECTOR_ENDPOINT_NAME,
            index_name=VECTOR_INDEX_NAME,
        )

        for columns in candidate_column_sets:
            try:
                results = index.similarity_search(
                    columns=columns,
                    query_text=user_query,
                    num_results=fetch_k,
                )
                rows = _parse_similarity_response(results)
                if rows:
                    filtered_rows = _filter_rows(rows)
                    if filtered_rows:
                        return filtered_rows
            except Exception as exc:  # pragma: no cover - service exceptions vary by runtime
                last_error = exc
    except Exception as exc:  # pragma: no cover - service exceptions vary by runtime
        last_error = exc

    local_rows = _retrieve_policy_chunks_local_files(
        user_query=user_query,
        district_name=district_name,
        top_k=top_k,
    )
    if local_rows:
        return local_rows

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
    retrieval_mode = _resolve_forecast_retrieval_mode()

    if retrieval_mode == "local_csv":
        return _retrieve_forecast_rows_local_csv(
            district_name=district_name,
            horizon_days=horizon_days,
            fallback_reason="Configured local CSV forecast mode for this runtime.",
        )

    try:
        spark = _get_spark()
        df = spark.table(FORECAST_TABLE)
    except Exception as exc:
        return _retrieve_forecast_rows_local_csv(
            district_name=district_name,
            horizon_days=horizon_days,
            fallback_reason=str(exc),
        )

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
    table_name = metadata.get("table", FORECAST_TABLE)

    if not forecast_rows:
        reason = metadata.get("note", "No rows were returned from the forecast table query.")
        return (
            f"No 30-day forecast rows found for district '{district_name}' in table {table_name}. "
            f"Reason: {reason}"
        )

    pred_cols = metadata.get("prediction_columns", [])
    lines = [f"Forecast table: {table_name}"]
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
    district_profile_note: str,
    policy_context: str,
    forecast_context: str,
    diagnostics: str,
    response_language: str,
) -> List[ChatMessage]:
    system_prompt = SYSTEM_PROMPT.format(response_language=response_language)

    if ChatPromptTemplate is not None:
        prompt_template = ChatPromptTemplate.from_messages(
            [
                ("system", system_prompt),
                ("human", USER_PROMPT_TEMPLATE),
            ]
        )
        langchain_messages = prompt_template.format_messages(
            user_query=user_query,
            district_name=district_name,
            district_profile_note=district_profile_note,
            policy_context=policy_context,
            forecast_context=forecast_context,
            diagnostics=diagnostics,
            response_language=response_language,
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
        district_profile_note=district_profile_note,
        policy_context=policy_context,
        forecast_context=forecast_context,
        diagnostics=diagnostics,
        response_language=response_language,
    )
    return [
        ChatMessage(role=ChatMessageRole.SYSTEM, content=system_prompt),
        ChatMessage(role=ChatMessageRole.USER, content=rendered_user_prompt),
    ]


def _build_translation_messages(
    source_text: str,
    target_language: str,
) -> List[ChatMessage]:
    system_prompt = TRANSLATION_SYSTEM_PROMPT_TEMPLATE.format(target_language=target_language)
    user_prompt = (
        f"Target language: {target_language}\n\n"
        "Translate the following advisory while preserving every detail:\n\n"
        f"{source_text}"
    )
    return [
        ChatMessage(role=ChatMessageRole.SYSTEM, content=system_prompt),
        ChatMessage(role=ChatMessageRole.USER, content=user_prompt),
    ]


def _build_translation_repair_messages(
    source_text: str,
    translated_text: str,
    target_language: str,
    min_chars: int,
) -> List[ChatMessage]:
    system_prompt = TRANSLATION_REPAIR_PROMPT_TEMPLATE.format(target_language=target_language)
    user_prompt = (
        f"Target language: {target_language}\n\n"
        f"Minimum output length target: {min_chars} characters\n\n"
        "Source advisory:\n"
        f"{source_text}\n\n"
        "Current translation (too short):\n"
        f"{translated_text}\n\n"
        f"Rewrite the {target_language} translation with full detail parity."
    )
    return [
        ChatMessage(role=ChatMessageRole.SYSTEM, content=system_prompt),
        ChatMessage(role=ChatMessageRole.USER, content=user_prompt),
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


def _query_llm(
    messages: List[ChatMessage],
    temperature: float = 0.15,
    max_tokens: int = 1800,
    endpoint_name: Optional[str] = None,
) -> str:
    endpoint = (endpoint_name or ADVISORY_LLM_ENDPOINT_NAME).strip()
    workspace_client = WorkspaceClient()
    response = workspace_client.serving_endpoints.query(
        name=endpoint,
        messages=messages,
        temperature=temperature,
        max_tokens=max_tokens,
    )
    answer = _extract_llm_text(response)
    if not answer:
        raise RuntimeError(f"LLM endpoint '{endpoint}' returned an empty response payload.")
    return answer


def _is_endpoint_unavailable_error(exc: Exception) -> bool:
    error_text = str(exc).lower()
    return any(
        token in error_text
        for token in [
            "endpoint does not exist",
            "endpoint not found",
            "resource does not exist",
            "not found",
            "no such",
            "temporarily disabled due to a databricks-set rate limit of 0",
            "temporarily disabled due a databricks-set rate limit of 0",
        ]
    )


def _query_advisory_llm(
    messages: List[ChatMessage],
    temperature: float,
    max_tokens: int,
) -> str:
    endpoints: List[str] = []
    for endpoint in [ADVISORY_LLM_ENDPOINT_NAME, ADVISORY_FALLBACK_ENDPOINT_NAME]:
        clean_endpoint = (endpoint or "").strip()
        if clean_endpoint and clean_endpoint not in endpoints:
            endpoints.append(clean_endpoint)

    last_error: Optional[Exception] = None
    for endpoint in endpoints:
        if endpoint in _UNAVAILABLE_ADVISORY_ENDPOINTS:
            continue

        try:
            return _query_llm(
                messages,
                temperature=temperature,
                max_tokens=max_tokens,
                endpoint_name=endpoint,
            )
        except Exception as exc:  # pragma: no cover - endpoint errors vary by runtime
            last_error = exc
            if _is_endpoint_unavailable_error(exc):
                _UNAVAILABLE_ADVISORY_ENDPOINTS.add(endpoint)

    if last_error:
        raise RuntimeError(f"Advisory endpoint query failed: {last_error}") from last_error
    raise RuntimeError("No advisory endpoint is configured.")


def _translation_primary_endpoint(target_language: str) -> str:
    language = _resolve_response_language(target_language)
    if language == "Marathi":
        return TRANSLATION_LLM_ENDPOINT_MARATHI
    if language == "Hindi":
        return TRANSLATION_LLM_ENDPOINT_HINDI
    return TRANSLATION_LLM_ENDPOINT_DEFAULT


def _query_translation_llm(
    messages: List[ChatMessage],
    temperature: float,
    max_tokens: int,
    target_language: str,
    use_quality_fallback: bool = False,
) -> str:
    endpoints: List[str] = []

    quality_endpoint = TRANSLATION_QUALITY_FALLBACK_ENDPOINT_NAME if use_quality_fallback else ""
    for endpoint in [
        quality_endpoint,
        _translation_primary_endpoint(target_language),
        TRANSLATION_LLM_ENDPOINT_DEFAULT,
        TRANSLATION_FALLBACK_ENDPOINT_NAME,
        ADVISORY_LLM_ENDPOINT_NAME,
    ]:
        clean_endpoint = (endpoint or "").strip()
        if clean_endpoint and clean_endpoint not in endpoints:
            endpoints.append(clean_endpoint)

    last_error: Optional[Exception] = None
    for endpoint in endpoints:
        if endpoint in _MISSING_TRANSLATION_ENDPOINTS:
            continue

        try:
            return _query_llm(
                messages,
                temperature=temperature,
                max_tokens=max_tokens,
                endpoint_name=endpoint,
            )
        except Exception as exc:  # pragma: no cover - endpoint errors vary by runtime
            last_error = exc
            if _is_endpoint_unavailable_error(exc):
                _MISSING_TRANSLATION_ENDPOINTS.add(endpoint)

    if last_error:
        raise RuntimeError(f"Translation endpoint query failed: {last_error}") from last_error
    raise RuntimeError("No translation endpoint is configured.")


def _translate_with_repair(source_text: str, target_language: str) -> str:
    translated_answer = _query_translation_llm(
        _build_translation_messages(source_text=source_text, target_language=target_language),
        temperature=0.1,
        max_tokens=2300,
        target_language=target_language,
    )

    # Repair only when quality clearly fails (for example severe shortening or structural loss).
    if _translation_quality_failed(source_text, translated_answer):
        min_chars = int(len(source_text) * 0.75)
        translated_answer = _query_translation_llm(
            _build_translation_repair_messages(
                source_text=source_text,
                translated_text=translated_answer,
                target_language=target_language,
                min_chars=min_chars,
            ),
            temperature=0.1,
            max_tokens=2600,
            target_language=target_language,
        )

        # If translation is still low quality (for example repetitive loops), retry once with quality fallback.
        if _translation_quality_failed(source_text, translated_answer):
            translated_answer = _query_translation_llm(
                _build_translation_messages(source_text=source_text, target_language=target_language),
                temperature=0.1,
                max_tokens=2600,
                target_language=target_language,
                use_quality_fallback=True,
            )

            if _translation_quality_failed(source_text, translated_answer):
                raise RuntimeError(
                    f"{target_language} translation quality check failed after retry with fallback model."
                )

    return translated_answer


def translate_advice_from_english(source_text: str, target_language: str) -> str:
    language = _resolve_response_language(target_language)
    normalized_source = (source_text or "").strip()

    if not normalized_source:
        raise ValueError("source_text cannot be empty.")

    if language == "English":
        return normalized_source

    return _translate_with_repair(
        source_text=normalized_source,
        target_language=language,
    )


def get_bhujal_advice_bundle(
    user_query: str,
    district_name: str,
    response_language: str = "English",
    include_all_translations: bool = False,
) -> Dict[str, Any]:
    query = (user_query or "").strip()
    district = (district_name or "").strip()
    language = _resolve_response_language(response_language)

    if not query:
        raise ValueError("user_query cannot be empty.")
    if not district:
        raise ValueError("district_name cannot be empty.")

    diagnostics: List[str] = []

    try:
        policy_rows = _retrieve_policy_chunks(query, district_name=district, top_k=3)
    except Exception as exc:  # pragma: no cover - service exceptions vary by runtime
        policy_rows = []
        diagnostics.append(f"Policy retrieval warning: {exc}")

    try:
        forecast_rows, forecast_meta = _retrieve_forecast_rows(district, horizon_days=30)
    except Exception as exc:  # pragma: no cover - service exceptions vary by runtime
        forecast_rows, forecast_meta = [], {"note": str(exc)}
        diagnostics.append(f"Forecast retrieval warning: {exc}")

    district_profile_note = _resolve_district_profile_note(district)
    policy_context = _format_policy_context(policy_rows)
    forecast_context = _format_forecast_context(district, forecast_rows, forecast_meta)
    diagnostics_text = "\n".join(f"- {item}" for item in diagnostics) if diagnostics else "- none"

    messages = _build_databricks_messages(
        user_query=query,
        district_name=district,
        district_profile_note=district_profile_note,
        policy_context=policy_context,
        forecast_context=forecast_context,
        diagnostics=diagnostics_text,
        response_language="English",
    )
    english_answer = _query_advisory_llm(
        messages,
        temperature=0.12,
        max_tokens=2400,
    )

    advice_variants: Dict[str, str] = {"English": english_answer}

    if include_all_translations:
        target_languages = ["Hindi", "Marathi"]
    elif language != "English":
        target_languages = [language]
    else:
        target_languages = []

    for target_language in target_languages:
        if target_language in advice_variants:
            continue
        try:
            advice_variants[target_language] = translate_advice_from_english(
                source_text=english_answer,
                target_language=target_language,
            )
        except Exception as exc:  # pragma: no cover - service exceptions vary by runtime
            diagnostics.append(f"{target_language} translation warning: {exc}")

    answer = advice_variants.get(language, english_answer)

    policy_sources = []
    for row in policy_rows:
        source = row.get("source_name") or row.get("path") or row.get("source")
        if source and source not in policy_sources:
            policy_sources.append(str(source))

    return {
        "advice_text": answer,
        "english_advice_text": english_answer,
        "advice_variants": advice_variants,
        "district_name": district,
        "response_language": language,
        "policy_sources": policy_sources,
        "forecast_rows": forecast_rows,
        "forecast_metadata": forecast_meta,
        "diagnostics": diagnostics,
    }


def get_bhujal_advice(
    user_query: str,
    district_name: str,
    response_language: str = "English",
) -> str:
    """
    Build a district-aware groundwater advisory using:
    1) Top-3 policy chunks from Databricks Vector Search
    2) Up to 30 rows from the district forecast table
    3) A Databricks LLM endpoint response in one user-selected language
    """
    payload = get_bhujal_advice_bundle(
        user_query=user_query,
        district_name=district_name,
        response_language=response_language,
    )
    return str(payload.get("advice_text", "")).strip()


__all__ = [
    "get_bhujal_advice",
    "get_bhujal_advice_bundle",
    "translate_advice_from_english",
]


if __name__ == "__main__":
    sample = get_bhujal_advice(
        user_query="What should farmers do this month to reduce groundwater stress?",
        district_name="Pune",
    )
    print(sample)
