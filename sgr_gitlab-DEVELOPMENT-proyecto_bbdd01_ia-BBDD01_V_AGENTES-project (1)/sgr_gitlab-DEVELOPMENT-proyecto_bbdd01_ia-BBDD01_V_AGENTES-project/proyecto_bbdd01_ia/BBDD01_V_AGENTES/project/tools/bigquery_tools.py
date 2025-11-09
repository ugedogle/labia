# -*- coding: utf-8 -*-
"""
Utilidades de BigQuery: guardrails, dry-run, ejecución y helpers de esquema.
Diseñado para ser llamado por agentes (SQL Agent) y por el orquestador.
"""

from __future__ import annotations
import re, os
from typing import Any, Dict, List, Optional, Tuple

from google.cloud import bigquery

def format_bytes(n:int)->str:
    gb = n/1e9
    mb = n/1e6
    return f"{gb:.2f} GB ({mb:.0f} MB)" if n>=1e9 else f"{mb:.1f} MB"

# --- SETTINGS / Allowlist ---
from config.settings import (
    PROJECT_ID, BQ_LOCATION,
    ALLOWED_DATASETS, ALLOWED_TABLES,
    MAX_ROWS_RETURNED, LIMIT_DEFAULT,
    REQUIRE_FQN_TABLE,
    FORBIDDEN_KEYWORDS, DISALLOW_SELECT_STAR
)
import config.settings as _settings

# === Umbral efectivo (no lo pises con settings.BYTES_THRESHOLD si es None) ===
_ENV_MB = int(os.getenv('BQT_BYTES_THRESHOLD_MB', '10240'))  # 10GB por defecto en DEV
if getattr(_settings, 'BYTES_THRESHOLD', None):
    BYTES_THRESHOLD = int(_settings.BYTES_THRESHOLD)
else:
    BYTES_THRESHOLD = _ENV_MB * 1024 * 1024

SAFE_AGG_HIGH_CAP = int(os.getenv('BQT_SAFE_AGG_CAP_MB', '16384')) * 1024 * 1024

print(f"Umbral BigQuery activo: {BYTES_THRESHOLD//(1024*1024)} MB")

# ------------------------- Regex y utilidades -------------------------
_SELECT_WITH_RE = re.compile(r"^\s*(SELECT|WITH)\b", re.IGNORECASE)
_LIMIT_RE       = re.compile(r"\blimit\s+\d+\b", re.IGNORECASE)
_FQN_RE         = re.compile(r"(?:`?([\w-]+)\.([\w-]+)\.([\w$-]+)`?)")  # project.dataset.table

def _strip_backticks(s: str) -> str:
    return s.replace("`", "")

def _extract_table_refs(sql: str) -> List[Tuple[str, str, str]]:
    return _FQN_RE.findall(sql)

def _has_count_only(sql: str) -> bool:
    s = re.sub(r"\s+", " ", sql.strip().lower())
    return s.startswith("select count(") and " group by " not in s

# ------------------------- Guardrails SQL -------------------------
def validate_sql_readonly(sql: str) -> None:
    if not _SELECT_WITH_RE.search(sql or ""):
        raise ValueError("Solo se permiten consultas de lectura (SELECT/WITH).")
    upper = sql.upper()
    if any(kw in upper for kw in FORBIDDEN_KEYWORDS):
        raise ValueError("Operación no permitida (DML/DDL detectado).")
    if DISALLOW_SELECT_STAR and re.search(r"select\s+\*", sql, re.IGNORECASE):
        raise ValueError("SELECT * no permitido. Enumera columnas explícitamente.")

def validate_allowlist(sql: str) -> None:
    refs = [tuple(_strip_backticks(".".join(r)).split(".")) for r in _extract_table_refs(sql)]
    if not refs:
        if REQUIRE_FQN_TABLE:
            raise ValueError("Usa nombres fully-qualified con backticks (`proy.dataset.tabla`).")
        return
    for project, dataset, table in refs:
        if f"{project}.{dataset}" not in ALLOWED_DATASETS:
            raise PermissionError(f"Dataset no permitido: {project}.{dataset}")
        if ALLOWED_TABLES and f"{project}.{dataset}.{table}" not in ALLOWED_TABLES:
            raise PermissionError(f"Tabla no permitida: {project}.{dataset}.{table}")

def ensure_limit(sql: str, limit: Optional[int] = None) -> str:
    if _LIMIT_RE.search(sql) or _has_count_only(sql):
        return sql
    lim = limit or LIMIT_DEFAULT
    return sql.rstrip().rstrip(";") + f"\nLIMIT {int(lim)}"

# ------------------------- Dry-run y ejecución -------------------------
def dry_run_sql(sql: str, client: Optional[bigquery.Client] = None) -> int:
    client = client or bigquery.Client(project=PROJECT_ID)
    job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
    job = client.query(sql, job_config=job_config, location=BQ_LOCATION)
    return int(job.total_bytes_processed or 0)

def execute_sql(
    sql: str,
    client: Optional[bigquery.Client] = None,
    limit_if_missing: bool = True,
    return_as_dicts: bool = True
) -> Dict[str, Any]:
    if not sql or not sql.strip():
        raise ValueError("SQL vacío.")

    validate_sql_readonly(sql)
    validate_allowlist(sql)

    sql_used = ensure_limit(sql) if limit_if_missing else sql

    estimated_bytes = dry_run_sql(sql_used, client=client)
    if estimated_bytes > BYTES_THRESHOLD:
        raise RuntimeError(
            f"Consulta demasiado costosa (dry-run ≈ {format_bytes(estimated_bytes)} > umbral {format_bytes(BYTES_THRESHOLD)}). "
            "Restringe tiempo (MES), columnas o agrega."
        )

    client = client or bigquery.Client(project=PROJECT_ID)
    job = client.query(sql_used, location=BQ_LOCATION)
    rows_iter = job.result()
    schema = [f.name for f in rows_iter.schema]

    rows_list: List[Dict[str, Any]] = []
    if return_as_dicts:
        for i, r in enumerate(rows_iter):
            if i < MAX_ROWS_RETURNED:
                rows_list.append(dict(r))
            else:
                break

    stats = {
        "estimated_bytes": estimated_bytes,
        "total_rows": rows_iter.total_rows or len(rows_list),
        "job_id": job.job_id,
        "slot_ms": getattr(job, "slot_millis", None),
    }

    return {
        "ok": True,
        "rows": rows_list,
        "schema": schema,
        "stats": stats,
        "sql_used": sql_used,
    }

# ------------------------- Helpers de esquema -------------------------
def fetch_table_schema(table_fqn: str, client: Optional[bigquery.Client] = None) -> List[Dict[str, Any]]:
    client = client or bigquery.Client(project=PROJECT_ID)
    table_id = _strip_backticks(table_fqn)
    table = client.get_table(table_id)
    out = []
    for f in table.schema:
        out.append({
            "name": f.name,
            "type": f.field_type,
            "mode": f.mode,
            "description": getattr(f, "description", None)
        })
    return out

def list_columns(table_fqn: str, client: Optional[bigquery.Client] = None) -> List[str]:
    return [f["name"] for f in fetch_table_schema(table_fqn, client=client)]
