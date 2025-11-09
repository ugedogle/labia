# -*- coding: utf-8 -*-
"""
SQL Agent (robusto, con settings y metrics configurables)
- Lee modelo/temperatura desde config/settings.py (AGENT_MODELS, AGENT_TEMPERATURES, GEMINI_MODEL_DEFAULT).
- Carga metrics.yaml respetando settings.METRICS_FILE o settings.METRICS_PATHS.
- Transforma un Plan en SQL Standard SEGURO (filtros MES relativos + LAST_AVAILABLE, filtros eq/in/like/ilike).
"""

from __future__ import annotations

import re, difflib
from typing import List, Dict, Any, Optional, Tuple
from pathlib import Path
from dataclasses import dataclass
from datetime import datetime

# --- settings / configuración centralizada ---
from config import settings as _SET
from config.settings import PROJECT_ID, TABLA_BASE_FQN
from tools.bigquery_tools import list_columns
from tools.synonyms import smart_pick_column, _load_yaml  # reutilizamos loader

# Modelo y temperatura del agente (no llaman a LLM aquí, pero quedan accesibles y registrados)
try:
    GEMINI_MODEL_DEFAULT = getattr(_SET, "GEMINI_MODEL_DEFAULT", "gemini-2.5-pro")
except Exception:
    GEMINI_MODEL_DEFAULT = "gemini-2.5-pro"

try:
    _AGENT_MODELS = getattr(_SET, "AGENT_MODELS", {}) or {}
    SQL_AGENT_MODEL = _AGENT_MODELS.get("sql_agent", GEMINI_MODEL_DEFAULT)
except Exception:
    SQL_AGENT_MODEL = GEMINI_MODEL_DEFAULT

try:
    _AGENT_TEMPS = getattr(_SET, "AGENT_TEMPERATURES", {}) or {}
    SQL_AGENT_TEMPERATURE = float(_AGENT_TEMPS.get("sql_agent", 0.1))
except Exception:
    SQL_AGENT_TEMPERATURE = 0.1

# ------------------ util ------------------

def _og(obj, key: str, default=None):
    try:
        return getattr(obj, key)
    except Exception:
        if isinstance(obj, dict):
            return obj.get(key, default)
        return default

def _ci_equal(a: str, b: str) -> bool:
    return (a or "").strip().upper() == (b or "").strip().upper()

def _quote_ident(name: str) -> str:
    if not name:
        return name
    if name.startswith("`") and name.endswith("`"):
        return name
    if re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", name):
        return f"`{name}`"
    return name

def _resolve_table_identifier(t: str) -> str:
    if not t:
        return TABLA_BASE_FQN
    tt = t.strip().strip("`")
    if "<<" in tt or ">>" in tt or tt.upper() in {"TABLA_BASE_FQN", "<TABLA_BASE_FQN>", "TABLA_BASE_FQN"}:
        return TABLA_BASE_FQN
    if tt.count(".") == 1:
        tt = f"{PROJECT_ID}.{tt}"
    if tt.count(".") != 2:
        return TABLA_BASE_FQN
    return f"`{tt}`"

AUTO_FUZZY_DIM_MATCH = True
SUGGESTION_TOP_N = 3

# Sinónimos muestrales (amplía con config/synonyms.yaml via smart_pick_column si quieres)
SYNONYMS: Dict[str, List[str]] = {
    "SECTOR_ESTRATEGICO": ["SECTOR_COV19", "CALIFICACION_GRUPO", "COD_CNAE", "DESC_CNAE"],
    "SECTOR": ["SECTOR_COV19", "CALIFICACION_GRUPO", "COD_CNAE", "DESC_CNAE"],
}

# ------------------ metric helpers ------------------

def _metrics_paths_from_settings() -> List[Path]:
    """Construye una lista de paths candidatos usando settings y defaults."""
    candidates: List[Path] = []
    try:
        preferred = getattr(_SET, "METRICS_FILE", None)
        if preferred:
            p = Path(preferred)
            if p.exists():
                candidates.append(p)
    except Exception:
        pass
    try:
        extra = getattr(_SET, "METRICS_PATHS", None)
        if isinstance(extra, (list, tuple)):
            for e in extra:
                p = Path(e)
                if p.exists():
                    candidates.append(p)
    except Exception:
        pass
    # Defaults
    for p in [Path("config")/"metrics.yaml", Path("data")/"metrics.yaml"]:
        candidates.append(p)
    # De-dup preservando orden
    seen = set(); uniq = []
    for p in candidates:
        key = str(p.resolve()) if p.exists() else str(p)
        if key not in seen:
            uniq.append(p)
            seen.add(key)
    return uniq

def _load_metrics_cfg() -> Dict[str, Any]:
    for p in _metrics_paths_from_settings():
        try:
            if p.exists():
                return _load_yaml(p)
        except Exception:
            continue
    return {"metrics": {}}

def _resolve_metric_expr(m: str, metrics_cfg: Dict[str, Any], notes: List[str]) -> str:
    """
    Si m es un nombre definido en metrics.yaml → expr.
    Si ya parece expresión (contiene '(' o espacios/operadores) → se respeta.
    Siempre eliminamos ';' por seguridad.
    """
    m = (m or "").strip()
    m = re.sub(r";+", " ", m)
    metrics_map = (metrics_cfg or {}).get("metrics") or {}

    if re.search(r"\bAS\b", m, flags=re.IGNORECASE):
        return m

    if m in metrics_map and isinstance(metrics_map[m], dict) and "expr" in metrics_map[m]:
        expr = (metrics_map[m]["expr"] or "").strip()
        if not re.search(r"\bAS\s+[A-Za-z_][A-Za-z0-9_]*\b", expr, flags=re.IGNORECASE):
            expr = f"{expr} AS {m}"
        notes.append(f"Métrica '{m}' resuelta vía metrics.yaml.")
        return expr

    if "(" in m or " " in m or re.search(r"[+\-/*]", m):
        return m

    return f"{_quote_ident(m)} AS {m}"

# ------------------ dim helpers ------------------

def _resolve_dim(dim: str, cols: List[str]) -> Tuple[str, Optional[str]]:
    if dim in cols:
        return dim, None
    for c in cols:
        if _ci_equal(dim, c):
            return c, None
    for syn in SYNONYMS.get(dim, []):
        if syn in cols:
            return syn, f"Dimensión '{dim}' mapeada a sinónimo '{syn}'."
        for c in cols:
            if _ci_equal(syn, c):
                return c, f"Dimensión '{dim}' mapeada a sinónimo '{c}'."
    if AUTO_FUZZY_DIM_MATCH:
        suggs = difflib.get_close_matches(dim, cols, n=1)
        if suggs:
            return suggs[0], f"Dimensión '{dim}' auto-resuelta a '{suggs[0]}' (closest match)."
    raise ValueError(f"Dimensión no encontrada: {dim}. ¿Quizá quisiste: {difflib.get_close_matches(dim, cols, n=SUGGESTION_TOP_N)}?")

def _cols_referenced_in_metric(expr: str) -> List[str]:
    parts = re.split(r"\bAS\b", expr, flags=re.IGNORECASE)
    expr_no_alias = parts[0]
    tokens = re.findall(r"[A-Za-z_][A-Za-z0-9_]*", expr_no_alias)
    ignore = {
        "SUM","AVG","COUNT","MIN","MAX","CAST","SAFE_CAST","DISTINCT","CASE","WHEN","THEN","ELSE","END","NULL",
        "IF","AND","OR","NOT","DATE","DATETIME","TIMESTAMP","EXTRACT","DATE_TRUNC","COALESCE","ROUND","FLOOR","CEIL",
        "POWER","ABS","TRUE","FALSE","OVER","PARTITION","BY"
    }
    return [t for t in tokens if t.upper() not in ignore]

# ------------------ filtros ------------------

def _current_year() -> int:
    return datetime.now().year

def _mes_shortcut_to_sql(code: str) -> str:
    code = code.upper()
    if code == "LAST_1M":
        return ("CAST(MES AS INT64) BETWEEN "
                "CAST(FORMAT_DATE('%Y%m', DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)) AS INT64) "
                "AND CAST(FORMAT_DATE('%Y%m', CURRENT_DATE()) AS INT64)")
    if code == "LAST_3M":
        return ("CAST(MES AS INT64) BETWEEN "
                "CAST(FORMAT_DATE('%Y%m', DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH)) AS INT64) "
                "AND CAST(FORMAT_DATE('%Y%m', CURRENT_DATE()) AS INT64)")
    if code == "LAST_12M":
        return ("CAST(MES AS INT64) BETWEEN "
                "CAST(FORMAT_DATE('%Y%m', DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH)) AS INT64) "
                "AND CAST(FORMAT_DATE('%Y%m', CURRENT_DATE()) AS INT64)")
    if code == "YTD":
        return ("CAST(MES AS INT64) BETWEEN "
                "CAST(FORMAT_DATE('%Y%m', DATE_TRUNC(CURRENT_DATE(), YEAR)) AS INT64) "
                "AND CAST(FORMAT_DATE('%Y%m', CURRENT_DATE()) AS INT64)")
    if code == "MTD":
        return ("CAST(MES AS INT64) BETWEEN "
                "CAST(FORMAT_DATE('%Y%m', DATE_TRUNC(CURRENT_DATE(), MONTH)) AS INT64) "
                "AND CAST(FORMAT_DATE('%Y%m', CURRENT_DATE()) AS INT64)")
    return ""

def _build_mes_filter(filters: Dict[str, Any]) -> Optional[str]:
    f = filters.get("MES") if isinstance(filters, dict) else None
    if not f:
        return None

    if isinstance(f, str):
        s = _mes_shortcut_to_sql(f)
        return s or None

    def ym_int(v: str|int) -> int:
        v = int(v)
        if v < 190001 or v > 299912:
            raise ValueError(f"MES fuera de rango YYYYMM: {v}")
        return v

    t = f.get("type")
    if t == "range_ym":
        y1 = ym_int(f.get("from")); y2 = ym_int(f.get("to"))
        return f"CAST(MES AS INT64) BETWEEN {y1} AND {y2}"
    if t in ("between_year","year"):
        y = f.get("year")
        if isinstance(y, str) and y.lower() == "this":
            y = _current_year()
        y = int(y)
        return f"CAST(MES AS INT64) BETWEEN {y}01 AND {y}12"

    if "from" in f and "to" in f:
        y1 = ym_int(f["from"]); y2 = ym_int(f["to"])
        return f"CAST(MES AS INT64) BETWEEN {y1} AND {y2}"
    return None

def _escape_literal(v):
    if v is None:
        return "NULL"
    if isinstance(v, (int, float)):
        return str(v)
    s = str(v)
    return "'" + s.replace("'", "''") + "'"

def _build_extra_where(filters: Dict[str, Any], cols: List[str]) -> List[str]:
    if not isinstance(filters, dict):
        return []
    parts: List[str] = []

    # where crudo (bajo tu responsabilidad)
    where_raw = filters.get("where")
    if isinstance(where_raw, str) and where_raw.strip():
        parts.append("(" + where_raw.strip() + ")")

    # eq: {col: value}
    for col, val in (filters.get("eq") or {}).items():
        if col in cols or any(col.lower() == c.lower() for c in cols):
            parts.append(f"{_quote_ident(col)} = {_escape_literal(val)}")

    # in: {col: [v1, v2]}
    for col, arr in (filters.get("in") or {}).items():
        if not isinstance(arr, (list, tuple)):
            continue
        if col in cols or any(col.lower() == c.lower() for c in cols):
            vals = ", ".join(_escape_literal(v) for v in arr)
            parts.append(f"{_quote_ident(col)} IN ({vals})")

    # like / ilike: {col: pattern}
    for col, pat in (filters.get("like") or {}).items():
        if col in cols or any(col.lower() == c.lower() for c in cols):
            parts.append(f"{_quote_ident(col)} LIKE {_escape_literal(pat)}")
    for col, pat in (filters.get("ilike") or {}).items():
        if col in cols or any(col.lower() == c.lower() for c in cols):
            parts.append(f"LOWER({_quote_ident(col)}) LIKE LOWER({_escape_literal(pat)})")

    return parts

def _quote_fqn(fqn: str) -> str:
    s = fqn.strip()
    if s.startswith("`") and s.endswith("`"):
        return s
    if len(s.split(".")) == 3:
        return f"`{s}`"
    return s

# ------------------ core ------------------

@dataclass
class SqlBuildResult:
    sql: str
    used_table: str
    dims: List[str]
    metrics: List[str]
    order_by: List[str]
    limit: int
    notes: List[str]

def build_sql_from_plan(plan, table_fqn: Optional[str]=None) -> SqlBuildResult:
    ident = _resolve_table_identifier(table_fqn or (_og(plan, "tables", [None]) or [None])[0])
    tbl = _quote_fqn(ident.strip())
    cols = list_columns(tbl)

    notes: List[str] = []
    # registra modelo/temperatura usados desde settings para trazabilidad
    try:
        notes.append(f"sql_agent config → model='{SQL_AGENT_MODEL}', temperature={SQL_AGENT_TEMPERATURE}")
    except Exception:
        pass

    # 1) Dimensiones
    dims: List[str] = []
    for d in (_og(plan, "dimensions") or []):
        resolved, note = _resolve_dim(d, cols)
        dims.append(resolved)
        if note:
            notes.append(note)

    # 2) Métricas
    metrics_cfg = _load_metrics_cfg()
    metrics_exprs: List[str] = []
    for m in (_og(plan, "metrics") or []):
        expr = _resolve_metric_expr(m, metrics_cfg, notes)
        for ref in _cols_referenced_in_metric(expr):
            if ref not in cols and not any(_ci_equal(ref, c) for c in cols):
                if re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", ref):
                    sugg = difflib.get_close_matches(ref, cols, n=SUGGESTION_TOP_N)
                    raise ValueError(f"Columna en métrica no encontrada: {ref}. ¿Quizá: {sugg}?")
        metrics_exprs.append(expr)

    if not dims and not metrics_exprs:
        raise ValueError("Nada que seleccionar (sin dimensiones ni métricas).")

    # 3) WHERE
    where_clauses: List[str] = []

    # 3a) MES estándar
    mes_filter = _build_mes_filter(_og(plan, "filters", {}) or {})
    if mes_filter:
        where_clauses.append(mes_filter)

    # 3b) LAST_AVAILABLE
    mes_code = _og(plan, "filters", {}).get("MES")
    if isinstance(mes_code, str) and mes_code.upper() == "LAST_AVAILABLE":
        where_clauses.append(f"CAST(MES AS INT64) = (SELECT MAX(CAST(MES AS INT64)) FROM {tbl})")

    # 3c) Filtros generales
    where_clauses.extend(_build_extra_where(_og(plan, "filters", {}) or {}, cols))

    # 4) SELECT
    dims_quoted = [_quote_ident(d) for d in dims]
    select_parts: List[str] = []
    if dims_quoted:
        select_parts.extend(dims_quoted)
    select_parts.extend(metrics_exprs)
    select_sql = ",\n  ".join(select_parts)

    # 5) FROM + WHERE
    where_sql = ""
    if where_clauses:
        where_sql = "\nWHERE " + " AND ".join(where_clauses)

    # 6) GROUP BY (si hay dims)
    group_sql = ""
    if dims_quoted:
        group_sql = "\nGROUP BY " + ", ".join(dims_quoted)

    # 7) ORDER BY
    order_parts: List[str] = []
    for o in (_og(plan, "ordering") or []):
        by = _og(o, "by")
        if not by:
            continue
        dir_ = (_og(o, "dir", "DESC") or "DESC").upper()
        order_parts.append(f"{by} {dir_}")
    order_sql = ("\nORDER BY " + ", ".join(order_parts)) if order_parts else ""

    # 8) LIMIT
    lim = int(_og(plan, "limit", getattr(_SET, "LIMIT_DEFAULT", 1000)) or getattr(_SET, "LIMIT_DEFAULT", 1000))
    limit_sql = f"\nLIMIT {lim}"

    sql = f"""
SELECT
  {select_sql}
FROM {tbl}{where_sql}{group_sql}{order_sql}{limit_sql}
""".strip()

    return SqlBuildResult(
        sql=sql, used_table=tbl, dims=dims, metrics=metrics_exprs,
        order_by=order_parts, limit=lim, notes=notes
    )
