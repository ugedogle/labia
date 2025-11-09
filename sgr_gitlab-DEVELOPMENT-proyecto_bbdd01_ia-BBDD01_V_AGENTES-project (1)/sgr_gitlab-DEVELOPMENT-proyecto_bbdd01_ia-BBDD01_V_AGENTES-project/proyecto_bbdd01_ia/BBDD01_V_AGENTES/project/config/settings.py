# -*- coding: utf-8 -*-
"""
Nota: Umbrales BigQuery vía ENV (BQT_BYTES_THRESHOLD_MB, BQT_SAFE_AGG_CAP_MB) gestionados en tools/bigquery_tools.py.

Ajustes de proyecto y guardrails comunes.
Este módulo NO realiza llamadas; solo define constantes utilizadas por tools/ y agents/.
"""

import os

# --- Proyecto / Regiones ---
PROJECT_ID   = "go-cxb-bcx-data9-dtwsgrp01"
REGION       = "europe-southwest1"
BQ_LOCATION = "europe-southwest1"                     # Ubicación de BigQuery (multi-región)

# --- Dataset/tablas base ---
DATASET      = "dtwsgr_ds01"
TABLE_BASE   = "BBDD_01_LIGHT"          # Vista/tablas base “light” (ajustable)
TABLA_BASE_FQN = f"`{PROJECT_ID}.{DATASET}.{TABLE_BASE}`"

# --- Allowlist (datasets/tablas que se pueden consultar) ---
ALLOWED_DATASETS = {
    f"{PROJECT_ID}.{DATASET}",
}
ALLOWED_TABLES = {
    f"{PROJECT_ID}.{DATASET}.{TABLE_BASE}",
    # añade aquí CERT_* cuando estén listadas en catalog.json
}

# --- Límites y valores por defecto ---
MAX_ROWS_RETURNED   = 200               # tope de filas que devolvemos al front
LIMIT_DEFAULT       = 200               # LIMIT auto si falta
BYTES_THRESHOLD = None  # DEPRECATED: usar BQT_BYTES_THRESHOLD_MB / BQT_SAFE_AGG_CAP_MB vía tools/bigquery_tools.py
DEFAULT_MONTHS_WIN  = 12                # ventana por defecto si el plan exige MES

# --- Guardrails SQL ---
REQUIRE_FQN_TABLE   = True              # exigir nombres fully-qualified con backticks
FORBIDDEN_KEYWORDS  = (
    "INSERT","UPDATE","DELETE","MERGE","CREATE","DROP","ALTER","TRUNCATE",
    "GRANT","REVOKE","BEGIN","COMMIT","ROLLBACK"
)
DISALLOW_SELECT_STAR = True             # bloquear SELECT *
ALLOW_UNSAFE_JOINS   = False            # si True, el auditor podrá avisar (no bloquear)





# === (auto) Added by patch: PII / Auditoría mínima ===
PII_OUTPUT_MODE = "allow"

AUDIT_MIN_NON_NULL_RIESGO = 0.95  # % mínimo no-nulos en RIESGO para avisar

ENABLE_AUDIT_LOG = True

AUDIT_LOG_TABLE_FQN = f'`{PROJECT_ID}.{DATASET}.SEC_AUDIT_LOG`'

AUDIT_LOG_TABLE_ID = f'{PROJECT_ID}.{DATASET}.SEC_AUDIT_LOG'

AUDIT_LOG_TABLE = "SEC_AUDIT_LOG"



# --- LLM / Modelos por agente ---
GEMINI_MODEL_DEFAULT = "gemini-2.5-pro"

AGENT_MODELS = {
    "orchestrator": GEMINI_MODEL_DEFAULT,
    "sql_agent": GEMINI_MODEL_DEFAULT,
    "web_agent": GEMINI_MODEL_DEFAULT,
    "composer": GEMINI_MODEL_DEFAULT,
    "viz_agent": GEMINI_MODEL_DEFAULT,
    "final_auditor": GEMINI_MODEL_DEFAULT,
    "documents": GEMINI_MODEL_DEFAULT,
}

AGENT_TEMPERATURES = {
    "orchestrator": 0.2,
    "sql_agent": 0.1,
    "web_agent": 0.3,
    "composer": 0.35,
    "viz_agent": 0.25,
    "final_auditor": 0.0,
    "documents": 0.3,
}

# --- Documentos internos / soporte GCS ---
DOCS_BUCKET = (os.environ.get("DOCS_BUCKET") or None)
DOCS_CATALOG_PATH = os.environ.get("DOCS_CATALOG_PATH", "config/documents_catalog.json")
DOCS_SUMMARY_MODEL = AGENT_MODELS.get("documents", GEMINI_MODEL_DEFAULT)
DOCS_SUMMARY_TEMPERATURE = AGENT_TEMPERATURES.get("documents", 0.3)
