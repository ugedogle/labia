
# -*- coding: utf-8 -*-
from typing import Any, Dict, List, Optional
from google.cloud import bigquery
from config.settings import PROJECT_ID, BQ_LOCATION, AUDIT_LOG_TABLE_FQN, AUDIT_LOG_TABLE_ID, ENABLE_AUDIT_LOG

def _client():
    return bigquery.Client(project=PROJECT_ID, location=BQ_LOCATION)

def _ensure_table():
    client = _client()
    try:
        client.get_table(AUDIT_LOG_TABLE_ID)
        return
    except Exception:
        pass
    schema = [
        bigquery.SchemaField("run_ts", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("user_query", "STRING"),
        bigquery.SchemaField("notes", "STRING"),
        bigquery.SchemaField("summary_text", "STRING"),
    ]
    table = bigquery.Table(AUDIT_LOG_TABLE_ID, schema=schema)
    client.create_table(table)

def _existing_columns(client: bigquery.Client) -> List[str]:
    tbl = client.get_table(AUDIT_LOG_TABLE_ID)
    return [f.name for f in tbl.schema]

def log_interaction(**kwargs):
    """Inserta de forma laxa: solo columnas existentes. Nunca interrumpe el flujo."""
    if not ENABLE_AUDIT_LOG:
        return
    client = _client()
    try:
        _ensure_table()
    except Exception as e:
        print(f"[audit_log] No se pudo asegurar la tabla: {e}")
        return
    try:
        cols = set(_existing_columns(client))
        payload = {"run_ts": bigquery.ScalarQueryParameter("", "TIMESTAMP", None)}  # marcador
        # Campos permisibles (si existen)
        candidates = {
            "user_query": kwargs.get("user_query"),
            "notes": " | ".join([str(n) for n in (kwargs.get("notes") or [])]) if kwargs.get("notes") else None,
            "summary_text": kwargs.get("summary") or kwargs.get("text"),
        }
        row = {"run_ts": bigquery._helpers.utcnow()}
        for k, v in candidates.items():
            if k in cols and v is not None:
                row[k] = v
        errors = client.insert_rows_json(AUDIT_LOG_TABLE_ID, [row])
        if errors:
            print(f"[audit_log] Errores al insertar (filtrado): {errors}")
    except Exception as e:
        print(f"[audit_log] Error silenciado: {e}")
