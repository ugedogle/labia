
# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any, Dict, List, Optional
import re
import pandas as pd

from agents.orchestrator import Orchestrator
from agents.sql_agent import build_sql_from_plan
from agents.data_auditor import audit_dataset
from agents.viz_agent import pick_spec
from agents.artifact_auditor import audit_visual
from agents.composer import compose_response
from agents.final_auditor import final_check
from agents.web_agent import WebAgent
from agents.documents_agent import DocumentsAgent

from tools.bigquery_tools import execute_sql
from tools.audit_log import log_interaction
from tools.documents_index import select_documents
from config.settings import (
    TABLA_BASE_FQN,
    DOCS_BUCKET,
    DOCS_SUMMARY_MODEL,
    DOCS_SUMMARY_TEMPERATURE,
)

class ChatRunner:
    """
    Orquestación simple:
      1) Plan (orchestrator)
      2) Si web_only → web_agent → compose → final_check
      3) Si SQL → build_sql → execute_sql (fallback a 1 mes si excede) → audits → compose → final_check
    Todo lo que pueda fallar, añade nota y no rompe.
    """

    def __init__(self, model: str = "gemini-2.5-pro", temperature: float = 0.2):
        self.model = model
        self.temperature = temperature
        self._orc = Orchestrator(model=self.model, temperature=self.temperature)
        self._web = WebAgent(model=self.model, temperature=self.temperature)
        try:
            self._docs = DocumentsAgent(
                bucket=DOCS_BUCKET,
                summarizer_model=DOCS_SUMMARY_MODEL,
                summarizer_temperature=DOCS_SUMMARY_TEMPERATURE,
            )
            self._docs_error: Optional[str] = None
        except Exception as exc:
            self._docs = None
            self._docs_error = str(exc)

    # ---------- helpers ----------
    def _get_last_mes(self) -> Optional[int]:
        """Devuelve MAX(MES) de la tabla base."""
        q = f"SELECT MAX(MES) AS last_mes FROM {TABLA_BASE_FQN} LIMIT 1"
        try:
            r = execute_sql(q)
            rows = r.get("rows", [])
            if rows and (rows[0].get("last_mes") is not None):
                return int(rows[0]["last_mes"])
        except Exception as e:
            # Es un SELECT trivial; si fallara, devolvemos None y dejamos que suba el error original
            pass
        return None

    def _safe_log(self, user_query: str, notes: List[str], summary_text: str) -> None:
        try:
            log_interaction(user_query=user_query, notes=notes, summary=summary_text)
        except Exception as e:
            print(f"[audit_log] aviso: {e}")

    def _build_document_context(self, plan, user_query: str, notes: List[str]) -> Optional[Dict[str, Any]]:
        if not getattr(plan, "need_documents", False):
            return None

        if self._docs is None:
            reason = self._docs_error or "agente documental no configurado"
            notes.append(f"Contexto documental no disponible: {reason}")
            return None

        try:
            candidates = select_documents(plan=plan, user_query=user_query, limit=3)
        except Exception as exc:
            notes.append(f"No se pudo seleccionar documentos internos: {exc}")
            return None

        if not candidates:
            notes.append("El plan solicitó documentos internos pero el catálogo no devolvió coincidencias.")
            return None

        paths = [c.uri for c in candidates]
        try:
            payload = self._docs.read_many(paths)
        except Exception as exc:
            notes.append(f"No se pudieron leer documentos internos: {exc}")
            return None

        summary_res = self._docs.summarize_documents(user_query=user_query, documents=payload)
        sources = [c.as_source() for c in candidates]

        if summary_res.error:
            notes.append(f"Resumen documental incompleto: {summary_res.error}")
        elif summary_res.used_fallback:
            notes.append("Resumen documental generado mediante heurística por indisponibilidad del modelo.")

        if not summary_res.summary:
            notes.append("Documentos internos consultados, pero no se obtuvo resumen automático.")

        notes.append("Documentos internos consultados: " + ", ".join(paths))

        return {"summary": summary_res.summary, "sources": sources}

    # ---------- API ----------
    def answer(self, user_query: str, show_plotly_inline=None, prefer_web: bool = False, web_only: bool = False) -> Dict[str, Any]:
        notes: List[str] = []

        # 0) Plan
        plan = self._orc.plan(user_query, prefer_web=(prefer_web or web_only))
        doc_ctx = self._build_document_context(plan, user_query, notes)

        # 1) Rama web-only
        if web_only or (getattr(plan, "need_sql", True) is False and getattr(plan, "need_web", False)):
            web_ctx = self._web.search_and_summarize(user_query, max_results=5) or {"summary": "", "sources": []}
            text_raw = compose_response(
                plan=plan,
                df=None,
                spec=None,
                stats={},
                web_ctx=web_ctx,
                doc_ctx=doc_ctx,
                notes=notes,
            )
            text_fin = final_check(text_raw)
            text_out = text_fin["text"] if isinstance(text_fin, dict) else str(text_fin)
            self._safe_log(user_query=user_query, notes=notes, summary_text=text_out)
            return {"ok": True, "text": text_out, "sql": None, "df": None, "notes": notes, "web": web_ctx}

        # 2) SQL path
        build = build_sql_from_plan(plan)
        if getattr(build, "notes", None):
            notes.extend(build.notes)
        sql = build.sql

        # 2.1) Ejecuta con fallback si excede
        try:
            res = execute_sql(sql)
        except RuntimeError as e1:
            # Intento de fallback a un solo MES (MAX(MES))
            last_mes = self._get_last_mes()
            if last_mes:
                patt = r"CAST\(MES AS INT64\)\s+BETWEEN\s+\d{6}\s+AND\s+\d{6}"
                if re.search(patt, sql):
                    sql_fb = re.sub(patt, f"CAST(MES AS INT64) = {last_mes}", sql)
                else:
                    # Añadir condición si no hay BETWEEN
                    sql_clean = sql.rstrip().rstrip(";")
                    if re.search(r"\bWHERE\b", sql_clean, re.IGNORECASE):
                        sql_fb = sql_clean + f" AND CAST(MES AS INT64) = {last_mes}"
                    else:
                        sql_fb = sql_clean + f" WHERE CAST(MES AS INT64) = {last_mes}"
                notes.append(f"Consulta costosa ({str(e1)}). Fallback a un solo MES={last_mes}.")
                res = execute_sql(sql_fb)
                sql = sql_fb
            else:
                raise

        rows = res.get("rows", [])
        df = pd.DataFrame(rows)
        stats = res.get("stats", {})

        # 3) Auditoría de datos
        try:
            df_checked, audit_notes = audit_dataset(df, plan)
            if audit_notes:
                notes.extend(audit_notes)
        except Exception as e_aud:
            df_checked = df
            notes.append(f"Auditoría de datos omitida: {e_aud}")

        # 4) Visualización + auditoría
        spec = None
        try:
            spec = pick_spec(plan, df_checked)
            vnotes = audit_visual(spec, df_checked)
            if vnotes:
                notes.extend(vnotes)
        except Exception as e_v:
            notes.append(f"Visualización omitida: {e_v}")

        # 5) Redacción y control final
        text_raw = compose_response(
            plan=plan,
            df=df_checked,
            spec=spec,
            stats=stats,
            web_ctx=None,
            doc_ctx=doc_ctx,
            notes=notes,
        )
        text_fin = final_check(text_raw)
        text_out = text_fin["text"] if isinstance(text_fin, dict) else str(text_fin)

        # 6) Log laxo
        self._safe_log(user_query=user_query, notes=notes, summary_text=text_out)

        return {
            "ok": True,
            "text": text_out,
            "sql": sql,
            "df": df_checked.to_dict(orient="records"),
            "notes": notes,
            "stats": stats
        }
