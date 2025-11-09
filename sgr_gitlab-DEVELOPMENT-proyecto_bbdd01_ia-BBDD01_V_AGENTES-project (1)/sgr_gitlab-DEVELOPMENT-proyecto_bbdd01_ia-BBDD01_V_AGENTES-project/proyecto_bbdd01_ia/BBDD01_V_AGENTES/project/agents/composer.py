# -*- coding: utf-8 -*-
from __future__ import annotations
from typing import Dict, Any, Optional, List, Iterable
import pandas as pd

def _title_from_plan(plan) -> str:
    t = getattr(plan, "intent", None)
    return t if t else "Resultado del análisis"

def _format_tags(value: Any) -> str:
    if not value:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, Iterable):
        tags = [str(v).strip() for v in value if str(v).strip()]
        return ", ".join(dict.fromkeys(tags))
    return str(value)


def compose_response(plan,
                     df: Optional[pd.DataFrame],
                     stats: Dict[str, Any],
                     notes: List[str],
                     spec: Dict[str, Any],
                     web_ctx: Optional[Dict[str, Any]] = None,
                     doc_ctx: Optional[Dict[str, Any]] = None) -> str:
    lines: List[str] = []
    lines.append(f"### {_title_from_plan(plan)}")

    # Pequeño resumen de datos (si hay)
    if isinstance(df, pd.DataFrame) and not df.empty:
        lines.append(f"- Filas: {len(df)}")
        if "MES" in df.columns:
            try:
                minm = df["MES"].min()
                maxm = df["MES"].max()
                lines.append(f"- Rango MES: {minm}–{maxm}")
            except Exception:
                pass

    # Notas/avisos
    if notes:
        lines.append("**Notas:** " + " | ".join([str(n) for n in notes if n]))

    # Contexto documental interno
    if doc_ctx:
        summary = (doc_ctx.get("summary") or "").strip()
        if summary:
            lines.append("**Contexto documental interno:**")
            lines.append(summary)
        srcs = doc_ctx.get("sources") or []
        if srcs:
            lines.append("**Documentos consultados:**")
            for s in srcs:
                title = s.get("title") or s.get("url") or s.get("uri") or ""
                uri = s.get("url") or s.get("uri") or s.get("path") or ""
                details: List[str] = []
                desc = (s.get("description") or "").strip()
                if desc:
                    details.append(desc)
                tags = _format_tags(s.get("tags") or s.get("labels"))
                if tags:
                    details.append(f"tags: {tags}")
                suffix = f" ({'; '.join(details)})" if details else ""
                if uri:
                    lines.append(f"- {title} — {uri}{suffix}")
                else:
                    lines.append(f"- {title}{suffix}")

    # Contexto externo
    if web_ctx:
        summary = (web_ctx.get("summary") or "").strip()
        if summary:
            lines.append("**Contexto externo (resumen):**")
            lines.append(summary)
        srcs = web_ctx.get("sources") or []
        if srcs:
            lines.append("**Fuentes:**")
            for s in srcs:
                t = s.get("title") or s.get("url") or ""
                u = s.get("url") or ""
                d = s.get("date") or ""
                if u and d:
                    lines.append(f"- {t} — {u} ({d})")
                elif u:
                    lines.append(f"- {t} — {u}")
                else:
                    lines.append(f"- {t}")

    return "\n".join(lines)
