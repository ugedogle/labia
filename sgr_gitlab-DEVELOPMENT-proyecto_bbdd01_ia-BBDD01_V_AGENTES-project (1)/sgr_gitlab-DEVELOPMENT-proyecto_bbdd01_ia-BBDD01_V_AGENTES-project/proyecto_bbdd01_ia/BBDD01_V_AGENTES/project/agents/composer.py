# -*- coding: utf-8 -*-
from __future__ import annotations
from typing import Dict, Any, Optional, List
import pandas as pd

def _title_from_plan(plan) -> str:
    t = getattr(plan, "intent", None)
    return t if t else "Resultado del análisis"

def compose_response(plan,
                     df: Optional[pd.DataFrame],
                     stats: Dict[str, Any],
                     notes: List[str],
                     spec: Dict[str, Any],
                     web_ctx: Optional[Dict[str, Any]] = None) -> str:
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
