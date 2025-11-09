# -*- coding: utf-8 -*-
from __future__ import annotations
import re

def final_check(text: str, plan=None, df=None, spec=None) -> dict:
    """Comprueba detalles simples: presencia de periodo y de la unidad M€; agrega si falta."""
    out = text or ""
    if "M€" not in out and "millones" not in out.lower():
        out += "\n\n_Unidades: millones de euros (M€)._"
    if "Periodo:" not in out and plan is not None:
        try:
            mes = getattr(plan, "filters", {}).get("MES")
            if isinstance(mes, dict) and mes.get("from") and mes.get("to"):
                out = out + f"\n\n**Periodo:** {mes.get('from')}–{mes.get('to')}"
        except Exception:
            pass
    return {"ok": True, "text": out}
