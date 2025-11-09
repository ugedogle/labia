# -*- coding: utf-8 -*-
from __future__ import annotations
from typing import Dict, Any, List
import pandas as pd

__all__ = ["audit_visual"]

def audit_visual(df: pd.DataFrame, spec: Dict[str, Any]) -> Dict[str, Any]:
    """
    Valida de forma ligera el spec y añade notas (no bloquea).
    Reglas:
      - Debe tener type, x, y
      - Si 'bar' y demasiadas categorías únicas -> nota
      - Si 'line' y x=MES -> sugerir orden ascendente
    """
    notes: List[str] = []
    ok = True
    s = dict(spec or {})

    required = ["type", "x", "y"]
    missing = [k for k in required if k not in s]
    if missing:
        ok = False
        notes.append(f"Spec incompleto (faltan: {', '.join(missing)}).")
        return {"ok": ok, "spec": s, "notes": notes}

    t = (s.get("type") or "").lower()
    x = s.get("x")
    y = s.get("y")

    if x not in df.columns:
        ok = False
        notes.append(f"Columna X '{x}' no existe en el dataframe.")
    if y not in df.columns:
        ok = False
        notes.append(f"Columna Y '{y}' no existe en el dataframe.")

    if ok and t == "bar":
        try:
            nunique = df[x].nunique(dropna=False)
            if nunique > 30:
                notes.append(f"Demasiadas categorías en '{x}' ({nunique}); considera filtrar o agrupar.")
        except Exception:
            pass

    if ok and t == "line" and x == "MES":
        notes.append("Revisa que MES esté en orden ascendente para la lectura del gráfico.")

    return {"ok": ok, "spec": s, "notes": notes}
