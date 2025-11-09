# -*- coding: utf-8 -*-
from __future__ import annotations
from typing import Dict, Any, Optional
import pandas as pd

__all__ = ["audit_dataset"]

def audit_dataset(df: Optional[pd.DataFrame], plan=None) -> Dict[str, Any]:
    """
    Auditoría ligera del dataset:
      - No bloquea si df está vacío, sólo añade notas.
      - Señala si hay muchas filas.
      - Señala negativos en RIESGO.
      - Sugiere revisar orden de MES.
    """
    out: Dict[str, Any] = {"ok": True, "notes": [], "feedback": None}

    if df is None:
        out["notes"].append("Sin datos (df=None).")
        return out

    if df.empty:
        out["notes"].append("Sin filas devueltas en el rango solicitado.")
        return out

    n = len(df)
    if n > 2000:
        out["notes"].append(f"Resultado grande: {n} filas; considera agregar o acotar el rango.")

    try:
        if "RIESGO" in df.columns:
            s = pd.to_numeric(df["RIESGO"], errors="coerce").dropna()
            if (s < 0).any():
                out["notes"].append("RIESGO contiene valores negativos.")
    except Exception:
        pass

    try:
        if "MES" in df.columns:
            out["notes"].append("Verifica el orden de MES (ascendente) para series temporales.")
    except Exception:
        pass

    return out
