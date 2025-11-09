# -*- coding: utf-8 -*-
from __future__ import annotations
from typing import Dict, Any, Optional, List
import pandas as pd

__all__ = ["pick_spec"]

# Reglas simples para elegir spec:
# - Si hay MES -> línea (serie temporal)
# - Si hay una dimensión típica -> barras
# - Si no, toma la primera columna no-RIESGO como X y barras
DIM_CANDIDATAS = [
    "SECTOR_ESTRATEGICO", "SECTOR_COV19",
    "CALIFICACION_GRUPO", "PRIORIZACION_GRUPO",
    "DES_GESTORA_PAIS", "PROVINCIA", "TERRITORIAL",
    "DES_NOMBRE_GRUPO", "DES_NOMBRE_PERSONA",
    "IDEN_FISCAL_GRUPO", "IDE_FISCAL_PERSONA"
]

def _first_dim(df: pd.DataFrame) -> Optional[str]:
    cols = list(df.columns)
    for c in DIM_CANDIDATAS:
        if c in cols:
            return c
    for c in cols:
        if c != "RIESGO":
            return c
    return None

def pick_spec(df: pd.DataFrame, plan=None) -> Dict[str, Any]:
    """
    Devuelve un spec mínimo para tools.viz.render_chart:
      {'type': 'line'|'bar', 'x': <col>, 'y': 'RIESGO'}
    """
    if df is None or df.empty:
        return {}

    y = "RIESGO" if "RIESGO" in df.columns else None
    if y is None:
        # si no existe RIESGO, no proponemos gráfico
        return {}

    if "MES" in df.columns:
        return {"type": "line", "x": "MES", "y": y}

    x = _first_dim(df)
    if x is None:
        return {}
    return {"type": "bar", "x": x, "y": y}
