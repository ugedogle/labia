# -*- coding: utf-8 -*-
from __future__ import annotations
from typing import Dict, Any
import pandas as pd
import plotly.express as px

def render_chart(df: pd.DataFrame, spec: Dict[str, Any], show_plotly_inline):
    """
    Render mínimo basado en Plotly:
      - bar: ordena por Y descendente
      - line: si x=MES y es numérico, ordena por MES y lo pasa a str para eje
    """
    t = (spec.get("type") or "").lower()
    x = spec.get("x")
    y = spec.get("y")

    dfa = df.copy()

    if t == "bar":
        if y in dfa.columns:
            dfa = dfa.sort_values(by=y, ascending=False)
        fig = px.bar(dfa, x=x, y=y, title=f"{y} por {x}")
        return show_plotly_inline(fig)

    if t == "line":
        if x == "MES" and x in dfa.columns:
            try:
                dfa = dfa.sort_values(by=x)
                dfa[x] = dfa[x].astype(str)
            except Exception:
                pass
        fig = px.line(dfa, x=x, y=y, markers=True, title=f"{y} por {x}")
        return show_plotly_inline(fig)

    # fallback: nada
    return None
