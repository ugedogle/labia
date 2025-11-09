# -*- coding: utf-8 -*-
from __future__ import annotations
import re
import pandas as pd
from typing import Iterable
from config.settings import PII_OUTPUT_MODE

# Indicadores de columnas PII por NOMBRE (heurística simple)
PII_NAME_HINTS = {"NIF", "IDE_FISCAL", "ID_FISCAL", "DNI", "NUM_PERSONA", "IDEN_FISCAL", "NOMBRE"}

# Patrón aproximado para NIF/CIF/NIE (solo si se activa máscara)
PATTERN_NIF = re.compile(r"\b([A-Z]\d{7}[A-Z]|\d{8}[A-Z]|\d{9,12})\b", re.IGNORECASE)

def detect_pii_columns(columns: Iterable[str]) -> list[str]:
    up = [c.upper() for c in columns]
    hits = []
    for i, c in enumerate(up):
        if any(h in c for h in PII_NAME_HINTS):
            hits.append(list(columns)[i])
    return hits

def mask_text(s: str) -> str:
    # Enmascara parte central (si se activa política 'mask')
    return PATTERN_NIF.sub(lambda m: m.group(0)[:2] + "****" + m.group(0)[-2:], s)

def maybe_mask_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Política PII de salida:
    - allow : devuelve df tal cual (no enmascara)
    - mask  : enmascara patrones NIF/CIF/NIE en columnas de texto
    - forbid: lanza excepción si detecta columnas potencialmente PII
    """
    mode = (PII_OUTPUT_MODE or "allow").lower()
    if mode == "allow":
        return df

    pii_cols = detect_pii_columns(df.columns)
    if mode == "forbid" and pii_cols:
        raise RuntimeError(f"Salida bloqueada por PII (columnas: {pii_cols})")

    if mode == "mask":
        df2 = df.copy()
        text_cols = [c for c in df2.columns if df2[c].dtype == object]
        for c in text_cols:
            df2[c] = df2[c].astype(str).map(mask_text)
        return df2
    return df
