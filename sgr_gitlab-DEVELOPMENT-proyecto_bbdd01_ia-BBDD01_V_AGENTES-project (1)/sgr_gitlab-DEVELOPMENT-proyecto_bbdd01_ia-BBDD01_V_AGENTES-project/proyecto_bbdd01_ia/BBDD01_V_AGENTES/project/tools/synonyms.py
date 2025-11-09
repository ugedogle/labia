
# -*- coding: utf-8 -*-
import re, unicodedata
from typing import List, Optional, Tuple, Dict
from pathlib import Path
try:
    import yaml
except Exception:
    yaml = None

def _norm(s:str)->str:
    if not s: return ""
    s = unicodedata.normalize("NFKD", s)
    s = s.encode("ascii","ignore").decode("ascii")
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+","", s)
    return s

def _load_yaml(path:Path)->Dict:
    if yaml is None or not path.exists():
        # fallback mínimo si no hay PyYAML o falta el fichero
        return {
            "columns": {
                "IDE_FISCAL_PERSONA": ["nif","dni","nifpersona","idefiscalpersona","acreditado","persona","cliente"],
                "IDEN_FISCAL_GRUPO":  ["nifgrupo","idenfiscalgrupo","empresa","grupo","razonsocial"],
                "DES_NOMBRE_PERSONA": ["nombre","nombrepersona","titular","acreditado"],
                "DES_NOMBRE_GRUPO":   ["nombregrupo","razonsocial","empresa","grupo"],
                "CALIFICACION_GRUPO": ["calificacion","rating"],
                "SECTOR_COV19":       ["sector","sectorestrategico","sectoreco"],
                "MES":                ["mes","periodo","fechames"],
                "TOTAL_RIESGO":       ["riesgo","riesgototal","totalriesgo"],
                "IMP_CARTERA":        ["importe","cartera"],
            }
        }
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {"columns": {}}

def build_alias_index(columns_available:List[str], synonyms_cfg:Dict)->Dict[str, str]:
    """
    Devuelve dict alias_normalizado -> columna_real
    Incluye alias de synonyms.yaml + cada columna por su string normalizado.
    """
    alias2col: Dict[str,str] = {}
    # 1) Alias desde YAML
    for real_col, aliases in (synonyms_cfg.get("columns") or {}).items():
        for al in aliases or []:
            alias2col[_norm(al)] = real_col
    # 2) Auto: cada columna se mapea a sí misma
    for c in columns_available:
        alias2col[_norm(c)] = c
    return alias2col

def smart_pick_column(user_term:str,
                      columns_available:List[str],
                      user_query_text:str="",
                      synonyms_cfg:Optional[Dict]=None) -> Tuple[str, Optional[str]]:
    """
    Devuelve (columna_real, nota) o lanza ValueError si imposible decidir.
    Reglas:
      - "nif" ⇒ IDE_FISCAL_PERSONA si existe; si no, IDEN_FISCAL_GRUPO.
      - Si prompt contiene "grupo/empresa" ⇒ prioriza *_GRUPO; si "persona/acreditado" ⇒ *_PERSONA.
      - Si hay alias exacto/normalizado ⇒ se usa.
      - Si ambigüedad ⇒ mejor esfuerzo + nota explicativa.
    """
    nterm = _norm(user_term)
    nq = _norm(user_query_text or "")
    syn = synonyms_cfg or {}

    # Index alias -> real
    idx = build_alias_index(columns_available, syn)

    def _prefer_persona(cands:List[str])->Optional[str]:
        for p in ["IDE_FISCAL_PERSONA", "DES_NOMBRE_PERSONA"]:
            if p in columns_available: return p
        # Fallback: cualquiera que termine en _PERSONA
        for c in columns_available:
            if c.endswith("_PERSONA"): return c
        return None

    def _prefer_grupo(cands:List[str])->Optional[str]:
        for p in ["IDEN_FISCAL_GRUPO", "DES_NOMBRE_GRUPO","CALIFICACION_GRUPO"]:
            if p in columns_available: return p
        for c in columns_available:
            if c.endswith("_GRUPO"): return c
        return None

    # 1) pistas por query
    force_persona = any(k in nq for k in ["persona","acreditado","cliente","titular"])
    force_grupo   = any(k in nq for k in ["grupo","empresa","razonsocial"])

    # 2) atajo nif
    if nterm in ["nif","dni","nifpersona","idefiscalpersona"]:
        if force_grupo:
            cand = "IDEN_FISCAL_GRUPO" if "IDEN_FISCAL_GRUPO" in columns_available else None
            if cand: return cand, "Interpreté NIF como IDEN_FISCAL_GRUPO (pistas: grupo/empresa)."
        cand = "IDE_FISCAL_PERSONA" if "IDE_FISCAL_PERSONA" in columns_available else None
        if cand: return cand, "Interpreté NIF como IDE_FISCAL_PERSONA."
        cand = "IDEN_FISCAL_GRUPO" if "IDEN_FISCAL_GRUPO" in columns_available else None
        if cand: return cand, "Usé IDEN_FISCAL_GRUPO por ausencia de campo persona."
        raise ValueError("No encuentro columnas NIF (persona/grupo).")

    # 3) alias directo desde YAML/auto
    if nterm in idx:
        real = idx[nterm]
        return real, None

    # 4) heurística por intención (nombre, empresa, persona…)
    if nterm in ["nombre","nombrepersona","titular","acreditado"]:
        cand = _prefer_persona(columns_available)
        if cand: return cand, "Interpreté 'nombre' como DES_NOMBRE_PERSONA."
    if nterm in ["nombregrupo","razonsocial","empresa","grupo"]:
        cand = _prefer_grupo(columns_available)
        if cand: return cand, "Interpreté 'nombre' como DES_NOMBRE_GRUPO."

    # 5) ultimo recurso: fuzzy ligero por substring normalizado
    for c in columns_available:
        if _norm(c)==nterm: 
            return c, None
    for c in columns_available:
        if nterm and nterm in _norm(c):
            return c, f"Elegí {c} por similitud con '{user_term}'."

    raise ValueError(f"No puedo mapear '{user_term}' a las columnas disponibles.")
