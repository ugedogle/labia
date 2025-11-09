# -*- coding: utf-8 -*-
from __future__ import annotations
import json, re
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field
from google import genai
from google.genai.types import GenerateContentConfig, Tool, GoogleSearch, HttpOptions

from config.settings import TABLA_BASE_FQN

class Ordering(BaseModel):
    by: str
    dir: str = "DESC"

class VizPref(BaseModel):
    mode: str = "text"
    chart_type: Optional[str] = None

class Plan(BaseModel):
    intent: str = ""
    need_sql: bool = True
    tables: List[str] = Field(default_factory=list)
    metrics: List[str] = Field(default_factory=list)
    dimensions: List[str] = Field(default_factory=list)
    filters: Dict[str, Any] = Field(default_factory=dict)
    ordering: List[Ordering] = Field(default_factory=list)
    limit: int = 200
    viz_pref: VizPref = Field(default_factory=VizPref)
    need_web: bool = False
    privacy_mode: Optional[str] = None
    cost_guardrails: Optional[Dict[str, Any]] = None
    clarification_request: Optional[str] = None

_JSON_RE = re.compile(r'\{[\s\S]*\}', re.MULTILINE)

def _extract_json(text: str) -> str:
    m = _JSON_RE.search(text or "")
    if not m:
        raise ValueError("No se pudo extraer JSON del LLM.")
    return m.group(0)

def _normalize_metrics(metrics: List[str]) -> List[str]:
    out = []
    for m in metrics or []:
        mm = (m or "").strip()
        if mm and mm not in out:
            out.append(mm)
    return out

def _normalize_ordering(ordering: List[Ordering], metrics: List[str]) -> List[Ordering]:
    if not ordering:
        if metrics:
            # quita alias AS xxx si viene
            base = re.sub(r'\s+AS\s+\w+$', '', metrics[0], flags=re.I)
            return [Ordering(by=base, dir="DESC")]
        return []
    out = []
    for o in ordering:
        by = (o.by or "").strip()
        d  = (o.dir or "DESC").upper()
        if d not in ("ASC", "DESC"):
            d = "DESC"
        if by:
            out.append(Ordering(by=by, dir=d))
    return out

def _load_system_prompt() -> str:
    try:
        p = Path("prompts/orchestrator_system.md")
        if p.exists():
            return p.read_text(encoding="utf-8")
    except Exception:
        pass
    return (
        "Eres un planificador. Devuelve SOLO JSON con este esquema:\n"
        "{'intent': str, 'need_sql': bool, 'tables': [str], 'metrics': [str], 'dimensions': [str], "
        "'filters': {'MES': {'type':'range_ym','from':'YYYYMM','to':'YYYYMM'}, 'PII': {'mask': bool}}, "
        "'ordering':[{'by':str,'dir':'ASC|DESC'}], 'limit': int, "
        "'viz_pref': {'mode':'chart|table|text','chart_type': str|null}, "
        "'need_web': bool, 'privacy_mode': str|null, 'cost_guardrails': {'enforce_limit': bool, 'max_bytes': str}|null, "
        "'clarification_request': str|null}\n"
        f"Si no estás seguro, usa: tables=['{TABLA_BASE_FQN}'], metrics=['SUM(TOTAL_RIESGO) AS RIESGO'], limit=200, viz_pref.mode='text'."
    )

def _sanitize_tables(tbls: List[str]) -> List[str]:
    out = []
    for t in tbls or []:
        tt = (t or "").strip().strip("`")
        if not tt or "<<" in tt or ">>" in tt or tt.upper() in {"TABLA_BASE_FQN", "<TABLA_BASE_FQN>", "<<TABLA_BASE_FQN>>"}:
            out.append(TABLA_BASE_FQN)
        else:
            # si ya viene backtickeada, respetamos; si no, ponemos backticks a la FQN
            if tt.count(".") == 2:
                out.append(f"`{tt}`")
            else:
                # incompleta → forzamos tabla base
                out.append(TABLA_BASE_FQN)
    if not out:
        out = [TABLA_BASE_FQN]
    return out

class Orchestrator:
    def __init__(self, prompts_dir: str = "prompts", model: str = "gemini-2.5-pro", temperature: float = 0.2):
        self.model = model
        self.temperature = temperature
        self._client = genai.Client(http_options=HttpOptions(api_version="v1"))
        self._system = _load_system_prompt()
        self._gen_config = GenerateContentConfig(
            system_instruction=self._system,
            tools=[Tool(google_search=GoogleSearch())],
            temperature=self.temperature,
            response_mime_type="text/plain",
        )

    def _only_text(self, resp) -> str:
        txt = ""
        try:
            for c in (resp.candidates or []):
                parts = getattr(c.content, "parts", []) or []
                for p in parts:
                    t = getattr(p, "text", None)
                    if t:
                        txt += t + "\n"
        except Exception:
            pass
        return txt.strip()

    def plan(self, user_query: str, prefer_web: bool = False) -> Plan:
        if not user_query or not user_query.strip():
            raise ValueError("La consulta del usuario está vacía.")

        try:
            resp = self._client.models.generate_content(
                model=self.model,
                config=self._gen_config,
                contents=[{"role": "user", "parts": [{"text": user_query.strip()}]}],
            )
            text = self._only_text(resp)
            data = json.loads(_extract_json(text))
        except Exception:
            # Fallback: si no hay JSON, construimos un plan mínimo
            data = {
                "intent": user_query.strip(),
                "need_sql": not prefer_web,
                "tables": [TABLA_BASE_FQN],
                "metrics": ["SUM(TOTAL_RIESGO) AS RIESGO"],
                "dimensions": [],
                "filters": {},
                "ordering": [],
                "limit": 200,
                "viz_pref": {"mode": "text", "chart_type": None},
                "need_web": bool(prefer_web),
                "privacy_mode": "strict",
                "cost_guardrails": {"enforce_limit": True, "max_bytes": "2147483648"},
                "clarification_request": None,
            }

        # Defaults + normalización
        if not data.get("viz_pref"):
            data["viz_pref"] = {"mode": "text", "chart_type": None}
        if data.get("limit") is None:
            data["limit"] = 200
        if prefer_web:
            data["need_web"] = True
            if data.get("need_sql") is True:
                data["need_sql"] = False

        plan = Plan(**data)
        plan.metrics = _normalize_metrics(list(plan.metrics or []))
        plan.ordering = _normalize_ordering(plan.ordering, plan.metrics)
        plan.tables = _sanitize_tables(plan.tables)

        if not plan.tables:
            plan.tables = [TABLA_BASE_FQN]

        return plan
