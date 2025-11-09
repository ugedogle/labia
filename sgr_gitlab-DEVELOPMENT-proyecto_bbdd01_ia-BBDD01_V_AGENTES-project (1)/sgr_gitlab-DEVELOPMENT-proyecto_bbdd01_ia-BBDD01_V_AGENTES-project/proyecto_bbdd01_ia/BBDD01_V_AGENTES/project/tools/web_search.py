import re, json

from typing import List, Dict, Any
from google import genai
from google.genai.types import Tool, GoogleSearch, GenerateContentConfig

_JSON_RE = re.compile(r"\{[\s\S]*\}")

def _extract_json(text: str) -> Dict[str, Any]:
    if not text:
        return {}
    m = _JSON_RE.search(text)
    if not m:
        return {}
    try:
        return json.loads(m.group(0))
    except Exception:
        return {}

def search_google(query: str, max_results: int = 5) -> Dict[str, Any]:
    """
    Usa GoogleSearch (Vertex) para obtener resultados estructurados.
    Devuelve: {'results': [{'title','url','snippet','date'}...]}
    """
    client = genai.Client()
    cfg = GenerateContentConfig(
        tools=[Tool(google_search=GoogleSearch())],
        temperature=0.0,
        response_mime_type="text/plain",
    )
    prompt = (
        "Usa google_search para encontrar NOTICIAS RECIENTES y responde SOLO JSON con la forma:\n"
        "{\"results\":[{\"title\":str,\"url\":str,\"snippet\":str,\"date\":str}]}.\n"
        f"Máximo {max_results} resultados. Consulta: {query}"
    )
    resp = client.models.generate_content(
        model="gemini-2.5-pro",
        config=cfg,
        contents=[{'role':'user','parts':[{'text': prompt}]}],
    )
    text = ""
    if resp and resp.candidates:
        for c in resp.candidates:
            parts = getattr(c, "content", None) and getattr(c.content, "parts", []) or []
            for p in parts:
                t = getattr(p, "text", None)
                if t:
                    text += (t + "\n")
    data = _extract_json(text)
    if not isinstance(data, dict) or 'results' not in data:
        return {"results": []}
    # Sanea keys mínimas
    out = []
    for r in data.get("results", [])[:max_results]:
        out.append({
            "title": r.get("title"),
            "url": r.get("url"),
            "snippet": r.get("snippet"),
            "date": r.get("date"),
        })
    return {"results": out}
