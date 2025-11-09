
from typing import Dict, Any, List
from tools.web_search import search_google

def search_and_summarize(query: str, max_results: int = 5) -> Dict[str, Any]:
    res = search_google(query, max_results=max_results) or {"results": []}
    results = res.get("results") or []
    # Resumen simple a partir de snippets (sin LLM para evitar fallos)
    snippets = [r.get("snippet") for r in results if r.get("snippet")]
    summary = ""
    if snippets:
        uniq = []
        for s in snippets:
            s = s.strip()
            if s and s not in uniq:
                uniq.append(s)
        summary = " ".join(uniq[:4])  # breve
    return {
        "summary": summary,
        "sources": [{"title": r.get("title"), "url": r.get("url"), "date": r.get("date")} for r in results]
    }


class WebAgent:
    @staticmethod
    def search_and_summarize(query: str, max_results: int = 5) -> dict:
        return search_and_summarize(query, max_results=max_results)


# Compat: permitir instanciación con args ignorados
def _wa_init_noop(self, *args, **kwargs):
    pass

def _wa_search_inst(self, query: str, max_results: int = 5):
    return search_and_summarize(query, max_results=max_results)

# Inyectar métodos si faltan
WebAgent.__init__ = _wa_init_noop
WebAgent.search_and_summarize = _wa_search_inst
