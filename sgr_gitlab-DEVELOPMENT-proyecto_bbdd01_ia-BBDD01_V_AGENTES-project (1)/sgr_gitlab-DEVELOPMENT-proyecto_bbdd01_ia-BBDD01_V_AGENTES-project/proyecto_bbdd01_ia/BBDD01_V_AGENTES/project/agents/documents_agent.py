# -*- coding: utf-8 -*-
"""Agent helper to retrieve analyst documents stored in GCS."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, Optional

from google import genai
from google.genai.types import GenerateContentConfig, HttpOptions

from config.settings import GEMINI_MODEL_DEFAULT
from tools.gcs_documents import GCSDocumentLoader, SUPPORTED_EXTENSIONS

_SUMMARY_SYSTEM_INSTRUCTION = (
    "Eres un analista senior. Resumes documentos internos enfatizando definiciones, "
    "aclaraciones operativas y advertencias relevantes para responder preguntas sobre datos."
)


@dataclass
class SummaryResult:
    summary: str
    used_fallback: bool = False
    error: Optional[str] = None


class DocumentsAgent:
    """Facade around :class:`GCSDocumentLoader` with small conveniences."""

    def __init__(
        self,
        *,
        bucket: Optional[str] = None,
        loader: Optional[GCSDocumentLoader] = None,
        summarizer_model: str = GEMINI_MODEL_DEFAULT,
        summarizer_temperature: float = 0.3,
        client: Optional["genai.Client"] = None,
        generate_config: Optional[GenerateContentConfig] = None,
    ) -> None:
        self._loader = loader or GCSDocumentLoader(bucket=bucket)
        self._summarizer_model = summarizer_model or GEMINI_MODEL_DEFAULT
        self._summarizer_temperature = float(summarizer_temperature)
        self._client: Optional["genai.Client"] = client
        self._generate_config = generate_config

    @property
    def supported_extensions(self) -> Iterable[str]:
        return sorted(SUPPORTED_EXTENSIONS)

    def read_document(self, path: str) -> str:
        """Return textual content of the object located at ``path``."""

        return self._loader.read_text(path)

    def read_many(self, paths: Iterable[str]) -> dict[str, str]:
        """Fetch several documents at once, returning a mapping path → text."""

        out: dict[str, str] = {}
        for path in paths:
            out[path] = self.read_document(path)
        return out

    def read_word(self, path: str) -> str:
        """Explicit helper for Word files (.docx)."""

        text = self.read_document(path)
        if not path.lower().endswith(".docx"):
            raise ValueError("El helper read_word está pensado para ficheros .docx.")
        return text

    # --- summarization helpers ---------------------------------------

    def summarize_documents(
        self,
        user_query: str,
        documents: Dict[str, str],
        *,
        max_documents: int = 5,
        max_chars_per_doc: int = 4000,
    ) -> SummaryResult:
        if not documents:
            return SummaryResult(summary="")

        chunks = []
        for idx, (uri, raw_text) in enumerate(documents.items(), start=1):
            if max_documents and idx > max_documents:
                break
            snippet = (raw_text or "").strip()
            if not snippet:
                continue
            if max_chars_per_doc > 0 and len(snippet) > max_chars_per_doc:
                snippet = snippet[:max_chars_per_doc]
            chunks.append(f"Documento {idx}: {uri}\n{snippet}")

        if not chunks:
            return SummaryResult(summary="")

        prompt_intro = ""
        user_query = (user_query or "").strip()
        if user_query:
            prompt_intro = f"Pregunta del usuario: {user_query}\n\n"

        documents_block = "\n\n".join(chunks)
        prompt = (
            f"{prompt_intro}Documentos internos disponibles:\n{documents_block}\n\n"
            "Redacta un resumen conciso en español (máximo 5 viñetas) con los puntos más relevantes para atender la "
            "pregunta del usuario. Destaca definiciones, supuestos o advertencias que condicionen el análisis. "
            "Si la información no es relevante, indícalo brevemente."
        )

        try:
            client = self._ensure_client()
            resp = client.models.generate_content(
                model=self._summarizer_model,
                config=self._ensure_generate_config(),
                contents=[{"role": "user", "parts": [{"text": prompt}]}],
            )
            summary_text = self._only_text(resp).strip()
            if summary_text:
                return SummaryResult(summary=summary_text)
        except Exception as exc:  # pragma: no cover - exercised in runtime integrations
            fallback = self._fallback_summary(documents)
            return SummaryResult(summary=fallback, used_fallback=True, error=str(exc))

        # fallback si el modelo devolvió vacío
        fallback = self._fallback_summary(documents)
        return SummaryResult(summary=fallback, used_fallback=bool(fallback))

    def _ensure_client(self) -> "genai.Client":
        if self._client is None:
            self._client = genai.Client(http_options=HttpOptions(api_version="v1"))
        return self._client

    def _ensure_generate_config(self) -> GenerateContentConfig:
        if self._generate_config is None:
            self._generate_config = GenerateContentConfig(
                system_instruction=_SUMMARY_SYSTEM_INSTRUCTION,
                temperature=self._summarizer_temperature,
                response_mime_type="text/plain",
            )
        return self._generate_config

    @staticmethod
    def _only_text(resp) -> str:
        text = ""
        try:
            for cand in getattr(resp, "candidates", []) or []:
                parts = getattr(cand.content, "parts", []) or []
                for part in parts:
                    t = getattr(part, "text", None)
                    if t:
                        text += t + "\n"
        except Exception:
            pass
        return text.strip()

    @staticmethod
    def _fallback_summary(documents: Dict[str, str], *, max_sources: int = 3, max_lines_per_doc: int = 2) -> str:
        lines = []
        for idx, (uri, raw_text) in enumerate(documents.items()):
            if max_sources and idx >= max_sources:
                break
            snippet_lines = [ln.strip() for ln in (raw_text or "").splitlines() if ln.strip()]
            if not snippet_lines:
                continue
            snippet = " ".join(snippet_lines[:max_lines_per_doc])
            if snippet:
                lines.append(f"- {uri}: {snippet}")
        return "\n".join(lines)


__all__ = ["DocumentsAgent", "SummaryResult"]
