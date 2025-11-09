# -*- coding: utf-8 -*-
"""Agent helper to retrieve analyst documents stored in GCS."""

from __future__ import annotations

from typing import Iterable, Optional

from tools.gcs_documents import GCSDocumentLoader, SUPPORTED_EXTENSIONS


class DocumentsAgent:
    """Facade around :class:`GCSDocumentLoader` with small conveniences."""

    def __init__(
        self,
        *,
        bucket: Optional[str] = None,
        loader: Optional[GCSDocumentLoader] = None,
    ) -> None:
        if loader is None and bucket is None:
            raise ValueError("Debes indicar un bucket por defecto o un loader personalizado.")
        self._loader = loader or GCSDocumentLoader(bucket=bucket)

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


__all__ = ["DocumentsAgent"]
