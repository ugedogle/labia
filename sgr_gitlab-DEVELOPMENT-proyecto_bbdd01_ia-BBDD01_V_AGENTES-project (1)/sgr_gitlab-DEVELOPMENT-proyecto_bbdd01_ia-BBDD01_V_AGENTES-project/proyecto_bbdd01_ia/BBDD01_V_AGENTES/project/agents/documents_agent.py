# -*- coding: utf-8 -*-
"""Agente para acceder a documentos almacenados en GCS."""
from __future__ import annotations

import sys
from typing import Any, Iterable, List, Optional, Sequence, TYPE_CHECKING

try:  # pragma: no cover - dependencia opcional
    from google.cloud import storage as _storage
except ImportError:  # pragma: no cover
    _storage = None  # type: ignore[assignment]
    _STORAGE_IMPORT_ERROR = sys.exc_info()[1]
else:  # pragma: no cover
    _STORAGE_IMPORT_ERROR = None

if TYPE_CHECKING:  # pragma: no cover - solo para type checkers
    from google.cloud import storage as _storage_types
    StorageClient = _storage_types.Client
else:
    StorageClient = Any

from config import settings
from tools.gcs_documents import (
    DocumentMetadata,
    DocumentPayload,
    GCSDocumentLoader,
)


class DocumentAgent:
    """Facilita el acceso a documentos ofimáticos en Google Cloud Storage."""

    def __init__(
        self,
        bucket_name: Optional[str] = None,
        prefix: Optional[str] = None,
        allowed_extensions: Optional[Sequence[str]] = None,
        storage_client: Optional[StorageClient] = None,
    ) -> None:
        bucket = bucket_name or settings.DOCS_GCS_BUCKET
        if not bucket:
            raise ValueError(
                "Configura DOCS_GCS_BUCKET (o pásalo al constructor) para usar DocumentAgent"
            )

        prefix = prefix if prefix is not None else settings.DOCS_GCS_PREFIX
        extensions = (
            tuple(e.lower() for e in allowed_extensions)
            if allowed_extensions is not None
            else settings.DOCS_ALLOWED_EXTENSIONS
        )

        self._loader = GCSDocumentLoader(
            bucket_name=bucket,
            default_prefix=prefix,
            allowed_extensions=extensions,
            storage_client=self._resolve_client(storage_client),
            project_id=settings.PROJECT_ID,
        )

    # ------------------------------------------------------------------
    # API pública
    # ------------------------------------------------------------------
    def list_documents(self, prefix: Optional[str] = None) -> List[DocumentMetadata]:
        """Devuelve metadatos de los documentos disponibles."""

        return self._loader.list_documents(prefix=prefix)

    def read_document(self, blob_name: str) -> DocumentPayload:
        """Descarga y convierte un documento a texto plano."""

        return self._loader.load_document(blob_name)

    def read_many(self, blob_names: Iterable[str]) -> List[DocumentPayload]:
        """Carga múltiples documentos en orden."""

        return [self.read_document(name) for name in blob_names]

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _resolve_client(self, client: Optional[StorageClient]) -> Optional[StorageClient]:
        if client is not None:
            return client
        if _storage is None:
            raise ImportError(
                "google-cloud-storage no está instalado; proporciona un storage_client o instala la dependencia."
            ) from _STORAGE_IMPORT_ERROR
        return _storage.Client(project=settings.PROJECT_ID)


__all__ = ["DocumentAgent", "DocumentMetadata", "DocumentPayload"]
