# -*- coding: utf-8 -*-
"""Herramientas para cargar documentos almacenados en Google Cloud Storage.

Incluye soporte para documentos Word (.docx) convirtiéndolos a texto plano.
"""
from __future__ import annotations

import io
import zipfile
import sys
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, TYPE_CHECKING
from urllib.parse import urlparse

try:  # pragma: no cover - dependencia opcional durante tests
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

from config.settings import PROJECT_ID


class DocumentParseError(Exception):
    """Error específico cuando no se puede interpretar un documento."""


_DOCX_MAIN_DOC = "word/document.xml"
_DOCX_NAMESPACE = "{http://schemas.openxmlformats.org/wordprocessingml/2006/main}"


@dataclass
class DocumentMetadata:
    """Metadatos básicos asociados a un objeto en GCS."""

    bucket: str
    name: str
    size: int
    uri: str
    updated: Optional[object]
    content_type: Optional[str]
    metadata: Optional[Dict[str, str]]


@dataclass
class DocumentPayload(DocumentMetadata):
    """Resultado de lectura de un documento."""

    text: str


class GCSDocumentLoader:
    """Carga y convierte documentos almacenados en Google Cloud Storage."""

    def __init__(
        self,
        bucket_name: str,
        default_prefix: str = "",
        allowed_extensions: Optional[Sequence[str]] = None,
        storage_client: Optional[StorageClient] = None,
        project_id: Optional[str] = None,
    ) -> None:
        if not bucket_name:
            raise ValueError("bucket_name es obligatorio para GCSDocumentLoader")

        self.bucket_name = bucket_name
        self.default_prefix = (default_prefix or "").strip("/")
        self.allowed_extensions = tuple(
            (ext or "").lower() for ext in (allowed_extensions or (".docx",))
        )
        if storage_client:
            self._client = storage_client
        else:
            if _storage is None:
                raise ImportError(
                    "google-cloud-storage no está instalado; pasa un storage_client o instala la dependencia."
                ) from _STORAGE_IMPORT_ERROR
            self._client = _storage.Client(project=project_id or PROJECT_ID)
        self._bucket = self._client.bucket(self.bucket_name)

    # ------------------------------------------------------------------
    # Normalización de rutas
    # ------------------------------------------------------------------
    def _resolve_blob_name(self, blob_name: str) -> str:
        if not blob_name or not blob_name.strip():
            raise ValueError("blob_name no puede estar vacío")

        name = blob_name.strip()
        if name.startswith("gs://"):
            parsed = urlparse(name)
            bucket = parsed.netloc
            if not bucket:
                raise ValueError("URI GCS mal formada")
            if bucket != self.bucket_name:
                raise ValueError(
                    f"El bucket '{bucket}' no coincide con el configurado '{self.bucket_name}'"
                )
            name = parsed.path.lstrip("/")
        else:
            name = name.lstrip("/")

        if self.default_prefix:
            prefix = self.default_prefix
            if name == prefix or name.startswith(prefix + "/"):
                return name
            name = f"{prefix}/{name}" if name else prefix
        return name

    def _full_uri(self, blob_name: str) -> str:
        return f"gs://{self.bucket_name}/{blob_name}".rstrip("/")

    # ------------------------------------------------------------------
    # Listado y lectura
    # ------------------------------------------------------------------
    def list_documents(self, prefix: Optional[str] = None) -> List[DocumentMetadata]:
        resolved_prefix = self._resolve_list_prefix(prefix)
        blobs = self._client.list_blobs(self.bucket_name, prefix=resolved_prefix)
        results: List[DocumentMetadata] = []
        for blob in blobs:
            if not self._is_allowed(blob.name):
                continue
            bucket_name = getattr(getattr(blob, "bucket", None), "name", self.bucket_name)
            results.append(
                DocumentMetadata(
                    bucket=bucket_name,
                    name=blob.name,
                    size=int(getattr(blob, "size", 0) or 0),
                    uri=self._full_uri(blob.name),
                    updated=getattr(blob, "updated", None),
                    content_type=getattr(blob, "content_type", None),
                    metadata=getattr(blob, "metadata", None),
                )
            )
        return results

    def _resolve_list_prefix(self, prefix: Optional[str]) -> Optional[str]:
        if prefix is None:
            return self.default_prefix or None
        pref = prefix.strip().lstrip("/")
        if not pref:
            return self.default_prefix or None
        if pref.startswith("gs://"):
            parsed = urlparse(pref)
            if parsed.netloc and parsed.netloc != self.bucket_name:
                raise ValueError(
                    f"El bucket '{parsed.netloc}' no coincide con el configurado '{self.bucket_name}'"
                )
            pref = parsed.path.lstrip("/")
        if self.default_prefix:
            base = self.default_prefix
            if pref == base or pref.startswith(base + "/"):
                return pref
            return f"{base}/{pref}" if pref else base
        return pref or None

    def load_document(self, blob_name: str) -> DocumentPayload:
        resolved_name = self._resolve_blob_name(blob_name)
        if not self._is_allowed(resolved_name):
            raise DocumentParseError(
                f"Extensión no soportada para '{resolved_name}'. Solo se admiten: {', '.join(self.allowed_extensions)}"
            )
        blob = self._bucket.get_blob(resolved_name)
        if blob is None:
            raise FileNotFoundError(
                f"No se encontró el objeto '{resolved_name}' en el bucket '{self.bucket_name}'"
            )
        data = blob.download_as_bytes()
        text = self._parse_blob(resolved_name, data)
        return DocumentPayload(
            bucket=self.bucket_name,
            name=resolved_name,
            size=len(data),
            uri=self._full_uri(resolved_name),
            updated=getattr(blob, "updated", None),
            content_type=getattr(blob, "content_type", None),
            metadata=getattr(blob, "metadata", None),
            text=text,
        )

    # ------------------------------------------------------------------
    # Parsing
    # ------------------------------------------------------------------
    def _is_allowed(self, blob_name: str) -> bool:
        return blob_name.lower().endswith(self.allowed_extensions)

    def _parse_blob(self, blob_name: str, data: bytes) -> str:
        blob_name_lower = blob_name.lower()
        if blob_name_lower.endswith(".docx"):
            return _parse_docx_bytes(data)
        if blob_name_lower.endswith(".txt"):
            return data.decode("utf-8-sig", errors="replace")
        raise DocumentParseError(
            f"No existe un parser registrado para '{blob_name_lower}'."
        )


# ----------------------------------------------------------------------
# Parseo de DOCX
# ----------------------------------------------------------------------

def _parse_docx_bytes(raw_bytes: bytes) -> str:
    if not raw_bytes:
        return ""
    try:
        with zipfile.ZipFile(io.BytesIO(raw_bytes)) as zf:
            with zf.open(_DOCX_MAIN_DOC) as fh:
                xml_bytes = fh.read()
    except KeyError as exc:
        raise DocumentParseError("El archivo DOCX no contiene word/document.xml") from exc
    except zipfile.BadZipFile as exc:
        raise DocumentParseError("El archivo DOCX está corrupto o no es válido") from exc

    return _extract_text_from_document_xml(xml_bytes)


def _extract_text_from_document_xml(xml_bytes: bytes) -> str:
    import xml.etree.ElementTree as ET

    try:
        tree = ET.fromstring(xml_bytes)
    except ET.ParseError as exc:
        raise DocumentParseError("No se pudo parsear el contenido XML del DOCX") from exc

    paragraphs: List[str] = []
    for para in tree.findall(f".//{_DOCX_NAMESPACE}p"):
        text_fragments: List[str] = []
        for node in para.iter():
            tag = node.tag
            if tag == f"{_DOCX_NAMESPACE}t":
                text_fragments.append(node.text or "")
            elif tag == f"{_DOCX_NAMESPACE}tab":
                text_fragments.append("\t")
            elif tag in {f"{_DOCX_NAMESPACE}br", f"{_DOCX_NAMESPACE}cr"}:
                text_fragments.append("\n")
        paragraph_text = "".join(text_fragments)
        if paragraph_text.strip():
            paragraphs.append(paragraph_text.strip())

    return "\n".join(paragraphs).strip()


__all__ = [
    "DocumentParseError",
    "DocumentMetadata",
    "DocumentPayload",
    "GCSDocumentLoader",
]
