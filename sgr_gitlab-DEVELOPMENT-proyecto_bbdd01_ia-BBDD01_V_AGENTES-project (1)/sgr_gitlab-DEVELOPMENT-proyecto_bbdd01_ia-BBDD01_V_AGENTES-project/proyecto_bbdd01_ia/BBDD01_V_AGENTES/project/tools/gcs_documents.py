# -*- coding: utf-8 -*-
"""Utility helpers to fetch and parse documents stored in Google Cloud Storage."""

from __future__ import annotations

import io
import mimetypes
import os
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Optional, Tuple
import zipfile
from xml.etree import ElementTree as ET

try:  # pragma: no cover - defensive import, exercised in integration
    from google.cloud import storage  # type: ignore
except Exception as exc:  # pragma: no cover - executed only if dependency missing
    storage = None  # type: ignore[assignment]
    _STORAGE_IMPORT_ERROR = exc
else:  # pragma: no cover - executed when dependency available
    _STORAGE_IMPORT_ERROR = None

_DOCX_NAMESPACE = "{http://schemas.openxmlformats.org/wordprocessingml/2006/main}"
SUPPORTED_EXTENSIONS = {".txt", ".md", ".csv", ".json", ".docx"}


@dataclass(frozen=True)
class GCSPath:
    """Normalized representation of a path inside GCS."""

    bucket: str
    blob: str

    @classmethod
    def parse(cls, value: str, default_bucket: Optional[str] = None) -> "GCSPath":
        """Parse either ``gs://bucket/blob`` or a plain blob name."""

        if not value:
            raise ValueError("La ruta del documento está vacía.")

        value = value.strip()
        if value.startswith("gs://"):
            without_scheme = value[5:]
            parts = without_scheme.split("/", 1)
            if len(parts) == 1 or not parts[1]:
                raise ValueError("La ruta GCS debe incluir objeto dentro del bucket.")
            return cls(bucket=parts[0], blob=parts[1])

        if not default_bucket:
            raise ValueError(
                "Falta el bucket de GCS; proporciona un URI completo gs:// o especifica bucket por defecto."
            )

        return cls(bucket=default_bucket, blob=value)

    @property
    def uri(self) -> str:
        return f"gs://{self.bucket}/{self.blob}"


class GCSDocumentLoader:
    """Load documents from Google Cloud Storage and expose their textual content."""

    def __init__(
        self,
        bucket: Optional[str] = None,
        *,
        storage_client: Optional["storage.Client"] = None,
        text_max_bytes: int = 5 * 1024 * 1024,
    ) -> None:
        if storage_client is None:
            if storage is None:
                raise RuntimeError(
                    "google.cloud.storage no está disponible; instala google-cloud-storage o injecta un cliente."
                ) from _STORAGE_IMPORT_ERROR
            storage_client = storage.Client()

        self._client = storage_client
        self._default_bucket = bucket
        self._text_max_bytes = max(int(text_max_bytes), 0)

    # --- Public API -----------------------------------------------------

    def read_text(self, path: str) -> str:
        """Return textual representation of the given object."""

        gcs_path = GCSPath.parse(path, default_bucket=self._default_bucket)
        data, content_type = self._download_blob(gcs_path)
        return self._bytes_to_text(data, gcs_path.blob, content_type)

    # --- Internal helpers -----------------------------------------------

    def _download_blob(self, gcs_path: GCSPath) -> Tuple[bytes, Optional[str]]:
        bucket = self._client.bucket(gcs_path.bucket)
        blob = bucket.blob(gcs_path.blob)
        blob.reload()  # ensures metadata such as content_type/size are available

        size = blob.size or 0
        if self._text_max_bytes and size > self._text_max_bytes:
            raise ValueError(
                f"El fichero {gcs_path.uri} pesa {size} bytes y supera el máximo permitido ({self._text_max_bytes})."
            )

        content_type = getattr(blob, "content_type", None)
        return blob.download_as_bytes(), content_type

    def _bytes_to_text(self, data: bytes, blob_name: str, content_type: Optional[str]) -> str:
        ext = self._guess_extension(blob_name, content_type)
        if ext == ".docx":
            return _extract_docx_text(data)
        if ext in {".txt", ".md", ".csv", ".json"}:
            return data.decode(_detect_encoding(data), errors="replace")

        raise ValueError(
            f"Formato de fichero no soportado para {blob_name!r}. Extensiones permitidas: {sorted(SUPPORTED_EXTENSIONS)}."
        )

    @staticmethod
    def _guess_extension(blob_name: str, content_type: Optional[str]) -> str:
        blob_ext = PurePosixPath(blob_name).suffix.lower()
        if blob_ext:
            return blob_ext

        if content_type:
            guessed = mimetypes.guess_extension(content_type)
            if guessed:
                return guessed
        return ""


def _detect_encoding(data: bytes) -> str:
    if data.startswith(b"\xff\xfe"):
        return "utf-16"
    if data.startswith(b"\xfe\xff"):
        return "utf-16"
    if data.startswith(b"\xef\xbb\xbf"):
        return "utf-8-sig"
    return "utf-8"


def _extract_docx_text(data: bytes) -> str:
    if not data:
        return ""

    try:
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            with zf.open("word/document.xml") as document_xml:
                xml_bytes = document_xml.read()
    except KeyError as exc:
        raise ValueError("El documento DOCX no contiene word/document.xml.") from exc
    except zipfile.BadZipFile as exc:
        raise ValueError("El fichero DOCX está corrupto o no es válido.") from exc

    root = ET.fromstring(xml_bytes)
    paragraphs = []
    for paragraph in root.iter(f"{_DOCX_NAMESPACE}p"):
        parts = []
        for node in paragraph.iter():
            tag = node.tag.split("}")[-1] if "}" in node.tag else node.tag
            if tag == "t" and node.text:
                parts.append(node.text)
            elif tag == "tab":
                parts.append("\t")
            elif tag in {"br", "cr"}:
                parts.append("\n")
        text = "".join(parts).strip()
        if text:
            paragraphs.append(text)
    return os.linesep.join(paragraphs)


__all__ = ["GCSDocumentLoader", "GCSPath", "SUPPORTED_EXTENSIONS"]
