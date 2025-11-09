# -*- coding: utf-8 -*-
"""Helpers to map user plans to internal GCS documents."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Iterable, List, Optional, Sequence, Tuple

from config.settings import DOCS_CATALOG_PATH, DOCS_BUCKET


@dataclass(frozen=True)
class DocumentEntry:
    uri: str
    title: str
    description: str
    keywords: Tuple[str, ...]
    tags: Tuple[str, ...]
    tables: Tuple[str, ...]
    always: bool
    path: str


@dataclass(frozen=True)
class DocumentCandidate:
    uri: str
    title: str
    description: str = ""
    tags: Tuple[str, ...] = ()
    score: float = 0.0
    path: Optional[str] = None

    def as_source(self) -> dict:
        data = {"title": self.title or self.uri, "url": self.uri}
        if self.description:
            data["description"] = self.description
        if self.tags:
            data["tags"] = list(self.tags)
        if self.path:
            data["path"] = self.path
        return data


def _normalize_iterable(values: Optional[Iterable[Any]]) -> Tuple[str, ...]:
    if values is None:
        return tuple()

    if isinstance(values, (str, bytes)):
        iterable = [values]
    else:
        iterable = values

    out: List[str] = []
    for value in iterable:
        if not isinstance(value, (str, bytes)):
            continue
        vv = str(value).strip()
        if vv and vv not in out:
            out.append(vv)
    return tuple(out)


def _default_title(path: str) -> str:
    name = PurePosixPath(path).name
    return name or path


def _load_entries(catalog_path: Optional[str] = None) -> List[DocumentEntry]:
    target = Path(catalog_path or DOCS_CATALOG_PATH)
    if not target.exists():
        return []

    try:
        raw = json.loads(target.read_text(encoding="utf-8"))
    except Exception:
        return []

    default_bucket = DOCS_BUCKET
    documents: Sequence[dict] = []

    if isinstance(raw, dict):
        default_bucket = raw.get("bucket") or default_bucket
        documents = raw.get("documents") or []
    elif isinstance(raw, list):
        documents = raw
    else:
        return []

    entries: List[DocumentEntry] = []
    seen: set[str] = set()

    for item in documents:
        if not isinstance(item, dict):
            continue
        raw_path = item.get("path") or item.get("uri") or item.get("url")
        if not raw_path:
            continue
        bucket = item.get("bucket") or default_bucket
        raw_path = str(raw_path).strip()
        if not raw_path:
            continue
        uri = raw_path
        if not uri.startswith("gs://"):
            if bucket:
                uri = f"gs://{bucket.strip('/')}/{raw_path.lstrip('/')}"
            else:
                uri = raw_path
        if uri in seen:
            continue
        seen.add(uri)
        entry = DocumentEntry(
            uri=uri,
            title=item.get("title") or _default_title(raw_path),
            description=item.get("description") or item.get("summary") or "",
            keywords=_normalize_iterable(item.get("keywords")),
            tags=_normalize_iterable(item.get("tags")),
            tables=_normalize_iterable(item.get("tables")),
            always=bool(item.get("always")),
            path=raw_path,
        )
        entries.append(entry)

    return entries


def _collect_haystack(plan, user_query: str) -> Tuple[str, List[str]]:
    parts: List[str] = []
    tables: List[str] = []

    if user_query:
        parts.append(user_query)

    intent = getattr(plan, "intent", "") or ""
    if intent:
        parts.append(intent)

    metrics = getattr(plan, "metrics", []) or []
    parts.extend(metrics)

    dimensions = getattr(plan, "dimensions", []) or []
    parts.extend(dimensions)

    filters = getattr(plan, "filters", {}) or {}
    parts.extend(list(filters.keys()))
    for value in filters.values():
        if isinstance(value, dict):
            parts.extend(str(v) for v in value.values())
        else:
            parts.append(str(value))

    tables = [t for t in getattr(plan, "tables", []) or []]

    doc_topics = getattr(plan, "doc_topics", []) or []
    parts.extend(doc_topics)

    haystack = " ".join(p for p in parts if p).lower()
    tables_normalized = [t.lower() for t in tables if isinstance(t, str)]
    return haystack, tables_normalized


def select_documents(plan, user_query: str = "", limit: int = 3, catalog_path: Optional[str] = None) -> List[DocumentCandidate]:
    entries = _load_entries(catalog_path)
    if not entries or limit <= 0:
        return []

    haystack, tables = _collect_haystack(plan, user_query)
    table_text = " ".join(tables)

    candidates: List[DocumentCandidate] = []

    for entry in entries:
        score = 0.0
        for kw in entry.keywords:
            if kw and kw.lower() in haystack:
                score += 2.0
        for tag in entry.tags:
            if tag and tag.lower() in haystack:
                score += 1.0
        for table in entry.tables:
            table_norm = table.lower()
            if table_norm in tables or table_norm in table_text:
                score += 2.5
        if entry.always:
            score += 0.5
        if score <= 0 and not entry.always:
            continue
        candidates.append(
            DocumentCandidate(
                uri=entry.uri,
                title=entry.title,
                description=entry.description,
                tags=entry.tags,
                score=score,
                path=entry.path,
            )
        )

    if not candidates and getattr(plan, "need_documents", False):
        # Fallback: devuelve los primeros documentos definidos en el catálogo
        for entry in entries[:limit]:
            candidates.append(
                DocumentCandidate(
                    uri=entry.uri,
                    title=entry.title,
                    description=entry.description,
                    tags=entry.tags,
                    score=0.1,
                    path=entry.path,
                )
            )

    candidates.sort(key=lambda c: (-c.score, c.title.lower()))
    return candidates[:limit]


__all__ = ["DocumentCandidate", "select_documents"]
