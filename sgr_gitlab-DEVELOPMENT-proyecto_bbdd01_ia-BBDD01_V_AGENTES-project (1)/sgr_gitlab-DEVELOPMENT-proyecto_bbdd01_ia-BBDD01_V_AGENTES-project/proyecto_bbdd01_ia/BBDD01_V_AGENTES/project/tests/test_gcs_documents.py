# -*- coding: utf-8 -*-
import datetime as dt
import io
import zipfile
import pathlib
import sys

import pytest

ROOT_DIR = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from tools import gcs_documents
from tools.gcs_documents import GCSDocumentLoader


def _build_docx(paragraphs):
    xml_body = [
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>",
        '<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">',
        "<w:body>",
    ]
    for text in paragraphs:
        xml_body.append("<w:p><w:r><w:t>{}</w:t></w:r></w:p>".format(text))
    xml_body.append("</w:body></w:document>")
    xml = "".join(xml_body)
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as zf:
        zf.writestr("word/document.xml", xml)
    return buffer.getvalue()


class _DummyBlob:
    def __init__(self, name, payload, content_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document"):
        self.name = name
        self._payload = payload
        self.size = len(payload)
        self.updated = dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc)
        self.content_type = content_type
        self.metadata = {"source": "unit-test"}

    def download_as_bytes(self):
        return self._payload


class _DummyBucket:
    def __init__(self, name, blobs):
        self.name = name
        self._blobs = dict(blobs)

    def get_blob(self, name):
        return self._blobs.get(name)


class _DummyClient:
    def __init__(self, bucket):
        self._bucket = bucket

    def bucket(self, name):
        assert name == self._bucket.name
        return self._bucket

    def list_blobs(self, bucket_name, prefix=None):
        assert bucket_name == self._bucket.name
        for name, blob in self._bucket._blobs.items():
            if prefix and not name.startswith(prefix):
                continue
            yield blob


@pytest.fixture
def dummy_loader():
    docx = _build_docx(["Hola", "segunda línea"])
    blob = _DummyBlob("docs/sample.docx", docx)
    bucket = _DummyBucket("demo-bucket", {blob.name: blob})
    client = _DummyClient(bucket)
    return GCSDocumentLoader(
        bucket_name="demo-bucket",
        default_prefix="docs",
        allowed_extensions=(".docx",),
        storage_client=client,
        project_id="demo-project",
    )


def test_parse_docx_bytes_extracts_paragraphs():
    docx = _build_docx(["Primera línea", "Segunda línea"])
    text = gcs_documents._parse_docx_bytes(docx)
    assert "Primera línea" in text
    assert "Segunda línea" in text
    assert "\n" in text


def test_load_document_returns_text(dummy_loader):
    payload = dummy_loader.load_document("sample.docx")
    assert payload.name == "docs/sample.docx"
    assert payload.uri == "gs://demo-bucket/docs/sample.docx"
    assert "Hola" in payload.text
    assert payload.metadata["source"] == "unit-test"


def test_list_documents_filters_by_extension(dummy_loader):
    other = _DummyBlob("docs/skip.txt", b"hola", content_type="text/plain")
    dummy_loader._bucket._blobs[other.name] = other
    docs = dummy_loader.list_documents()
    assert len(docs) == 1
    assert docs[0].name.endswith("sample.docx")


def test_gs_uri_with_wrong_bucket_raises(dummy_loader):
    with pytest.raises(ValueError):
        dummy_loader.load_document("gs://otro-bucket/docs/sample.docx")
