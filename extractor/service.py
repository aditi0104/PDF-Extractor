from __future__ import annotations

from dataclasses import asdict, dataclass
import logging
from pathlib import Path
import shutil
import subprocess
import tempfile
from typing import Iterable
from xml.etree import ElementTree
from zipfile import ZipFile

from pypdf import PdfReader
from .cache import ExtractionCache, build_cache_key


TEXT_EXTENSIONS = {".txt", ".md", ".log", ".csv"}
IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".tif", ".tiff", ".bmp"}
WORD_EXTENSIONS = {".docx"}
PDF_EXTENSIONS = {".pdf"}
SUPPORTED_EXTENSIONS = TEXT_EXTENSIONS | IMAGE_EXTENSIONS | WORD_EXTENSIONS | PDF_EXTENSIONS
PDF_TEXT_THRESHOLD = 100

# Suppress noisy parser warnings for malformed PDFs; we handle them explicitly.
logging.getLogger("pypdf").setLevel(logging.ERROR)


@dataclass
class ExtractedDocument:
    file_name: str
    file_path: str
    file_type: str
    extraction_method: str
    char_count: int
    page_count: int | None
    text: str
    status: str
    error: str | None = None

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def extract_documents_from_folder(folder_path: str, tesseract_path: str | None = None) -> list[ExtractedDocument]:
    root = Path(folder_path).expanduser()
    if not root.exists():
        raise FileNotFoundError(f"Folder not found: {folder_path}")
    if not root.is_dir():
        raise NotADirectoryError(f"Not a folder: {folder_path}")

    paths = [path for path in root.rglob("*") if path.is_file() and path.suffix.lower() in SUPPORTED_EXTENSIONS]
    return extract_documents_from_paths(paths, tesseract_path=tesseract_path)


def extract_documents_from_paths(
    paths: Iterable[str | Path],
    tesseract_path: str | None = None,
    cache: ExtractionCache | None = None,
) -> list[ExtractedDocument]:
    documents: list[ExtractedDocument] = []
    for raw_path in paths:
        path = Path(raw_path)
        suffix = path.suffix.lower()
        cache_key = build_cache_key(path)
        if cache is not None:
            cached_payload = cache.get(cache_key)
            if cached_payload is not None:
                documents.append(ExtractedDocument(**cached_payload))
                continue
        try:
            if suffix in TEXT_EXTENSIONS:
                document = _extract_text_file(path)
            elif suffix in PDF_EXTENSIONS:
                document = _extract_pdf(path)
            elif suffix in WORD_EXTENSIONS:
                document = _extract_docx(path)
            elif suffix in IMAGE_EXTENSIONS:
                document = _extract_image(path, tesseract_path=tesseract_path)
            else:
                continue
            documents.append(document)
            if cache is not None:
                cache.set(cache_key, document.to_dict())
        except Exception as exc:  # pragma: no cover - surface extraction failures to UI
            error_document = ExtractedDocument(
                file_name=path.name,
                file_path=str(path),
                file_type=_file_type_for_suffix(suffix),
                extraction_method="error",
                char_count=0,
                page_count=None,
                text="",
                status="error",
                error=str(exc),
            )
            documents.append(error_document)
            if cache is not None:
                cache.set(cache_key, error_document.to_dict())
    return documents


def write_uploaded_files(uploaded_files: Iterable[object]) -> list[Path]:
    temp_dir = Path(tempfile.mkdtemp(prefix="ingestion_uploads_"))
    saved_paths: list[Path] = []
    for uploaded_file in uploaded_files:
        destination = temp_dir / uploaded_file.name
        destination.write_bytes(uploaded_file.getbuffer())
        saved_paths.append(destination)
    return saved_paths


def cleanup_paths(paths: Iterable[Path]) -> None:
    temp_roots = {path.parent for path in paths}
    for root in temp_roots:
        if root.exists() and root.name.startswith("ingestion_uploads_"):
            shutil.rmtree(root, ignore_errors=True)


def _extract_text_file(path: Path) -> ExtractedDocument:
    text = path.read_text(encoding="utf-8", errors="ignore")
    return ExtractedDocument(
        file_name=path.name,
        file_path=str(path),
        file_type="text",
        extraction_method="plain_text",
        char_count=len(text),
        page_count=None,
        text=text,
        status="ok",
    )


def _extract_pdf(path: Path) -> ExtractedDocument:
    header = path.read_bytes()[:5]
    if header != b"%PDF-":
        return ExtractedDocument(
            file_name=path.name,
            file_path=str(path),
            file_type="pdf",
            extraction_method="invalid_pdf_rejected",
            char_count=0,
            page_count=None,
            text="",
            status="warning",
            error="Invalid/corrupt PDF file (missing PDF header). Cannot process.",
        )

    try:
        reader = PdfReader(str(path), strict=False)
        pages: list[str] = []
        for page in reader.pages:
            pages.append(page.extract_text() or "")
        text = "\n\n".join(segment.strip() for segment in pages if segment is not None).strip()
    except Exception:
        return ExtractedDocument(
            file_name=path.name,
            file_path=str(path),
            file_type="pdf",
            extraction_method="corrupt_pdf_rejected",
            char_count=0,
            page_count=None,
            text="",
            status="warning",
            error="Corrupt or unsupported PDF structure. Cannot process.",
        )

    if len(text) >= PDF_TEXT_THRESHOLD:
        return ExtractedDocument(
            file_name=path.name,
            file_path=str(path),
            file_type="pdf",
            extraction_method="pypdf_text",
            char_count=len(text),
            page_count=len(reader.pages),
            text=text,
            status="ok",
        )

    return ExtractedDocument(
        file_name=path.name,
        file_path=str(path),
        file_type="pdf",
        extraction_method="scanned_pdf_rejected",
        char_count=0,
        page_count=len(reader.pages),
        text="",
        status="warning",
        error="Scanned PDF detected (no text layer). Cannot process.",
    )


def _extract_docx(path: Path) -> ExtractedDocument:
    paragraphs = _read_docx_paragraphs(path)
    text = "\n".join(paragraphs)
    return ExtractedDocument(
        file_name=path.name,
        file_path=str(path),
        file_type="docx",
        extraction_method="docx_xml",
        char_count=len(text),
        page_count=None,
        text=text,
        status="ok",
    )


def _extract_image(path: Path, tesseract_path: str | None = None) -> ExtractedDocument:
    executable = _resolve_tesseract_path(tesseract_path)
    completed = subprocess.run(
        [str(executable), str(path), "stdout", "--oem", "1", "--psm", "6", "-l", "eng"],
        capture_output=True,
        check=False,
    )
    stdout = _decode_process_output(completed.stdout).strip()
    stderr = _decode_process_output(completed.stderr).strip()
    if completed.returncode != 0 and not stdout:
        raise RuntimeError((stderr or "Tesseract OCR failed.").strip())

    if not stdout:
        return ExtractedDocument(
            file_name=path.name,
            file_path=str(path),
            file_type="image",
            extraction_method="image_ocr_unreadable",
            char_count=0,
            page_count=1,
            text="",
            status="warning",
            error="Scanned or low-quality image detected. Cannot process.",
        )

    return ExtractedDocument(
        file_name=path.name,
        file_path=str(path),
        file_type="image",
        extraction_method="tesseract_ocr",
        char_count=len(stdout),
        page_count=1,
        text=stdout,
        status="ok",
        error=None,
    )


def _read_docx_paragraphs(path: Path) -> list[str]:
    namespace = {"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"}
    with ZipFile(path) as archive:
        document_xml = archive.read("word/document.xml")
    root = ElementTree.fromstring(document_xml)
    paragraphs: list[str] = []
    for paragraph in root.findall(".//w:p", namespace):
        text_parts = [
            node.text
            for node in paragraph.findall(".//w:t", namespace)
            if node.text and node.text.strip()
        ]
        if text_parts:
            paragraphs.append("".join(text_parts).strip())
    return paragraphs


def _resolve_tesseract_path(tesseract_path: str | None) -> Path:
    candidates = [Path(tesseract_path)] if tesseract_path else []
    candidates.append(Path(r"C:\Program Files\Tesseract-OCR\tesseract.exe"))
    for candidate in candidates:
        if candidate.exists():
            return candidate
    raise FileNotFoundError(
        "Tesseract executable not found. Install Tesseract OCR or provide the executable path in the UI."
    )


def _decode_process_output(data: bytes | str | None) -> str:
    if data is None:
        return ""
    if isinstance(data, str):
        return data
    for encoding in ("utf-8", "utf-8-sig", "cp1252", "latin-1"):
        try:
            return data.decode(encoding)
        except UnicodeDecodeError:
            continue
    return data.decode("utf-8", errors="ignore")


def _file_type_for_suffix(suffix: str) -> str:
    if suffix in TEXT_EXTENSIONS:
        return "text"
    if suffix in PDF_EXTENSIONS:
        return "pdf"
    if suffix in WORD_EXTENSIONS:
        return "docx"
    if suffix in IMAGE_EXTENSIONS:
        return "image"
    return "unknown"
