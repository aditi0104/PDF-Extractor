from __future__ import annotations

import json
from pathlib import Path

import streamlit as st

from extractor.cache import ExtractionCache
from extractor.jobs import ExtractionJobManager
from extractor.service import (
    SUPPORTED_EXTENSIONS,
    cleanup_paths,
    write_uploaded_files,
)


st.set_page_config(page_title="Document Ingestion Check", layout="wide")


def main() -> None:
    st.title("Document Ingestion Check")
    st.caption("Fast ingestion mode: cached extraction + background processing.")

    _ensure_state()
    job_manager = get_job_manager()
    cache = get_extraction_cache()

    with st.sidebar:
        st.header("Settings")
        tesseract_path = st.text_input(
            "Tesseract executable path",
            value=r"C:\Program Files\Tesseract-OCR\tesseract.exe",
        )
        max_folder_files = st.number_input("Max files per folder run", min_value=10, max_value=5000, value=500, step=10)
        st.caption("Scanned PDFs are rejected. Images are processed with OCR.")
        stats = cache.stats()
        st.caption(f"Extraction cache entries: {stats['entries']}")

    render_job_status(job_manager)
    has_active_job = st.session_state.get("active_job_id") is not None

    mode = st.radio("Input mode", ["Upload files", "Scan folder"], horizontal=True)

    if mode == "Upload files":
        uploaded_files = st.file_uploader(
            "Upload PDF, DOCX, TXT, CSV, MD, LOG, PNG, JPG, TIFF, BMP files",
            accept_multiple_files=True,
            type=["pdf", "docx", "txt", "csv", "md", "log", "png", "jpg", "jpeg", "tif", "tiff", "bmp"],
        )
        if st.button("Start extraction job", type="primary", disabled=(not uploaded_files) or has_active_job):
            paths = write_uploaded_files(uploaded_files or [])
            st.session_state["upload_temp_paths"] = [str(path) for path in paths]
            job_id = job_manager.submit_paths(paths, tesseract_path=tesseract_path, cache=cache)
            st.session_state["active_job_id"] = job_id
            st.session_state["latest_results"] = None
            st.rerun()
    else:
        folder_path = st.text_input("Folder path", value=r"C:\Users\aditi\Downloads")
        if st.button("Start extraction job", type="primary", disabled=has_active_job):
            paths = list_supported_files(folder_path)
            if not paths:
                st.warning("No supported files found in that folder.")
            else:
                paths = paths[: int(max_folder_files)]
                job_id = job_manager.submit_paths(paths, tesseract_path=tesseract_path, cache=cache)
                st.session_state["active_job_id"] = job_id
                st.session_state["latest_results"] = None
                st.rerun()

    if st.session_state.get("latest_results"):
        render_results(st.session_state["latest_results"])

    render_usage_notes()


def _ensure_state() -> None:
    if "active_job_id" not in st.session_state:
        st.session_state["active_job_id"] = None
    if "latest_results" not in st.session_state:
        st.session_state["latest_results"] = None
    if "upload_temp_paths" not in st.session_state:
        st.session_state["upload_temp_paths"] = []


@st.cache_resource
def get_job_manager() -> ExtractionJobManager:
    return ExtractionJobManager(max_workers=2)


@st.cache_resource
def get_extraction_cache() -> ExtractionCache:
    return ExtractionCache(Path("data") / "extraction_cache.json")


def render_job_status(job_manager: ExtractionJobManager) -> None:
    job_id = st.session_state.get("active_job_id")
    if not job_id:
        return

    job = job_manager.get(job_id)
    if job.status == "running":
        st.info(f"Extraction job running: {job.job_id}")
        col1, col2, col3 = st.columns(3)
        with col1:
            if st.button("Refresh job status"):
                st.rerun()
        with col2:
            if st.button("Cancel display (job keeps running)"):
                st.session_state["active_job_id"] = None
                st.rerun()
        with col3:
            if st.button("Stop current job"):
                job_manager.cancel(job.job_id)
                st.rerun()
        return

    if job.status == "cancelling":
        st.warning(f"Extraction job cancelling: {job.job_id}")
        if st.button("Refresh job status"):
            st.rerun()
        return

    if job.status == "completed":
        st.success(f"Extraction job completed: {job.job_id}")
        st.session_state["latest_results"] = job.result
        _cleanup_temp_upload_paths()
        st.session_state["active_job_id"] = None
        return

    if job.status == "cancelled":
        st.warning(f"Extraction job cancelled: {job.job_id}")
        st.session_state["latest_results"] = job.result or []
        _cleanup_temp_upload_paths()
        st.session_state["active_job_id"] = None
        return

    if job.status == "failed":
        st.error(f"Extraction job failed: {job.error}")
        _cleanup_temp_upload_paths()
        st.session_state["active_job_id"] = None
        return

    st.warning("Job no longer available.")
    st.session_state["active_job_id"] = None


def _cleanup_temp_upload_paths() -> None:
    temp_paths = [Path(raw) for raw in st.session_state.get("upload_temp_paths", [])]
    if temp_paths:
        cleanup_paths(temp_paths)
    st.session_state["upload_temp_paths"] = []


def list_supported_files(folder_path: str) -> list[Path]:
    root = Path(folder_path).expanduser()
    if not root.exists() or not root.is_dir():
        return []
    return [path for path in root.rglob("*") if path.is_file() and path.suffix.lower() in SUPPORTED_EXTENSIONS]


def render_results(results: list) -> None:
    st.subheader("Extraction Results")

    total_files = len(results)
    ok_files = sum(1 for result in results if result.status == "ok")
    warning_files = sum(1 for result in results if result.status == "warning")
    error_files = sum(1 for result in results if result.status == "error")

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Files processed", total_files)
    col2.metric("OK", ok_files)
    col3.metric("Warnings", warning_files)
    col4.metric("Errors", error_files)

    rows = [
        {
            "file_name": result.file_name,
            "file_type": result.file_type,
            "status": result.status,
            "method": result.extraction_method,
            "char_count": result.char_count,
            "page_count": result.page_count,
            "error": result.error or "",
        }
        for result in results
    ]
    headers = ["file_name", "file_type", "status", "method", "char_count", "page_count", "error"]
    markdown_lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(["---"] * len(headers)) + " |",
    ]
    for row in rows:
        markdown_lines.append(
            "| "
            + " | ".join(
                str(row.get(header, "")).replace("\n", " ").replace("|", "/") for header in headers
            )
            + " |"
        )
    st.markdown("\n".join(markdown_lines))

    st.download_button(
        "Download extracted JSON",
        data=json.dumps([result.to_dict() for result in results], indent=2),
        file_name="extracted_documents.json",
        mime="application/json",
    )

    for result in results:
        title = f"{result.file_name} | {result.file_type} | {result.status} | {result.char_count} chars"
        with st.expander(title):
            st.write(f"Path: `{result.file_path}`")
            st.write(f"Method: `{result.extraction_method}`")
            if result.error:
                st.warning(result.error)
            st.text_area(
                "Extracted text",
                value=result.text,
                height=260,
                key=f"text_{result.file_path}",
            )


def render_usage_notes() -> None:
    with st.expander("Notes"):
        st.markdown(
            """
            - PDFs are read with `pypdf` text extraction.
            - Scanned PDFs are flagged as non-processable.
            - Image files are processed with `Tesseract OCR`.
            - Extraction is backgrounded to keep UI responsive.
            - Results are cached by file hash for much faster repeat runs.
            """
        )


if __name__ == "__main__":
    main()
