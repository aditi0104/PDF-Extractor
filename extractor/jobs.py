from __future__ import annotations

from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from threading import Event, Lock
import uuid

from .cache import ExtractionCache
from .service import ExtractedDocument, extract_documents_from_paths


@dataclass
class JobRecord:
    job_id: str
    status: str
    result: list[ExtractedDocument] | None
    error: str | None


class ExtractionJobManager:
    def __init__(self, max_workers: int = 2) -> None:
        self._executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="extractor")
        self._futures: dict[str, Future] = {}
        self._cancel_events: dict[str, Event] = {}
        self._lock = Lock()

    def submit_paths(
        self,
        paths: list[Path],
        tesseract_path: str | None = None,
        cache: ExtractionCache | None = None,
    ) -> str:
        job_id = str(uuid.uuid4())
        cancel_event = Event()

        def run_job() -> list[ExtractedDocument]:
            results: list[ExtractedDocument] = []
            for path in paths:
                if cancel_event.is_set():
                    break
                results.extend(
                    extract_documents_from_paths([path], tesseract_path=tesseract_path, cache=cache)
                )
            return results

        future = self._executor.submit(run_job)
        with self._lock:
            self._futures[job_id] = future
            self._cancel_events[job_id] = cancel_event
        return job_id

    def get(self, job_id: str) -> JobRecord:
        with self._lock:
            future = self._futures.get(job_id)
            cancel_event = self._cancel_events.get(job_id)
        if future is None:
            return JobRecord(job_id=job_id, status="not_found", result=None, error="Job not found.")
        if not future.done():
            if cancel_event is not None and cancel_event.is_set():
                return JobRecord(job_id=job_id, status="cancelling", result=None, error=None)
            return JobRecord(job_id=job_id, status="running", result=None, error=None)
        try:
            result = future.result()
            status = "cancelled" if cancel_event is not None and cancel_event.is_set() else "completed"
            return JobRecord(job_id=job_id, status=status, result=result, error=None)
        except Exception as exc:  # pragma: no cover - background exceptions surfaced to UI
            return JobRecord(job_id=job_id, status="failed", result=None, error=str(exc))

    def cancel(self, job_id: str) -> bool:
        with self._lock:
            cancel_event = self._cancel_events.get(job_id)
        if cancel_event is None:
            return False
        cancel_event.set()
        return True
