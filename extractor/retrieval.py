from __future__ import annotations

from collections import Counter
import math
import re
from typing import Iterable

from .service import ExtractedDocument


TOKEN_PATTERN = re.compile(r"[A-Za-z0-9]{2,}")


def two_stage_search(
    query: str,
    documents: Iterable[ExtractedDocument],
    shortlist_size: int = 20,
    top_k: int = 5,
) -> list[dict[str, object]]:
    query_tokens = _tokens(query)
    if not query_tokens:
        return []

    stage1 = []
    for document in documents:
        doc_tokens = _tokens(document.text)
        if not doc_tokens:
            continue
        overlap = _jaccard(query_tokens, doc_tokens)
        if overlap > 0:
            stage1.append((overlap, document))
    stage1.sort(key=lambda item: item[0], reverse=True)
    shortlist = [doc for _, doc in stage1[:shortlist_size]]

    stage2 = []
    for document in shortlist:
        score = _cosine_count(query_tokens, _tokens(document.text))
        stage2.append(
            {
                "file_name": document.file_name,
                "file_type": document.file_type,
                "status": document.status,
                "score": round(score * 100.0, 1),
                "preview": document.text[:220].replace("\n", " "),
            }
        )
    stage2.sort(key=lambda row: row["score"], reverse=True)
    return stage2[:top_k]


def _tokens(text: str) -> list[str]:
    return [token.lower() for token in TOKEN_PATTERN.findall(text or "")]


def _jaccard(left: list[str], right: list[str]) -> float:
    left_set = set(left)
    right_set = set(right)
    if not left_set or not right_set:
        return 0.0
    return len(left_set & right_set) / len(left_set | right_set)


def _cosine_count(left: list[str], right: list[str]) -> float:
    left_count = Counter(left)
    right_count = Counter(right)
    if not left_count or not right_count:
        return 0.0
    keys = set(left_count) | set(right_count)
    dot = sum(left_count[key] * right_count[key] for key in keys)
    norm_left = math.sqrt(sum(value * value for value in left_count.values()))
    norm_right = math.sqrt(sum(value * value for value in right_count.values()))
    if norm_left == 0 or norm_right == 0:
        return 0.0
    return dot / (norm_left * norm_right)
