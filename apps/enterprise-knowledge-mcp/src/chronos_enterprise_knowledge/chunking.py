"""Deterministic structure-aware chunking for enterprise documents."""

from __future__ import annotations

import array
import hashlib
import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import tiktoken

from chronos_enterprise_knowledge.models import (
    DocumentChunk,
    KnowledgeDocument,
)

_BLANK_LINES = re.compile(r"\n\s*\n")


@dataclass(frozen=True)
class ChunkingConfig:
    target_tokens: int = 512
    overlap_tokens: int = 64
    encoding: str = "cl100k_base"

    def __post_init__(self) -> None:
        if self.target_tokens < 64:
            raise ValueError("target_tokens must be at least 64")
        if self.overlap_tokens < 0:
            raise ValueError("overlap_tokens must be non-negative")
        if self.overlap_tokens >= self.target_tokens:
            raise ValueError("overlap_tokens must be smaller than target_tokens")


class EnterpriseChunker:
    """Keep natural blocks when possible and bound chunks by model tokens."""

    def __init__(self, config: ChunkingConfig | None = None):
        self.config = config or ChunkingConfig()
        self._hf_tokenizer: Any | None = None
        if self.config.encoding.startswith("hf:"):
            model = self.config.encoding.removeprefix("hf:").strip()
            if not model:
                raise ValueError("Hugging Face chunk encoding requires a model")
            try:
                from transformers import AutoTokenizer
            except ImportError as exc:  # pragma: no cover - dependency error path
                raise RuntimeError(
                    "transformers is required for Hugging Face token-aware chunking"
                ) from exc
            self._hf_tokenizer = AutoTokenizer.from_pretrained(model)
            self.encoding = None
        else:
            self.encoding = tiktoken.get_encoding(self.config.encoding)

    def chunk_text(
        self,
        document: KnowledgeDocument,
        index_text: str,
        *,
        context: Mapping[str, Any] | None = None,
    ) -> list[tuple[str, dict[str, Any]]]:
        header = self._context_header(document, context or {})
        header_tokens = len(self._encode(header))
        content_budget = max(32, self.config.target_tokens - header_tokens)
        blocks = [
            block.strip() for block in _BLANK_LINES.split(index_text) if block.strip()
        ]
        windows = self._pack_blocks(blocks, content_budget)
        chunks = []
        for ordinal, body in enumerate(windows):
            text = f"{header}\n\n{body}".strip()
            chunks.append(
                (
                    text,
                    {
                        "ordinal": ordinal,
                        "document_path": document.path,
                        "document_kind": document.kind,
                        **dict(context or {}),
                    },
                )
            )
        return chunks

    def with_embeddings(
        self,
        document: KnowledgeDocument,
        chunks: Sequence[tuple[str, Mapping[str, Any]]],
        embeddings: Sequence[Sequence[float]],
    ) -> tuple[DocumentChunk, ...]:
        if len(chunks) != len(embeddings):
            raise ValueError("chunks and embeddings must have the same length")
        result = []
        for ordinal, ((text, metadata), embedding) in enumerate(
            zip(chunks, embeddings, strict=True)
        ):
            normalized: Sequence[float]
            if getattr(embedding, "implicit_zero", False):
                normalized = embedding
            else:
                normalized = array.array(
                    "f",
                    (float(value) for value in embedding),
                )
            result.append(
                DocumentChunk(
                    id=f"{document.id}:{ordinal:05d}",
                    document_id=document.id,
                    ordinal=ordinal,
                    text=text,
                    embedding=(
                        normalized
                        if getattr(normalized, "implicit_zero", False)
                        else tuple(float(value) for value in normalized)
                    ),
                    metadata=dict(metadata),
                )
            )
        return tuple(result)

    def _pack_blocks(self, blocks: Iterable[str], budget: int) -> list[str]:
        result: list[str] = []
        current: list[int] = []
        for block in blocks:
            block_tokens = self._encode(block)
            if not block_tokens:
                continue
            if len(block_tokens) > budget:
                if current:
                    result.append(self._decode(current).strip())
                    current = []
                result.extend(self._token_windows(block_tokens, budget))
                continue
            separator = self._encode("\n\n") if current else []
            if len(current) + len(separator) + len(block_tokens) <= budget:
                current.extend(separator)
                current.extend(block_tokens)
                continue
            result.append(self._decode(current).strip())
            overlap = current[-self.config.overlap_tokens :] if current else []
            current = list(overlap)
            if current:
                current.extend(self._encode("\n\n"))
            current.extend(block_tokens)
            if len(current) > budget:
                result.extend(self._token_windows(current, budget)[:-1])
                current = self._encode(self._token_windows(current, budget)[-1])
        if current:
            result.append(self._decode(current).strip())
        return [chunk for chunk in result if chunk]

    def _token_windows(self, tokens: Sequence[int], budget: int) -> list[str]:
        step = max(1, budget - self.config.overlap_tokens)
        result = []
        for start in range(0, len(tokens), step):
            window = tokens[start : start + budget]
            if not window:
                break
            result.append(self._decode(window).strip())
            if start + budget >= len(tokens):
                break
        return [chunk for chunk in result if chunk]

    def _encode(self, text: str) -> list[int]:
        if self._hf_tokenizer is not None:
            return list(
                self._hf_tokenizer.encode(
                    text,
                    add_special_tokens=False,
                    verbose=False,
                )
            )
        # Enterprise source files can contain tokenizer sentinel strings in
        # code, logs, and model prompts. They are document text, not control
        # tokens for this chunking operation.
        assert self.encoding is not None
        return self.encoding.encode(text, disallowed_special=())

    def _decode(self, tokens: Sequence[int]) -> str:
        if self._hf_tokenizer is not None:
            return str(
                self._hf_tokenizer.decode(
                    list(tokens),
                    skip_special_tokens=False,
                    clean_up_tokenization_spaces=False,
                )
            )
        assert self.encoding is not None
        return self.encoding.decode(list(tokens))

    @staticmethod
    def _context_header(
        document: KnowledgeDocument,
        context: Mapping[str, Any],
    ) -> str:
        fields = [
            f"Title: {document.title}",
            f"Source: {document.source}",
            f"Path: {document.path}",
        ]
        for key in ("connector", "workspace", "project", "team", "channel"):
            value = context.get(key)
            if value not in (None, "", [], {}):
                fields.append(f"{key.replace('_', ' ').title()}: {value}")
        return "[Document context]\n" + "\n".join(fields)


def chunk_fingerprint(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


__all__ = [
    "ChunkingConfig",
    "EnterpriseChunker",
    "chunk_fingerprint",
]
