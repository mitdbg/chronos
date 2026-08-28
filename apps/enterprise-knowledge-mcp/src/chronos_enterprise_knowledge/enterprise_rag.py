"""EnterpriseRAG-Bench corpus discovery and structure-preserving extraction."""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any

import yaml

from chronos_enterprise_knowledge.models import KnowledgeDocument

_TITLE_FIELDS = (
    "title",
    "summary",
    "subject",
    "channel",
    "name",
    "company_name",
    "event_name",
    "meeting_title",
    "key",
)
_CONTEXT_FIELDS = (
    "key",
    "project",
    "team",
    "channel",
    "status",
    "priority",
    "severity",
    "assignee",
    "customer_company",
    "mailbox_owner",
    "thread_id",
    "repository",
    "artifact_type",
    "number",
    "state_at_cutoff",
)
_CODEBASE_EXCLUDED_DIRECTORIES = {
    ".git",
    ".next",
    ".venv",
    "__pycache__",
    "build",
    "coverage",
    "dist",
    "generated",
    "node_modules",
    "target",
}
_CODEBASE_EXCLUDED_RELATIVE_PREFIXES = {
    "langfuse/ee",
    "langfuse/web/src/ee",
    "langfuse/worker/src/ee",
}
_CODEBASE_EXCLUDED_FILENAMES = {
    "Cargo.lock",
    "package-lock.json",
    "pnpm-lock.yaml",
    "uv.lock",
    "yarn.lock",
}
_CODEBASE_TEXT_FILENAMES = {
    "AGENTS.md",
    "CLAUDE.md",
    "CONTRIBUTING.md",
    "Dockerfile",
    "LICENSE",
    "Makefile",
    "README",
    "README.md",
}
_CODEBASE_TEXT_SUFFIXES = {
    ".c",
    ".cc",
    ".cfg",
    ".cmake",
    ".cpp",
    ".css",
    ".cu",
    ".cuh",
    ".go",
    ".graphql",
    ".h",
    ".hpp",
    ".html",
    ".ini",
    ".java",
    ".js",
    ".json",
    ".jsx",
    ".md",
    ".mdx",
    ".mjs",
    ".proto",
    ".py",
    ".pyi",
    ".rs",
    ".scss",
    ".sh",
    ".sql",
    ".toml",
    ".ts",
    ".tsx",
    ".txt",
    ".yaml",
    ".yml",
}
_MAX_CODEBASE_FILE_BYTES = 1024 * 1024


@dataclass(frozen=True)
class CorpusRecord:
    document: KnowledgeDocument
    index_text: str
    context: dict[str, Any]
    source_path: Path


class EnterpriseRAGCorpus:
    """Read the generated corpus without mutating its source tree."""

    def __init__(self, root: str | Path):
        self.root = Path(root).expanduser().resolve()
        if not self.root.is_dir():
            raise FileNotFoundError(f"EnterpriseRAG corpus does not exist: {self.root}")
        self._path_to_id: dict[str, str] | None = None

    def iter_records(
        self,
        *,
        connectors: Sequence[str] | None = None,
        limit: int | None = None,
        include_root_documents: bool = True,
    ) -> Iterator[CorpusRecord]:
        allowed = {connector.casefold() for connector in connectors or ()}
        emitted = 0
        for path in self._iter_paths(
            include_root_documents=include_root_documents,
            connectors=allowed,
        ):
            relative = path.relative_to(self.root)
            connector = self._connector(relative)
            yield self.read_record(path)
            emitted += 1
            if limit is not None and emitted >= limit:
                return

    def iter_paths(
        self,
        *,
        connectors: Sequence[str] | None = None,
        include_root_documents: bool = True,
    ) -> Iterator[Path]:
        """Yield corpus documents in a stable order without reading contents."""

        allowed = {connector.casefold() for connector in connectors or ()}
        yield from self._iter_paths(
            include_root_documents=include_root_documents,
            connectors=allowed,
        )

    def relative_path(self, path: str | Path) -> Path:
        source_path = Path(path).expanduser().absolute()
        resolved = source_path.resolve()
        if not resolved.is_relative_to(self.root):
            raise ValueError(f"document lies outside the corpus: {source_path}")
        try:
            return source_path.relative_to(self.root)
        except ValueError as exc:
            raise ValueError(
                f"document lies outside the corpus: {source_path}"
            ) from exc

    def connector_for_path(self, path: str | Path) -> str:
        candidate = Path(path).expanduser()
        if candidate.is_absolute():
            try:
                relative = candidate.relative_to(self.root)
            except ValueError:
                relative = self.relative_path(candidate)
        else:
            relative = self.relative_path(candidate)
        return self._connector(relative)

    def _iter_paths(
        self,
        *,
        include_root_documents: bool,
        connectors: set[str] | None = None,
    ) -> Iterable[Path]:
        allowed = connectors or set()
        if include_root_documents and (not allowed or "company" in allowed):
            for name in (
                "company_overview.md",
                "initiatives.md",
                "employee_directory.yaml",
                "project_list.txt",
            ):
                path = self.root / name
                if path.is_file():
                    yield path
        sources = self.root / "sources"
        if sources.is_dir():
            source_connectors = sorted(
                path
                for path in sources.iterdir()
                if path.is_dir()
                and (not allowed or path.name.casefold() in allowed)
            )
            for source_connector in source_connectors:
                for directory, child_directories, filenames in os.walk(
                    source_connector
                ):
                    child_directories.sort()
                    filenames.sort()
                    for filename in filenames:
                        if filename.startswith("."):
                            continue
                        yield Path(directory) / filename
        codebases = self.root / "codebases"
        if codebases.is_dir() and (not allowed or "codebase" in allowed):
            yield from self._iter_codebase_paths(codebases)

    def _iter_codebase_paths(self, codebases: Path) -> Iterator[Path]:
        for directory, child_directories, filenames in os.walk(codebases):
            current = Path(directory)
            child_directories[:] = sorted(
                child
                for child in child_directories
                if child not in _CODEBASE_EXCLUDED_DIRECTORIES
                and not self._excluded_codebase_path(
                    (current / child).relative_to(codebases)
                )
            )
            for filename in sorted(filenames):
                path = current / filename
                relative = path.relative_to(codebases)
                if (
                    filename.startswith(".")
                    or filename in _CODEBASE_EXCLUDED_FILENAMES
                    or self._excluded_codebase_path(relative)
                    or not self._is_codebase_text_file(path)
                ):
                    continue
                yield path

    @staticmethod
    def _excluded_codebase_path(relative: Path) -> bool:
        value = relative.as_posix()
        return any(
            value == prefix or value.startswith(f"{prefix}/")
            for prefix in _CODEBASE_EXCLUDED_RELATIVE_PREFIXES
        )

    @staticmethod
    def _is_codebase_text_file(path: Path) -> bool:
        if path.name in _CODEBASE_TEXT_FILENAMES:
            return path.stat().st_size <= _MAX_CODEBASE_FILE_BYTES
        return (
            path.suffix.casefold() in _CODEBASE_TEXT_SUFFIXES
            and path.stat().st_size <= _MAX_CODEBASE_FILE_BYTES
        )

    def read_record(self, path: str | Path) -> CorpusRecord:
        source_path = Path(path).expanduser().absolute()
        if not source_path.resolve().is_relative_to(self.root):
            raise ValueError(f"document lies outside the corpus: {source_path}")
        raw = source_path.read_text(encoding="utf-8", errors="replace")
        relative = source_path.relative_to(self.root)
        connector = self._connector(relative)
        parsed = self._parse(source_path, raw)
        title = self._title(source_path, parsed)
        context = self._context(relative, connector, parsed)
        if connector == "codebase":
            repository = relative.parts[1]
            workspace_path = (
                PurePosixPath("/code") / PurePosixPath(*relative.parts[1:])
            )
            source = f"EnterpriseRAG/codebase/{repository}"
            title = f"{repository}: {'/'.join(relative.parts[2:])}"
        else:
            workspace_path = PurePosixPath("/knowledge/company") / relative
            source = f"EnterpriseRAG/{connector}"
        document = KnowledgeDocument(
            id=self._document_id(relative),
            path=str(workspace_path),
            title=title,
            source=source,
            content=raw,
            kind="source",
            metadata={
                "corpus": "EnterpriseRAG-Bench",
                "relative_path": relative.as_posix(),
                **context,
            },
        )
        index_text = self._index_text(title, parsed, raw)
        return CorpusRecord(document, index_text, context, source_path)

    def _document_id(self, relative: Path) -> str:
        normalized = relative.as_posix()
        if normalized.startswith("sources/"):
            normalized = normalized.removeprefix("sources/")
        mapped = self._ids_by_path().get(normalized)
        if mapped is not None:
            return mapped
        digest = hashlib.sha256(relative.as_posix().encode()).hexdigest()[:24]
        return f"enterprise_{digest}"

    def _ids_by_path(self) -> Mapping[str, str]:
        if self._path_to_id is None:
            index_path = self.root / "uuid_index.json"
            if not index_path.is_file():
                self._path_to_id = {}
            else:
                by_id = json.loads(index_path.read_text())
                self._path_to_id = {
                    str(path): str(document_id) for document_id, path in by_id.items()
                }
        return self._path_to_id

    @staticmethod
    def _connector(relative: Path) -> str:
        parts = relative.parts
        if len(parts) >= 2 and parts[0] == "sources":
            return parts[1]
        if len(parts) >= 2 and parts[0] == "codebases":
            return "codebase"
        return "company"

    @staticmethod
    def _parse(path: Path, raw: str) -> Any:
        suffix = path.suffix.casefold()
        if suffix == ".json":
            try:
                return json.loads(raw)
            except json.JSONDecodeError:
                return raw
        if suffix in {".yaml", ".yml"}:
            try:
                return yaml.safe_load(raw)
            except yaml.YAMLError:
                return raw
        return raw

    @staticmethod
    def _title(path: Path, parsed: Any) -> str:
        if isinstance(parsed, Mapping):
            declared_field = parsed.get("title_field_name")
            fields = (
                (str(declared_field), *_TITLE_FIELDS)
                if isinstance(declared_field, str) and declared_field.strip()
                else _TITLE_FIELDS
            )
            for field in dict.fromkeys(fields):
                value = parsed.get(field)
                if isinstance(value, (str, int, float)) and str(value).strip():
                    return str(value).strip()
        return path.stem.replace("-", " ").replace("_", " ").strip()

    @staticmethod
    def _context(
        relative: Path,
        connector: str,
        parsed: Any,
    ) -> dict[str, Any]:
        context: dict[str, Any] = {
            "connector": connector,
        }
        parts = relative.parts
        if len(parts) >= 3 and parts[0] == "sources":
            if parts[1] == "github_public" and len(parts) >= 4:
                context["workspace"] = "/".join(parts[2:4])
            else:
                context["workspace"] = parts[2]
        elif len(parts) >= 2 and parts[0] == "codebases":
            context["workspace"] = parts[1]
            context["repository"] = parts[1]
            context["language"] = relative.suffix.lstrip(".").casefold()
        if isinstance(parsed, Mapping):
            for field in _CONTEXT_FIELDS:
                value = parsed.get(field)
                if isinstance(value, (str, int, float, bool)):
                    context[field] = value
        return context

    @staticmethod
    def _index_text(title: str, parsed: Any, raw: str) -> str:
        if isinstance(parsed, (Mapping, list)):
            return f"# {title}\n\n{_render_structured(parsed)}"
        return raw


def _render_structured(value: Any, *, level: int = 0, key: str | None = None) -> str:
    prefix = "  " * level
    if isinstance(value, Mapping):
        lines = []
        for child_key, child in value.items():
            if isinstance(child, (Mapping, list)):
                lines.append(f"{prefix}## {str(child_key).replace('_', ' ').title()}")
                lines.append(_render_structured(child, level=level + 1))
            else:
                lines.append(
                    f"{prefix}{str(child_key).replace('_', ' ').title()}: {child}"
                )
        return "\n\n".join(line for line in lines if line)
    if isinstance(value, list):
        lines = []
        for item in value:
            if isinstance(item, (Mapping, list)):
                lines.append(_render_structured(item, level=level + 1))
            else:
                lines.append(f"{prefix}- {item}")
        return "\n".join(lines)
    label = f"{key}: " if key else ""
    return f"{prefix}{label}{value}"


__all__ = [
    "CorpusRecord",
    "EnterpriseRAGCorpus",
]
