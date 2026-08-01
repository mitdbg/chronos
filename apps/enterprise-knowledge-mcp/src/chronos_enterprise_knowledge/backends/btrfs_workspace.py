"""Writable branch checkouts backed by Btrfs subvolume snapshots."""

from __future__ import annotations

import hashlib
import os
import re
import shutil
import subprocess
import uuid
from collections.abc import Callable
from pathlib import Path

from chronos_enterprise_knowledge.models import normalize_workspace_path

CommandRunner = Callable[[list[str]], subprocess.CompletedProcess[str]]


class BtrfsWorkspaceStore:
    """Manage one writable Btrfs subvolume and fork snapshot per branch."""

    def __init__(
        self,
        root: str | Path,
        *,
        command_runner: CommandRunner | None = None,
        filesystem_type: str | None = None,
    ):
        self.root = Path(root).expanduser().resolve()
        self._run_command = command_runner or _run_checked
        self.root.parent.mkdir(parents=True, exist_ok=True)
        actual_type = filesystem_type or _filesystem_type(self.root.parent)
        if actual_type != "btrfs":
            raise RuntimeError(
                f"Btrfs baseline root must reside on a btrfs filesystem: "
                f"{self.root} is on {actual_type!r}"
            )
        self.root.mkdir(parents=True, exist_ok=True)
        self.branches_root = self.root / "branches"
        self.bases_root = self.root / "fork-bases"
        self.bindings_root = self.root / "bindings"
        self.branches_root.mkdir(exist_ok=True)
        self.bases_root.mkdir(exist_ok=True)
        self.bindings_root.mkdir(exist_ok=True)
        self._bindings: dict[str, Path] = {}
        main = self.branch_path("main")
        if not main.exists():
            self._subvolume_create(main)
        elif not self._is_subvolume(main):
            raise RuntimeError(
                f"existing main workspace is not a subvolume: {main}"
            )

    @property
    def storage_component(self) -> str:
        return "btrfs-subvolume-snapshots"

    def branch_path(self, branch_id: str) -> Path:
        return self.branches_root / _safe_branch_path(branch_id)

    def base_path(self, branch_id: str) -> Path:
        return self.bases_root / _safe_branch_path(branch_id)

    def create_branch(self, branch_id: str, parent_branch: str) -> None:
        source = self.require_branch(parent_branch)
        destination = self.branch_path(branch_id)
        base = self.base_path(branch_id)
        if destination.exists() or base.exists():
            raise ValueError(f"Btrfs state already exists for branch: {branch_id}")
        try:
            self._snapshot(source, destination)
            self._snapshot(source, base, read_only=True)
        except Exception:
            self._delete_if_subvolume(destination)
            self._delete_if_subvolume(base)
            raise

    def delete_branch(self, branch_id: str) -> None:
        if branch_id == "main":
            raise ValueError("cannot delete main")
        binding = self._bindings.pop(branch_id, None)
        if binding is not None and binding.is_symlink():
            binding.unlink()
        self._delete_if_subvolume(self.branch_path(branch_id))
        self._delete_if_subvolume(self.base_path(branch_id))

    def require_branch(self, branch_id: str) -> Path:
        path = self.branch_path(branch_id)
        if not path.exists() or not self._is_subvolume(path):
            raise ValueError(f"unknown Btrfs branch: {branch_id}")
        return path

    def checkout(
        self,
        branch_id: str,
        mount_path: str | Path | None = None,
    ) -> Path:
        source = self.require_branch(branch_id)
        if mount_path is None:
            return source
        destination = Path(mount_path).expanduser().resolve()
        if destination == source:
            return source
        if destination.exists():
            if destination.is_symlink():
                if destination.resolve() != source:
                    raise ValueError(
                        f"checkout path points to another branch: {destination}"
                    )
                return destination
            if any(destination.iterdir()):
                raise ValueError(f"checkout path must be empty: {destination}")
            destination.rmdir()
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.symlink_to(source, target_is_directory=True)
        self._bindings[branch_id] = destination
        return destination

    def write(self, branch_id: str, path: str, content: bytes) -> None:
        root = self.require_branch(branch_id)
        destination = _workspace_path(root, path)
        destination.parent.mkdir(parents=True, exist_ok=True)
        temporary = destination.with_name(
            f".{destination.name}.tmp-{uuid.uuid4().hex}"
        )
        try:
            temporary.write_bytes(bytes(content))
            os.replace(temporary, destination)
        finally:
            if temporary.exists():
                temporary.unlink()

    def read(self, branch_id: str, path: str) -> bytes:
        source = _workspace_path(self.require_branch(branch_id), path)
        if not source.is_file():
            raise FileNotFoundError(normalize_workspace_path(path))
        return source.read_bytes()

    def delete(self, branch_id: str, path: str) -> bool:
        source = _workspace_path(self.require_branch(branch_id), path)
        if not source.is_file():
            return False
        source.unlink()
        _remove_empty_parents(source.parent, self.require_branch(branch_id))
        return True

    def files(
        self,
        branch_id: str,
        *,
        use_fork_base: bool = False,
    ) -> dict[str, bytes]:
        root = (
            self.base_path(branch_id)
            if use_fork_base
            else self.require_branch(branch_id)
        )
        if not root.exists():
            raise ValueError(f"branch has no fork snapshot: {branch_id}")
        result: dict[str, bytes] = {}
        resolved_root = root.resolve()
        for path in root.rglob("*"):
            if not path.is_file() or path.is_symlink():
                continue
            resolved = path.resolve()
            if not resolved.is_relative_to(resolved_root):
                continue
            result["/" + path.relative_to(root).as_posix()] = path.read_bytes()
        return result

    def apply_delta(
        self,
        source_branch: str,
        base_branch: str,
        target_branch: str,
    ) -> int:
        source = self.files(source_branch)
        base = self.files(base_branch, use_fork_base=True)
        changed = 0
        for path in sorted(source.keys() | base.keys()):
            source_content = source.get(path)
            if source_content == base.get(path):
                continue
            changed += 1
            if source_content is None:
                self.delete(target_branch, path)
            else:
                self.write(target_branch, path, source_content)
        return changed

    def apply_paths_delta(
        self,
        source_branch: str,
        base_branch: str,
        target_branch: str,
        paths: list[str] | set[str] | tuple[str, ...],
    ) -> int:
        """Apply a three-way delta for a known sparse set of paths."""

        source_root = self.require_branch(source_branch)
        base_root = self.base_path(base_branch)
        if not base_root.exists():
            raise ValueError(f"branch has no fork snapshot: {base_branch}")
        changed = 0
        for path in sorted(paths):
            source_content = _optional_content(source_root, path)
            base_content = _optional_content(base_root, path)
            if source_content == base_content:
                continue
            changed += 1
            if source_content is None:
                self.delete(target_branch, path)
            else:
                self.write(target_branch, path, source_content)
        return changed

    def logical_bytes(self) -> int:
        total = 0
        for root in (self.branches_root, self.bases_root):
            for path in root.rglob("*"):
                if path.is_file() and not path.is_symlink():
                    total += path.stat().st_size
        return total

    def exclusive_bytes(self) -> int | None:
        subvolumes = [
            path
            for root in (self.branches_root, self.bases_root)
            for path in root.iterdir()
            if self._is_subvolume(path)
        ]
        if not subvolumes:
            return 0
        try:
            result = self._run_command(
                [
                    "btrfs",
                    "filesystem",
                    "du",
                    "--raw",
                    "-s",
                    *(str(path) for path in subvolumes),
                ]
            )
        except subprocess.CalledProcessError:
            return None
        exclusive = 0
        matched = False
        for line in result.stdout.splitlines():
            fields = line.split()
            if len(fields) < 4 or not fields[0].isdigit():
                continue
            exclusive += int(fields[1])
            matched = True
        return exclusive if matched else None

    def filesystem_used_bytes(self) -> int:
        """Return physical bytes used by the dedicated Btrfs filesystem.

        The enterprise benchmark provisions one Btrfs filesystem solely for
        native workspace snapshots. Filesystem-level usage therefore counts
        shared extents once and avoids recursively walking every snapshot.
        """

        self._run_command(
            ["btrfs", "filesystem", "sync", str(self.root)]
        )
        result = self._run_command(
            ["btrfs", "filesystem", "usage", "--raw", str(self.root)]
        )
        match = re.search(r"^\s*Used:\s*(\d+)\s*$", result.stdout, re.MULTILINE)
        if match is None:
            raise RuntimeError(
                "could not parse Btrfs filesystem used bytes"
            )
        return int(match.group(1))

    def close(self) -> None:
        for destination in self._bindings.values():
            if destination.is_symlink():
                destination.unlink()
        self._bindings.clear()

    def destroy(self) -> None:
        self.close()
        for root in (self.bases_root, self.branches_root):
            for path in sorted(root.iterdir(), reverse=True):
                self._delete_if_subvolume(path)
        shutil.rmtree(self.root)

    def _is_subvolume(self, path: Path) -> bool:
        try:
            # Every Btrfs subvolume root has inode 256. Unlike
            # ``btrfs subvolume show``, stat does not require CAP_SYS_ADMIN.
            if path.stat().st_ino == 256:
                return True
        except FileNotFoundError:
            return False
        result = subprocess.run(
            ["btrfs", "subvolume", "show", str(path)],
            check=False,
            text=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        return result.returncode == 0

    def _subvolume_create(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self._run_command(["btrfs", "subvolume", "create", str(path)])

    def _snapshot(
        self,
        source: Path,
        destination: Path,
        *,
        read_only: bool = False,
    ) -> None:
        destination.parent.mkdir(parents=True, exist_ok=True)
        command = ["btrfs", "subvolume", "snapshot"]
        if read_only:
            command.append("-r")
        command.extend([str(source), str(destination)])
        self._run_command(command)

    def _delete_if_subvolume(self, path: Path) -> None:
        if not path.exists():
            return
        if self._is_subvolume(path):
            try:
                # A read-only fork base must be made writable before an
                # unprivileged owner can remove it.
                self._run_command(
                    [
                        "btrfs",
                        "property",
                        "set",
                        str(path),
                        "ro",
                        "false",
                    ]
                )
            except subprocess.CalledProcessError:
                pass
            self._run_command(["btrfs", "subvolume", "delete", str(path)])
        elif path.is_dir():
            shutil.rmtree(path)
        else:
            path.unlink()


def _run_checked(command: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        check=True,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


def _filesystem_type(path: Path) -> str:
    result = subprocess.run(
        ["findmnt", "-n", "-o", "FSTYPE", "-T", str(path)],
        check=True,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    return result.stdout.strip()


def _safe_branch_path(branch_id: str) -> str:
    slug = re.sub(r"[^A-Za-z0-9._-]+", "-", branch_id).strip("-")[:64]
    digest = hashlib.sha256(branch_id.encode()).hexdigest()[:12]
    return f"{slug or 'branch'}-{digest}"


def _workspace_path(root: Path, path: str) -> Path:
    normalized = normalize_workspace_path(path)
    target = (root / normalized.lstrip("/")).resolve()
    if not target.is_relative_to(root.resolve()):
        raise ValueError(f"path escapes checkout: {path}")
    return target


def _optional_content(root: Path, path: str) -> bytes | None:
    source = _workspace_path(root, path)
    if not source.is_file():
        return None
    return source.read_bytes()


def _remove_empty_parents(path: Path, root: Path) -> None:
    resolved_root = root.resolve()
    current = path
    while current.resolve().is_relative_to(resolved_root) and current != root:
        try:
            current.rmdir()
        except OSError:
            return
        current = current.parent


__all__ = ["BtrfsWorkspaceStore"]
