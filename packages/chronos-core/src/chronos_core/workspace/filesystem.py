from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import tempfile
import threading
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal


class FilesystemStoreError(Exception):
    """Raised when filesystem branch operations fail."""


@dataclass(frozen=True)
class FilesystemCheckpointInfo:
    checkpoint_id: str
    branch_id: str
    layers: tuple[str, ...]
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class FilesystemPathChange:
    path: str
    change: Literal["added", "deleted", "modified"]
    before_hash: str | None = None
    after_hash: str | None = None
    before_type: str | None = None
    after_type: str | None = None


@dataclass(frozen=True)
class FilesystemDiff:
    left: str
    right: str
    changes: list[FilesystemPathChange]


@dataclass(frozen=True)
class FilesystemMergeResult:
    source: str
    target: str
    applied: int


@dataclass
class _BranchState:
    layers: list[str]
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class _MountState:
    branch_id: str
    merged: Path
    upper: Path
    work: Path


class FilesystemBranchSession:
    """Checked-out POSIX filesystem branch."""

    def __init__(self, store: ChronosFilesystemStore, branch_id: str, path: Path):
        self._store = store
        self.branch_id = branch_id
        self.path = path

    def run(
        self,
        argv: list[str],
        *,
        cwd: str | Path = ".",
        env: dict[str, str] | None = None,
        timeout: float | None = None,
    ) -> subprocess.CompletedProcess[str]:
        run_cwd = (self.path / cwd).resolve()
        try:
            run_cwd.relative_to(self.path.resolve())
        except ValueError as exc:
            raise FilesystemStoreError(f"cwd escapes branch mount: {cwd}") from exc
        return subprocess.run(
            argv,
            cwd=run_cwd,
            env=env,
            timeout=timeout,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )


class ChronosFilesystemStore:
    """Persistent OverlayFS/fuse-overlayfs branch store.

    The store exposes branch state through mounted POSIX directories. Branch
    checkpoints seal the current upperdir as an immutable delta layer and
    remount the branch with that layer in its lowerdir stack.
    """

    def __init__(
        self,
        root: str | Path,
        state_dir: str | Path | None = None,
        mount_strategy: Literal["fuse-overlayfs"] = "fuse-overlayfs",
    ):
        self.root = Path(root).resolve()
        if not self.root.is_dir():
            raise FilesystemStoreError(f"root is not a directory: {self.root}")
        if mount_strategy != "fuse-overlayfs":
            raise FilesystemStoreError(
                f"unsupported mount strategy: {mount_strategy}"
            )
        if shutil.which("fuse-overlayfs") is None:
            raise FilesystemStoreError("fuse-overlayfs is not installed")
        if shutil.which("fusermount3") is None and shutil.which("fusermount") is None:
            raise FilesystemStoreError("fusermount3 or fusermount is required")

        self.state_dir = (
            Path(state_dir).resolve()
            if state_dir is not None
            else Path(tempfile.mkdtemp(prefix="chronos-fs-store-")).resolve()
        )
        self.layers_dir = self.state_dir / "layers"
        self.uppers_dir = self.state_dir / "uppers"
        self.works_dir = self.state_dir / "works"
        self.mounts_dir = self.state_dir / "mounts"
        self.meta_path = self.state_dir / "metadata.json"
        for path in (
            self.layers_dir,
            self.uppers_dir,
            self.works_dir,
            self.mounts_dir,
        ):
            path.mkdir(parents=True, exist_ok=True)

        self._lock = threading.RLock()
        self._mounts: dict[str, _MountState] = {}
        self._branches: dict[str, _BranchState] = {}
        self._checkpoints: dict[str, FilesystemCheckpointInfo] = {}
        self._load_metadata()
        self.ensure()

    def ensure(self) -> None:
        with self._lock:
            if "main" not in self._branches:
                self._branches["main"] = _BranchState(layers=[])
                self._save_metadata()

    def create_branch(
        self,
        branch_id: str,
        from_branch: str = "main",
        metadata: dict[str, Any] | None = None,
    ) -> None:
        with self._lock:
            if branch_id in self._branches:
                raise FilesystemStoreError(f"branch already exists: {branch_id}")
            parent = self._require_branch(from_branch)
            self._seal_branch_upper_if_needed(from_branch)
            self._branches[branch_id] = _BranchState(
                layers=list(parent.layers),
                metadata=dict(metadata or {}),
            )
            self._reset_upper_work(branch_id)
            self._save_metadata()

    def create_branch_from_checkpoint(
        self,
        branch_id: str,
        checkpoint: str,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        with self._lock:
            if branch_id in self._branches:
                raise FilesystemStoreError(f"branch already exists: {branch_id}")
            info = self._checkpoints.get(checkpoint)
            if info is None:
                raise FilesystemStoreError(f"checkpoint not found: {checkpoint}")
            self._branches[branch_id] = _BranchState(
                layers=list(info.layers),
                metadata=dict(metadata or {}),
            )
            self._reset_upper_work(branch_id)
            self._save_metadata()

    def delete_branch(self, branch_id: str) -> None:
        with self._lock:
            if branch_id == "main":
                raise FilesystemStoreError("cannot delete main branch")
            self._require_branch(branch_id)
            self.unmount(branch_id)
            self._branches.pop(branch_id, None)
            self._remove_branch_dirs(branch_id)
            self._save_metadata()

    def checkout(self, branch_id: str = "main") -> FilesystemBranchSession:
        with self._lock:
            self._require_branch(branch_id)
            mount = self._mount_branch(branch_id)
            return FilesystemBranchSession(self, branch_id, mount.merged)

    def checkout_checkpoint(self, checkpoint: str) -> FilesystemBranchSession:
        with self._lock:
            info = self._checkpoints.get(checkpoint)
            if info is None:
                raise FilesystemStoreError(f"checkpoint not found: {checkpoint}")
            branch_id = f"checkpoint-{checkpoint}"
            self._branches[branch_id] = _BranchState(
                layers=list(info.layers),
                metadata={"readonly_checkpoint": checkpoint},
            )
            self._reset_upper_work(branch_id)
            mount = self._mount_branch(branch_id)
            return FilesystemBranchSession(self, branch_id, mount.merged)

    def create_checkpoint(
        self,
        checkpoint: str,
        branch: str = "main",
        metadata: dict[str, Any] | None = None,
    ) -> FilesystemCheckpointInfo:
        with self._lock:
            if checkpoint in self._checkpoints:
                raise FilesystemStoreError(f"checkpoint already exists: {checkpoint}")
            state = self._require_branch(branch)
            self._seal_branch_upper_if_needed(branch)
            info = FilesystemCheckpointInfo(
                checkpoint_id=checkpoint,
                branch_id=branch,
                layers=tuple(state.layers),
                metadata=dict(metadata or {}),
            )
            self._checkpoints[checkpoint] = info
            self._save_metadata()
            return info

    def diff(self, left: str, right: str) -> FilesystemDiff:
        left_manifest = self._manifest(left)
        right_manifest = self._manifest(right)
        changes: list[FilesystemPathChange] = []
        for path in sorted(set(left_manifest) | set(right_manifest)):
            before = left_manifest.get(path)
            after = right_manifest.get(path)
            if before is None and after is not None:
                changes.append(
                    FilesystemPathChange(
                        path=path,
                        change="added",
                        after_hash=after["hash"],
                        after_type=after["type"],
                    )
                )
            elif before is not None and after is None:
                changes.append(
                    FilesystemPathChange(
                        path=path,
                        change="deleted",
                        before_hash=before["hash"],
                        before_type=before["type"],
                    )
                )
            elif before != after:
                assert before is not None and after is not None
                changes.append(
                    FilesystemPathChange(
                        path=path,
                        change="modified",
                        before_hash=before["hash"],
                        after_hash=after["hash"],
                        before_type=before["type"],
                        after_type=after["type"],
                    )
                )
        return FilesystemDiff(left=left, right=right, changes=changes)

    def merge_apply(self, source: str, target: str) -> FilesystemMergeResult:
        """Apply source's file-level changes into target.

        This is intentionally conservative and path based. It does not yet do
        three-way conflict detection; callers that need conflict reporting
        should compare manifests against an ancestor before calling this.
        """
        diff = self.diff(target, source)
        source_session = self.checkout(source)
        target_session = self.checkout(target)
        applied = 0
        for change in diff.changes:
            src = source_session.path / change.path
            dst = target_session.path / change.path
            if change.change == "deleted":
                self._delete_visible_path(dst)
            else:
                self._copy_visible_path(src, dst)
            applied += 1
        return FilesystemMergeResult(source=source, target=target, applied=applied)

    def unmount(self, branch_id: str) -> None:
        with self._lock:
            mount = self._mounts.pop(branch_id, None)
            if mount is None:
                return
            self._unmount_path(mount.merged)

    def close(self) -> None:
        with self._lock:
            for branch_id in list(self._mounts):
                self.unmount(branch_id)

    def cleanup(self) -> None:
        self.close()
        shutil.rmtree(self.state_dir, ignore_errors=True)

    @property
    def branches(self) -> tuple[str, ...]:
        return tuple(sorted(self._branches))

    def layers_for_branch(self, branch_id: str) -> tuple[str, ...]:
        return tuple(self._require_branch(branch_id).layers)

    def _mount_branch(self, branch_id: str) -> _MountState:
        existing = self._mounts.get(branch_id)
        if existing is not None:
            return existing

        upper = self._branch_upper(branch_id)
        work = self._branch_work(branch_id)
        merged = self._branch_mount(branch_id)
        upper.mkdir(parents=True, exist_ok=True)
        work.mkdir(parents=True, exist_ok=True)
        merged.mkdir(parents=True, exist_ok=True)

        lowerdir = self._lowerdir(self._require_branch(branch_id).layers)
        opts = f"lowerdir={lowerdir},upperdir={upper},workdir={work}"
        result = subprocess.run(
            ["fuse-overlayfs", "-o", opts, str(merged)],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            raise FilesystemStoreError(
                f"failed to mount branch {branch_id}: {result.stderr.strip()}"
            )
        mount = _MountState(branch_id=branch_id, merged=merged, upper=upper, work=work)
        self._mounts[branch_id] = mount
        return mount

    def _seal_branch_upper_if_needed(self, branch_id: str) -> str | None:
        state = self._require_branch(branch_id)
        was_mounted = branch_id in self._mounts
        if was_mounted:
            self.unmount(branch_id)

        upper = self._branch_upper(branch_id)
        upper.mkdir(parents=True, exist_ok=True)
        if not any(upper.iterdir()):
            if was_mounted:
                self._reset_work(branch_id)
                self._mount_branch(branch_id)
            return None

        layer_id = self._new_layer_id(branch_id)
        layer_path = self.layers_dir / layer_id
        if layer_path.exists():
            raise FilesystemStoreError(f"layer already exists: {layer_id}")
        os.rename(upper, layer_path)
        state.layers.append(layer_id)
        upper.mkdir(parents=True, exist_ok=True)
        self._reset_work(branch_id)
        self._save_metadata()

        if was_mounted:
            self._mount_branch(branch_id)
        return layer_id

    def _manifest(self, branch_id: str) -> dict[str, dict[str, str]]:
        session = self.checkout(branch_id)
        root = session.path
        manifest: dict[str, dict[str, str]] = {}
        for current_root, dirs, files in os.walk(root):
            current = Path(current_root)
            dirs[:] = [d for d in dirs if d not in {".git"}]
            for name in files:
                path = current / name
                rel = path.relative_to(root).as_posix()
                try:
                    st = path.lstat()
                except OSError:
                    continue
                if path.is_symlink():
                    manifest[rel] = {
                        "type": "symlink",
                        "hash": hashlib.sha256(os.readlink(path).encode()).hexdigest(),
                        "mode": oct(st.st_mode & 0o777),
                    }
                elif path.is_file():
                    manifest[rel] = {
                        "type": "file",
                        "hash": self._file_hash(path),
                        "mode": oct(st.st_mode & 0o777),
                    }
        return manifest

    def _load_metadata(self) -> None:
        if not self.meta_path.exists():
            self._branches = {}
            self._checkpoints = {}
            return
        data = json.loads(self.meta_path.read_text())
        self._branches = {
            branch_id: _BranchState(
                layers=list(payload.get("layers", [])),
                metadata=dict(payload.get("metadata", {})),
            )
            for branch_id, payload in data.get("branches", {}).items()
        }
        self._checkpoints = {
            checkpoint_id: FilesystemCheckpointInfo(
                checkpoint_id=checkpoint_id,
                branch_id=payload["branch_id"],
                layers=tuple(payload.get("layers", [])),
                metadata=dict(payload.get("metadata", {})),
            )
            for checkpoint_id, payload in data.get("checkpoints", {}).items()
        }

    def _save_metadata(self) -> None:
        data = {
            "branches": {
                branch_id: {
                    "layers": state.layers,
                    "metadata": state.metadata,
                }
                for branch_id, state in sorted(self._branches.items())
            },
            "checkpoints": {
                checkpoint_id: {
                    "branch_id": info.branch_id,
                    "layers": list(info.layers),
                    "metadata": info.metadata,
                }
                for checkpoint_id, info in sorted(self._checkpoints.items())
            },
        }
        tmp = self.meta_path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(data, indent=2, sort_keys=True))
        os.replace(tmp, self.meta_path)

    def _require_branch(self, branch_id: str) -> _BranchState:
        state = self._branches.get(branch_id)
        if state is None:
            raise FilesystemStoreError(f"branch not found: {branch_id}")
        return state

    def _lowerdir(self, layers: list[str]) -> str:
        paths = [self.layers_dir / layer_id for layer_id in reversed(layers)]
        paths.append(self.root)
        return ":".join(str(path) for path in paths)

    def _branch_upper(self, branch_id: str) -> Path:
        return self.uppers_dir / self._safe_id(branch_id)

    def _branch_work(self, branch_id: str) -> Path:
        return self.works_dir / self._safe_id(branch_id)

    def _branch_mount(self, branch_id: str) -> Path:
        return self.mounts_dir / self._safe_id(branch_id)

    def _reset_upper_work(self, branch_id: str) -> None:
        shutil.rmtree(self._branch_upper(branch_id), ignore_errors=True)
        self._branch_upper(branch_id).mkdir(parents=True, exist_ok=True)
        self._reset_work(branch_id)

    def _reset_work(self, branch_id: str) -> None:
        shutil.rmtree(self._branch_work(branch_id), ignore_errors=True)
        self._branch_work(branch_id).mkdir(parents=True, exist_ok=True)

    def _remove_branch_dirs(self, branch_id: str) -> None:
        for path in (
            self._branch_upper(branch_id),
            self._branch_work(branch_id),
            self._branch_mount(branch_id),
        ):
            shutil.rmtree(path, ignore_errors=True)

    def _unmount_path(self, merged: Path) -> None:
        commands = []
        if shutil.which("fusermount3"):
            commands.append(["fusermount3", "-u", str(merged)])
        if shutil.which("fusermount"):
            commands.append(["fusermount", "-u", str(merged)])
        commands.append(["umount", "-l", str(merged)])

        last_error = ""
        for command in commands:
            result = subprocess.run(command, capture_output=True, text=True)
            if result.returncode == 0:
                return
            last_error = result.stderr.strip()
        raise FilesystemStoreError(f"failed to unmount {merged}: {last_error}")

    def _delete_visible_path(self, path: Path) -> None:
        if path.is_dir() and not path.is_symlink():
            shutil.rmtree(path)
        elif path.exists() or path.is_symlink():
            path.unlink()

    def _copy_visible_path(self, src: Path, dst: Path) -> None:
        if not src.exists() and not src.is_symlink():
            raise FilesystemStoreError(f"source path does not exist: {src}")
        dst.parent.mkdir(parents=True, exist_ok=True)
        if dst.exists() or dst.is_symlink():
            self._delete_visible_path(dst)
        if src.is_symlink():
            os.symlink(os.readlink(src), dst)
        elif src.is_dir():
            shutil.copytree(src, dst, symlinks=True)
        else:
            shutil.copy2(src, dst)

    @staticmethod
    def _file_hash(path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    @staticmethod
    def _safe_id(value: str) -> str:
        digest = hashlib.sha1(value.encode("utf-8")).hexdigest()[:10]
        safe = "".join(c if c.isalnum() or c in "-_" else "_" for c in value)
        return f"{safe[:48]}-{digest}"

    @staticmethod
    def _new_layer_id(branch_id: str) -> str:
        token = "".join(c if c.isalnum() or c in "-_" else "_" for c in branch_id)
        return f"{token[:32]}-{uuid.uuid4().hex[:12]}"
