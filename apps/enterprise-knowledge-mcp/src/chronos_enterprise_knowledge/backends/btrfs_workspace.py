"""Writable branch checkouts backed by Btrfs subvolume snapshots."""

from __future__ import annotations

import hashlib
import os
import re
import shutil
import subprocess
import tempfile
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
        self._use_native_incremental_diff = command_runner is None
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
        self.diffs_root = self.root / "diff-snapshots"
        self.branches_root.mkdir(exist_ok=True)
        self.bases_root.mkdir(exist_ok=True)
        self.bindings_root.mkdir(exist_ok=True)
        self.diffs_root.mkdir(exist_ok=True)
        for path in self.diffs_root.iterdir():
            self._delete_if_subvolume(path)
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

    def changed_file_paths(
        self,
        source_branch: str,
        fork_branch: str,
    ) -> set[str]:
        """Return the final file delta from a branch's relevant fork snapshot."""

        source = self.require_branch(source_branch)
        base = self.base_path(fork_branch)
        if not base.exists():
            raise ValueError(f"branch has no fork snapshot: {fork_branch}")
        if self._use_native_incremental_diff:
            try:
                return self._btrfs_find_new_file_paths(source, base)
            except (OSError, subprocess.CalledProcessError, ValueError):
                # Keep the exact final-tree comparison available when the
                # kernel or btrfs-progs does not expose find-new.
                pass
        return self._different_file_paths(source, base)

    def differing_file_paths(
        self,
        left_branch: str,
        right_branch: str,
    ) -> set[str]:
        """Return regular files whose final contents differ between heads."""

        left = self.require_branch(left_branch)
        right = self.require_branch(right_branch)
        if self._use_native_incremental_diff:
            left_base = self.base_path(left_branch)
            right_base = self.base_path(right_branch)
            try:
                if (
                    left_base.exists()
                    and right_base.exists()
                    and _btrfs_branches_are_adjacent(
                        left,
                        right,
                        self._run_command,
                    )
                ):
                    candidates = self._btrfs_find_new_file_paths(left, left_base)
                    candidates.update(
                        self._btrfs_find_new_file_paths(right, right_base)
                    )
                    return {
                        path
                        for path in candidates
                        if _candidate_file_differs(left, right, path)
                    }
            except (OSError, subprocess.CalledProcessError, ValueError):
                # The full final-tree comparison remains the correctness
                # fallback for unrelated branches or older btrfs-progs.
                pass
        return self._different_file_paths(left, right)

    def _different_file_paths(self, left: Path, right: Path) -> set[str]:
        if self._use_native_incremental_diff:
            try:
                return self._btrfs_incremental_file_paths(left, right)
            except (OSError, subprocess.CalledProcessError, ValueError):
                # Btrfs send requires CAP_SYS_ADMIN on current kernels. Keep
                # the baseline usable in unprivileged environments without
                # changing its final-tree semantics.
                pass
        return _different_file_paths(left, right)

    def _btrfs_find_new_file_paths(
        self,
        left: Path,
        right: Path,
    ) -> set[str]:
        generation = _btrfs_creation_generation(right, self._run_command)
        result = self._run_command(
            [
                "sudo",
                "-n",
                "btrfs",
                "subvolume",
                "find-new",
                str(left),
                str(generation),
            ]
        )
        candidates = _btrfs_find_new_candidates(result.stdout)
        candidates.update(
            _regular_file_paths(left) ^ _regular_file_paths(right)
        )
        return {
            path
            for path in candidates
            if _candidate_file_differs(left, right, path)
        }

    def _btrfs_incremental_file_paths(
        self,
        left: Path,
        right: Path,
    ) -> set[str]:
        token = uuid.uuid4().hex
        left_snapshot = self.diffs_root / f"left-{token}"
        right_snapshot = self.diffs_root / f"right-{token}"
        stream_path: Path | None = None
        try:
            self._snapshot(left, left_snapshot, read_only=True)
            self._snapshot(right, right_snapshot, read_only=True)
            with tempfile.NamedTemporaryFile(
                prefix="chronos-btrfs-diff-",
                suffix=".stream",
                delete=False,
            ) as stream:
                stream_path = Path(stream.name)
            self._run_command(
                [
                    "sudo",
                    "-n",
                    "btrfs",
                    "send",
                    "--no-data",
                    "-p",
                    str(right_snapshot),
                    "-f",
                    str(stream_path),
                    str(left_snapshot),
                ]
            )
            dumped = self._run_command(
                [
                    "btrfs",
                    "receive",
                    "--dump",
                    "-f",
                    str(stream_path),
                ]
            )
            (
                candidates,
                recursive_candidates,
                one_sided_candidates,
                one_sided_file_candidates,
            ) = _btrfs_dump_candidates_with_presence(
                dumped.stdout,
                left_snapshot.name,
            )
            return _different_candidate_file_paths(
                left,
                right,
                candidates,
                recursive_candidates=recursive_candidates,
                one_sided_candidates=one_sided_candidates,
                one_sided_file_candidates=one_sided_file_candidates,
            )
        finally:
            self._delete_if_subvolume(left_snapshot)
            self._delete_if_subvolume(right_snapshot)
            if stream_path is not None:
                stream_path.unlink(missing_ok=True)

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

    def conflicting_paths(
        self,
        source_branch: str,
        base_branch: str,
        target_branch: str,
        paths: list[str] | set[str] | tuple[str, ...],
    ) -> set[str]:
        """Return paths changed differently in source and target from base."""

        source_root = self.require_branch(source_branch)
        base_root = self.base_path(base_branch)
        if not base_root.exists():
            raise ValueError(f"branch has no fork snapshot: {base_branch}")
        target_root = self.require_branch(target_branch)
        conflicts: set[str] = set()
        for path in paths:
            source_content = _optional_content(source_root, path)
            base_content = _optional_content(base_root, path)
            target_content = _optional_content(target_root, path)
            if (
                source_content != base_content
                and target_content != base_content
                and target_content != source_content
            ):
                conflicts.add(path)
        return conflicts

    def logical_bytes(self) -> int:
        total = 0
        for root in (self.branches_root, self.bases_root):
            for path in root.rglob("*"):
                if path.is_file() and not path.is_symlink():
                    total += path.stat().st_size
        return total

    def exclusive_bytes(self) -> int | None:
        return self.subvolume_usage().get("subvolume_exclusive_bytes")

    def subvolume_usage(self) -> dict[str, int | None]:
        """Return logical, exclusive, and shared bytes for owned subvolumes.

        ``btrfs filesystem usage`` describes the complete mounted filesystem,
        which can include unrelated service directories.  This per-subvolume
        view is recorded alongside it so a benchmark can distinguish shared
        snapshot extents from data outside the native workspace scope.
        """

        subvolumes = [
            path
            for root in (self.branches_root, self.bases_root)
            if root.exists()
            for path in root.iterdir()
            if self._is_subvolume(path)
        ]
        if not subvolumes:
            return {
                "subvolume_count": 0,
                "subvolume_total_bytes": 0,
                "subvolume_exclusive_bytes": 0,
                "subvolume_shared_bytes": 0,
            }
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
            return {
                "subvolume_count": len(subvolumes),
                "subvolume_total_bytes": None,
                "subvolume_exclusive_bytes": None,
                "subvolume_shared_bytes": None,
            }
        total = exclusive = shared = 0
        matched = 0
        for line in result.stdout.splitlines():
            fields = line.split()
            # ``btrfs filesystem du --raw -s`` begins data rows with
            # Total, Exclusive, and Set shared byte counts.  Ignore headers
            # and summary text without relying on localized labels.
            if len(fields) < 4 or not all(
                field.isdigit() for field in fields[:3]
            ):
                continue
            total += int(fields[0])
            exclusive += int(fields[1])
            shared += int(fields[2])
            matched += 1
        if matched == 0:
            return {
                "subvolume_count": len(subvolumes),
                "subvolume_total_bytes": None,
                "subvolume_exclusive_bytes": None,
                "subvolume_shared_bytes": None,
            }
        return {
            "subvolume_count": matched,
            "subvolume_total_bytes": total,
            "subvolume_exclusive_bytes": exclusive,
            "subvolume_shared_bytes": shared,
        }

    def filesystem_usage(self) -> dict[str, int]:
        """Return synchronized aggregate and allocator-level Btrfs usage."""

        self._run_command(["btrfs", "filesystem", "sync", str(self.root)])
        result = self._run_command(
            ["btrfs", "filesystem", "usage", "--raw", str(self.root)]
        )
        overall = re.search(
            r"^\s*Used:\s*(\d+)\s*$", result.stdout, re.MULTILINE
        )
        if overall is None:
            raise RuntimeError("could not parse Btrfs filesystem used bytes")
        usage: dict[str, int] = {"filesystem_used_bytes": int(overall.group(1))}
        for component in ("Data", "Metadata", "System"):
            match = re.search(
                rf"^\s*{component}[^:]*:.*?\bUsed:\s*(\d+)",
                result.stdout,
                re.MULTILINE,
            )
            usage[f"{component.lower()}_used_bytes"] = (
                int(match.group(1)) if match is not None else 0
            )
        return usage

    def filesystem_used_bytes(self) -> int:
        """Return physical bytes used by the dedicated Btrfs filesystem.

        The enterprise benchmark provisions one Btrfs filesystem solely for
        native workspace snapshots. Filesystem-level usage therefore counts
        shared extents once and avoids recursively walking every snapshot.
        """

        return self.filesystem_usage()["filesystem_used_bytes"]

    def close(self) -> None:
        for destination in self._bindings.values():
            if destination.is_symlink():
                destination.unlink()
        self._bindings.clear()

    def destroy(self) -> None:
        self.close()
        for root in (self.diffs_root, self.bases_root, self.branches_root):
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
            try:
                self._run_command(["btrfs", "subvolume", "delete", str(path)])
            except subprocess.CalledProcessError:
                # Large cleanups can leave a deletion pending in the current
                # Btrfs transaction.  Commit only on the exceptional path.
                if not path.exists():
                    return
                self._run_command(
                    [
                        "btrfs",
                        "subvolume",
                        "delete",
                        "--commit-after",
                        str(path),
                    ]
                )
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


def _different_file_paths(left: Path, right: Path) -> set[str]:
    left_files = _regular_files(left)
    right_files = _regular_files(right)
    changed = set(left_files.keys() ^ right_files.keys())
    for path in left_files.keys() & right_files.keys():
        if not _same_file_contents(left_files[path], right_files[path]):
            changed.add(path)
    return changed


def _btrfs_creation_generation(
    path: Path,
    run_command: CommandRunner,
) -> int:
    result = run_command(
        ["sudo", "-n", "btrfs", "subvolume", "show", str(path)]
    )
    match = re.search(
        r"^\s*Gen at creation:\s*(\d+)\s*$",
        result.stdout,
        re.MULTILINE,
    )
    if match is None:
        raise ValueError(f"could not determine Btrfs creation generation: {path}")
    return int(match.group(1))


def _btrfs_branches_are_adjacent(
    left: Path,
    right: Path,
    run_command: CommandRunner,
) -> bool:
    left_uuid, left_parent_uuid = _btrfs_subvolume_uuids(left, run_command)
    right_uuid, right_parent_uuid = _btrfs_subvolume_uuids(right, run_command)
    return left_parent_uuid == right_uuid or right_parent_uuid == left_uuid


def _btrfs_subvolume_uuids(
    path: Path,
    run_command: CommandRunner,
) -> tuple[str, str]:
    result = run_command(
        ["sudo", "-n", "btrfs", "subvolume", "show", str(path)]
    )
    uuid_match = re.search(r"^\s*UUID:\s*(\S+)\s*$", result.stdout, re.MULTILINE)
    parent_match = re.search(
        r"^\s*Parent UUID:\s*(\S+)\s*$",
        result.stdout,
        re.MULTILINE,
    )
    if uuid_match is None or parent_match is None:
        raise ValueError(f"could not determine Btrfs ancestry: {path}")
    return uuid_match.group(1), parent_match.group(1)


def _btrfs_find_new_candidates(output: str) -> set[str]:
    candidates: set[str] = set()
    for line in output.splitlines():
        match = re.search(r"\bflags\s+\S+\s+(.+?)\s*$", line)
        if match is None:
            continue
        path = normalize_workspace_path(match.group(1).strip())
        if path != "/":
            candidates.add(path)
    return candidates


def _regular_file_paths(root: Path) -> set[str]:
    paths: set[str] = set()
    root_path = os.fspath(root)
    pending = [(root_path, "")]
    while pending:
        directory, relative_directory = pending.pop()
        with os.scandir(directory) as entries:
            for entry in entries:
                relative = os.path.join(relative_directory, entry.name)
                if entry.is_dir(follow_symlinks=False):
                    pending.append((entry.path, relative))
                elif entry.is_file(follow_symlinks=False):
                    paths.add("/" + relative.replace(os.sep, "/"))
    return paths


def _btrfs_dump_candidate_paths(
    output: str,
    snapshot_name: str,
) -> set[str]:
    candidates, _ = _btrfs_dump_candidates(output, snapshot_name)
    return candidates


def _btrfs_dump_candidates(
    output: str,
    snapshot_name: str,
) -> tuple[set[str], set[str]]:
    candidates, recursive_candidates, _, _ = _btrfs_dump_candidates_with_presence(
        output,
        snapshot_name,
    )
    return candidates, recursive_candidates


def _btrfs_dump_candidates_with_presence(
    output: str,
    snapshot_name: str,
) -> tuple[set[str], set[str], set[str], set[str]]:
    """Return direct candidates and paths requiring directory expansion.

    A Btrfs send stream emits metadata operations for ancestor directories of
    every changed file. Treating every such directory as a recursive candidate
    makes a small diff scan an entire checkout (for example, an ``utimes`` on
    ``/knowledge``). Only structural operations can change the paths of files
    below a directory and therefore require recursive verification.
    """

    candidates: set[str] = set()
    recursive_candidates: set[str] = set()
    # ``send --no-data`` compares the source snapshot against the supplied
    # parent snapshot.  A final ``mkfile``/``unlink`` therefore establishes a
    # one-sided file without a stat or content read.  Keep enough operation
    # history to avoid treating a temporary ``mkfile`` followed by ``rename``
    # or ``unlink`` as a published change.
    one_sided_candidates: set[str] = set()
    one_sided_file_candidates: set[str] = set()
    added_candidates: set[str] = set()
    removed_candidates: set[str] = set()
    file_candidates: set[str] = set()
    directory_candidates: set[str] = set()
    removed_file_candidates: set[str] = set()
    removed_directory_candidates: set[str] = set()
    prefix = f"./{snapshot_name}"
    for line in output.splitlines():
        fields = line.split(maxsplit=2)
        if len(fields) < 2:
            continue
        operation = fields[0]
        # Access-time updates are incidental reads, not logical workspace
        # changes.  Treating every ``utimes`` record as a candidate would
        # force a full tree walk after a read-heavy workflow because Btrfs
        # records atime changes for many otherwise unchanged files.
        if operation == "utimes":
            continue
        path = _btrfs_dump_path(fields[1], prefix)
        if path is not None:
            candidates.add(path)
            if operation in {"rename", "rmdir"}:
                recursive_candidates.add(path)
            if operation in {"mkfile", "link", "symlink"}:
                file_candidates.add(path)
                if path not in removed_candidates:
                    one_sided_candidates.add(path)
                    one_sided_file_candidates.add(path)
                added_candidates.add(path)
            elif operation == "mkdir":
                # Btrfs materializes a new directory through a temporary
                # directory followed by rename.  Track it as temporary, but
                # do not fast-path it as a file candidate.
                directory_candidates.add(path)
                added_candidates.add(path)
            elif operation == "rmdir":
                removed_candidates.add(path)
                removed_directory_candidates.add(path)
            elif operation == "unlink":
                if path in added_candidates:
                    candidates.discard(path)
                    recursive_candidates.discard(path)
                    one_sided_candidates.discard(path)
                    one_sided_file_candidates.discard(path)
                else:
                    one_sided_candidates.add(path)
                    one_sided_file_candidates.add(path)
                removed_candidates.add(path)
                removed_file_candidates.add(path)
            elif operation == "rename":
                # A temporary source created in this stream is not a final
                # one-sided path.  A destination preceded by unlink existed
                # in the parent and needs a normal content comparison.
                if path in added_candidates:
                    candidates.discard(path)
                    recursive_candidates.discard(path)
                    one_sided_candidates.discard(path)
                    one_sided_file_candidates.discard(path)
                else:
                    one_sided_candidates.add(path)
                    if path in file_candidates or path in removed_file_candidates:
                        one_sided_file_candidates.add(path)
                    elif (
                        path in directory_candidates
                        or path in removed_directory_candidates
                    ):
                        one_sided_file_candidates.discard(path)
        if len(fields) < 3:
            continue
        match = re.search(r"(?:^|\s)dest=(\S+)", fields[2])
        if match is None:
            continue
        destination = _btrfs_dump_path(match.group(1), prefix)
        if destination is not None:
            candidates.add(destination)
            if operation == "rename":
                recursive_candidates.add(destination)
                if destination in removed_candidates:
                    one_sided_candidates.discard(destination)
                    one_sided_file_candidates.discard(destination)
                else:
                    one_sided_candidates.add(destination)
                    if (
                        path in file_candidates
                        or path in removed_file_candidates
                    ):
                        one_sided_file_candidates.add(destination)
                added_candidates.add(destination)
                if path in file_candidates or path in removed_file_candidates:
                    file_candidates.add(destination)
                elif (
                    path in directory_candidates
                    or path in removed_directory_candidates
                ):
                    directory_candidates.add(destination)
    return (
        candidates,
        recursive_candidates,
        one_sided_candidates,
        one_sided_file_candidates,
    )


def _btrfs_dump_path(value: str, prefix: str) -> str | None:
    if value == prefix or value == f"{prefix}/":
        return None
    if value.startswith(f"{prefix}/"):
        value = value[len(prefix) + 1 :]
    elif value.startswith("./"):
        return None
    normalized = normalize_workspace_path(value)
    return None if normalized == "/" else normalized


def _different_candidate_file_paths(
    left: Path,
    right: Path,
    candidates: set[str],
    *,
    recursive_candidates: set[str] | None = None,
    one_sided_candidates: set[str] | None = None,
    one_sided_file_candidates: set[str] | None = None,
) -> set[str]:
    recursive = recursive_candidates or set()
    one_sided = one_sided_candidates or set()
    one_sided_files = one_sided_file_candidates or set()
    expanded: set[str] = set()
    for candidate in candidates:
        if candidate in one_sided_files:
            expanded.add(candidate)
            continue
        found_directory = False
        for root in (left, right):
            path = _workspace_path(root, candidate)
            if path.is_symlink() or not path.is_dir():
                continue
            found_directory = True
            if candidate in recursive:
                for child in path.rglob("*"):
                    if child.is_symlink() or not child.is_file():
                        continue
                    expanded.add("/" + child.relative_to(root).as_posix())
        if not found_directory:
            expanded.add(candidate)
    return {
        path
        for path in expanded
        if path in one_sided or _candidate_file_differs(left, right, path)
    }


def _candidate_file_differs(left: Path, right: Path, path: str) -> bool:
    """Compare one send candidate without reading one-sided files.

    Btrfs ``send --no-data`` already tells us that a path was created or
    removed.  Reading a newly-created file merely to compare it with a
    missing path defeats the point of the metadata-only diff, especially for
    generated trees such as virtual environments.  Only candidates present
    in both snapshots need content comparison.
    """

    left_path = _workspace_path(left, path)
    right_path = _workspace_path(right, path)
    left_exists = left_path.is_file()
    right_exists = right_path.is_file()
    if left_exists != right_exists:
        return True
    if not left_exists:
        return False
    return not _same_file_contents(left_path, right_path)


def _regular_files(root: Path) -> dict[str, Path]:
    resolved_root = root.resolve()
    result: dict[str, Path] = {}
    for path in root.rglob("*"):
        if path.is_symlink() or not path.is_file():
            continue
        resolved = path.resolve()
        if not resolved.is_relative_to(resolved_root):
            continue
        result["/" + path.relative_to(root).as_posix()] = path
    return result


def _same_file_contents(left: Path, right: Path) -> bool:
    if left.stat().st_size != right.stat().st_size:
        return False
    with left.open("rb") as left_file, right.open("rb") as right_file:
        while True:
            left_chunk = left_file.read(1024 * 1024)
            right_chunk = right_file.read(1024 * 1024)
            if left_chunk != right_chunk:
                return False
            if not left_chunk:
                return True


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
