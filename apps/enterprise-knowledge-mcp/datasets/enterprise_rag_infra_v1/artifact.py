#!/usr/bin/env python3
"""Package/restore the retained paper corpus without recrawling GitHub.

Archives are streamed into <=1 GiB Git LFS objects; no unsharded temporary
archive is created. Python 3.11+, GNU tar and zstd are required.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import shutil
import subprocess

HERE = Path(__file__).resolve().parent
CHUNK = 4 * 1024 * 1024
PART_SIZE = 1024**3
EXCLUDES = [".git", "./codebases/langfuse/ee",
            "./codebases/langfuse/web/src/ee", "./codebases/langfuse/worker/src/ee"]


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(CHUNK), b""):
            digest.update(block)
    return digest.hexdigest()


def split_stream(stream, output: Path, name: str, part_size: int = PART_SIZE):
    parts = []
    while True:
        block = stream.read(min(CHUNK, part_size))
        if not block:
            return parts
        path = output / f"{name}.part-{len(parts):03d}"
        digest = hashlib.sha256()
        size = 0
        with path.open("xb") as target:
            while block:
                target.write(block)
                digest.update(block)
                size += len(block)
                if size == part_size:
                    break
                block = stream.read(min(CHUNK, part_size - size))
        parts.append({"path": path.name, "bytes": size, "sha256": digest.hexdigest()})
        print(f"Wrote {path.name}: {size:,} bytes", flush=True)


def verify_parts(root: Path, record: dict) -> None:
    for part in record["parts"]:
        name = part["path"]
        if Path(name).name != name:
            raise ValueError(f"Invalid archive part name: {name}")
        path = root / name
        if path.stat().st_size != part["bytes"] or sha256(path) != part["sha256"]:
            raise ValueError(f"Checksum mismatch or Git LFS pointer instead of payload: {path}")


def verify_stream(root: Path, name: str, record: dict, manifest: dict) -> None:
    """Check decompression without requiring space for a second SQLite file."""
    joined = subprocess.Popen(["cat", *[str(root / part["path"]) for part in record["parts"]]],
                              stdout=subprocess.PIPE)
    if name == "snapshot":
        decoder = subprocess.Popen(["zstd", "-d", "-c"], stdin=joined.stdout, stdout=subprocess.PIPE)
        joined.stdout.close()
        digest = hashlib.sha256()
        size = 0
        for block in iter(lambda: decoder.stdout.read(CHUNK), b""):
            digest.update(block)
            size += len(block)
        decoder.stdout.close()
    else:
        decoder = subprocess.Popen(["zstd", "--quiet", "--test"], stdin=joined.stdout)
        joined.stdout.close()
    decoder_returncode, joined_returncode = decoder.wait(), joined.wait()
    if decoder_returncode or joined_returncode:
        raise ValueError(f"Compressed stream is invalid: {name}")
    if name == "snapshot" and (size != manifest["snapshot_sqlite_bytes"] or
                                digest.hexdigest() != manifest["snapshot_sqlite_sha256"]):
        raise ValueError("Uncompressed SQLite hash/size mismatch")


def pack(args) -> None:
    root = args.artifacts
    root.mkdir(parents=True, exist_ok=True)
    if (root / "manifest.json").exists() or list(root.glob("*.part-*")):
        raise SystemExit("Refusing to overwrite an existing artifact; choose an empty directory")
    corpus = args.corpus.resolve()
    snapshot = args.snapshot.resolve()
    snapshot_file = snapshot / "snapshot.sqlite"
    for suffix in ["-wal", "-journal"]:
        sidecar = Path(str(snapshot_file) + suffix)
        if sidecar.exists() and sidecar.stat().st_size:
            raise SystemExit("Snapshot has a nonempty SQLite journal; checkpoint a disposable copy before packaging")
    snapshot_stat = snapshot_file.stat()
    manifest = {
        "schema_version": 1,
        "description": "Retained enterprise corpus and ingestion snapshot used by incident-response captures",
        "source_excludes": EXCLUDES,
        "snapshot_manifest": json.loads((snapshot / "manifest.json").read_text()),
        "snapshot_sqlite_bytes": snapshot_stat.st_size,
        "snapshot_sqlite_sha256": sha256(snapshot_file),
        "archives": {},
    }
    tar = subprocess.Popen([
        "tar", "--create", "--file=-", "--sort=name", "--mtime=@0",
        "--owner=0", "--group=0", "--numeric-owner",
        *[f"--exclude={pattern}" for pattern in EXCLUDES],
        "--directory", str(corpus), ".",
    ], stdout=subprocess.PIPE)
    compressor = subprocess.Popen(["zstd", "-3", "-T2", "-c"],
                                  stdin=tar.stdout, stdout=subprocess.PIPE)
    tar.stdout.close()
    source_parts = split_stream(compressor.stdout, root, "source.tar.zst")
    compressor.stdout.close()
    compressor_returncode, tar_returncode = compressor.wait(), tar.wait()
    if compressor_returncode or tar_returncode:
        raise RuntimeError("Source archive failed; incomplete parts must not be published")
    manifest["archives"]["source"] = {"format": "tar.zst", "parts": source_parts}
    compressor = subprocess.Popen(["zstd", "-3", "-T2", "-c", str(snapshot_file)],
                                  stdout=subprocess.PIPE)
    snapshot_parts = split_stream(compressor.stdout, root, "snapshot.sqlite.zst")
    compressor.stdout.close()
    if compressor.wait():
        raise RuntimeError("Snapshot compression failed")
    after = snapshot_file.stat()
    if (after.st_size, after.st_mtime_ns) != (snapshot_stat.st_size, snapshot_stat.st_mtime_ns):
        raise RuntimeError("Snapshot changed during packaging; stop the writer and package again")
    manifest["archives"]["snapshot"] = {"format": "sqlite.zst", "parts": snapshot_parts}
    (root / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")


def unpack(args) -> None:
    root = args.artifacts
    manifest = json.loads((root / "manifest.json").read_text())
    for name in ([args.dataset] if args.dataset != "all" else ["source", "snapshot"]):
        record = manifest["archives"][name]
        verify_parts(root, record)
        if args.command == "verify":
            if args.deep:
                verify_stream(root, name, record, manifest)
            print(f"Verified {name}: {len(record['parts'])} archive part(s)")
            continue
        destination = args.output / ("generated_data_infra_v1" if name == "source" else "document-snapshot")
        if destination.exists() and any(destination.iterdir()):
            raise SystemExit(f"Refusing to restore into nonempty directory: {destination}")
        destination.mkdir(parents=True, exist_ok=True)
        if name == "source":
            decoder = subprocess.Popen(["tar", "--extract", "--zstd", "--file=-",
                                        "--no-same-owner", "--no-same-permissions",
                                        "--directory", str(destination)], stdin=subprocess.PIPE)
            target = None
        else:
            target = (destination / "snapshot.sqlite").open("xb")
            decoder = subprocess.Popen(["zstd", "-d", "-c"], stdin=subprocess.PIPE, stdout=target)
        try:
            for part in record["parts"]:
                with (root / part["path"]).open("rb") as source:
                    shutil.copyfileobj(source, decoder.stdin, CHUNK)
            decoder.stdin.close()
            if decoder.wait():
                raise RuntimeError(f"Archive extraction failed: {name}")
        finally:
            if target:
                target.close()
        if name == "snapshot":
            if sha256(destination / "snapshot.sqlite") != manifest["snapshot_sqlite_sha256"]:
                raise ValueError("Decompressed SQLite checksum mismatch")
            (destination / "manifest.json").write_text(json.dumps(manifest["snapshot_manifest"], indent=2) + "\n")
        print(f"Restored {name} to {destination}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["pack", "verify", "restore"])
    parser.add_argument("--artifacts", type=Path, default=HERE / "artifacts")
    parser.add_argument("--dataset", choices=["all", "source", "snapshot"], default="all")
    parser.add_argument("--deep", action="store_true", help="Also verify decompression and the uncompressed SQLite hash")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--corpus", type=Path)
    parser.add_argument("--snapshot", type=Path)
    args = parser.parse_args()
    if args.command == "pack":
        if not args.corpus or not args.snapshot:
            parser.error("pack requires --corpus and --snapshot; snapshot must have no active writers")
        pack(args)
    else:
        if args.command == "restore" and not args.output:
            parser.error("restore requires --output (allow at least 30 GiB for both datasets)")
        unpack(args)


if __name__ == "__main__":
    main()
