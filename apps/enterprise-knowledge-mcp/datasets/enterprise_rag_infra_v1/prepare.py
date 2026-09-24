#!/usr/bin/env python3
"""Prepare the EnterpriseRAG corpus used by the Chronos agent-state study."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import urllib.error
import urllib.request
from pathlib import Path
from typing import Sequence


HERE = Path(__file__).resolve().parent
ENTERPRISE_RAG_URL = "https://github.com/onyx-dot-app/EnterpriseRAG-Bench.git"
ENTERPRISE_RAG_COMMIT = "d36685e273713975ee20299bbf1ab64165575b3c"
BUILDER_PHASES = {
    "init",
    "crawl",
    "normalize",
    "generate-internal",
    "validate",
    "all",
}


def run(
    command: Sequence[str],
    *,
    cwd: Path | None = None,
    env: dict[str, str] | None = None,
) -> None:
    printable = " ".join(command)
    print(f"+ {printable}", flush=True)
    subprocess.run(command, cwd=cwd, env=env, check=True)


def git_output(checkout: Path, *arguments: str) -> str:
    return subprocess.check_output(
        ["git", "-C", str(checkout), *arguments], text=True
    ).strip()


def validate_github_credential() -> None:
    token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
    if not token:
        raise RuntimeError("Set GITHUB_TOKEN or GH_TOKEN before the crawl phase.")
    request = urllib.request.Request(
        "https://api.github.com/rate_limit",
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "User-Agent": "chronos-enterprise-rag-preflight",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            if response.status != 200:
                raise RuntimeError(
                    f"GitHub credential preflight returned HTTP {response.status}."
                )
    except urllib.error.HTTPError as error:
        raise RuntimeError(
            f"GitHub rejected the configured credential with HTTP {error.code}."
        ) from error
    except urllib.error.URLError as error:
        raise RuntimeError(
            f"GitHub credential preflight failed: {error.reason}"
        ) from error


def checkout_at_commit(url: str, commit: str, destination: Path) -> None:
    created = False
    if not destination.exists():
        destination.parent.mkdir(parents=True, exist_ok=True)
        run(
            [
                "git",
                "clone",
                "--filter=blob:none",
                "--depth=1",
                "--no-checkout",
                url,
                str(destination),
            ]
        )
        created = True
    if not (destination / ".git").exists():
        raise RuntimeError(f"Existing path is not a Git checkout: {destination}")
    try:
        head = git_output(destination, "rev-parse", "HEAD")
    except subprocess.CalledProcessError:
        head = ""
    if created or head != commit:
        if not created and git_output(destination, "status", "--porcelain"):
            raise RuntimeError(
                f"Refusing to change revision of a dirty checkout: {destination}"
            )
        run(["git", "-C", str(destination), "fetch", "--depth=1", "origin", commit])
        run(["git", "-C", str(destination), "checkout", "--detach", commit])
    actual = git_output(destination, "rev-parse", "HEAD")
    if actual != commit:
        raise RuntimeError(f"Expected {destination} at {commit}, found {actual}")


def install_seed_files(base: Path) -> None:
    seed_root = HERE / "seed"
    for source in sorted(path for path in seed_root.rglob("*") if path.is_file()):
        relative = source.relative_to(seed_root)
        destination = base / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        source_payload = source.read_bytes()
        payload = source_payload
        if relative.parent == Path("codebases/upstream-issues"):
            payload = source_payload.rstrip(b"\n") + b"\n\n"
        # These five files are owned by this recipe. Refreshing them makes an
        # interrupted preparation resumable after documentation corrections.
        shutil.copy2(source, destination)
        if payload != source_payload:
            destination.write_bytes(payload)


def prepare_codebases(base: Path) -> None:
    manifest_path = HERE / "seed" / "codebases" / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    codebases = base / "codebases"
    codebases.mkdir(parents=True, exist_ok=True)
    for repository in manifest["repositories"]:
        checkout_at_commit(
            repository["upstream"],
            repository["commit"],
            codebases / repository["name"],
        )
    install_seed_files(base)


def resolve_enterprise_rag_checkout(args: argparse.Namespace) -> Path:
    if args.enterprise_rag_checkout:
        checkout = args.enterprise_rag_checkout.expanduser().resolve()
        if not checkout.is_dir():
            raise FileNotFoundError(
                f"EnterpriseRAG-Bench checkout not found: {checkout}"
            )
        actual = git_output(checkout, "rev-parse", "HEAD")
        if actual != ENTERPRISE_RAG_COMMIT:
            raise RuntimeError(
                "EnterpriseRAG-Bench must be checked out at "
                f"{ENTERPRISE_RAG_COMMIT}; found {actual}"
            )
        return checkout
    checkout = args.workspace.expanduser().resolve() / "EnterpriseRAG-Bench"
    checkout_at_commit(ENTERPRISE_RAG_URL, ENTERPRISE_RAG_COMMIT, checkout)
    return checkout


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument(
        "--workspace",
        type=Path,
        default=Path.cwd() / "enterprise-rag-workspace",
        help="Parent directory for a managed EnterpriseRAG-Bench checkout.",
    )
    result.add_argument(
        "--enterprise-rag-checkout",
        type=Path,
        help="Use an existing checkout instead of cloning one under --workspace.",
    )
    result.add_argument(
        "--output",
        type=Path,
        help="Derivative output (default: CHECKOUT/generated_data_infra_v1).",
    )
    result.add_argument(
        "--phase",
        choices=("source", *sorted(BUILDER_PHASES)),
        default="all",
        help="Run only one resumable phase; source only prepares pinned inputs.",
    )
    result.add_argument(
        "--llm-generated-internal",
        action="store_true",
        help="Generate the 200 internal records with EnterpriseRAG-Bench's configured LLMs. "
        "The paper artifact used the deterministic default.",
    )
    result.add_argument(
        "--allow-incomplete",
        action="store_true",
        help="Pass the builder's diagnostic incomplete-data mode to validation.",
    )
    result.add_argument(
        "--repository",
        choices=("vllm-project/vllm", "BerriAI/litellm", "langfuse/langfuse"),
        help="Restrict a crawl or normalization phase to one public repository.",
    )
    return result


def main(argv: Sequence[str] | None = None) -> int:
    args = parser().parse_args(argv)
    if args.phase in {"crawl", "all"}:
        validate_github_credential()
    checkout = resolve_enterprise_rag_checkout(args)
    base = checkout / "generated_data"
    if not base.is_dir():
        raise FileNotFoundError(
            f"The pinned checkout does not contain generated_data: {base}"
        )
    prepare_codebases(base)
    if args.phase == "source":
        print(f"Prepared pinned source corpus at {base}")
        return 0

    output = (
        args.output.expanduser().resolve()
        if args.output
        else checkout / "generated_data_infra_v1"
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    command = [
        sys.executable,
        str(HERE / "build_infra_dataset.py"),
        args.phase,
        "--base",
        str(base),
        "--output",
        str(output),
    ]
    if not args.llm_generated_internal:
        command.append("--template-fallback")
    if args.allow_incomplete:
        command.append("--allow-incomplete")
    if args.repository:
        command.extend(("--repository", args.repository))

    environment = dict(os.environ)
    existing_pythonpath = environment.get("PYTHONPATH")
    environment["PYTHONPATH"] = (
        f"{checkout}{os.pathsep}{existing_pythonpath}"
        if existing_pythonpath
        else str(checkout)
    )
    run(command, cwd=checkout, env=environment)
    print(f"Prepared derivative corpus at {output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
