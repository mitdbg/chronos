from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import textwrap
import time
from pathlib import Path
from tempfile import TemporaryDirectory


def _fuse_available() -> tuple[bool, str]:
    missing = [name for name in ("fusermount3", "mountpoint") if shutil.which(name) is None]
    if missing:
        return False, f"missing FUSE tools: {', '.join(missing)}"
    if not Path("/dev/fuse").exists():
        return False, "/dev/fuse is not available"
    return True, ""


def main() -> None:
    available, reason = _fuse_available()
    if not available:
        print(f"ChronosFS FUSE control-plane example skipped: {reason}")
        return

    with TemporaryDirectory(prefix="chronos-example-") as temp_dir:
        root = Path(temp_dir)
        db_path = root / "chronosfs.sqlite"
        mountpoint = root / "mnt"
        mountpoint.mkdir()
        mount_script = root / "mount_chronosfs.py"
        mount_script.write_text(
            textwrap.dedent(
                f"""
                from chronos_core.workspace.chronosfs import ChronosFSStore, mount_chronosfs

                fs = ChronosFSStore.connect("sqlite:///{db_path}", backend="interval")
                fs.ensure()
                try:
                    mount_chronosfs(fs, {str(mountpoint)!r})
                finally:
                    fs.close()
                """
            )
        )

        env = os.environ.copy()
        proc = subprocess.Popen(
            [sys.executable, str(mount_script)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=env,
        )
        try:
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                if proc.poll() is not None:
                    stdout, stderr = proc.communicate()
                    raise RuntimeError(
                        f"ChronosFS mount exited early with {proc.returncode}\n"
                        f"stdout:\n{stdout}\nstderr:\n{stderr}"
                    )
                probe = subprocess.run(
                    ["mountpoint", "-q", str(mountpoint)],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                )
                if probe.returncode == 0:
                    break
                time.sleep(0.05)
            else:
                raise RuntimeError("timed out waiting for ChronosFS mount")

            (mountpoint / "data.txt").write_bytes(b"aaaaaaaa\nbbbbbbbb\n")
            (mountpoint / ".chronos" / "branches" / "agent").mkdir()
            (mountpoint / ".chronos" / "current").write_text("agent\n")
            with (mountpoint / "data.txt").open("r+b") as handle:
                handle.seek(9)
                handle.write(b"AGNT")
            (mountpoint / ".chronos" / "current").write_text("main\n")
            with (mountpoint / "data.txt").open("r+b") as handle:
                handle.seek(9)
                handle.write(b"MAIN")

            preview_path = mountpoint / ".chronos" / "merge-preview" / "agent..main.json"
            preview = json.loads(preview_path.read_text())
            assert len(preview["conflicts"]) == 1
            conflict_id = preview["conflicts"][0]["conflict_id"]
            resolution = {
                "policy": "manual_review",
                "conflicts": {conflict_id: "source"},
            }
            apply_path = mountpoint / ".chronos" / "merge-apply" / "agent..main"
            apply_path.write_text(json.dumps(resolution))
            assert (mountpoint / "data.txt").read_bytes() == b"aaaaaaaa\nAGNTbbbb\n"
        finally:
            subprocess.run(
                ["fusermount3", "-u", str(mountpoint)],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait(timeout=5)

    print("ChronosFS FUSE control-plane example passed")


if __name__ == "__main__":
    main()
