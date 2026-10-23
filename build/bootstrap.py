"""Create or update the development environment with uv.

Creates .venv from the Python version in .python-version (uv downloads it if
missing, without touching PATH or the default python), installs snowlib in
editable mode with the dev extra, registers the "snowlib" Jupyter kernel, and
points the nbstripout git filter at the venv.

    uv run --isolated --no-project build/bootstrap.py              create, or update in place
    uv run --isolated --no-project build/bootstrap.py --recreate   delete .venv and rebuild

--isolated matters: without it uv runs the script with the project's .venv when
one exists, and Windows cannot delete the running interpreter, so --recreate
refuses to run from inside .venv.
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent.parent
VENV_DIR = PROJECT_DIR / ".venv"
KERNEL_NAME = "snowlib"
INVOCATION = "uv run --isolated --no-project build/bootstrap.py"


def venv_python() -> Path:
    """Path to the venv's interpreter on this platform"""
    if sys.platform == "win32":
        return VENV_DIR / "Scripts" / "python.exe"
    return VENV_DIR / "bin" / "python"


def run(description: str, cmd: list[str]) -> str:
    """Run a command from the project directory, exiting on failure"""
    print(f"[*] {description}")
    result = subprocess.run(cmd, cwd=PROJECT_DIR, capture_output=True, text=True)
    if result.returncode != 0:
        sys.exit(f"[ERROR] {' '.join(cmd)}\n{result.stderr.strip()}")
    return result.stdout.strip()


def wanted_version() -> str:
    """The Python version pinned in .python-version"""
    return (PROJECT_DIR / ".python-version").read_text(encoding="utf-8").strip()


def venv_version() -> str | None:
    """The venv's major.minor version, or None when it has no working interpreter"""
    if not venv_python().exists():
        return None
    result = subprocess.run(
        [str(venv_python()), "-c", "import sys; print('%d.%d' % sys.version_info[:2])"],
        capture_output=True,
        text=True,
    )
    return result.stdout.strip() if result.returncode == 0 else None


def running_from_venv() -> bool:
    """Whether this script's interpreter lives inside .venv"""
    return Path(sys.prefix).resolve() == VENV_DIR.resolve()


def configure_nbstripout() -> None:
    """Point the required nbstripout filter and notebook diff at the venv"""
    python = venv_python().as_posix()
    settings = {
        "filter.nbstripout.clean": f'"{python}" -m nbstripout --unix-newlines',
        "filter.nbstripout.smudge": "cat",
        "filter.nbstripout.required": "true",
        "filter.nbstripout.extrakeys": "metadata.language_info.version",
        "diff.ipynb.textconv": f'"{python}" -m nbstripout -t --unix-newlines',
    }
    print("[*] Configuring the nbstripout git filter")
    for key, value in settings.items():
        run(f"    {key}", ["git", "config", "--local", key, value])


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Create or update the development environment with uv"
    )
    parser.add_argument(
        "--recreate", action="store_true", help="delete .venv and build it again"
    )
    parser.add_argument(
        "--no-kernel", action="store_true", help="skip registering the Jupyter kernel"
    )
    args = parser.parse_args()

    if shutil.which("uv") is None:
        sys.exit(
            "[ERROR] uv not found on PATH: https://docs.astral.sh/uv/getting-started/installation/"
        )

    version = wanted_version()
    current = venv_version()
    print(f"Project: {PROJECT_DIR}")
    print(f"Python:  {version} (.venv has {current or 'none'})")

    if args.recreate and running_from_venv():
        sys.exit(
            f"[ERROR] Running from .venv, which --recreate deletes. Run: {INVOCATION} --recreate"
        )
    if current is not None and current != version and not args.recreate:
        sys.exit(
            f"[ERROR] .venv is Python {current}, not {version}. Run: {INVOCATION} --recreate"
        )

    if args.recreate or current is None:
        run(
            f"Creating .venv with Python {version}",
            ["uv", "venv", "--clear", "--seed", "--python", version, str(VENV_DIR)],
        )

    python = str(venv_python())
    run(
        "Installing snowlib in editable mode with the dev extra",
        ["uv", "pip", "install", "--python", python, "-e", ".[dev]"],
    )

    if not args.no_kernel:
        run(
            f"Registering the '{KERNEL_NAME}' Jupyter kernel",
            [
                python,
                "-m",
                "ipykernel",
                "install",
                "--user",
                f"--name={KERNEL_NAME}",
                f"--display-name={KERNEL_NAME}",
            ],
        )

    configure_nbstripout()

    print(f"\n[OK] Ready: {run('Checking the interpreter', [python, '-V'])}")
    print("Activate with: . .\\venv.ps1")


if __name__ == "__main__":
    main()
