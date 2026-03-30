#!/usr/bin/env python3
"""
Edge Catcher launcher — works on Windows, Linux, and Pi.
Usage: python start.py [--port 8080] [--no-browser]
"""
from __future__ import annotations

import argparse
import os
import platform
import shutil
import subprocess
import sys
import time
import webbrowser
from pathlib import Path

ROOT = Path(__file__).parent.resolve()
VENV = ROOT / ".venv"


def _venv_python() -> Path:
    if platform.system() == "Windows":
        return VENV / "Scripts" / "python.exe"
    return VENV / "bin" / "python"


def _venv_pip() -> Path:
    if platform.system() == "Windows":
        return VENV / "Scripts" / "pip.exe"
    return VENV / "bin" / "pip"


def check_python_version() -> None:
    if sys.version_info < (3, 10):
        print(f"ERROR: Python 3.10+ required, got {sys.version}")
        sys.exit(1)


def ensure_venv() -> None:
    if not _venv_python().exists():
        print("Creating virtual environment...")
        subprocess.run([sys.executable, "-m", "venv", str(VENV)], check=True)

    print("Installing/verifying Python dependencies...")
    subprocess.run(
        [str(_venv_python()), "-m", "pip", "install", "-e", ".[ui]", "--quiet"],
        cwd=ROOT,
        check=True,
    )


def ensure_ui(port: int) -> None:
    ui_dir = ROOT / "ui"
    dist_dir = ui_dir / "dist"

    # Check if npm is available
    if not shutil.which("npm"):
        print(
            "ERROR: npm not found. Please install Node.js from https://nodejs.org/ and try again."
        )
        sys.exit(1)

    # Determine if dist is stale or missing
    needs_build = not dist_dir.exists()
    if not needs_build and (ui_dir / "src").exists():
        # Check if any src file is newer than dist/index.html
        dist_index = dist_dir / "index.html"
        if dist_index.exists():
            dist_mtime = dist_index.stat().st_mtime
            for src_file in (ui_dir / "src").rglob("*"):
                if src_file.is_file() and src_file.stat().st_mtime > dist_mtime:
                    needs_build = True
                    break

    if needs_build:
        print("Building UI...")
        subprocess.run("npm install", cwd=ui_dir, check=True, shell=True)
        subprocess.run("npm run build", cwd=ui_dir, check=True, shell=True)
    else:
        print("UI dist is up to date, skipping build.")


def start_server(port: int) -> subprocess.Popen:
    print(f"Starting server on port {port}...")
    return subprocess.Popen(
        [
            str(_venv_python()),
            "-m",
            "uvicorn",
            "api.main:app",
            "--host",
            "0.0.0.0",
            "--port",
            str(port),
        ],
        cwd=ROOT,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Start Edge Catcher")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--no-browser", action="store_true")
    args = parser.parse_args()

    check_python_version()
    ensure_venv()
    ensure_ui(args.port)

    proc = start_server(args.port)

    url = f"http://localhost:{args.port}"
    if not args.no_browser:
        print(f"Opening {url} in browser in 2 seconds...")
        time.sleep(2)
        webbrowser.open(url)

    print(f"Edge Catcher running at {url} — press Ctrl+C to stop")
    try:
        proc.wait()
    except KeyboardInterrupt:
        print("\nShutting down...")
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
        print("Stopped.")


if __name__ == "__main__":
    main()
