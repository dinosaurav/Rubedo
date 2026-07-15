"""Setup hook: builds the web UI before packaging if assets are missing.

setuptools runs this file during build (even with pyproject.toml as the
primary config). If src/rubedo/web_static/index.html doesn't exist and
npm + the web/ directory are available, it runs `npm run build` to
produce the bundled assets. Silent no-op otherwise — rubedo serve falls
back to API-only mode if the web UI isn't built.
"""
import os
import shutil
import subprocess
import sys
from pathlib import Path

from setuptools import setup

_root = Path(__file__).parent
_static = _root / "src" / "rubedo" / "web_static"
_web = _root / "web"

if not (_static / "index.html").exists() and _web.is_dir():
    npm = shutil.which("npm")
    if npm:
        try:
            subprocess.run(
                [npm, "run", "build"],
                cwd=str(_web),
                check=True,
                capture_output=True,
                timeout=120,
            )
            print("rubedo: web UI built successfully", file=sys.stderr)
        except Exception as e:
            print(f"rubedo: web UI build skipped: {e}", file=sys.stderr)

setup()
