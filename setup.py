"""Setup hook: includes pre-built web assets in the package if present.

Does NOT build the web UI — that must be done beforehand with
`cd web && npm install && npm run build`. The built assets in
src/rubedo/web_static/ are included in the wheel via MANIFEST.in.

If the assets are missing (e.g. git install without building), rubedo
serve falls back to API-only mode with a helpful message.
"""
from setuptools import setup

setup()
