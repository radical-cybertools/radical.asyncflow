# tests/conftest.py
import os

# Debug mode (runtime type checks) for the test suite; read when
# radical.asyncflow is imported, so set it before any test module imports it.
# An explicit RADICAL_ASYNCFLOW_DEBUG in the environment wins.
os.environ.setdefault("RADICAL_ASYNCFLOW_DEBUG", "1")

import shutil  # noqa: E402
from pathlib import Path


def pytest_sessionfinish(session, exitstatus):
    root = Path(__file__).parent
    for pycache_dir in root.rglob("__pycache__"):
        shutil.rmtree(pycache_dir)
