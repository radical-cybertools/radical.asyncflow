# tests/conftest.py
import shutil
from pathlib import Path


def pytest_sessionfinish(session, exitstatus):
    root = Path(__file__).parent
    for pycache_dir in root.rglob('__pycache__'):
        shutil.rmtree(pycache_dir)
