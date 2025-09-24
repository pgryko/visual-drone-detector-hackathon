"""Test bootstrap: add repo root to sys.path so absolute imports work.
This avoids needing PYTHONPATH or packaging the project during test runs.
"""
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

