# tests/conftest.py
import sys
from pathlib import Path

# Add src to sys.path so tests can import src modules
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

import pytest

def pytest_configure(config):
    config.addinivalue_line("markers", "web: Flask route/page tests")
    config.addinivalue_line("markers", "buttons: Pull Data and Update Analysis behavior")
    config.addinivalue_line("markers", "analysis: formatting/rounding of analysis output")
    config.addinivalue_line("markers", "db: database schema/inserts/selects")
    config.addinivalue_line("markers", "integration: end-to-end flows")
