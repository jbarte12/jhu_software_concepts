# tests/conftest.py
import sys
from pathlib import Path
import pytest

# Add project root (module_4) to sys.path so `src` is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# Import the Flask app factory
from src.app import create_app

# ------------------------------
# Pytest fixture for Flask app
# ------------------------------
@pytest.fixture
def app():
    """Return a Flask app instance for testing."""
    flask_app = create_app()
    flask_app.config.update({
        "TESTING": True,   # Flask testing mode
        "WTF_CSRF_ENABLED": False,  # disable CSRF for testing
    })
    yield flask_app

# ------------------------------
# Markers for pytest
# ------------------------------
def pytest_configure(config):
    config.addinivalue_line("markers", "web: Flask route/page tests")
    config.addinivalue_line("markers", "buttons: Pull Data and Update Analysis behavior")
    config.addinivalue_line("markers", "analysis: formatting/rounding of analysis output")
    config.addinivalue_line("markers", "db: database schema/inserts/selects")
    config.addinivalue_line("markers", "integration: end-to-end flows")
