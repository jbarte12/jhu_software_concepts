import pytest
from src.app import create_app

# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def app():
    """Create and configure a testable Flask app."""
    app = create_app()
    app.config.update({
        "TESTING": True,
    })
    return app

@pytest.fixture
def client(app):
    """Create a test client for the Flask app."""
    return app.test_client()


# --------------------------
# Web Page Tests
# --------------------------
@pytest.mark.web
def test_app_exists(app):
    """Ensure the Flask app instance exists."""
    assert app is not None

@pytest.mark.web
def test_routes(client, app):
    """Automatically test all registered routes for valid responses."""
    for rule in app.url_map.iter_rules():
        # Skip static files
        if rule.rule.startswith("/static"):
            continue

        # Determine the testable method
        if "GET" in rule.methods:
            response = client.get(rule.rule)
        elif "POST" in rule.methods:
            response = client.post(rule.rule)
        else:
            continue  # Skip unsupported methods

        # Assert the response is OK (200) or redirect (302)
        assert response.status_code in (200, 302)

@pytest.mark.web
def test_page_load(client):
    """Test that the main page loads successfully."""
    response = client.get("/")
    assert response.status_code == 200
    assert b"GradCafe" in response.data  # Example: check that page contains 'GradCafe'

@pytest.mark.web
def test_analysis_page_load(client):
    """Test that the analysis page loads successfully."""
    response = client.get("/analysis")
    assert response.status_code == 200
    assert b"Analysis" in response.data  # Example: check page contains 'Analysis'
