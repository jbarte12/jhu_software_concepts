import pytest
from app import create_app

# ----------------------------------------
# Test client fixture
# ----------------------------------------
@pytest.fixture
def client():
    """Create a test client using the Flask app factory."""
    app = create_app()
    app.testing = True
    with app.test_client() as client:
        yield client

# ----------------------------------------
# Web tests
# ----------------------------------------
@pytest.mark.web
def test_analysis_page_load(client):
    response = client.get("/analysis")
    assert response.status_code == 200

@pytest.mark.web
def test_refresh_post(client):
    response = client.post("/refresh")
    assert response.status_code == 302
    assert "/analysis" in response.headers["Location"]

@pytest.mark.web
def test_update_analysis_post(client):
    response = client.post("/update-analysis")
    assert response.status_code == 200

@pytest.mark.web
def test_analysis_content(client):
    response = client.get("/analysis")
    html = response.data.decode("utf-8")
    assert "GradCafe Application Statistics" in html
    assert "Pull Data" in html
    assert "Update Analysis" in html
