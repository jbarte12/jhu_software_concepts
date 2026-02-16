from src.app import create_app
import pytest

@pytest.fixture
def app():
    app = create_app()
    app.config["TESTING"] = True
    yield app

@pytest.fixture
def client(app):
    return app.test_client()

@pytest.mark.web
def test_home_redirect(client):
    response = client.get("/")
    assert response.status_code == 302
    assert "/analysis" in response.headers["Location"]
