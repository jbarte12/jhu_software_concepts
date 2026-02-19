from src.app import create_app
import os

def start_app(test_mode=False):
    app = create_app()

    if test_mode:
        app.config.update({
            "TESTING": True,
            "DEBUG": False,
            "SQLALCHEMY_DATABASE_URI": "sqlite:///:memory:",
            "WTF_CSRF_ENABLED": False,
            "SECRET_KEY": "dev",
        })

    if not test_mode:
        app.run(use_reloader=False)  # pragma: no cover

    return app

if __name__ == "__main__":
    test_mode = os.environ.get("TEST_MAIN") == "1"
    start_app(test_mode=test_mode)
