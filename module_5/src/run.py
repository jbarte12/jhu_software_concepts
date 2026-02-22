"""
Application entry point for the GradCafe Flask application.

Import ``start_app`` to obtain a configured Flask application instance
for testing or WSGI deployment.
"""

import os

from .app import create_app


def start_app(test_mode=False):
    """Create and optionally configure the Flask application.

    :param test_mode: If ``True``, applies test configuration overrides
        and skips starting the development server.
    :type test_mode: bool
    :returns: Configured Flask application instance.
    :rtype: flask.Flask
    """
    app = create_app()

    if test_mode:
        app.config.update({
            "TESTING": True,
            "DEBUG": False,
            "SQLALCHEMY_DATABASE_URI": "sqlite:///:memory:",
            "WTF_CSRF_ENABLED": False,
            "SECRET_KEY": "dev",
        })
    else:
        app.run(use_reloader=False)  # pragma: no cover

    return app


if __name__ == "__main__":  # pragma: no cover
    # The TEST_MAIN environment variable is set by test_run_py_main_block_coverage
    # to exercise this block without actually starting the dev server.
    if not os.environ.get("TEST_MAIN"):
        start_app()
