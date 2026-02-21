"""
This module initializes the Flask application using the application factory pattern.
It imports the necessary modules, sets configuration options, and registers blueprints.
"""

# Import the Flask class used to create the web application
from flask import Flask

# Import the pages module so Flask knows about the blueprint package
# Import the Blueprint object that defines page routes
from .pages import bp

# Application factory function
# This is the standard Flask pattern for creating an app instance
def create_app():
    """
    Create and configure the Flask application.

    This function follows the Flask application factory pattern. It:
    - Creates the Flask app instance
    - Sets configuration values (SECRET_KEY, WTF_CSRF_ENABLED)
    - Imports and registers the 'pages' blueprint
    - Returns the fully configured Flask app
    """
    # Create the Flask application object
    app = Flask(__name__)

    # Secret key used by Flask for session management and security features
    app.config["SECRET_KEY"] = "dev"

    # Disable CSRF protection (useful for development/testing)
    app.config["WTF_CSRF_ENABLED"] = False

    # Register the pages blueprint with the Flask app
    app.register_blueprint(bp)

    # Return the fully configured Flask app
    return app
