# Import the Flask class used to create the web application
from flask import Flask

# Import the pages module so Flask knows about the blueprint package
# (app = main Flask application, pages = blueprint containing routes)
from . import pages

# Application factory function
# This is the standard Flask pattern for creating an app instance
def create_app():
    # Create the Flask application object
    app = Flask(__name__)

    # Secret key used by Flask for session management and security features
    app.config["SECRET_KEY"] = "dev"

    # Disable CSRF protection (useful for development/testing)
    app.config["WTF_CSRF_ENABLED"] = False

    # Import the Blueprint object that defines page routes
    from .pages import bp

    # Register the pages blueprint with the Flask app
    app.register_blueprint(bp)

    # Return the fully configured Flask app
    return app

