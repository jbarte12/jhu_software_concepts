# Import flask
from flask import Flask

# Connect "pages" blueprint to flask project (app = folder, pages = blueprint)
from app import pages

# Initialize app - create_app is the application factory
def create_app():
    app = Flask(__name__)

    # Register the pages blueprint
    app.register_blueprint(pages.bp)

    # Return the app
    return app