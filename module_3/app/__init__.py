# Import flask
from flask import Flask

# Connect "pages" blueprint to flask project (app = folder, pages = blueprint)
from app import pages

# Initialize app - create_app is the application factory
def create_app():
    app = Flask(__name__)

    app.config["SECRET_KEY"] = "dev"
    app.config["WTF_CSRF_ENABLED"] = False

    from app.pages import bp
    app.register_blueprint(bp)

    return app
