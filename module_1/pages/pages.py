from flask import Blueprint, render_template

# instance named bp
# pages = name of blueprint
bp = Blueprint("pages", __name__)

# Create a route to the home page
@bp.route("/")
def home():
    return render_template("home.html")

# Create a route to the about page
@bp.route("/contact_info")
def contact_info():
    return render_template("contact_info.html")

# Create a route to the about page
@bp.route("/projects")
def projects():
    return render_template("projects.html")