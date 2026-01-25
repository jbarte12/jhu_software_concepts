#Import Blueprint and render_template from flask
from flask import Blueprint, render_template

# Instance named bp, blueprint name = pages
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