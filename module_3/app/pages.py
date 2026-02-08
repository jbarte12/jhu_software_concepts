#Import Blueprint and render_template from flask
from flask import Blueprint, render_template
from query_data import get_application_stats

# Instance named bp, blueprint name = pages
bp = Blueprint("pages", __name__)

# Create a route to the home page
@bp.route("/")
def grad_cafe():
    stats = get_application_stats()   # <-- CALL your function here
    return render_template("gradcafe_stats.html", stats=stats)

@bp.route("/refresh", methods=["POST"])
def refresh_gradcafe():
    update_gradcafe_data()   # pull only NEW data
    return redirect(url_for("pages.grad_cafe"))