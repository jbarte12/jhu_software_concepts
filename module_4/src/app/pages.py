# Import Flask helpers for routing, rendering templates, and redirects
from flask import Blueprint, render_template, redirect, url_for

# Import threading so long-running jobs don’t block the web app
import threading

# Import json to read/write application state to a file
import json

# Import functions for querying, refreshing, updating, and syncing data
from ..query_data import get_application_stats
from ..refresh_gradcafe import refresh
from ..update_data import update_data
from ..load_data import sync_db_from_llm_file

import os
from ..paths import STATE_FILE

# Create a Flask Blueprint for page routes
bp = Blueprint("pages", __name__)

# -------------------------------
# STATE FUNCTIONS
# -------------------------------
def read_state():
    """Read the current pull/update state from disk."""
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            state = json.load(f)
            # Ensure all expected keys exist
            state.setdefault("pulling_data", False)
            state.setdefault("updating_analysis", False)
            state.setdefault("pull_complete", False)
            state.setdefault("analysis_complete", False)
            state.setdefault("message", None)
            return state
    except FileNotFoundError:
        return {
            "pulling_data": False,
            "updating_analysis": False,
            "pull_complete": False,
            "analysis_complete": False,
            "message": None
        }

def write_state(pulling_data=False, updating_analysis=False, pull_complete=False,
                analysis_complete=False, message=None):
    """Write the current pull/update state to disk."""
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump({
            "pulling_data": pulling_data,
            "updating_analysis": updating_analysis,
            "pull_complete": pull_complete,
            "analysis_complete": analysis_complete,
            "message": message
        }, f)

# -------------------------------
# MAIN STATS PAGE
# -------------------------------
@bp.route("/")
@bp.route("/analysis")
def grad_cafe():
    """Render the main GradCafe stats page."""
    stats = get_application_stats()
    state = read_state()
    return render_template(
        "gradcafe_stats.html",
        stats=stats,
        message=state.get("message"),
        pulling_data=state["pulling_data"],
        updating_analysis=state["updating_analysis"],
        pull_complete=state["pull_complete"],
        analysis_complete=state["analysis_complete"]
    )

# -------------------------------
# PULL DATA BUTTON
# -------------------------------
@bp.route("/refresh", methods=["POST"])
def refresh_gradcafe():
    """Handle 'Pull Data' button click asynchronously."""
    state = read_state()
    if state["pulling_data"] or state["updating_analysis"]:
        # Return HTTP 409 if a process is already running
        return "", 409

    # Mark that pull has started
    write_state(pulling_data=True, updating_analysis=False,
                pull_complete=False, analysis_complete=False, message=None)

    def background_job():
        try:
            refresh()
            # Mark pull complete
            write_state(pulling_data=False, updating_analysis=False,
                        pull_complete=True, analysis_complete=False, message=None)
        except Exception as e:
            write_state(pulling_data=False, updating_analysis=False,
                        pull_complete=False, analysis_complete=False,
                        message=f"Pull failed: {e}")

    threading.Thread(target=background_job, daemon=True).start()
    return redirect(url_for("pages.grad_cafe"))

# -------------------------------
# UPDATE ANALYSIS BUTTON
# -------------------------------
@bp.route("/update-analysis", methods=["POST"])
def update_analysis():
    """Handle 'Update Analysis' button click asynchronously."""
    state = read_state()
    if state["pulling_data"] or state["updating_analysis"]:
        # Return HTTP 409 if a process is already running
        return "", 409

    # Mark analysis as started
    write_state(pulling_data=False, updating_analysis=True,
                pull_complete=state["pull_complete"], analysis_complete=False, message=None)

    def background_job():
        try:
            processed = update_data()
            sync_db_from_llm_file()

            # Build completion message
            msg = f"Analysis updated with {processed} new applicants." if processed > 0 else "No new applicants to analyze."

            # Reset flags and mark analysis complete
            write_state(pulling_data=False, updating_analysis=False,
                        pull_complete=state["pull_complete"],
                        analysis_complete=True, message=msg)
        except Exception as e:
            write_state(pulling_data=False, updating_analysis=False,
                        pull_complete=state["pull_complete"],
                        analysis_complete=False, message=f"Analysis failed: {e}")

    threading.Thread(target=background_job, daemon=True).start()
    return redirect(url_for("pages.grad_cafe"))
