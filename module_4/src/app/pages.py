# Import Flask helpers for routing, rendering templates, and redirects
from flask import Blueprint, render_template, redirect, url_for

# Import threading so long-running jobs don’t block the web app
import threading

# Import json to read/write application state to a file
import json

# Import function that queries the database and returns analytics stats
from query_data import get_application_stats

# Import function that scrapes GradCafe and writes raw data to JSON
from refresh_gradcafe import refresh

# Import function that runs analysis / LLM processing on new data
from update_data import update_data

# Import function that syncs LLM-processed data into the database
from load_data import sync_db_from_llm_file

# File used to persist pull/update state between requests
STATE_FILE = "pull_state.json"

# Create a Flask Blueprint for page routes
bp = Blueprint("pages", __name__)

def read_state():

    # Attempt to read the pull/update state from disk
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:

            # Load and return the JSON contents as a dictionary
            return json.load(f)

    except FileNotFoundError:

        # If the state file doesn’t exist yet, return default values
        return {"pulling_data": False, "pull_complete": False}


def write_state(pulling_data, pull_complete):

    # Write the current pull/update state to disk
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(
            {
                # Whether data is currently being pulled
                "pulling_data": pulling_data,
                # Whether the pull has finished
                "pull_complete": pull_complete,
            },
            f,
        )

@bp.route("/")
def grad_cafe():

    # Query the database and compute all application statistics
    stats = get_application_stats()

    # Read the current pull/update state from disk
    state = read_state()

    # Render the main stats page with current data and state flags
    return render_template(
        "gradcafe_stats.html",
        stats=stats,
        message=None,
        pulling_data=state["pulling_data"],
        pull_complete=state["pull_complete"],
    )

# PULL BUTTON
@bp.route("/refresh", methods=["POST"])
def refresh_gradcafe():

    # Read the current pull/update state
    state = read_state()

    # If a pull is already in progress, do nothing and return to main page
    if state["pulling_data"]:
        return redirect(url_for("pages.grad_cafe"))

    # Mark that a pull has started and is not yet complete
    write_state(True, False)

    # Define a background job so scraping runs asynchronously
    def background_job():
        try:
            # Scrape GradCafe and write new_applicant_data.json
            refresh()
        finally:
            # Always mark pull as finished, even if scraping errors
            write_state(False, True)

    # Start the background job in a daemon thread
    threading.Thread(target=background_job, daemon=True).start()

    # Redirect back to the main page immediately
    return redirect(url_for("pages.grad_cafe"))

# UPDATE ANALYSIS BUTTON
@bp.route("/update-analysis", methods=["POST"])
def update_analysis():
    # Read the current pull/update state
    state = read_state()

    # Prevent analysis updates while a pull is still running
    if state["pulling_data"]:
        stats = get_application_stats()
        return render_template(
            "gradcafe_stats.html",
            stats=stats,
            message="Cannot update analysis while data is being pulled.",
            pulling_data=True,
            pull_complete=False,
        )

    # Run LLM processing on new_applicant_data.json and append results
    processed = update_data()

    # Sync newly appended LLM-generated rows into the database
    sync_db_from_llm_file()

    # Recompute statistics after database update
    stats = get_application_stats()

    # Reset state flags after update completes
    write_state(False, False)

    # Build a user-facing status message based on rows processed
    message = (
        f"Analysis updated with {processed} new applicants."
        if processed > 0
        else "No new applicants to analyze."
    )

    # Render the stats page with updated data and message
    return render_template(
        "gradcafe_stats.html",
        stats=stats,
        message=message,
        pulling_data=False,
        pull_complete=False,
    )
