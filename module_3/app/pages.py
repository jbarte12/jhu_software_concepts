# pages.py

from flask import Blueprint, render_template, redirect, url_for
import threading
import json

from query_data import get_application_stats
from refresh_gradcafe import refresh
from update_data import update_data
from load_data import sync_db_from_llm_file


STATE_FILE = "pull_state.json"

bp = Blueprint("pages", __name__)


def read_state():
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return {"pulling_data": False, "pull_complete": False}


def write_state(pulling_data, pull_complete):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(
            {
                "pulling_data": pulling_data,
                "pull_complete": pull_complete,
            },
            f,
        )


@bp.route("/")
def grad_cafe():
    stats = get_application_stats()
    state = read_state()

    return render_template(
        "gradcafe_stats.html",
        stats=stats,
        message=None,
        pulling_data=state["pulling_data"],
        pull_complete=state["pull_complete"],
    )


# --------------------------------------------------
# PULL BUTTON
# --------------------------------------------------
@bp.route("/refresh", methods=["POST"])
def refresh_gradcafe():
    state = read_state()

    if state["pulling_data"]:
        return redirect(url_for("pages.grad_cafe"))

    write_state(True, False)

    def background_job():
        try:
            refresh()  # scrape + write new_applicant_data.json
        finally:
            write_state(False, True)

    threading.Thread(target=background_job, daemon=True).start()
    return redirect(url_for("pages.grad_cafe"))


# --------------------------------------------------
# UPDATE ANALYSIS BUTTON
# --------------------------------------------------
@bp.route("/update-analysis", methods=["POST"])
def update_analysis():
    state = read_state()

    if state["pulling_data"]:
        stats = get_application_stats()
        return render_template(
            "gradcafe_stats.html",
            stats=stats,
            message="Cannot update analysis while data is being pulled.",
            pulling_data=True,
            pull_complete=False,
        )

    # 🔑 Run LLM on new_applicant_data.json and APPEND results
    processed = update_data()

    # 🔑 Sync newly appended LLM rows into DB
    sync_db_from_llm_file()

    stats = get_application_stats()

    write_state(False, False)

    message = (
        f"Analysis updated with {processed} new applicants."
        if processed > 0
        else "No new applicants to analyze."
    )

    return render_template(
        "gradcafe_stats.html",
        stats=stats,
        message=message,
        pulling_data=False,
        pull_complete=False,
    )
