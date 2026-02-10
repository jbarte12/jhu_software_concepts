# pages.py

from flask import Blueprint, render_template, redirect, url_for
from query_data import get_application_stats
from refresh_gradcafe import refresh
from load_data import sync_db_from_llm_file
import threading
import json

STATE_FILE = "pull_state.json"


def read_state():
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return {"pulling_data": False, "pull_complete": False}


def write_state(pulling_data, pull_complete):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(
            {"pulling_data": pulling_data, "pull_complete": pull_complete},
            f
        )


bp = Blueprint("pages", __name__)


@bp.route("/")
def grad_cafe():
    stats = get_application_stats()
    state = read_state()

    return render_template(
        "gradcafe_stats.html",
        stats=stats,
        message=None,
        pulling_data=state["pulling_data"],
        pull_complete=state["pull_complete"]
    )


@bp.route("/refresh", methods=["POST"])
def refresh_gradcafe():
    state = read_state()

    if state["pulling_data"]:
        return redirect(url_for("pages.grad_cafe"))

    write_state(True, False)

    def background_job():
        try:
            refresh()  # scrape + LLM + append (UNCHANGED)
        finally:
            write_state(False, True)

    threading.Thread(target=background_job, daemon=True).start()
    return redirect(url_for("pages.grad_cafe"))


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
            pull_complete=False
        )

    # 🔑 Pull any new LLM rows into DB
    sync_db_from_llm_file()

    stats = get_application_stats()

    # reset completion flag so UI is clean
    write_state(False, False)

    return render_template(
        "gradcafe_stats.html",
        stats=stats,
        message="Analysis updated from LLM data.",
        pulling_data=False,
        pull_complete=False
    )
