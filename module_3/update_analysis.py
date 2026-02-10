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
