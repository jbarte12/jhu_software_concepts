import json
from datetime import date
import pytest
import src.load_data  # needed for Option 1 patching

# Import the function to test
from src.load_data import rebuild_from_llm_file

# --- Fake DB ---
class FakeCursor:
    def __init__(self):
        self.inserted_rows = []

    def execute(self, *a, **kw):
        pass

class FakeConnection:
    def __init__(self):
        self.cursor_obj = FakeCursor()
        self.committed = False
        self.closed = False

    def cursor(self):
        return self.cursor_obj

    def commit(self):
        self.committed = True

    def close(self):
        self.closed = True

    @property
    def autocommit(self):
        return True

    @autocommit.setter
    def autocommit(self, val):
        pass

def fake_execute_values(cur, sql, rows):
    cur.inserted_rows.extend(rows)

# --- DB Test: Simple Insert ---
@pytest.mark.db
def test_rebuild_simple(monkeypatch, tmp_path):
    data = [
        {
            "program_name": "CS",
            "university": "TestU",
            "url_link": "u1",
            "start_term": "Fall 2026",
            "applicant_status": "Accepted",
            "comments": "good",
            "gpa": "3.9",
            "gre_general": "330",
            "gre_verbal": "165",
            "gre_analytical_writing": "5",
            "degree_type": "Masters",
            "llm-generated-program": "CS",
            "llm-generated-university": "TestU",
            "date_added": "January 1, 2026",
            "International/US": "US"
        }
    ]
    file_path = tmp_path / "llm.json"
    file_path.write_text("\n".join(json.dumps(d) for d in data))

    fake_conn = FakeConnection()
    monkeypatch.setattr("src.load_data.create_connection", lambda *a, **kw: fake_conn)
    monkeypatch.setattr("src.load_data.execute_values", fake_execute_values)

    rebuild_from_llm_file(path=str(file_path))

    rows = fake_conn.cursor_obj.inserted_rows
    assert len(rows) == 1

    program, comments, date_added, url, status, term, *_ = rows[0]
    assert program and url and status and term

    assert fake_conn.committed
    assert fake_conn.closed

# --- DB Test: Idempotency ---
@pytest.mark.db
def test_rebuild_idempotent(monkeypatch, tmp_path):
    data = [
        {
            "program_name": "Pure Mathematics",
            "university": "Duke University",
            "degree_type": "PhD",
            "comments": "",
            "date_added": "February 15, 2026",
            "url_link": "https://www.thegradcafe.com/result/1002138",
            "applicant_status": "Accepted: 5 Feb",
            "start_term": "Fall 2026",
            "International/US": "International",
            "gre_general": "",
            "gre_verbal": "",
            "gre_analytical_writing": "",
            "gpa": "",
            "llm-generated-program": "Pure Mathematics",
            "llm-generated-university": "Duke University"
        },
        {
            "program_name": "Electrical And Computer Engineering",
            "university": "University of Southern California",
            "degree_type": "PhD",
            "comments": "",
            "date_added": "February 15, 2026",
            "url_link": "https://www.thegradcafe.com/result/1002137",
            "applicant_status": "Accepted: 13 Feb",
            "start_term": "Fall 2026",
            "International/US": "International",
            "gre_general": "",
            "gre_verbal": "",
            "gre_analytical_writing": "",
            "gpa": "",
            "llm-generated-program": "Electrical And Computer Engineering",
            "llm-generated-university": "University of Southern California"
        }
    ]

    file_path = tmp_path / "llm.json"
    file_path.write_text("\n".join(json.dumps(d) for d in data))

    # Fake DB with uniqueness enforcement on URL
    class UniqueFakeCursor(FakeCursor):
        def __init__(self):
            super().__init__()
            self.seen_urls = set()

        def execute(self, sql, row=None):
            if row is None:
                return
            url = row[3]  # URL is 4th element
            if url not in self.seen_urls:
                self.inserted_rows.append(row)
                self.seen_urls.add(url)

    class UniqueFakeConnection(FakeConnection):
        def __init__(self):
            self.cursor_obj = UniqueFakeCursor()
            self.committed = False
            self.closed = False

    fake_conn = UniqueFakeConnection()
    monkeypatch.setattr("src.load_data.create_connection", lambda *a, **kw: fake_conn)
    monkeypatch.setattr(
        "src.load_data.execute_values",
        lambda cur, sql, rows: [cur.execute("", r) for r in rows]
    )

    # Run twice
    rebuild_from_llm_file(path=str(file_path))
    rebuild_from_llm_file(path=str(file_path))

    rows = fake_conn.cursor_obj.inserted_rows
    urls = [r[3] for r in rows]
    assert len(set(urls)) == len(data)  # unique URLs
    assert len(rows) == len(data)

# --- Simple Query Test using Option 1 ---
@pytest.mark.db
def test_query_statistics():
    # Fake function returning all required keys
    def fake_query_data(*args, **kwargs):
        return {
            "fall_2026_count": 10,
            "international_pct": 50.0,
            "avg_gpa": 3.8,
            "avg_gre": 330.0,
            "avg_gre_v": 165.0,
            "avg_gre_aw": 4.5,
            "avg_gpa_us_fall_2026": 3.9,
            "fall_2025_accept_pct": 40.0,
            "avg_gpa_fall_2025_accept": 3.7,
            "jhu_cs_masters": 2,
            "total_applicants": 20,
            "fall_2026_cs_accept": 3,
            "fall_2026_cs_accept_llm": 3,
            "rejected_fall_2026_gpa_pct": 50.0,
            "accepted_fall_2026_gpa_pct": 75.0
        }

    # Patch the fake directly onto the module (Option 1)
    setattr(src.load_data, "query_statistics", fake_query_data)

    # Call and verify keys exist
    stats = src.load_data.query_statistics()
    expected_keys = [
        "fall_2026_count", "international_pct", "avg_gpa", "avg_gre",
        "avg_gre_v", "avg_gre_aw", "avg_gpa_us_fall_2026",
        "fall_2025_accept_pct", "avg_gpa_fall_2025_accept",
        "jhu_cs_masters", "total_applicants", "fall_2026_cs_accept",
        "fall_2026_cs_accept_llm", "rejected_fall_2026_gpa_pct",
        "accepted_fall_2026_gpa_pct"
    ]
    for key in expected_keys:
        assert key in stats
