import json
import subprocess

from scrape.llm_hosting.app import _call_llm


def run_pipeline():
    # 1) Run scraper (main.py) inside scrape folder
    subprocess.run(["python", "scrape/main.py"], check=True)

    # 2) Load scraped data
    with open("../module_3/applicant_data.json", "r", encoding="utf-8") as f:
        rows = json.load(f)

    # 3) Run LLM for each row
    with open("../module_3/llm_extend_applicant_data.json", "w", encoding="utf-8") as out:
        for row in rows:
            program_text = f"{row.get('program_name','')}, {row.get('university','')}"
            result = _call_llm(program_text)

            row["llm-generated-program"] = result["standardized_program"]
            row["llm-generated-university"] = result["standardized_university"]

            out.write(json.dumps(row, ensure_ascii=False) + "\n")
            out.flush()


if __name__ == "__main__":
    run_pipeline()
