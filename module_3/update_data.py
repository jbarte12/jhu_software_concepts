# Import JSON utilities
import json

# Import LLM helper
from scrape.llm_hosting.app import _call_llm


# Run LLM on new applicant data and append results
def update_data(input_file="new_applicant_data.json"):
    # Open the newly scraped applicant data
    with open(input_file, "r", encoding="utf-8") as f:
        rows = json.load(f)

    # Append enriched rows to the master LLM file
    with open("llm_extend_applicant_data.json", "a", encoding="utf-8") as out:
        for row in rows:
            program_text = (
                f"{row.get('program_name','')}, "
                f"{row.get('university','')}"
            )

            result = _call_llm(program_text)

            row["llm-generated-program"] = result["standardized_program"]
            row["llm-generated-university"] = result["standardized_university"]

            out.write(json.dumps(row, ensure_ascii=False) + "\n")

    print(f"LLM processed and appended {len(rows)} records")
