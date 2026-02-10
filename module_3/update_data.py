import json
from scrape.llm_hosting.app import _call_llm


def update_data(
    new_data_path="new_applicant_data.json",
    llm_output_path="llm_extend_applicant_data.json",
):
    """
    Runs LLM on newly scraped applicants and appends results
    to llm_extend_applicant_data.json (NDJSON).

    After successful processing, clears new_applicant_data.json.
    """

    print("🔥 update_data() CALLED")

    try:
        with open(new_data_path, "r", encoding="utf-8") as f:
            rows = json.load(f)
    except FileNotFoundError:
        print("No new_applicant_data.json found")
        return 0

    if not rows:
        print("No new records to analyze")
        return 0

    processed = 0

    # Append analyzed rows
    with open(llm_output_path, "a", encoding="utf-8") as out:
        for row in rows:
            program_text = f"{row.get('program_name','')}, {row.get('university','')}"
            result = _call_llm(program_text)

            row["llm-generated-program"] = result.get("standardized_program")
            row["llm-generated-university"] = result.get("standardized_university")

            out.write(json.dumps(row, ensure_ascii=False) + "\n")
            processed += 1

    # ✅ CLEAR staging file AFTER successful processing
    with open(new_data_path, "w", encoding="utf-8") as f:
        json.dump([], f, indent=2)

    print(f"LLM analysis complete; processed {processed} records")
    print("🧹 new_applicant_data.json cleared")

    return processed
