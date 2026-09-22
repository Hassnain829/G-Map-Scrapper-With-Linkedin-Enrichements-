from __future__ import annotations

import argparse
import csv
import shutil
from pathlib import Path
from typing import Dict, List

from lead_enricher import (
    ENRICHMENT_VERSION,
    best_email,
    clean_domain,
    is_usable_email,
)


EMAIL_LIST_FIELDS = [
    "Decision Maker Work Emails",
    "Decision Maker Personal Emails",
    "Decision Maker All Emails",
    "Website Emails",
    "Social Emails",
]


def split_emails(value: str) -> List[str]:
    normalized = (value or "").replace(";", ",")
    output: List[str] = []
    seen = set()

    for item in normalized.split(","):
        email = item.strip().lower()
        if not email or email in seen or not is_usable_email(email):
            continue
        seen.add(email)
        output.append(email)

    return output


def repair_row(row: Dict[str, str]) -> bool:
    changed = False

    for field in EMAIL_LIST_FIELDS:
        cleaned = split_emails(row.get(field, ""))
        new_value = ", ".join(cleaned)
        if new_value != (row.get(field, "") or ""):
            row[field] = new_value
            changed = True

    current_email = (row.get("Email") or "").strip().lower()
    decision_email = (row.get("Decision Maker Email") or "").strip().lower()

    bad_current = bool(current_email and not is_usable_email(current_email))
    bad_decision = bool(decision_email and not is_usable_email(decision_email))

    if not (bad_current or bad_decision):
        return changed

    if bad_decision:
        row["Decision Maker Email"] = ""
        row["Decision Maker Email Type"] = ""
        row["ContactOut Email Status"] = ""
        changed = True

    website_emails = split_emails(row.get("Website Emails", ""))
    social_emails = split_emails(row.get("Social Emails", ""))
    domain = clean_domain(row.get("Website", ""))

    website_email, website_score, website_source = best_email(
        website_emails,
        domain,
    )
    social_email, social_score, _ = best_email(
        social_emails,
        domain,
    )

    choices = []
    if website_email and website_source != "Third-Party Website Email":
        choices.append((website_score, website_email, website_source))
    if social_email:
        choices.append((social_score, social_email, "Social Public Email"))

    if choices:
        choices.sort(reverse=True)
        score, email, source = choices[0]
        row["Email"] = email
        row["Email Source"] = source
        row["Confidence Score"] = str(score)
    else:
        row["Email"] = ""
        row["Email Source"] = "Not Found"
        row["Confidence Score"] = "0"

    row["Lead Status"] = "Needs Review"
    row["Enrichment Version"] = ENRICHMENT_VERSION

    message = "Removed invalid placeholder ContactOut email"
    old_error = (row.get("Error") or "").strip()
    if message.lower() not in old_error.lower():
        row["Error"] = " | ".join(
            part for part in [old_error, message] if part
        )[:1500]

    return True


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Remove placeholder emails such as email2@gmail.com and reuse "
            "already-extracted website emails without spending API credits."
        )
    )
    parser.add_argument(
        "--input",
        default="enriched_leads.csv",
        help="Enriched CSV to repair in place.",
    )
    args = parser.parse_args()

    path = Path(args.input)
    if not path.exists():
        raise FileNotFoundError(f"CSV not found: {path}")

    backup = path.with_suffix(path.suffix + ".before_placeholder_fix.bak")
    shutil.copy2(path, backup)

    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        fieldnames = list(reader.fieldnames or [])
        rows = [dict(row) for row in reader]

    repaired = 0
    for row in rows:
        if repair_row(row):
            repaired += 1

    temp = path.with_suffix(path.suffix + ".tmp")
    with temp.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=fieldnames,
            extrasaction="ignore",
        )
        writer.writeheader()
        writer.writerows(rows)

    temp.replace(path)

    print(f"Repaired rows: {repaired}")
    print(f"Updated CSV: {path}")
    print(f"Backup: {backup}")


if __name__ == "__main__":
    main()
