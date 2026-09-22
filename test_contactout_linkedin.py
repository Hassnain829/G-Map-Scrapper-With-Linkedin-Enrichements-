from __future__ import annotations

import argparse
import json
from typing import Any

from dotenv import load_dotenv

from provider_clients import ContactOutClient, extract_contactout_emails


load_dotenv()


def mask_payload(value: Any) -> Any:
    """Mask likely secrets while keeping ContactOut response shape useful."""
    if isinstance(value, dict):
        output = {}
        for k, v in value.items():
            key = str(k)
            if key.lower() in {"token", "api_key", "authorization"}:
                continue
            output[key] = mask_payload(v)
        return output
    if isinstance(value, list):
        return [mask_payload(v) for v in value]
    return value


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Test ContactOut LinkedIn email lookup for one profile."
    )
    parser.add_argument("linkedin_url")
    parser.add_argument("--name", default="")
    parser.add_argument("--company", default="")
    parser.add_argument("--domain", default="")
    parser.add_argument("--stats", action="store_true", help="Also check ContactOut API stats/credits.")
    parser.add_argument("--raw", action="store_true")
    args = parser.parse_args()

    client = ContactOutClient()
    if not client.configured:
        raise SystemExit("CONTACTOUT_API_KEY missing in .env")

    print("ContactOut token loaded: yes")
    print("Authorization header enabled: yes")
    print("Profile:", args.linkedin_url)

    if args.stats:
        stats = client.api_stats()
        print("\nAPI stats:")
        print("ok:", stats.ok, "status_code:", stats.status_code, "error:", (stats.error or "")[:180])
        if args.raw:
            print(json.dumps(mask_payload(stats.data), indent=2, ensure_ascii=False)[:8000])

    result = client.linkedin_profile_emails(
        args.linkedin_url,
        full_name=args.name,
        company_name=args.company,
        company_domain=args.domain,
    )
    parsed = extract_contactout_emails(result)

    print("\nLookup result:")
    print("ok:", result.ok)
    print("status_code:", result.status_code)
    if result.error:
        print("error:", result.error[:300])

    print("\nAttempts:")
    attempts = []
    if isinstance(result.data, dict):
        attempts = result.data.get("attempts") or []
    for item in attempts:
        if not isinstance(item, dict):
            continue
        availability = item.get("availability") if isinstance(item.get("availability"), dict) else {}
        extra = ""
        if availability:
            extra = " availability=" + ",".join(f"{k}:{availability.get(k)}" for k in sorted(availability.keys()))
        print(
            f"- {item.get('name')}: ok={item.get('ok')} "
            f"status={item.get('status_code')} emails={item.get('emails')} "
            f"error={item.get('error') or ''}{extra}"
        )

    print("\nEmails:")
    print("work:", ", ".join(parsed["work_emails"]) or "none")
    print("personal:", ", ".join(parsed["personal_emails"]) or "none")
    print("other/combined:", ", ".join(parsed["other_emails"]) or "none")
    print("all:", ", ".join(parsed["all_emails"]) or "none")
    print("primary:", parsed["primary_email"] or "none", parsed["primary_type"] or "")

    if attempts and not parsed["all_emails"]:
        status_attempts = [a for a in attempts if isinstance(a, dict) and str(a.get("name", "")).endswith("email_status")]
        print("\nInterpretation:")
        if status_attempts:
            print("- Status checker results are shown above as availability=email:True/False.")
        print("- If all contact-info attempts are 200 but emails=0, the ContactOut API returned no address for this profile.")
        print("- If the Chrome extension shows an email for the same profile, that is an extension/API data-match difference, not a CSV column overwrite.")

    if args.raw:
        print("\nMasked merged response:")
        print(json.dumps(mask_payload(result.data), indent=2, ensure_ascii=False)[:16000])


if __name__ == "__main__":
    main()
