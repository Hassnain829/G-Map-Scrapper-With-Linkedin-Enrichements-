from __future__ import annotations

import csv
import tempfile
from pathlib import Path

from lead_enricher import (
    extract_emails,
    extract_phone_numbers,
    facebook_candidate_urls,
    first_social_url_from_text,
    google_person_result_matches_company,
    is_usable_email,
    normalize_facebook_url,
    owner_founder_title,
    person_name_looks_like_company,
)
from main import slugify_search_term
from pipeline_state import PipelineState
from provider_clients import ApiResult, extract_contactout_emails


def main() -> None:
    assert owner_founder_title(
        "Chief Executive Officer"
    ) == "CEO"
    assert owner_founder_title(
        "Co-Founder & CEO"
    ) == "Co-Founder"
    assert person_name_looks_like_company(
        "The SkySolar",
        "Smart Sky Solar",
    )
    assert not is_usable_email(
        "email1@example.com"
    )
    assert not is_usable_email(
        "email2@gmail.com"
    )
    assert not is_usable_email(
        "user@domain.com"
    )
    assert not is_usable_email(
        "ecom-swiper@11.0.5.js"
    )
    assert not is_usable_email(
        "abc@sentry.wixpress.com"
    )
    assert slugify_search_term(
        "Solar company in Chicago IL"
    ) == (
        "solar_company_in_chicago_il"
    )
    assert extract_emails(
        "Contact: owner\\u0040mybizsolar.com"
    ) == ["owner@mybizsolar.com"]
    assert normalize_facebook_url(
        "https://www.facebook.com/ExampleBiz/posts/12345"
    ) == "https://www.facebook.com/ExampleBiz/"
    assert facebook_candidate_urls(
        "https://www.facebook.com/p/Wisconsin-Auto-Truck-Repair-100063831991379/"
    ) == [
        "https://www.facebook.com/p/Wisconsin-Auto-Truck-Repair-100063831991379/"
    ]
    assert not any(
        "/about" in url.lower()
        for url in facebook_candidate_urls(
            "https://www.facebook.com/ExampleBiz/"
        )
    )
    assert extract_phone_numbers(
        "Call us at (920) 257-4875 or +1 608-271-9009"
    ) == [
        "+1 920-257-4875",
        "+1 608-271-9009",
    ]
    assert first_social_url_from_text(
        "https://example.com | https://www.facebook.com/ExampleBiz/ | x",
        "facebook.com",
    ) == "https://www.facebook.com/ExampleBiz/"
    assert google_person_result_matches_company(
        {
            "title": "Jane Smith - Example Solar | LinkedIn",
            "snippet": "Jane Smith works at Example Solar",
            "url": "https://www.linkedin.com/in/jane-smith",
        },
        "Example Solar",
        "examplesolar.com",
        "",
        [],
    ) is False

    contactout_both = extract_contactout_emails(
        ApiResult(
            ok=True,
            data={
                "profile": {
                    "email": [
                        "owner@company.com",
                        "owner@gmail.com",
                    ],
                    "work_email": [
                        "owner@company.com",
                    ],
                    "personal_email": [
                        "owner@gmail.com",
                    ],
                    "work_email_status": {
                        "owner@company.com": "Verified",
                    },
                }
            },
        )
    )
    assert contactout_both["primary_email"] == "owner@company.com"
    assert contactout_both["primary_type"] == "Work"
    assert contactout_both["personal_emails"] == ["owner@gmail.com"]
    assert len(contactout_both["all_emails"]) == 2

    contactout_placeholder = extract_contactout_emails(
        ApiResult(
            ok=True,
            data={
                "profile": {
                    "personal_email": [
                        "email2@gmail.com",
                    ],
                    "work_email": [
                        "email1@example.com",
                    ],
                }
            },
        )
    )
    assert contactout_placeholder["all_emails"] == []
    assert contactout_placeholder["primary_email"] == ""

    contactout_personal_only = extract_contactout_emails(
        ApiResult(
            ok=True,
            data={
                "data": {
                    "profile": {
                        "personalEmail": "person@yahoo.com",
                    }
                }
            },
        )
    )
    assert contactout_personal_only["primary_email"] == "person@yahoo.com"
    assert contactout_personal_only["primary_type"] == "Personal"

    contactout_search_shape = extract_contactout_emails(
        ApiResult(
            ok=True,
            data={
                "profiles": {
                    "https://linkedin.com/in/test-owner": {
                        "contact_info": {
                            "emails": ["other@company.com"],
                            "work_emails": ["work@company.com"],
                            "personal_emails": ["home@gmail.com"],
                            "work_email_status": {
                                "work@company.com": "Verified",
                            },
                        }
                    }
                }
            },
        )
    )
    assert contactout_search_shape["primary_email"] == "work@company.com"
    assert contactout_search_shape["personal_emails"] == ["home@gmail.com"]
    assert "other@company.com" in contactout_search_shape["all_emails"]

    contactout_batch_shape = extract_contactout_emails(
        ApiResult(
            ok=True,
            data={
                "profiles": {
                    "https://linkedin.com/in/test-owner": [
                        "batch@company.com"
                    ]
                }
            },
        )
    )
    assert contactout_batch_shape["primary_email"] == "batch@company.com"


    with tempfile.TemporaryDirectory() as tmp:
        state_path = str(
            Path(tmp)
            / "state.sqlite3"
        )
        row = {
            "name": "Example Solar",
            "phone": "+1 312-555-0123",
            "city": "Chicago",
            "website_link": (
                "https://example-solar.test"
            ),
        }
        enriched = {
            "Company": "Example Solar",
            "LinkedIn URL": (
                "https://www.linkedin.com/"
                "in/example-owner"
            ),
            "Decision Maker Title": "CEO",
        }

        with PipelineState(
            state_path
        ) as state:
            assert not state.is_known_business(
                row
            )
            state.register_business(
                row
            )
            assert state.is_known_business(
                row
            )
            state.save_enrichment(
                row,
                enriched,
            )
            cached = (
                state.get_cached_enrichment(
                    row
                )
            )
            assert cached
            assert (
                cached[
                    "Decision Maker Title"
                ]
                == "CEO"
            )

    print(
        "Self-test passed."
    )


if __name__ == "__main__":
    main()
