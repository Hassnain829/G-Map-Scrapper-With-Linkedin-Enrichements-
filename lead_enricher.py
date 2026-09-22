from __future__ import annotations

import csv
import json
import base64
import os
import random
import re
import time
from html import unescape
from dataclasses import dataclass, field
from pathlib import Path
from typing import (
    Any,
    Dict,
    Iterable,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
)
from urllib.parse import (
    parse_qs,
    quote_plus,
    unquote,
    urljoin,
    urlparse,
)

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from playwright.sync_api import (
    BrowserContext,
    Page,
    sync_playwright,
)
from rapidfuzz import fuzz

from pipeline_state import PipelineState

from provider_clients import (
    ContactOutClient,
    extract_contactout_emails,
    extract_contactout_profiles,
)


load_dotenv()


ENRICHMENT_VERSION = "2026-08-15-contactout-stable-api-v9"


OUTPUT_FIELDS = [
    "Name",
    "Email",
    "Phone",
    "City",
    "Industry",
    "Website",
    "Company",
    "Business Address",
    "Facebook",
    "Instagram",
    "LinkedIn URL",
    "LinkedIn Profile Name",
    "LinkedIn Company Name",
    "LinkedIn Company URL",
    "Decision Maker Name",
    "Decision Maker Title",
    "Decision Maker Email",
    "Decision Maker Work Emails",
    "Decision Maker Personal Emails",
    "Decision Maker All Emails",
    "Decision Maker Email Type",
    "Website Emails",
    "Social Emails",
    "Facebook Phone",
    "Social Phones",
    "All Emails",
    "ContactOut Email Status",
    "Email Source",
    "Confidence Score",
    "Lead Status",
    "Status",
    "Subject",
    "Sent At",
    "Error",
    "Enrichment Version",
]


EMAIL_RE = re.compile(
    r"[A-Z0-9._%+\-]+@"
    r"[A-Z0-9.\-]+\.[A-Z]{2,}",
    re.I,
)


PHONE_RE = re.compile(
    r"(?:\+?1[\s.\-()]*)?"
    r"(?:\(?\d{3}\)?[\s.\-]*)"
    r"\d{3}[\s.\-]*\d{4}"
    r"(?:\s*(?:ext|extension|x)\s*\d{1,5})?",
    re.I,
)


COMMON_PATHS = [
    "/",
    "/contact",
    "/contact-us",
    "/about",
    "/about-us",
    "/team",
    "/our-team",
    "/staff",
    "/leadership",
    "/management",
    "/doctors",
    "/providers",
]


DECISION_TITLES = [
    "owner",
    "co-owner",
    "founder",
    "co-founder",
    "ceo",
    "chief executive officer",
]


GENERIC_PREFIXES = {
    "info",
    "contact",
    "hello",
    "support",
    "admin",
    "office",
    "appointments",
    "booking",
    "sales",
    "service",
    "customerservice",
    "reception",
}


BAD_EMAIL_DOMAINS = {
    "example.com",
    "sentry.io",
    "wixpress.com",
    "wordpress.org",
    "schema.org",
}


PLACEHOLDER_EMAIL_LOCAL_PARTS = {
    "email",
    "email1",
    "email2",
    "email3",
    "example",
    "test",
    "demo",
    "sample",
    "placeholder",
    "fake",
    "user",
    "username",
    "your",
    "youremail",
    "name",
    "someone",
    "johnsmith",
    "janedoe",
    "noreply",
    "no-reply",
}

PLACEHOLDER_EMAILS = {
    "email1@example.com",
    "email2@gmail.com",
    "test@example.com",
    "demo@example.com",
    "sample@example.com",
    "user@domain.com",
    "your@email.com",
    "youremail@domain.com",
}

PLACEHOLDER_LOCAL_RE = re.compile(
    r"^(?:email|test|demo|sample|example|placeholder|fake|"
    r"user|username|your(?:email)?|name|someone)\d*$",
    re.I,
)

BAD_EMAIL_TLDS = {
    "js",
    "css",
    "json",
    "xml",
    "map",
    "png",
    "jpg",
    "jpeg",
    "gif",
    "svg",
    "webp",
    "ico",
    "woff",
    "woff2",
    "ttf",
    "eot",
}


PLACEHOLDER_PERSON_NAMES = {
    "example person",
    "test person",
    "sample person",
    "placeholder person",
    "john doe",
    "jane doe",
    "test user",
    "demo user",
    "unknown person",
}

PLACEHOLDER_PERSON_TOKENS = {
    "example",
    "sample",
    "placeholder",
    "dummy",
    "unknown",
    "test",
    "demo",
}

COMPANY_GENERIC_WORDS = {
    "the",
    "and",
    "company",
    "co",
    "inc",
    "llc",
    "ltd",
    "group",
    "services",
    "service",
    "spa",
    "salon",
    "clinic",
    "center",
    "centre",
    "chicago",
    "illinois",
    "health",
    "care",
}

DECISION_TEXT_TOKENS = (
    "owner",
    "co-owner",
    "business owner",
    "founder",
    "co-founder",
    "cofounder",
    "ceo",
    "chief executive officer",
)


USER_AGENT = (
    "Mozilla/5.0 "
    "(Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 "
    "(KHTML, like Gecko) "
    "Chrome/124 Safari/537.36"
)


class SkipBusiness(Exception):
    pass


class QuitPipeline(Exception):
    pass


@dataclass
class Candidate:
    name: str = ""
    title: str = ""
    linkedin_url: str = ""
    company_linkedin_url: str = ""
    person_id: str = ""
    email: str = ""
    email_status: str = ""
    email_type: str = ""
    work_emails: List[str] = field(default_factory=list)
    personal_emails: List[str] = field(default_factory=list)
    all_emails: List[str] = field(default_factory=list)
    source: str = ""
    score: int = 0
    raw: Dict[str, Any] = field(
        default_factory=dict
    )


@dataclass
class WebsiteData:
    emails: List[str] = field(
        default_factory=list
    )
    candidates: List[Candidate] = field(
        default_factory=list
    )
    linkedin_person_urls: List[str] = field(
        default_factory=list
    )
    linkedin_company_urls: List[str] = field(
        default_factory=list
    )
    facebook: str = ""
    instagram: str = ""


def safe(value: object) -> str:
    return str(value or "").strip()


def env_bool(
    name: str,
    default: bool,
) -> bool:
    value = os.getenv(name)

    if value is None:
        return default

    return value.strip().lower() in {
        "1",
        "true",
        "yes",
        "y",
        "on",
    }


def normalize_spaces(
    value: str,
) -> str:
    return re.sub(
        r"\s+",
        " ",
        safe(value),
    ).strip()


def clean_person_name(value: str) -> str:
    name = normalize_spaces(value)

    # Google AI Overview anchors often look like:
    # "Christopher Gersch LinkedIn profile" or
    # "Christopher Gersch's LinkedIn Profile".
    name = re.sub(
        r"\s*(?:['’]s)?\s+LinkedIn(?:\s+Profile)?[.]?$",
        "",
        name,
        flags=re.I,
    )

    # Ignore LinkedIn post labels; they are not profile titles.
    name = re.sub(
        r"\s*(?:['’]s)?\s+Post\s*(?:\||[-–—])\s*LinkedIn.*$",
        "",
        name,
        flags=re.I,
    )

    name = re.sub(
        r"\s*(?:\||[-–—])\s*LinkedIn.*$",
        "",
        name,
        flags=re.I,
    )

    name = re.sub(
        r"\s+(?:Owner|Co-Owner|Founder|Co-Founder|CEO|Chief Executive Officer)\b.*$",
        "",
        name,
        flags=re.I,
    )

    return name.strip(" -–—|,:;.")

def is_usable_person_name(value: str) -> bool:
    name = clean_person_name(value)
    normalized = normalize_match_text(name)

    if not normalized or normalized in PLACEHOLDER_PERSON_NAMES:
        return False

    words = normalized.split()

    if not 2 <= len(words) <= 5:
        return False

    if any(word in PLACEHOLDER_PERSON_TOKENS for word in words):
        return False

    if any(
        token in normalized
        for token in [
            "linkedin member",
            "linkedin user",
            "business owner",
            "company owner",
            "owner founder",
        ]
    ):
        return False

    return all(re.fullmatch(r"[a-z][a-z'’-]*", word) for word in words)


def person_name_looks_like_company(
    person_name: str,
    company: str,
) -> bool:
    """Reject LinkedIn company-like labels such as 'The SkySolar'."""
    person = normalize_match_text(
        clean_person_name(person_name)
    )
    business = normalize_match_text(
        company
    )

    if not person or not business:
        return False

    if person.startswith("the "):
        return True

    if fuzz.ratio(person, business) >= 72:
        return True

    if fuzz.token_set_ratio(
        person,
        business,
    ) >= 88:
        return True

    person_tokens = set(person.split())
    company_tokens = set(business.split())

    return bool(
        person_tokens
        and person_tokens.issubset(
            company_tokens | {"the"}
        )
    )


def owner_founder_title(text: str) -> str:
    """Normalize an accepted decision-maker role."""
    normalized = normalize_spaces(text).lower()

    if "co-founder" in normalized or "cofounder" in normalized:
        return "Co-Founder"
    if "founder" in normalized:
        return "Founder"
    if "co-owner" in normalized:
        return "Co-Owner"
    if "business owner" in normalized or re.search(r"\bowner\b", normalized):
        return "Owner"
    if (
        "chief executive officer" in normalized
        or re.search(r"\bceo\b", normalized)
    ):
        return "CEO"

    return ""


def has_owner_founder_role(text: str) -> bool:
    return bool(owner_founder_title(text))


def person_name_from_linkedin_url(url: str) -> str:
    linked = normalize_linkedin_url(url)

    if "/in/" not in linked.lower():
        return ""

    slug = urlparse(linked).path.split("/in/", 1)[-1].split("/", 1)[0]
    slug = re.sub(r"[-_]?\d{5,}[a-z0-9]*$", "", slug, flags=re.I)
    slug = re.sub(r"[-_]+", " ", slug)
    candidate = " ".join(part.capitalize() for part in slug.split())

    return candidate if is_usable_person_name(candidate) else ""


def normalize_url(
    url: str,
) -> str:
    url = safe(url)

    if not url:
        return ""

    if url.startswith("//"):
        return "https:" + url

    if not url.startswith(
        ("http://", "https://")
    ):
        return "https://" + url

    return url


def clean_domain(
    url_or_domain: str,
) -> str:
    raw = safe(url_or_domain)

    if not raw:
        return ""

    if "://" not in raw:
        raw = "https://" + raw

    parsed = urlparse(raw)

    return (
        parsed.netloc.lower()
        .replace("www.", "")
        .split(":")[0]
    )


def normalize_linkedin_url(url: str) -> str:
    url = normalize_url(url)

    if "linkedin.com/" not in url.lower():
        return ""

    parsed = urlparse(url)
    path = parsed.path.rstrip("/")
    lower = path.lower()

    for marker in ["/in/", "/pub/", "/company/"]:
        if marker not in lower:
            continue

        # Keep only the canonical profile/company slug. Google sometimes
        # exposes /company/<id>/admin, /people, /about, etc.
        before, after = path.split(marker, 1)
        slug = after.split("/", 1)[0].strip()
        if not slug:
            return ""

        return f"https://www.linkedin.com{marker}{slug}"

    return ""

def business_key(
    row: Dict[str, str],
) -> str:
    value = "|".join(
        [
            safe(
                row.get("name")
                or row.get("Company")
            ).lower(),

            safe(
                row.get("phone")
                or row.get("Phone")
            ).lower(),

            clean_domain(
                row.get("website_link")
                or row.get("Website")
                or ""
            ),
        ]
    )

    return re.sub(
        r"[^a-z0-9|]+",
        "",
        value,
    )


def extract_city_from_search_term(
    search_term: str,
) -> str:
    value = normalize_spaces(search_term)

    match = re.search(
        r"\bin\s+([A-Za-z .'-]+?)"
        r"(?:,\s*|\s+)([A-Z]{2})\b",
        value,
        re.I,
    )

    if match:
        return normalize_spaces(
            match.group(1)
        ).title()

    return ""


def extract_city(
    address: str,
    search_term: str = "",
) -> str:
    parts = [
        normalize_spaces(part)
        for part in safe(address).split(",")
        if normalize_spaces(part)
    ]

    if (
        parts
        and parts[-1].lower()
        in {
            "united states",
            "usa",
            "us",
        }
    ):
        parts.pop()

    # Standard US address:
    # street, city, IL 60629
    if len(parts) >= 3:
        state_zip = parts[-1]

        if re.search(
            r"\b[A-Z]{2}\s+\d{5}"
            r"(?:-\d{4})?\b",
            state_zip,
            re.I,
        ):
            return parts[-2]

    # Sometimes state/ZIP is joined with the city.
    if len(parts) >= 2:
        match = re.match(
            r"(.+?)\s+[A-Z]{2}\s+"
            r"\d{5}(?:-\d{4})?$",
            parts[-1],
            re.I,
        )

        if match:
            return normalize_spaces(
                match.group(1)
            )

    return extract_city_from_search_term(
        search_term
    )


def split_name(
    full_name: str,
) -> Tuple[str, str]:
    parts = normalize_spaces(
        full_name
    ).split()

    if not parts:
        return "", ""

    if len(parts) == 1:
        return parts[0], ""

    return (
        parts[0],
        " ".join(parts[1:]),
    )



def normalize_match_text(value: str) -> str:
    return re.sub(
        r"[^a-z0-9]+",
        " ",
        normalize_spaces(value).lower(),
    ).strip()


def is_usable_email(value: str) -> bool:
    email = safe(value).lower().strip(
        ".,;:()[]{}<>\\\"'"
    )

    if not re.fullmatch(
        r"[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,}",
        email,
        flags=re.I,
    ):
        return False

    local_part, domain = email.rsplit("@", 1)
    domain_labels = [
        label
        for label in domain.split(".")
        if label
    ]
    tld = domain_labels[-1] if domain_labels else ""

    if email in PLACEHOLDER_EMAILS:
        return False

    if (
        local_part in PLACEHOLDER_EMAIL_LOCAL_PARTS
        or PLACEHOLDER_LOCAL_RE.fullmatch(local_part)
    ):
        return False

    if any(
        domain == blocked
        or domain.endswith("." + blocked)
        for blocked in BAD_EMAIL_DOMAINS
    ):
        return False

    if domain in {
        "domain.com",
        "yourdomain.com",
        "mailinator.com",
    }:
        return False

    if tld in BAD_EMAIL_TLDS:
        return False

    if (
        not domain_labels
        or any(label.isdigit() for label in domain_labels[:-1])
        or domain.startswith(("localhost", "127.0.0.1"))
    ):
        return False

    if (
        "example" in domain
        or "placeholder" in email
        or "fake@" in email
    ):
        return False

    return True


def linkedin_company_slug(
    company_linkedin_url: str,
) -> str:
    url = normalize_linkedin_url(
        company_linkedin_url
    )

    if "/company/" not in url.lower():
        return ""

    path = urlparse(url).path.rstrip("/")
    slug = path.split("/company/", 1)[-1]

    return normalize_spaces(
        re.sub(r"[-_]+", " ", slug)
    )


def distinctive_company_tokens(
    company: str,
    domain: str = "",
    company_linkedin_url: str = "",
) -> Set[str]:
    values = [
        company,
        linkedin_company_slug(
            company_linkedin_url
        ),
    ]

    if domain:
        labels = [
            label
            for label in clean_domain(domain).split(".")
            if label
        ]

        if len(labels) >= 2:
            values.append(labels[-2])

        values.extend(labels[:-1])

    output: Set[str] = set()

    for value in values:
        for token in normalize_match_text(value).split():
            if (
                len(token) >= 4
                and token not in COMPANY_GENERIC_WORDS
            ):
                output.add(token)

    return output


def company_evidence_score(
    text: str,
    company: str,
    domain: str = "",
    company_linkedin_url: str = "",
) -> int:
    normalized = normalize_match_text(text)

    if not normalized:
        return 0

    score = 0
    company_normalized = normalize_match_text(
        company
    )

    if (
        company_normalized
        and company_normalized in normalized
    ):
        score = max(score, 100)

    slug = normalize_match_text(
        linkedin_company_slug(
            company_linkedin_url
        )
    )

    if slug and slug in normalized:
        score = max(score, 92)

    tokens = distinctive_company_tokens(
        company,
        domain,
        company_linkedin_url,
    )

    hits = sum(
        1
        for token in tokens
        if token in normalized.split()
        or token in normalized
    )

    if tokens:
        if hits == len(tokens):
            score = max(score, 85)
        elif hits >= 2:
            score = max(score, 72)
        elif hits == 1 and len(tokens) == 1:
            score = max(score, 62)

    if (
        company_normalized
        and fuzz.partial_ratio(
            company_normalized,
            normalized,
        ) >= 88
    ):
        score = max(score, 78)

    return score


def has_decision_role(text: str) -> bool:
    # The pipeline accepts owners, founders, and CEOs.
    return has_owner_founder_role(text)


def candidate_matches_company(
    candidate: Candidate,
    company: str,
    domain: str,
    city: str,
    company_linkedin_url: str = "",
) -> bool:
    source = candidate.source.lower()

    if "business website linkedin" in source:
        return True

    raw_text = json.dumps(
        candidate.raw,
        ensure_ascii=False,
    )

    evidence = company_evidence_score(
        raw_text,
        company,
        domain,
        company_linkedin_url
        or candidate.company_linkedin_url,
    )

    if evidence >= 62:
        return True

    if source.startswith("website:"):
        return True

    return False


def google_person_result_matches_company(
    result: Dict[str, str],
    company: str,
    domain: str,
    company_linkedin_url: str,
    ai_hints: Sequence[Candidate] = (),
) -> bool:
    text = (
        f"{result.get('title', '')} "
        f"{result.get('snippet', '')}"
    )

    if (
        "linkedin.com/in/"
        not in safe(result.get("url")).lower()
    ):
        return False

    title = normalize_spaces(result.get("title", ""))
    result_name = clean_person_name(
        re.split(
            r"\s+[-–—|]\s+",
            title,
            maxsplit=1,
        )[0]
    )

    if not is_usable_person_name(result_name):
        result_name = person_name_from_linkedin_url(
            result.get("url", "")
        )

    # An AI hint is only created after the overview itself strongly matches
    # the current company. Therefore an exact name match can rescue a direct
    # LinkedIn profile whose Google snippet does not repeat the company name.
    for hint in ai_hints:
        if (
            is_usable_person_name(hint.name)
            and owner_founder_title(hint.title)
            and fuzz.token_sort_ratio(
                normalize_match_text(result_name),
                normalize_match_text(hint.name),
            ) >= 88
        ):
            return True

    if company_evidence_score(
        text,
        company,
        domain,
        company_linkedin_url,
    ) < 62:
        return False

    return has_owner_founder_role(text)

def google_company_result_matches_company(
    result: Dict[str, str],
    company: str,
    domain: str,
) -> bool:
    text = (
        f"{result.get('title', '')} "
        f"{result.get('snippet', '')} "
        f"{result.get('url', '')}"
    )

    return company_evidence_score(
        text,
        company,
        domain,
        result.get("url", ""),
    ) >= 62


def extract_emails(
    text: str,
) -> List[str]:
    if not text:
        return []

    cleaned = unescape(str(text))
    # Social pages and JSON-heavy websites often encode emails as
    # name\u0040domain.com, name%40domain.com, or name&#64;domain.com.
    # Decode the common safe forms before running the regex.
    cleaned = re.sub(r"\\u0*40", "@", cleaned, flags=re.I)
    cleaned = re.sub(r"\\x40", "@", cleaned, flags=re.I)
    cleaned = re.sub(r"%40", "@", cleaned, flags=re.I)
    cleaned = re.sub(r"&#0*64;|&commat;", "@", cleaned, flags=re.I)
    cleaned = re.sub(r"\\u0*2e", ".", cleaned, flags=re.I)
    cleaned = re.sub(r"\\x2e", ".", cleaned, flags=re.I)

    replacements = [
        (
            r"\s*\[\s*at\s*\]\s*",
            "@",
        ),
        (
            r"\s*\(\s*at\s*\)\s*",
            "@",
        ),
        (
            r"\s+at\s+",
            "@",
        ),
        (
            r"\s*\[\s*dot\s*\]\s*",
            ".",
        ),
        (
            r"\s*\(\s*dot\s*\)\s*",
            ".",
        ),
        (
            r"\s+dot\s+",
            ".",
        ),
    ]

    for pattern, replacement in replacements:
        cleaned = re.sub(
            pattern,
            replacement,
            cleaned,
            flags=re.I,
        )

    output: Set[str] = set()

    for email in EMAIL_RE.findall(cleaned):
        email = email.lower().strip(
            ".,;:()[]{}<>\"'"
        )

        if not is_usable_email(email):
            continue

        if email.endswith(
            (
                ".png",
                ".jpg",
                ".jpeg",
                ".gif",
                ".webp",
                ".svg",
            )
        ):
            continue

        output.add(email)

    return sorted(output)


def normalize_phone_number(value: str) -> str:
    """Normalize US/Canada-style public phone numbers to +1 XXX-XXX-XXXX."""
    raw = normalize_spaces(value)
    if not raw:
        return ""

    extension = ""
    ext_match = re.search(
        r"(?:ext|extension|x)\s*(\d{1,5})\b",
        raw,
        flags=re.I,
    )
    if ext_match:
        extension = " x" + ext_match.group(1)

    digits = re.sub(r"\D+", "", raw)

    # Drop extension digits from the main number when present.
    if extension and ext_match:
        ext_digits = ext_match.group(1)
        if digits.endswith(ext_digits):
            digits = digits[: -len(ext_digits)]

    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]

    if len(digits) != 10:
        return ""

    if digits.startswith(("000", "111")):
        return ""

    return f"+1 {digits[:3]}-{digits[3:6]}-{digits[6:]}{extension}"


def extract_phone_numbers(text: str) -> List[str]:
    """Extract likely public US/Canada phone numbers from text/HTML."""
    if not text:
        return []

    cleaned = unescape(str(text))
    cleaned = re.sub(r"\\u0*2b", "+", cleaned, flags=re.I)
    cleaned = re.sub(r"%2b", "+", cleaned, flags=re.I)

    found: List[str] = []
    for match in PHONE_RE.finditer(cleaned):
        normalized = normalize_phone_number(match.group(0))
        if normalized and normalized not in found:
            found.append(normalized)

    return found


def email_score(
    email: str,
    business_domain: str = "",
) -> Tuple[int, str]:
    email = safe(email).lower()

    if not is_usable_email(email):
        return 0, "Invalid"

    prefix, domain = email.split(
        "@",
        1,
    )

    free_domains = {
        "gmail.com",
        "yahoo.com",
        "outlook.com",
        "hotmail.com",
        "icloud.com",
        "aol.com",
        "comcast.net",
        "proton.me",
        "protonmail.com",
    }

    business_domain = clean_domain(
        business_domain
    )
    same_business_domain = bool(
        business_domain
        and (
            domain == business_domain
            or domain.endswith(
                "." + business_domain
            )
        )
    )

    business_stem = ""
    if business_domain:
        business_stem = re.sub(
            r"[^a-z0-9]+",
            "",
            business_domain.split(".")[0],
        )

    local_normalized = re.sub(
        r"[^a-z0-9]+",
        "",
        prefix,
    )

    score = 30
    source = "Public Business Email"

    if same_business_domain:
        score += 50

    if prefix in GENERIC_PREFIXES:
        score += 15
        source = "Website Generic Email"

    elif any(
        word in prefix
        for word in [
            "owner",
            "founder",
            "ceo",
            "president",
        ]
    ):
        score += 55
        source = "Website Decision Email"

    elif any(
        separator in prefix
        for separator in [
            ".",
            "_",
            "-",
        ]
    ):
        score += 40
        source = "Website Personal Work Email"

    elif len(prefix) >= 3:
        score += 35
        source = "Website Personal Work Email"

    if domain in free_domains:
        score -= 5
        source = "Public Personal/Business Email"

        # Prefer a company-branded Gmail/Yahoo address over unrelated vendor
        # emails embedded in website templates or footer scripts.
        if (
            len(business_stem) >= 5
            and (
                business_stem in local_normalized
                or local_normalized in business_stem
            )
        ):
            score += 35

    elif (
        business_domain
        and not same_business_domain
    ):
        # External domains found in HTML are often web designers, plugins,
        # agencies, or template authors. Keep them as a last resort only.
        score -= 25
        source = "Third-Party Website Email"

    return (
        min(max(score, 0), 100),
        source,
    )


def best_email(
    emails: Iterable[str],
    business_domain: str = "",
) -> Tuple[str, int, str]:
    scored = []

    for email in set(emails):
        score, source = email_score(
            email,
            business_domain,
        )

        scored.append(
            (
                score,
                email,
                source,
            )
        )

    if not scored:
        return "", 0, ""

    scored.sort(reverse=True)

    score, email, source = scored[0]

    return email, score, source


def title_score(title: str) -> int:
    normalized = normalize_spaces(title).lower()

    if "co-founder" in normalized or "cofounder" in normalized:
        return 100
    if "founder" in normalized:
        return 98
    if "co-owner" in normalized:
        return 96
    if "business owner" in normalized or re.search(r"\bowner\b", normalized):
        return 94
    if (
        "chief executive officer" in normalized
        or re.search(r"\bceo\b", normalized)
    ):
        return 90

    return 0


def score_candidate(
    candidate: Candidate,
    company: str,
    domain: str,
    city: str,
    company_linkedin_url: str = "",
) -> int:
    score = title_score(
        candidate.title
    )

    raw_text = json.dumps(
        candidate.raw,
        ensure_ascii=False,
    )

    evidence = company_evidence_score(
        raw_text,
        company,
        domain,
        company_linkedin_url
        or candidate.company_linkedin_url,
    )

    score += min(evidence // 3, 35)

    if candidate.linkedin_url:
        score += 12

    if (
        candidate.email
        and is_usable_email(candidate.email)
    ):
        score += 25

    if (
        city
        and city.lower() in raw_text.lower()
    ):
        score += 8

    return score


def dedupe_candidates(
    candidates: Iterable[Candidate],
) -> List[Candidate]:
    output: Dict[str, Candidate] = {}

    for candidate in candidates:
        key = (
            candidate.linkedin_url.lower()
            or re.sub(
                r"[^a-z0-9]+",
                "",
                (
                    candidate.name
                    + "|"
                    + candidate.title
                ).lower(),
            )
        )

        if not key:
            continue

        current = output.get(key)

        if (
            current is None
            or candidate.score > current.score
        ):
            output[key] = candidate

    return sorted(
        output.values(),
        key=lambda item: item.score,
        reverse=True,
    )



def candidate_from_contactout(
    profile: Dict[str, Any],
    company: str,
    domain: str,
    city: str,
) -> Candidate:
    company_data = profile.get("company") or {}

    raw_name = safe(
        profile.get("full_name")
        or profile.get("name")
    )
    name = clean_person_name(raw_name)

    if not is_usable_person_name(name):
        name = ""

    raw_title = safe(
        profile.get("title")
        or profile.get("headline")
    )
    title = owner_founder_title(raw_title)

    candidate = Candidate(
        name=name,
        title=title,
        linkedin_url=normalize_linkedin_url(
            profile.get("linkedin_url")
            or profile.get("url")
            or profile.get("linkedin")
            or ""
        ),
        company_linkedin_url=normalize_linkedin_url(
            company_data.get("linkedin_url")
            or company_data.get("linkedin")
            or ""
        ),
        source="ContactOut Decision Makers",
        raw=profile,
    )

    candidate.score = score_candidate(
        candidate,
        company,
        domain,
        city,
    )

    return candidate


def decode_cloudflare_email(encoded: str) -> str:
    """Decode Cloudflare email-protection data-cfemail values."""
    encoded = safe(encoded)

    if (
        len(encoded) < 4
        or len(encoded) % 2
        or not re.fullmatch(r"[0-9a-fA-F]+", encoded)
    ):
        return ""

    try:
        key = int(encoded[:2], 16)
        decoded = "".join(
            chr(int(encoded[index:index + 2], 16) ^ key)
            for index in range(2, len(encoded), 2)
        )
    except (TypeError, ValueError):
        return ""

    return decoded if is_usable_email(decoded) else ""


def extract_website_emails(
    html: str,
    soup: Optional[BeautifulSoup] = None,
) -> List[str]:
    """
    Extract real website emails from raw HTML, visible text, mailto links,
    data attributes, obfuscated text, and Cloudflare protection.
    """
    if not html:
        return []

    if soup is None:
        soup = BeautifulSoup(
            html,
            "html.parser",
        )

    found: Set[str] = set()

    def add_many(values: Iterable[str]) -> None:
        for email in values:
            email = safe(email).lower()
            if is_usable_email(email):
                found.add(email)

    decoded_html = unescape(html)
    add_many(extract_emails(decoded_html))

    try:
        visible_text = soup.get_text(
            " ",
            strip=True,
        )
        add_many(extract_emails(unescape(visible_text)))
    except Exception:
        pass

    for anchor in soup.select("a[href]"):
        href = unescape(
            safe(anchor.get("href"))
        )

        if href.lower().startswith("mailto:"):
            target = href.split(":", 1)[1]
            target = target.split("?", 1)[0]
            add_many(extract_emails(target))

    for node in soup.select("[data-cfemail]"):
        decoded = decode_cloudflare_email(
            safe(node.get("data-cfemail"))
        )
        if decoded:
            found.add(decoded)

    for attribute in [
        "data-email",
        "data-mail",
        "data-contact-email",
        "data-user",
    ]:
        for node in soup.select(f"[{attribute}]"):
            add_many(
                extract_emails(
                    unescape(
                        safe(node.get(attribute))
                    )
                )
            )

    return sorted(found)


def first_social_url_from_text(
    text: str,
    social_domain: str,
) -> str:
    """Return the first clean public social URL from Maps all_found_links."""
    text = safe(text)
    if not text:
        return ""

    for raw in re.split(r"\s*\|\s*|\s+", text):
        candidate = safe(raw).strip()
        if social_domain not in candidate.lower():
            continue
        if social_domain == "facebook.com":
            normalized = normalize_facebook_url(candidate)
        else:
            normalized = normalize_url(candidate).split("?", 1)[0]
        if normalized:
            return normalized
    return ""


def normalize_facebook_url(url: str) -> str:
    """Normalize a Facebook business/page URL and discard post/reel/photo URLs."""
    url = normalize_url(unquote(safe(url)))
    if "facebook.com/" not in url.lower() and "fb.com/" not in url.lower():
        return ""

    parsed = urlparse(url)
    host = parsed.netloc.lower().replace("m.facebook.com", "www.facebook.com")
    if "fb.com" in host:
        host = "www.facebook.com"

    path = parsed.path or "/"
    lower_path = path.lower()

    blocked = [
        "/posts/", "/photos/", "/photo", "/reel/", "/videos/",
        "/story.php", "/permalink.php", "/events/", "/groups/",
    ]
    if any(token in lower_path for token in blocked):
        parts = [part for part in path.split("/") if part]
        # Convert https://facebook.com/PageName/posts/... to the page URL.
        if parts and parts[0].lower() not in {"posts", "photos", "reel", "videos", "story.php", "permalink.php"}:
            path = "/" + parts[0] + "/"
        else:
            return ""

    if lower_path.startswith("/profile.php"):
        query = parse_qs(parsed.query)
        page_id = safe((query.get("id") or [""])[0])
        return f"https://www.facebook.com/profile.php?id={page_id}" if page_id else ""

    parts = [part for part in path.split("/") if part]
    if not parts:
        return ""

    # Keep /p/... pages exactly as Google Maps saved them. Facebook local
    # pages sometimes use /p/<name>-<id>/ with only two path segments.
    if parts[0].lower() == "p" and len(parts) >= 2:
        clean_path = "/" + "/".join(parts) + "/"
    else:
        clean_path = "/" + parts[0] + "/"

    return f"https://www.facebook.com{clean_path}"


def facebook_candidate_urls(url: str) -> List[str]:
    """
    Return only the direct Facebook URL from Google Maps/CSV.

    Important: do NOT append /about, /about_contact_and_basic_info, /p/about,
    or any other Facebook sub-path. The user specifically wants the scraper to
    open the same page URL already saved in google_maps_data.csv.
    """
    base = normalize_facebook_url(url)
    return [base] if base else []


def scrape_facebook_contact(
    facebook_url: str,
    *,
    delay_seconds: float = 1,
    page: Optional[Page] = None,
) -> Dict[str, List[str]]:
    """Scrape public email/phone from the direct Facebook page URL only."""
    if not facebook_url:
        return {"emails": [], "phones": []}

    emails: Set[str] = set()
    phones: Set[str] = set()

    for url in facebook_candidate_urls(facebook_url):
        # Direct URL only. No /about fallback is attempted.
        print(f"Facebook public page open: {url}")

        html = request_html(url, timeout=12)
        if html:
            soup = BeautifulSoup(html, "html.parser")
            emails.update(extract_website_emails(html, soup))
            phones.update(extract_phone_numbers(html))

        # Requests often sees a reduced Facebook page. In visible-browser runs,
        # also read the exact same direct URL with Playwright. No login bypass,
        # no extension, and no Facebook about-path manipulation.
        if page is not None and env_bool("FACEBOOK_BROWSER_FALLBACK", True):
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=25000)
                page.wait_for_timeout(1800)
                body = page.locator("body").inner_text(timeout=5000)
                content = page.content()
                combined = body + " " + content
                emails.update(extract_emails(combined))
                phones.update(extract_phone_numbers(combined))
            except Exception:
                pass

        # The requested behavior is: if email/phone is not visible on this
        # direct page, skip and move on. Do not try /about variants.
        time.sleep(max(delay_seconds, 0))
        break

    return {
        "emails": sorted(email for email in emails if is_usable_email(email)),
        "phones": sorted(phone for phone in phones if phone),
    }


def scrape_facebook_emails(
    facebook_url: str,
    *,
    delay_seconds: float = 1,
    page: Optional[Page] = None,
) -> List[str]:
    """Backward-compatible wrapper: return direct Facebook public emails only."""
    return scrape_facebook_contact(
        facebook_url,
        delay_seconds=delay_seconds,
        page=page,
    )["emails"]


def request_html(
    url: str,
    timeout: int = 6,
) -> str:
    try:
        response = requests.get(
            normalize_url(url),
            headers={
                "User-Agent": USER_AGENT,
                "Accept-Language": (
                    "en-US,en;q=0.9"
                ),
            },
            timeout=timeout,
            allow_redirects=True,
        )

        if response.status_code >= 400:
            return ""

        content_type = response.headers.get(
            "content-type",
            "",
        ).lower()

        if (
            "text/html" not in content_type
            and "text/plain"
            not in content_type
        ):
            return ""

        return response.text or ""

    except requests.RequestException:
        return ""


def extract_candidate_pairs(
    text: str,
    source: str,
) -> List[Candidate]:
    text = normalize_spaces(text)

    if not text:
        return []

    title_group = (
        r"Owner|Co-Owner|Business Owner|"
        r"Founder|Co-Founder|Cofounder|CEO|Chief Executive Officer"
    )

    patterns = [
        (
            rf"([A-Z][A-Za-z'’-]+"
            rf"(?:\s+[A-Z][A-Za-z'’-]+)"
            rf"{{1,2}})\s*[-–|,:]\s*"
            rf"({title_group})"
        ),
        (
            rf"({title_group})"
            rf"\s*[-–|,:]\s*"
            rf"([A-Z][A-Za-z'’-]+"
            rf"(?:\s+[A-Z][A-Za-z'’-]+)"
            rf"{{1,2}})"
        ),
    ]

    output: List[Candidate] = []

    for pattern_index, pattern in enumerate(
        patterns
    ):
        for match in re.findall(
            pattern,
            text,
        ):
            if pattern_index == 0:
                name, title = match
            else:
                title, name = match

            candidate = Candidate(
                name=normalize_spaces(name),
                title=normalize_spaces(title),
                source=source,
                raw={
                    "text_match": (
                        f"{name} | {title}"
                    )
                },
            )

            candidate.score = (
                title_score(candidate.title)
                + 15
            )

            output.append(candidate)

    return output


def same_domain(
    url: str,
    domain: str,
) -> bool:
    return bool(
        domain
        and clean_domain(url) == domain
    )


def crawl_website(
    website: str,
    delay_seconds: float = 1,
) -> WebsiteData:
    website = normalize_url(website)

    if not website:
        return WebsiteData()

    parsed = urlparse(website)

    base = (
        f"{parsed.scheme}://"
        f"{parsed.netloc}"
    )

    domain = clean_domain(website)

    urls: List[str] = []
    for candidate in [
        website,
        base + "/",
        *[
            urljoin(base, path)
            for path in COMMON_PATHS
        ],
    ]:
        if candidate not in urls:
            urls.append(candidate)

    seen: Set[str] = set()
    result = WebsiteData()

    index = 0
    max_pages = max(
        1,
        int(
            os.getenv(
                "WEBSITE_MAX_PAGES",
                "5",
            )
        ),
    )
    visited_pages = 0

    while (
        index < len(urls)
        and visited_pages < max_pages
    ):
        url = urls[index]
        index += 1

        normalized = url.split(
            "#",
            1,
        )[0]

        if normalized in seen:
            continue

        seen.add(normalized)
        visited_pages += 1

        html = request_html(
            normalized
        )

        if not html:
            continue

        soup = BeautifulSoup(
            html,
            "html.parser",
        )

        result.emails.extend(
            extract_website_emails(
                html,
                soup,
            )
        )

        for tag in soup(
            [
                "script",
                "style",
                "noscript",
                "svg",
            ]
        ):
            tag.decompose()

        text = soup.get_text(
            " ",
            strip=True,
        )

        result.candidates.extend(
            extract_candidate_pairs(
                text[:30000],
                f"Website: {normalized}",
            )
        )

        discovered_priority: List[str] = []

        for anchor in soup.select(
            "a[href]"
        ):
            href = urljoin(
                normalized,
                safe(
                    anchor.get("href")
                ),
            )

            lower = href.lower()

            if (
                "linkedin.com/in/" in lower
                or "linkedin.com/pub/" in lower
            ):
                linked = normalize_linkedin_url(
                    href
                )

                if linked:
                    result.linkedin_person_urls.append(
                        linked
                    )

            elif (
                "linkedin.com/company/"
                in lower
            ):
                linked = normalize_linkedin_url(
                    href
                )

                if linked:
                    result.linkedin_company_urls.append(
                        linked
                    )

            elif (
                "facebook.com/" in lower
                and not result.facebook
            ):
                if not any(
                    token in lower
                    for token in [
                        "/posts/",
                        "/photos/",
                        "/reel/",
                        "/videos/",
                    ]
                ):
                    result.facebook = href.split(
                        "?",
                        1,
                    )[0]

            elif (
                "instagram.com/" in lower
                and not result.instagram
            ):
                if not any(
                    token in lower
                    for token in [
                        "/p/",
                        "/reel/",
                        "/stories/",
                    ]
                ):
                    result.instagram = href.split(
                        "?",
                        1,
                    )[0]

            anchor_text = normalize_spaces(
                anchor.get_text(
                    " ",
                    strip=True,
                )
            ).lower()

            useful_internal = (
                same_domain(href, domain)
                and any(
                    keyword in anchor_text
                    or f"/{keyword}" in lower
                    for keyword in [
                        "contact",
                        "email",
                        "about",
                        "team",
                        "staff",
                        "leadership",
                        "management",
                    ]
                )
            )

            if (
                useful_internal
                and href not in seen
                and href not in urls
            ):
                discovered_priority.append(
                    href
                )

        for href in reversed(
            discovered_priority
        ):
            urls.insert(
                index,
                href,
            )

        time.sleep(
            max(delay_seconds, 0)
        )

    result.emails = sorted(
        {
            email
            for email in result.emails
            if is_usable_email(email)
        }
    )

    result.linkedin_person_urls = sorted(
        set(
            result.linkedin_person_urls
        )
    )

    result.linkedin_company_urls = sorted(
        set(
            result.linkedin_company_urls
        )
    )

    for linkedin_url in (
        result.linkedin_person_urls
    ):
        result.candidates.append(
            Candidate(
                linkedin_url=linkedin_url,
                source=(
                    "Business Website LinkedIn"
                ),
                score=45,
                raw={
                    "linkedin_url": linkedin_url
                },
            )
        )

    result.candidates = dedupe_candidates(
        result.candidates
    )

    return result


def scrape_public_social_emails(
    urls: Sequence[str],
    delay_seconds: float = 1,
) -> List[str]:
    emails: Set[str] = set()

    for url in urls:
        url = normalize_url(url)

        if not url:
            continue

        html = request_html(
            url,
            timeout=12,
        )

        emails.update(
            extract_emails(html)
        )

        time.sleep(
            max(delay_seconds, 0)
        )

    return sorted(emails)


def google_redirect_target(
    url: str,
) -> str:
    url = safe(url)

    if not url:
        return ""

    if url.startswith("/url?"):
        query = parse_qs(
            urlparse(url).query
        )

        return safe(
            (
                query.get("q")
                or query.get("url")
                or [""]
            )[0]
        )

    parsed = urlparse(url)

    if "google." in parsed.netloc:
        query = parse_qs(
            parsed.query
        )

        target = (
            (query.get("q") or [""])[0]
            or (
                query.get("url")
                or [""]
            )[0]
        )

        if target:
            return unquote(target)

    return url


def beep() -> None:
    try:
        import winsound

        winsound.Beep(
            1000,
            700,
        )

    except Exception:
        print(
            "\a",
            end="",
            flush=True,
        )


def captcha_detected(
    page: Page,
) -> bool:
    current_url = page.url.lower()

    if (
        "sorry/index" in current_url
        or "captcha" in current_url
    ):
        return True

    try:
        body = page.locator(
            "body"
        ).inner_text(
            timeout=3000
        ).lower()

    except Exception:
        return False

    return any(
        phrase in body
        for phrase in [
            "unusual traffic",
            "our systems have detected",
            "not a robot",
            "captcha",
            # Bing challenge pages
            "one last step",
            "solve the challenge",
            "verify you are a human",
            "verify you are human",
        ]
    )


def captcha_action(
    page: Page,
) -> str:
    beep()

    print(
        "\nSearch-engine CAPTCHA detected.\n"
        "Solve it manually in the browser window.\n"
        "[C] Continue\n"
        "[S] Skip query\n"
        "[B] Skip business\n"
        "[Q] Save and quit"
    )

    while True:
        if os.getenv("PIPELINE_WEB") == "1":
            # The web UI (app.py) watches for this marker and answers on stdin.
            print("@@CAPTCHA_WAIT@@", flush=True)

        action = (
            input("Choice: ")
            .strip()
            .lower()
            or "c"
        )

        if action == "c":
            page.wait_for_timeout(
                1500
            )

            if captcha_detected(page):
                print(
                    "The CAPTCHA has not "
                    "been solved yet."
                )
                continue

            return "continue"

        if action == "s":
            return "skip_query"

        if action == "b":
            raise SkipBusiness()

        if action == "q":
            raise QuitPipeline()

        print(
            "Please enter C, S, B or Q."
        )


def extract_google_results(
    page: Page,
) -> List[Dict[str, str]]:
    """Extract standard results plus LinkedIn links from AI Overview/panels."""
    output: Dict[str, Dict[str, str]] = {}

    def add_result(link: Any, require_h3: bool = False) -> None:
        try:
            if not link.is_visible():
                return

            raw_url = safe(link.get_attribute("href"))
            url = normalize_linkedin_url(
                google_redirect_target(raw_url)
            ) or google_redirect_target(raw_url)

            if not url.startswith(("http://", "https://")):
                return

            if "google." in urlparse(url).netloc.lower():
                return

            title = ""
            if require_h3:
                title = normalize_spaces(
                    link.locator("h3").first.inner_text(timeout=1200)
                )
            else:
                title = normalize_spaces(link.inner_text(timeout=1200))
                if link.locator("h3").count() > 0:
                    title = normalize_spaces(
                        link.locator("h3").first.inner_text(timeout=1200)
                    ) or title

            snippet = ""
            ancestor_selectors = [
                "xpath=ancestor::div[contains(concat(' ',normalize-space(@class),' '),' tF2Cxc ')][1]",
                "xpath=ancestor::div[contains(concat(' ',normalize-space(@class),' '),' MjjYud ')][1]",
                "xpath=ancestor::div[contains(concat(' ',normalize-space(@class),' '),' g ')][1]",
                # AI Overview and knowledge-panel links often have no h3.
                "xpath=ancestor::div[string-length(normalize-space(.)) > 20][1]",
            ]

            for selector in ancestor_selectors:
                try:
                    block = link.locator(selector)
                    if block.count() > 0:
                        candidate = normalize_spaces(
                            block.first.inner_text(timeout=1200)
                        )
                        if candidate:
                            snippet = candidate[:2200]
                            break
                except Exception:
                    continue

            if not snippet:
                snippet = title

            if not title:
                title = person_name_from_linkedin_url(url) or url

            record = {
                "title": title,
                "snippet": snippet,
                "url": url,
            }

            current = output.get(url)
            if current is None or len(snippet) > len(current.get("snippet", "")):
                output[url] = record
        except Exception:
            return

    standard = page.locator(
        "a[jsname='UWckNb']:has(h3), "
        "a.zReHs:has(h3), "
        "div.MjjYud a:has(h3), "
        "div.g a:has(h3), "
        "a:has(h3)"
    )

    try:
        for index in range(min(standard.count(), 60)):
            add_result(standard.nth(index), require_h3=True)
    except Exception:
        pass

    # Crucial for AI Overview/right-side panels: these LinkedIn anchors often
    # do not contain h3 tags, so the old scraper never saw them.
    all_links = page.locator("a[href]")
    try:
        for index in range(min(all_links.count(), 350)):
            link = all_links.nth(index)
            raw = safe(link.get_attribute("href"))
            target = google_redirect_target(raw).lower()
            decoded = unquote(raw).lower()
            if (
                "linkedin.com/in/" in target
                or "linkedin.com/company/" in target
                or "linkedin.com%2fin%2f" in decoded
                or "linkedin.com%2fcompany%2f" in decoded
            ):
                add_result(link, require_h3=False)
    except Exception:
        pass

    return list(output.values())


# ---------------------------------------------------------------------------
# Multi-engine web search (Bing + Google) for the owner/founder/CEO lookup.
# Google Maps scraping is unchanged and still uses Google only.
# ---------------------------------------------------------------------------

# engine -> epoch seconds until which that engine is considered blocked.
_ENGINE_BLOCKED_UNTIL: Dict[str, float] = {}


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, "").strip() or default)
    except ValueError:
        return default


def search_engine_order() -> List[str]:
    raw = os.getenv("SEARCH_ENGINE_ORDER", "bing,google")
    engines = [
        item.strip().lower()
        for item in raw.split(",")
        if item.strip().lower() in {"bing", "google"}
    ]
    return list(dict.fromkeys(engines)) or ["bing", "google"]


def engine_available(engine: str) -> bool:
    return time.time() >= _ENGINE_BLOCKED_UNTIL.get(engine, 0.0)


def block_engine(engine: str) -> None:
    cooldown_minutes = _env_float("SEARCH_ENGINE_COOLDOWN_MINUTES", 12)
    _ENGINE_BLOCKED_UNTIL[engine] = time.time() + cooldown_minutes * 60


def engine_search_url(engine: str, query: str) -> str:
    if engine == "bing":
        # Keep the URL plain: extra setlang/cc/mkt parameters made Bing
        # return empty or off-topic results in testing.
        return "https://www.bing.com/search?q=" + quote_plus(query)
    return "https://www.google.com/search?q=" + quote_plus(query)


def polite_engine_delay(engine: str, google_base: float) -> None:
    """Randomised pause; fixed intervals look robotic."""
    if engine == "bing":
        base = _env_float("BING_SEARCH_DELAY_SECONDS", 4)
        low, high = 0.7, 1.6
    else:
        base = google_base
        low, high = 0.8, 1.5
    time.sleep(max(base, 0) * random.uniform(low, high))


def bing_redirect_target(url: str) -> str:
    """Decode bing.com/ck/a?...&u=a1<base64> tracking redirects."""
    url = safe(url)
    parsed = urlparse(url)

    if not (
        parsed.netloc.lower().endswith("bing.com")
        and parsed.path.startswith("/ck/")
    ):
        return url

    token = (parse_qs(parsed.query).get("u") or [""])[0]

    if token.startswith("a1"):
        encoded = token[2:]
        encoded += "=" * (-len(encoded) % 4)
        try:
            decoded = base64.urlsafe_b64decode(encoded).decode("utf-8")
            if decoded.startswith(("http://", "https://")):
                return decoded
        except Exception:
            pass

    return url


def extract_bing_results(page: Page) -> List[Dict[str, str]]:
    """Same record shape as extract_google_results: title, snippet, url."""
    output: Dict[str, Dict[str, str]] = {}

    def add(link: Any, block_selector: str) -> None:
        try:
            raw_url = safe(link.get_attribute("href"))
            url = normalize_linkedin_url(
                bing_redirect_target(raw_url)
            ) or bing_redirect_target(raw_url)

            if not url.startswith(("http://", "https://")):
                return

            if urlparse(url).netloc.lower().endswith(
                ("bing.com", "microsoft.com")
            ):
                return

            title = normalize_spaces(link.inner_text(timeout=1200))
            snippet = ""

            block = link.locator(block_selector)
            if block.count() > 0:
                snippet = normalize_spaces(
                    block.first.inner_text(timeout=1200)
                )[:2200]

            snippet = snippet or title
            title = title or person_name_from_linkedin_url(url) or url

            current = output.get(url)
            if current is None or len(snippet) > len(current["snippet"]):
                output[url] = {
                    "title": title,
                    "snippet": snippet,
                    "url": url,
                }
        except Exception:
            return

    block_selector = "xpath=ancestor::li[contains(@class,'b_algo')][1]"

    try:
        organic = page.locator("li.b_algo h2 a")
        for index in range(min(organic.count(), 40)):
            add(organic.nth(index), block_selector)
    except Exception:
        pass

    # LinkedIn links in answer/side panels that are not organic results.
    try:
        all_links = page.locator("a[href]")
        for index in range(min(all_links.count(), 300)):
            link = all_links.nth(index)
            target = unquote(
                bing_redirect_target(safe(link.get_attribute("href")))
            ).lower()
            if (
                "linkedin.com/in/" in target
                or "linkedin.com/company/" in target
            ):
                add(link, block_selector)
    except Exception:
        pass

    return list(output.values())


def run_query_on_engines(
    *,
    query: str,
    get_engine_page: Any,
    company: str,
    domain: str,
    company_linkedin_url: str,
) -> Tuple[List[Dict[str, str]], List["Candidate"], str]:
    """Run one query, moving to the next engine when one shows a CAPTCHA.

    Returns (results, ai_hints, engine_used). The user is asked to solve a
    CAPTCHA only when every engine that was tried is blocked.
    """
    order = search_engine_order()
    engines = [engine for engine in order if engine_available(engine)]

    if not engines:
        # Everything is cooling down: retry the one that unblocks soonest.
        engines = [
            min(
                order,
                key=lambda name: _ENGINE_BLOCKED_UNTIL.get(name, 0.0),
            )
        ]

    blocked: List[Tuple[str, Page]] = []

    for engine in engines:
        page = get_engine_page(engine)
        print(f"{engine.title()} LinkedIn search: {query}")

        try:
            page.goto(
                engine_search_url(engine, query),
                wait_until="domcontentloaded",
                timeout=35000,
            )
        except Exception as error:
            print(f"{engine.title()} search failed ({error}); trying next engine.")
            continue

        # A page that already shows organic Bing results is not a challenge,
        # even if the word "captcha" appears somewhere in its text.
        has_bing_results = (
            engine == "bing" and page.locator("li.b_algo").count() > 0
        )

        if not has_bing_results and captcha_detected(page):
            block_engine(engine)
            blocked.append((engine, page))
            print(
                f"{engine.title()} showed a CAPTCHA; cooling it down and "
                "switching engine."
            )
            continue

        return (
            *_extract_engine_results(
                engine, page, company, domain, company_linkedin_url
            ),
            engine,
        )

    if not blocked:
        return [], [], ""

    # Every attempted engine was blocked: ask the user once.
    engine, page = blocked[0]
    if captcha_action(page) == "skip_query":
        return [], [], engine

    _ENGINE_BLOCKED_UNTIL.pop(engine, None)
    return (
        *_extract_engine_results(
            engine, page, company, domain, company_linkedin_url
        ),
        engine,
    )


def _extract_engine_results(
    engine: str,
    page: Page,
    company: str,
    domain: str,
    company_linkedin_url: str,
) -> Tuple[List[Dict[str, str]], List["Candidate"]]:
    if engine == "bing":
        try:
            page.wait_for_selector("li.b_algo", timeout=5000)
        except Exception:
            pass
        return extract_bing_results(page), []

    try:
        page.wait_for_selector(
            "a:has(h3), text=AI Overview",
            timeout=5000,
        )
    except Exception:
        pass

    return (
        extract_google_results(page),
        extract_ai_overview_hints(
            page, company, domain, company_linkedin_url
        ),
    )


def google_result_score(
    result: Dict[str, str],
    company: str,
    domain: str,
    company_linkedin_url: str = "",
    ai_hints: Sequence[Candidate] = (),
) -> int:
    url = result["url"].lower()

    text = (
        f"{result['title']} "
        f"{result['snippet']}"
    ).lower()

    score = 0

    if "linkedin.com/in/" in url:
        if not google_person_result_matches_company(
            result,
            company,
            domain,
            company_linkedin_url,
            ai_hints,
        ):
            return -100

        score += 140

    elif "linkedin.com/company/" in url:
        if not google_company_result_matches_company(
            result,
            company,
            domain,
        ):
            return -100

        score += 100

    elif same_domain(url, domain):
        score += 85

        if any(
            token in url
            for token in [
                "/about",
                "/team",
                "/staff",
                "/leadership",
            ]
        ):
            score += 20

    elif any(
        token in url
        for token in [
            "about",
            "team",
            "leadership",
            "owner",
            "founder",
            "ceo",
        ]
    ):
        score += 35

    evidence = company_evidence_score(
        text,
        company,
        domain,
        company_linkedin_url,
    )

    score += min(
        evidence // 2,
        50,
    )

    if has_decision_role(text):
        score += 35

    result_name = clean_person_name(
        re.split(
            r"\s+[-–—|]\s+",
            normalize_spaces(
                result.get("title", "")
            ),
            maxsplit=1,
        )[0]
    )

    for hint in ai_hints:
        if fuzz.token_sort_ratio(
            normalize_match_text(
                result_name
            ),
            normalize_match_text(
                hint.name
            ),
        ) >= 88:
            score += 30
            break

    return score


def extract_ai_overview_text(
    page: Page,
) -> str:
    """Return the visible AI Overview section as a weak search hint."""
    try:
        body = page.locator(
            "body"
        ).inner_text(
            timeout=4000
        )
    except Exception:
        return ""

    lower = body.lower()
    start = lower.find(
        "ai overview"
    )

    if start < 0:
        return ""

    segment = body[
        start:start + 5000
    ]

    for marker in [
        "People also ask",
        "Web results",
        "Related searches",
    ]:
        index = segment.lower().find(
            marker.lower(),
            80,
        )

        if index > 0:
            segment = segment[:index]

    lines = [
        normalize_spaces(line)
        for line in segment.splitlines()
        if normalize_spaces(line)
    ]

    return "\n".join(lines)


def extract_ai_overview_hints(
    page: Page,
    company: str,
    domain: str,
    company_linkedin_url: str,
) -> List[Candidate]:
    overview = extract_ai_overview_text(page)
    if not overview:
        return []

    if company_evidence_score(
        overview,
        company,
        domain,
        company_linkedin_url,
    ) < 50:
        return []

    role_group = (
        r"(?i:Co-Founder|Cofounder|Founder|Co-Owner|Owner|"
        r"CEO|Chief Executive Officer)"
    )
    name_group = (
        r"[A-Z][A-Za-z'’.-]+"
        r"(?:\s+[A-Z][A-Za-z'’.-]+){1,3}"
    )

    found: List[Tuple[str, str]] = []

    patterns = [
        (rf"({name_group})\s*\(([^)]*{role_group}[^)]*)\)", False),
        (rf"({name_group})\s*(?:-|–|—|:|\bis\b|\bwas\b)\s*(?:the\s+)?({role_group})", False),
        (rf"({role_group})\s*(?:-|–|—|:|\bis\b|\bwas\b)\s*({name_group})", True),
        # "RxSun was founded by energy entrepreneur Christopher Gersch."
        (rf"(?i:was\s+|is\s+)?founded\s+by(?:\s+[a-z][a-z-]*){{0,6}}\s+({name_group})(?=[.,;]|$)", False),
        # "Christopher Gersch founded RxSun"
        (rf"({name_group})\s+(?:co-)?founded\s+{re.escape(company)}", False),
        # "The founder/CEO of RxSun is Christopher Gersch"
        (rf"({role_group})\s+of\s+{re.escape(company)}\s+(?:is|was)\s+({name_group})", True),
        # "RxSun's CEO is Christopher Gersch"
        (rf"{re.escape(company)}(?:['’]s)?\s+({role_group})\s+(?:is|was)\s+({name_group})", True),
    ]

    for pattern, title_first in patterns:
        for match in re.findall(pattern, overview, flags=re.M):
            if not isinstance(match, tuple):
                match = (match, "")

            if title_first:
                raw_title, raw_name = match[0], match[1]
            else:
                raw_name = match[0]
                raw_title = match[1] if len(match) > 1 else "Founder"

            # The "founded by" / "X founded company" patterns imply Founder.
            if not owner_founder_title(raw_title):
                raw_title = "Founder"

            found.append((raw_name, raw_title))

    output: List[Candidate] = []
    for raw_name, raw_title in found:
        raw_name = re.sub(
            r"^(?:And|The)\s+",
            "",
            normalize_spaces(raw_name),
        )
        name = clean_person_name(raw_name)
        title = owner_founder_title(raw_title)

        if (
            "ai overview" in name.lower()
            or not is_usable_person_name(name)
            or not title
            or person_name_looks_like_company(name, company)
        ):
            continue

        output.append(
            Candidate(
                name=name,
                title=title,
                source="Google AI Overview Hint",
                score=70,
                raw={"ai_overview": overview, "company": company},
            )
        )

    return dedupe_candidates(output)

def matching_ai_hint(
    name: str,
    ai_hints: Sequence[Candidate],
) -> Optional[Candidate]:
    if not is_usable_person_name(name):
        return None

    normalized = normalize_match_text(
        name
    )

    for hint in ai_hints:
        if fuzz.token_sort_ratio(
            normalized,
            normalize_match_text(
                hint.name
            ),
        ) >= 88:
            return hint

    return None


def linkedin_result_identity(
    result: Dict[str, str],
    ai_hints: Sequence[Candidate] = (),
) -> Tuple[str, str]:
    title = normalize_spaces(
        result.get("title", "")
    )
    snippet = normalize_spaces(
        result.get("snippet", "")
    )
    role = owner_founder_title(
        f"{title} {snippet}"
    )

    cleaned = re.sub(
        r"\s*\|\s*LinkedIn.*$",
        "",
        title,
        flags=re.I,
    )

    parts = [
        normalize_spaces(part)
        for part in re.split(
            r"\s+[-–—|]\s+",
            cleaned,
        )
        if normalize_spaces(part)
    ]

    name = ""

    # Common Google title: Person Name - Owner/Founder/CEO - Company
    if (
        parts
        and not has_owner_founder_role(
            parts[0]
        )
    ):
        first = clean_person_name(
            parts[0]
        )

        if is_usable_person_name(
            first
        ):
            name = first

    # Alternate title: CEO - Person Name - Company
    if (
        not name
        and len(parts) >= 2
        and has_owner_founder_role(
            parts[0]
        )
    ):
        second = clean_person_name(
            parts[1]
        )

        if is_usable_person_name(
            second
        ):
            name = second

    if not is_usable_person_name(name):
        name = (
            person_name_from_linkedin_url(
                result.get("url", "")
            )
        )

    hint = matching_ai_hint(
        name,
        ai_hints,
    )

    if not role and hint:
        role = owner_founder_title(
            hint.title
        )

    return (
        (
            name
            if is_usable_person_name(
                name
            )
            else ""
        ),
        role,
    )


def inspect_result_page(
    page: Page,
    result: Dict[str, str],
) -> Tuple[
    List[Candidate],
    List[str],
    str,
    str,
]:
    url = result["url"]
    lower = url.lower()

    # Do not bypass the LinkedIn login/bot wall.
    # Save the URL and hand it to ContactOut instead.
    if (
        "linkedin.com/in/" in lower
        or "linkedin.com/pub/" in lower
    ):
        linked = normalize_linkedin_url(
            url
        )

        profile_name, profile_title = (
            linkedin_result_identity(result)
        )

        candidate = Candidate(
            name=profile_name,
            title=profile_title,
            linkedin_url=linked,
            source="Google LinkedIn Result",
            score=(
                115
                if profile_name
                else 100
            ),
            raw=result,
        )

        return [
            candidate
        ], [], linked, ""

    if "linkedin.com/company/" in lower:
        return (
            [],
            [],
            "",
            normalize_linkedin_url(url),
        )

    try:
        page.goto(
            url,
            wait_until="domcontentloaded",
            timeout=35000,
        )

        page.wait_for_timeout(
            1200
        )

        body = page.locator(
            "body"
        ).inner_text(
            timeout=5000
        )

        html = page.content()

    except Exception:
        return [], [], "", ""

    candidates = extract_candidate_pairs(
        (
            f"{result['title']} "
            f"{result['snippet']} "
            f"{body[:25000]}"
        ),
        f"Google opened page: {url}",
    )

    emails = extract_emails(
        html + " " + body
    )

    linkedin_person = ""
    linkedin_company = ""

    try:
        links = page.locator(
            "a[href]"
        )

        for index in range(
            min(links.count(), 150)
        ):
            href = safe(
                links.nth(index)
                .get_attribute("href")
            )

            if (
                "linkedin.com/in/" in href
                and not linkedin_person
            ):
                linkedin_person = (
                    normalize_linkedin_url(
                        href
                    )
                )

            elif (
                "linkedin.com/company/"
                in href
                and not linkedin_company
            ):
                linkedin_company = (
                    normalize_linkedin_url(
                        href
                    )
                )

    except Exception:
        pass

    if linkedin_person:
        candidates.append(
            Candidate(
                linkedin_url=linkedin_person,
                source=(
                    "Google opened page LinkedIn"
                ),
                score=70,
                raw=result,
            )
        )

    return (
        candidates,
        emails,
        linkedin_person,
        linkedin_company,
    )


def google_search_fallback(
    *,
    company: str,
    city: str,
    domain: str,
    search_term: str,
    headless: bool,
    query_limit: int,
    open_limit: int,
    delay_seconds: float,
    company_linkedin_url: str = "",
    browser_context: Optional[
        BrowserContext
    ] = None,
    search_page: Optional[
        Page
    ] = None,
) -> Dict[str, Any]:
    """
    Use simple Google searches only:

    Company owner LinkedIn
    Company founder LinkedIn
    Company CEO LinkedIn

    AI Overview is treated as a weak name/title hint only. A LinkedIn
    person result still has to match the company before it is accepted.
    """
    resolved_company_linkedin = (
        normalize_linkedin_url(
            company_linkedin_url
        )
    )
    candidates: List[
        Candidate
    ] = []
    ai_hints: List[
        Candidate
    ] = []
    linkedin_person = ""

    own_playwright = None
    own_browser = None
    own_context = None
    own_search_page = False

    # Google reuses the pipeline's search tab; Bing gets its own fresh tab
    # per business so the two engines never share a page.
    extra_engine_pages: Dict[str, Page] = {}
    last_engine = ""

    generic_query_budget = max(1, min(query_limit, 3))
    # Reserve one extra query for an exact person named by AI Overview.
    max_queries = generic_query_budget + 1

    query_queue = [
        f"{company} owner LinkedIn",
        f"{company} founder LinkedIn",
        f"{company} CEO LinkedIn",
    ]
    searched_queries: Set[
        str
    ] = set()
    executed_queries = 0

    try:
        context = browser_context

        if context is None:
            own_playwright = (
                sync_playwright()
                .start()
            )
            own_browser = (
                own_playwright
                .chromium
                .launch(
                    headless=headless
                )
            )
            own_context = (
                own_browser
                .new_context(
                    locale="en-US",
                    viewport={
                        "width": 1440,
                        "height": 1000,
                    },
                )
            )
            context = own_context

        if search_page is None:
            search_page = (
                context.new_page()
            )
            own_search_page = True

        def get_engine_page(engine: str) -> Page:
            if engine == "google":
                return search_page
            if engine not in extra_engine_pages:
                extra_engine_pages[engine] = context.new_page()
            return extra_engine_pages[engine]

        while (
            query_queue
            and executed_queries
            < max_queries
        ):
            query = query_queue.pop(0)
            query_key = (
                normalize_spaces(
                    query
                ).lower()
            )

            if query_key in searched_queries:
                continue

            searched_queries.add(
                query_key
            )
            executed_queries += 1

            (
                results,
                new_hints,
                last_engine,
            ) = run_query_on_engines(
                query=query,
                get_engine_page=get_engine_page,
                company=company,
                domain=domain,
                company_linkedin_url=(
                    resolved_company_linkedin
                ),
            )

            if not last_engine:
                # Every engine failed to load; nothing to parse.
                continue

            for hint in new_hints:
                if not matching_ai_hint(
                    hint.name,
                    ai_hints,
                ):
                    ai_hints.append(
                        hint
                    )

            # Capture/upgrade the company page from the same result set.
            # A numeric /company/<id> URL is valid but a vanity slug gives
            # stronger company evidence for later person matching.
            current_company_slug = linkedin_company_slug(
                resolved_company_linkedin
            )
            should_upgrade_company_url = (
                not resolved_company_linkedin
                or current_company_slug.isdigit()
            )
            if should_upgrade_company_url:
                for item in results:
                    if (
                        "linkedin.com/company/" in item["url"].lower()
                        and google_company_result_matches_company(
                            item,
                            company,
                            domain,
                        )
                    ):
                        candidate_company_url = normalize_linkedin_url(
                            item["url"]
                        )
                        if candidate_company_url:
                            resolved_company_linkedin = candidate_company_url
                            break

            query_role_hint = owner_founder_title(query)

            def result_matches_current_query(item: Dict[str, str]) -> bool:
                if google_person_result_matches_company(
                    item,
                    company,
                    domain,
                    resolved_company_linkedin,
                    ai_hints,
                ):
                    return True

                # Some Google results have the right LinkedIn profile and
                # company evidence, but the snippet omits Owner/Founder/CEO.
                # Because this exact search query was already owner/founder/CEO
                # scoped, allow that query role as a fallback title so the
                # verified LinkedIn URL can still reach ContactOut.
                if (
                    query_role_hint
                    and "linkedin.com/in/" in safe(item.get("url")).lower()
                    and company_evidence_score(
                        f"{item.get('title', '')} {item.get('snippet', '')}",
                        company,
                        domain,
                        resolved_company_linkedin,
                    ) >= 62
                ):
                    return True

                return False

            person_results = [
                item
                for item in results
                if result_matches_current_query(item)
            ]

            person_results.sort(
                key=lambda item: (
                    google_result_score(
                        item,
                        company,
                        domain,
                        resolved_company_linkedin,
                        ai_hints,
                    )
                ),
                reverse=True,
            )

            for item in person_results:
                (
                    profile_name,
                    profile_title,
                ) = (
                    linkedin_result_identity(
                        item,
                        ai_hints,
                    )
                )
                if not profile_title and query_role_hint:
                    profile_title = query_role_hint
                linked = (
                    normalize_linkedin_url(
                        item["url"]
                    )
                )

                if (
                    not linked
                    or not is_usable_person_name(
                        profile_name
                    )
                    or person_name_looks_like_company(
                        profile_name,
                        company,
                    )
                    or not has_owner_founder_role(
                        profile_title
                    )
                ):
                    continue

                direct_role = (
                    owner_founder_title(
                        (
                            f"{item.get('title', '')} "
                            f"{item.get('snippet', '')}"
                        )
                    )
                    or query_role_hint
                )

                candidate = Candidate(
                    name=profile_name,
                    title=profile_title,
                    linkedin_url=linked,
                    company_linkedin_url=(
                        resolved_company_linkedin
                    ),
                    source=(
                        "Google LinkedIn "
                        "Decision Maker Search"
                        if direct_role
                        else (
                            "Google AI Overview "
                            "Hint + LinkedIn Result"
                        )
                    ),
                    score=(
                        128
                        if direct_role
                        else 112
                    ),
                    raw={
                        **item,
                        # google_person_result_matches_company already
                        # verified this result against the company or an exact
                        # AI Overview hint. Keep that evidence for the later
                        # candidate filter as well.
                        "matched_company": company,
                        "matched_domain": domain,
                        "ai_hints": [
                            {
                                "name": hint.name,
                                "title": hint.title,
                            }
                            for hint in ai_hints
                        ],
                    },
                )

                if candidate_matches_company(
                    candidate,
                    company,
                    domain,
                    city,
                    resolved_company_linkedin,
                ):
                    print(
                        "Verified LinkedIn decision maker: "
                        f"{candidate.name} | {candidate.title} | "
                        f"{candidate.linkedin_url}"
                    )
                    candidates.append(candidate)
                    linkedin_person = linked
                    break

            if linkedin_person:
                break

            # If the AI Overview named an exact person, run the next simple
            # query with that name. No site:/OR/city operators are used.
            if ai_hints:
                for hint in ai_hints:
                    targeted_query = (
                        f"{hint.name} "
                        f"{company} LinkedIn"
                    )
                    targeted_key = (
                        targeted_query.lower()
                    )

                    if (
                        targeted_key
                        not in searched_queries
                        and all(
                            targeted_key
                            != item.lower()
                            for item in query_queue
                        )
                    ):
                        hint_role = owner_founder_title(
                            hint.title
                        )

                        if hint_role in {
                            "Founder",
                            "Co-Founder",
                        }:
                            query_queue = [
                                item
                                for item in query_queue
                                if item.lower()
                                != (
                                    f"{company} founder LinkedIn"
                                ).lower()
                            ]
                        elif hint_role in {
                            "Owner",
                            "Co-Owner",
                        }:
                            query_queue = [
                                item
                                for item in query_queue
                                if item.lower()
                                != (
                                    f"{company} owner LinkedIn"
                                ).lower()
                            ]
                        elif hint_role == "CEO":
                            query_queue = [
                                item
                                for item in query_queue
                                if item.lower()
                                != (
                                    f"{company} CEO LinkedIn"
                                ).lower()
                            ]

                        query_queue.insert(
                            0,
                            targeted_query,
                        )
                        break

            if (
                query_queue
                and executed_queries
                < max_queries
            ):
                polite_engine_delay(
                    last_engine or "google",
                    delay_seconds,
                )

    finally:
        for extra_page in extra_engine_pages.values():
            try:
                extra_page.close()
            except Exception:
                pass

        if (
            own_search_page
            and search_page is not None
        ):
            search_page.close()
        if own_context is not None:
            own_context.close()
        if own_browser is not None:
            own_browser.close()
        if own_playwright is not None:
            own_playwright.stop()

    return {
        "candidates": dedupe_candidates(
            candidates
        ),
        "emails": [],
        "linkedin_person_url": (
            linkedin_person
        ),
        "linkedin_company_url": (
            resolved_company_linkedin
        ),
        "ai_hints": [
            {
                "name": hint.name,
                "title": hint.title,
            }
            for hint in ai_hints
        ],
    }


def choose_best_candidate(
    candidates: Iterable[Candidate],
    company: str,
    domain: str,
    city: str,
    company_linkedin_url: str = "",
) -> Optional[Candidate]:
    output: List[Candidate] = []

    for candidate in candidates:
        candidate.name = clean_person_name(candidate.name)
        candidate.title = owner_founder_title(candidate.title)

        if not candidate.linkedin_url:
            continue

        if not is_usable_person_name(candidate.name):
            candidate.name = person_name_from_linkedin_url(
                candidate.linkedin_url
            )

        if (
            not is_usable_person_name(candidate.name)
            or person_name_looks_like_company(
                candidate.name,
                company,
            )
            or not has_owner_founder_role(candidate.title)
        ):
            continue

        if not candidate_matches_company(
            candidate,
            company,
            domain,
            city,
            company_linkedin_url,
        ):
            continue

        candidate.score = max(
            candidate.score,
            score_candidate(
                candidate,
                company,
                domain,
                city,
                company_linkedin_url,
            ),
        )
        output.append(candidate)

    output = dedupe_candidates(output)
    return output[0] if output else None


def apply_contactout_emails(
    *,
    candidate: Candidate,
    parsed: Dict[str, Any],
    source_prefix: str,
    score_bonus: int,
    errors: List[str],
) -> bool:
    """Store every usable ContactOut email and choose one primary email."""
    raw_work = [safe(value) for value in parsed.get("work_emails", [])]
    raw_personal = [safe(value) for value in parsed.get("personal_emails", [])]
    raw_all = [safe(value) for value in parsed.get("all_emails", [])]

    work_emails = list(dict.fromkeys(
        email.lower()
        for email in raw_work
        if is_usable_email(email)
    ))
    personal_emails = list(dict.fromkeys(
        email.lower()
        for email in raw_personal
        if is_usable_email(email)
    ))
    all_emails = list(dict.fromkeys(
        email.lower()
        for email in (raw_work + raw_personal + raw_all)
        if is_usable_email(email)
    ))

    rejected = list(dict.fromkeys(
        email.lower()
        for email in (raw_work + raw_personal + raw_all)
        if email and not is_usable_email(email)
    ))
    for email in rejected:
        errors.append(
            "ContactOut placeholder/invalid email rejected: " + email
        )

    candidate.work_emails = work_emails
    candidate.personal_emails = personal_emails
    candidate.all_emails = all_emails

    selected = ""
    selected_type = ""

    if work_emails:
        selected = work_emails[0]
        selected_type = "Work"
    elif personal_emails:
        selected = personal_emails[0]
        selected_type = "Personal"
    elif all_emails:
        selected = all_emails[0]
        selected_type = safe(parsed.get("primary_type")) or "Other"

    if not selected:
        return False

    statuses = parsed.get("statuses") or {}
    candidate.email = selected
    candidate.email_type = selected_type
    candidate.email_status = safe(
        statuses.get(selected)
        if isinstance(statuses, dict)
        else ""
    ) or safe(parsed.get("primary_status"))

    if selected_type == "Work":
        candidate.source = source_prefix + " Work Email"
    elif selected_type == "Personal":
        candidate.source = source_prefix + " Personal Email"
    else:
        candidate.source = source_prefix + " Email"

    candidate.score += score_bonus

    print(
        "ContactOut emails found for verified profile: "
        f"{len(work_emails)} work, "
        f"{len(personal_emails)} personal, "
        f"{len(all_emails)} total."
    )
    print(
        "Primary ContactOut email selected: "
        f"{selected} | {selected_type}"
    )
    return True


def enrich_candidate(
    *,
    candidate: Candidate,
    company: str,
    domain: str,
    city: str,
    contactout: ContactOutClient,
    errors: List[str],
) -> Candidate:
    candidate.name = clean_person_name(candidate.name)
    candidate.title = owner_founder_title(candidate.title)

    if (
        not candidate.linkedin_url
        or not is_usable_person_name(candidate.name)
        or person_name_looks_like_company(
            candidate.name,
            company,
        )
        or not has_owner_founder_role(candidate.title)
        or not candidate_matches_company(
            candidate,
            company,
            domain,
            city,
            candidate.company_linkedin_url,
        )
    ):
        return candidate

    if (
        contactout.configured
        and env_bool("CONTACTOUT_ENABLE", True)
    ):
        print(
            "ContactOut LinkedIn lookup: "
            f"{candidate.name} | {candidate.linkedin_url}"
        )
        if hasattr(contactout, "linkedin_profile_emails"):
            result = contactout.linkedin_profile_emails(
                candidate.linkedin_url,
                full_name=candidate.name,
                company_name=company,
                company_domain=domain,
            )
        else:
            result = contactout.linkedin_emails(
                candidate.linkedin_url
            )

        if result.ok:
            parsed = extract_contactout_emails(result)

            if apply_contactout_emails(
                candidate=candidate,
                parsed=parsed,
                source_prefix="ContactOut LinkedIn",
                score_bonus=25,
                errors=errors,
            ):
                return candidate

            attempts = []
            if isinstance(result.data, dict):
                attempts = result.data.get("attempts") or []

            if attempts:
                parts = []
                for item in attempts:
                    if not isinstance(item, dict):
                        continue
                    extra = ""
                    availability = item.get("availability")
                    if isinstance(availability, dict) and availability:
                        extra = "/availability=" + ",".join(
                            f"{key}:{availability.get(key)}"
                            for key in sorted(availability.keys())
                        )
                    parts.append(
                        f"{safe(item.get('name'))}:"
                        f"{safe(item.get('status_code'))}/"
                        f"emails={safe(item.get('emails'))}"
                        f"{extra}"
                    )
                attempt_text = "; ".join(parts)
                print(
                    "ContactOut API returned no usable work or personal "
                    "email for this exact LinkedIn profile. Attempts: "
                    + attempt_text[:1000]
                )
                errors.append(
                    "ContactOut no email. Attempts: " + attempt_text[:1000]
                )
            else:
                print(
                    "ContactOut API returned no usable work or personal "
                    "email for this exact LinkedIn profile."
                )
        elif result.error and not result.inaccessible:
            print(
                "ContactOut LinkedIn lookup failed: "
                + result.error[:180]
            )
            errors.append(
                "ContactOut LinkedIn: " + result.error[:180]
            )

    # Disabled by default: it adds a second API request. Enable only when
    # the main LinkedIn Contact Info endpoint is unavailable for the account.
    if not env_bool("CONTACTOUT_PEOPLE_ENRICH_FALLBACK", False):
        return candidate

    result = contactout.people_enrich(
        full_name=candidate.name,
        company_name=company,
        company_domain=domain,
        job_title=candidate.title,
        location=city,
        linkedin_url=candidate.linkedin_url,
    )

    if not result.ok:
        if result.error and not result.inaccessible:
            errors.append("ContactOut enrich: " + result.error[:180])
        return candidate

    profile = result.data.get("profile") or result.data.get("data") or {}
    if isinstance(profile, dict) and isinstance(profile.get("profile"), dict):
        profile = profile["profile"]

    profile_linkedin = normalize_linkedin_url(
        profile.get("linkedin_url")
        or profile.get("linkedinUrl")
        or profile.get("url")
        or profile.get("linkedin")
        or ""
    )

    if profile_linkedin and profile_linkedin != candidate.linkedin_url:
        errors.append(
            "ContactOut returned a different LinkedIn profile; ignored."
        )
        return candidate

    returned_name = clean_person_name(
        profile.get("full_name")
        or profile.get("fullName")
        or profile.get("name")
        or ""
    )
    if is_usable_person_name(returned_name):
        candidate.name = returned_name

    returned_title = owner_founder_title(
        profile.get("title")
        or profile.get("headline")
        or ""
    )
    if returned_title:
        candidate.title = returned_title

    parsed = extract_contactout_emails(result)
    apply_contactout_emails(
        candidate=candidate,
        parsed=parsed,
        source_prefix="ContactOut People Enrich",
        score_bonus=22,
        errors=errors,
    )

    return candidate

def output_template(
    row: Dict[str, str],
) -> Dict[str, Any]:
    company = safe(
        row.get("name")
    )

    address = safe(
        row.get("address")
    )

    return {
        "Name": "",
        "Email": "",
        "Phone": safe(
            row.get("phone")
        ),
        "City": safe(
            row.get("city")
        ) or extract_city(
            address,
            safe(row.get("search_term")),
        ),
        "Industry": safe(
            row.get("industry")
            or row.get("search_term")
        ),
        "Website": safe(
            row.get("website_link")
        ),
        "Company": company,
        "Business Address": address,
        "Facebook": normalize_facebook_url(
            row.get("facebook") or first_social_url_from_text(
                row.get("all_found_links", ""),
                "facebook.com",
            )
        ),
        "Instagram": safe(
            row.get("instagram") or first_social_url_from_text(
                row.get("all_found_links", ""),
                "instagram.com",
            )
        ),
        # Maps person URLs are untrusted seeds and are not copied here.
        "LinkedIn URL": "",
        "LinkedIn Profile Name": "",
        "LinkedIn Company Name": company,
        "LinkedIn Company URL": normalize_linkedin_url(
            row.get("linkedin_company_url") or ""
        ),
        "Decision Maker Name": "",
        "Decision Maker Title": "",
        "Decision Maker Email": "",
        "Decision Maker Work Emails": "",
        "Decision Maker Personal Emails": "",
        "Decision Maker All Emails": "",
        "Decision Maker Email Type": "",
        "Website Emails": "",
        "Social Emails": "",
        "Facebook Phone": "",
        "Social Phones": "",
        "All Emails": "",
        "ContactOut Email Status": "",
        "Email Source": "",
        "Confidence Score": 0,
        "Lead Status": "Started",
        "Status": "",
        "Subject": "",
        "Sent At": "",
        "Error": "",
        "Enrichment Version": ENRICHMENT_VERSION,
    }


def finalize_with_candidate(
    result: Dict[str, Any],
    candidate: Candidate,
) -> Dict[str, Any]:
    valid_email = candidate.email if is_usable_email(candidate.email) else ""
    valid_name = (
        clean_person_name(candidate.name)
        if (
            is_usable_person_name(candidate.name)
            and not person_name_looks_like_company(
                candidate.name,
                result["Company"],
            )
        )
        else ""
    )
    valid_title = owner_founder_title(candidate.title)

    result["Name"] = valid_name or result["Company"]
    result["Email"] = valid_email
    result["Decision Maker Name"] = valid_name
    result["Decision Maker Title"] = valid_title if valid_name else ""
    result["Decision Maker Email"] = valid_email if valid_name else ""

    work_emails = list(dict.fromkeys(
        email.lower()
        for email in candidate.work_emails
        if is_usable_email(email)
    ))
    personal_emails = list(dict.fromkeys(
        email.lower()
        for email in candidate.personal_emails
        if is_usable_email(email)
    ))
    all_emails = list(dict.fromkeys(
        email.lower()
        for email in (
            candidate.all_emails
            + work_emails
            + personal_emails
            + ([valid_email] if valid_email else [])
        )
        if is_usable_email(email)
    ))

    result["Decision Maker Work Emails"] = ", ".join(work_emails)
    result["Decision Maker Personal Emails"] = ", ".join(personal_emails)
    result["Decision Maker All Emails"] = ", ".join(all_emails)
    result["Decision Maker Email Type"] = (
        candidate.email_type
        if valid_email and valid_name
        else ""
    )
    result["LinkedIn URL"] = (
        candidate.linkedin_url if valid_name and valid_title else ""
    )
    result["LinkedIn Profile Name"] = (
        valid_name if result["LinkedIn URL"] else ""
    )
    result["LinkedIn Company Name"] = result["Company"]
    result["LinkedIn Company URL"] = (
        candidate.company_linkedin_url
        or result["LinkedIn Company URL"]
    )
    result["Email Source"] = candidate.source if valid_email else "Not Found"
    result["Confidence Score"] = (
        min(candidate.score, 100)
        if valid_name and valid_title
        else 0
    )

    if "ContactOut" in candidate.source:
        result["ContactOut Email Status"] = candidate.email_status

    result["Lead Status"] = (
        "Ready"
        if valid_email and result["LinkedIn URL"] and candidate.score >= 55
        else "Needs Review"
    )
    result["Enrichment Version"] = ENRICHMENT_VERSION

    return result


def enrich_one_business(
    *,
    row: Dict[str, str],
    contactout: ContactOutClient,
    website_delay: float,
    google_delay: float,
    headless: bool,
    google_open_limit: int,
    google_query_limit: int,
    browser_context: Optional[BrowserContext] = None,
    google_page: Optional[Page] = None,
) -> Dict[str, Any]:
    result = output_template(row)

    company = result["Company"]
    city = result["City"]
    website = result["Website"]
    domain = clean_domain(website)
    search_term = safe(
        row.get("search_term")
        or row.get("industry")
    )

    errors: List[str] = []
    candidates: List[Candidate] = []
    contactout_email_found = False

    print(
        "\n--- Enriching: "
        f"{company} | "
        f"{domain or 'no domain'} ---"
    )

    # 1. Crawl the business website.
    website_data = crawl_website(
        website,
        delay_seconds=website_delay,
    )

    result["Website Emails"] = (
        ", ".join(
            email
            for email in website_data.emails
            if is_usable_email(email)
        )
    )

    if not result["Facebook"]:
        result["Facebook"] = normalize_facebook_url(
            website_data.facebook
        )

    if not result["Instagram"]:
        result["Instagram"] = (
            website_data.instagram
        )

    if (
        not result[
            "LinkedIn Company URL"
        ]
        and website_data.linkedin_company_urls
    ):
        result[
            "LinkedIn Company URL"
        ] = (
            website_data
            .linkedin_company_urls[0]
        )

    candidates.extend(
        website_data.candidates
    )

    (
        website_email,
        website_score,
        website_source,
    ) = best_email(
        website_data.emails,
        domain,
    )

    # 2. Resolve company LinkedIn first, then search for a matched decision maker.
    try:
        google_data = (
            google_search_fallback(
                company=company,
                city=city,
                domain=domain,
                search_term=search_term,
                headless=headless,
                query_limit=(
                    google_query_limit
                ),
                open_limit=(
                    google_open_limit
                ),
                delay_seconds=google_delay,
                company_linkedin_url=(
                    result[
                        "LinkedIn Company URL"
                    ]
                ),
                browser_context=browser_context,
                search_page=google_page,
            )
        )

    except SkipBusiness:
        result["Lead Status"] = "Skipped"
        result["Error"] = (
            "Skipped by user during "
            "Google LinkedIn search."
        )
        return result

    if google_data[
        "linkedin_company_url"
    ]:
        result[
            "LinkedIn Company URL"
        ] = google_data[
            "linkedin_company_url"
        ]

    candidates.extend(
        google_data["candidates"]
    )

    best = choose_best_candidate(
        candidates,
        company,
        domain,
        city,
        result["LinkedIn Company URL"],
    )

    if best and best.linkedin_url:
        best.company_linkedin_url = (
            best.company_linkedin_url
            or result[
                "LinkedIn Company URL"
            ]
        )
        result["LinkedIn URL"] = (
            best.linkedin_url
        )
        result[
            "LinkedIn Profile Name"
        ] = best.name
        result[
            "Decision Maker Name"
        ] = best.name
        result[
            "Decision Maker Title"
        ] = best.title

        best = enrich_candidate(
            candidate=best,
            company=company,
            domain=domain,
            city=city,
            contactout=contactout,
            errors=errors,
        )

        if (
            best.email
            and is_usable_email(
                best.email
            )
        ):
            result = finalize_with_candidate(
                result,
                best,
            )
            contactout_email_found = True

    # 3. ContactOut company decision-maker fallback.
    if (
        contactout.configured
        and env_bool("CONTACTOUT_ENABLE", True)
        and env_bool("CONTACTOUT_DECISION_MAKERS_FALLBACK", False)
    ):
        contact_result = (
            contactout.decision_makers(
                domain=domain,
                company_name=company,
                company_linkedin_url=(
                    result[
                        "LinkedIn Company URL"
                    ]
                ),
            )
        )

        if contact_result.ok:
            contact_candidates = [
                candidate_from_contactout(
                    profile,
                    company,
                    domain,
                    city,
                )
                for profile
                in extract_contactout_profiles(
                    contact_result
                )
            ]

            contact_candidates = [
                candidate
                for candidate in contact_candidates
                if (
                    candidate.linkedin_url
                    and candidate_matches_company(
                        candidate,
                        company,
                        domain,
                        city,
                        result[
                            "LinkedIn Company URL"
                        ],
                    )
                )
            ]

            candidates.extend(
                contact_candidates
            )

            best = choose_best_candidate(
                candidates,
                company,
                domain,
                city,
                result[
                    "LinkedIn Company URL"
                ],
            )

            if best and best.linkedin_url:
                best.company_linkedin_url = (
                    best.company_linkedin_url
                    or result[
                        "LinkedIn Company URL"
                    ]
                )

                best = enrich_candidate(
                    candidate=best,
                    company=company,
                    domain=domain,
                    city=city,
                    contactout=contactout,
                    errors=errors,
                )

                result["LinkedIn URL"] = (
                    best.linkedin_url
                )
                result[
                    "LinkedIn Profile Name"
                ] = best.name
                result[
                    "Decision Maker Name"
                ] = best.name
                result[
                    "Decision Maker Title"
                ] = best.title

                if (
                    best.email
                    and is_usable_email(
                        best.email
                    )
                ):
                    result = finalize_with_candidate(
                        result,
                        best,
                    )
                    contactout_email_found = True

        elif contact_result.inaccessible:
            print(
                "ContactOut Decision Makers "
                "current API access par "
                "unavailable."
            )

        elif contact_result.error:
            errors.append(
                "ContactOut decision makers: "
                + contact_result.error[:180]
            )

    # 4. Public email fallbacks. Facebook is enabled by default because
    # many Google Maps records include a Facebook business page with a public
    # contact email. Instagram remains behind SCRAPE_SOCIAL_EMAILS.
    social_emails: List[str] = []
    social_phones: List[str] = []

    if result["Facebook"] and env_bool("SCRAPE_FACEBOOK_EMAILS", True):
        facebook_contact = scrape_facebook_contact(
            result["Facebook"],
            delay_seconds=website_delay,
            page=google_page,
        )
        facebook_emails = facebook_contact.get("emails", [])
        facebook_phones = facebook_contact.get("phones", [])

        if facebook_emails:
            print(
                "Facebook public email found: "
                + ", ".join(facebook_emails[:3])
            )
        else:
            print("Facebook public email not found; skipping Facebook email fallback.")

        if facebook_phones:
            print(
                "Facebook public phone found: "
                + ", ".join(facebook_phones[:3])
            )

        social_emails.extend(facebook_emails)
        social_phones.extend(facebook_phones)
        result["Facebook Phone"] = ", ".join(facebook_phones)

    if result["Instagram"] and env_bool("SCRAPE_SOCIAL_EMAILS", False):
        social_emails.extend(
            scrape_public_social_emails(
                [result["Instagram"]],
                delay_seconds=website_delay,
            )
        )

    social_emails = [
        email
        for email in dict.fromkeys(social_emails)
        if is_usable_email(email)
    ]

    result["Social Emails"] = (
        ", ".join(social_emails)
    )

    social_phones = [
        phone
        for phone in dict.fromkeys(social_phones)
        if phone
    ]
    result["Social Phones"] = ", ".join(social_phones)
    if not result["Phone"] and social_phones:
        result["Phone"] = social_phones[0]

    (
        social_email,
        social_score,
        social_source,
    ) = best_email(
        social_emails,
        domain,
    )

    (
        google_email,
        google_score,
        google_source,
    ) = best_email(
        google_data["emails"],
        domain,
    )

    def _csv_emails(value: str) -> List[str]:
        output: List[str] = []
        for part in re.split(r"\s*,\s*|\s*;\s*", safe(value)):
            if is_usable_email(part) and part.lower() not in output:
                output.append(part.lower())
        return output

    def _refresh_all_emails() -> None:
        combined: List[str] = []
        for source_value in [
            result.get("Decision Maker All Emails", ""),
            result.get("Decision Maker Email", ""),
            result.get("Email", ""),
            result.get("Social Emails", ""),
            result.get("Website Emails", ""),
        ]:
            for email in _csv_emails(source_value):
                if email not in combined:
                    combined.append(email)
        for email in google_data.get("emails", []) or []:
            email = safe(email).lower()
            if is_usable_email(email) and email not in combined:
                combined.append(email)
        result["All Emails"] = ", ".join(combined)

    _refresh_all_emails()

    if contactout_email_found:
        # ContactOut owner/founder email remains primary, but Facebook/website
        # emails are preserved in their own columns and All Emails.
        result["Lead Status"] = "Ready"
        if errors:
            result["Error"] = (
                " | ".join(dict.fromkeys(errors))[:1500]
            )
        return result

    fallbacks = [
        (
            website_score,
            website_email,
            website_source,
        ),
        (
            social_score,
            social_email,
            (
                "Social Public Email"
                if social_email
                else social_source
            ),
        ),
        (
            google_score,
            google_email,
            (
                "Google Opened Page Email"
                if google_email
                else google_source
            ),
        ),
    ]

    fallbacks = [
        item
        for item in fallbacks
        if (
            is_usable_email(item[1])
            and item[2] != "Third-Party Website Email"
        )
    ]

    if fallbacks:
        fallbacks.sort(reverse=True)

        (
            fallback_score,
            fallback_email,
            fallback_source,
        ) = fallbacks[0]

        # Keep Decision Maker Email blank when ContactOut found no direct
        # address, but still place the best public website/social email in the
        # main Email column and label the source honestly.
        result["Email"] = fallback_email
        result["Email Source"] = fallback_source
        result["Confidence Score"] = fallback_score
        result["Lead Status"] = "Needs Review"

    else:
        result["Email"] = ""
        result["Email Source"] = "Not Found"
        result["Lead Status"] = "Needs Review"

    final_person_name = clean_person_name(
        result.get("Decision Maker Name")
        or result.get("LinkedIn Profile Name")
        or ""
    )
    final_person_title = owner_founder_title(
        result.get("Decision Maker Title", "")
    )

    if (
        result["LinkedIn URL"]
        and is_usable_person_name(final_person_name)
        and not person_name_looks_like_company(
            final_person_name,
            company,
        )
        and final_person_title
    ):
        result["Name"] = final_person_name
        result["LinkedIn Profile Name"] = final_person_name
        result["Decision Maker Name"] = final_person_name
        result["Decision Maker Title"] = final_person_title
    else:
        result["Name"] = company
        result["LinkedIn URL"] = ""
        result["LinkedIn Profile Name"] = ""
        result["Decision Maker Name"] = ""
        result["Decision Maker Title"] = ""

    # A public website/social email is a business contact, not proof that it
    # belongs to the identified owner/founder.
    result["Decision Maker Email"] = (
        result["Email"]
        if (
            result["LinkedIn URL"]
            and result["Email Source"].startswith("ContactOut")
        )
        else ""
    )

    result["LinkedIn Company Name"] = (
        result["Company"]
    )

    _refresh_all_emails()

    if errors:
        result["Error"] = (
            " | ".join(dict.fromkeys(errors))[:1500]
        )

    return result



def load_output_rows_by_key(
    output_csv: str,
) -> Dict[str, Dict[str, str]]:
    """Return the latest saved enrichment row for every business."""
    path = Path(output_csv)
    if not path.exists() or path.stat().st_size == 0:
        return {}

    rows_by_key: Dict[str, Dict[str, str]] = {}
    try:
        with path.open(
            "r",
            encoding="utf-8-sig",
            newline="",
        ) as handle:
            for row in csv.DictReader(handle):
                key = business_key(row)
                if key:
                    rows_by_key[key] = row
    except (OSError, csv.Error):
        return {}

    return rows_by_key


def enrichment_row_needs_retry(
    row: Dict[str, str],
) -> bool:
    """Classify missing, failed, or known-placeholder rows for retry."""
    if not row:
        return False

    saved_email = safe(
        row.get("Decision Maker Email")
        or row.get("Email")
    )

    # Repair rows created by the old placeholder bug even if marked Ready.
    if saved_email and not is_usable_email(
        saved_email
    ):
        return True

    if safe(row.get("Lead Status")).lower() == "ready":
        return False

    return not bool(
        safe(row.get("Decision Maker Email"))
        or (
            safe(row.get("Email"))
            and safe(row.get("Email Source")).startswith("ContactOut")
            and is_usable_email(
                safe(row.get("Email"))
            )
        )
    )


def build_enrichment_queue(
    rows: List[Dict[str, str]],
    output_csv: str,
    *,
    resume: bool,
    force_refresh: bool,
    retry_failed: bool,
) -> Tuple[
    List[Tuple[Dict[str, str], bool]],
    int,
    int,
    int,
]:
    """
    Build a two-tier queue.

    Tier 1: Maps rows that have never been written to enriched CSV.
    Tier 2: Existing failed/needs-review rows, only when retry_failed=True.

    The boolean in each tuple is True for an explicit retry row. New/pending
    records are always processed before retries, so old failures cannot starve
    the backlog or repeatedly spend ContactOut credits.
    """
    if not resume or force_refresh:
        return (
            [(row, bool(force_refresh)) for row in rows],
            len(rows),
            0,
            0,
        )

    existing = load_output_rows_by_key(output_csv)
    pending: List[Tuple[Dict[str, str], bool]] = []
    retries: List[Tuple[Dict[str, str], bool]] = []
    already_saved = 0

    for row in rows:
        saved = existing.get(business_key(row))
        if saved is None:
            pending.append((row, False))
            continue

        already_saved += 1
        if retry_failed and enrichment_row_needs_retry(saved):
            retries.append((row, True))

    return (
        pending + retries,
        len(pending),
        len(retries),
        already_saved,
    )


def enrichment_workflow_state(
    enriched: Dict[str, Any],
) -> str:
    if safe(enriched.get("Decision Maker Email")):
        return "enriched_decision_maker"
    if safe(enriched.get("Email")):
        return "enriched_generic"
    if safe(enriched.get("LinkedIn URL")):
        return "researched_no_email"
    if safe(enriched.get("Error")):
        return "error"
    return "researched_not_found"


def load_completed_keys(
    output_csv: str,
) -> Set[str]:
    path = Path(output_csv)

    if not path.exists():
        return set()

    keys: Set[str] = set()

    with path.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as handle:
        for row in csv.DictReader(handle):
            # Old rows or generic-only rows must be allowed through so newer
            # decision-maker logic can repair them instead of "Resume skip".
            reusable = (
                safe(row.get("Enrichment Version")) == ENRICHMENT_VERSION
                and (
                    safe(row.get("LinkedIn URL"))
                    or safe(row.get("Decision Maker Email"))
                    or safe(row.get("Email Source")).startswith("ContactOut")
                )
            )
            if reusable:
                keys.add(business_key(row))

    return keys

def ensure_output_schema(
    output_csv: str,
) -> None:
    """Add new CSV columns without deleting or duplicating existing rows."""
    path = Path(output_csv)
    if not path.exists() or path.stat().st_size == 0:
        return

    try:
        with path.open(
            "r",
            encoding="utf-8-sig",
            newline="",
        ) as handle:
            reader = csv.DictReader(handle)
            current_fields = list(reader.fieldnames or [])
            rows = list(reader)
    except (OSError, csv.Error):
        return

    if current_fields == OUTPUT_FIELDS:
        return

    normalized_rows = [
        {
            field: safe(row.get(field, ""))
            for field in OUTPUT_FIELDS
        }
        for row in rows
    ]

    temp_path = path.with_suffix(path.suffix + ".schema.tmp")
    with temp_path.open(
        "w",
        encoding="utf-8-sig",
        newline="",
    ) as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=OUTPUT_FIELDS,
            extrasaction="ignore",
        )
        writer.writeheader()
        writer.writerows(normalized_rows)

    temp_path.replace(path)


def append_output(
    output_csv: str,
    row: Dict[str, Any],
) -> None:
    """Upsert one business row instead of blindly appending duplicates."""
    path = Path(output_csv)
    path.parent.mkdir(parents=True, exist_ok=True)

    normalized_row = {
        field: safe(row.get(field, ""))
        for field in OUTPUT_FIELDS
    }
    target_key = business_key(normalized_row)

    existing_rows: List[Dict[str, str]] = []
    if path.exists() and path.stat().st_size > 0:
        try:
            with path.open(
                "r",
                encoding="utf-8-sig",
                newline="",
            ) as handle:
                existing_rows = list(csv.DictReader(handle))
        except (OSError, csv.Error):
            existing_rows = []

    replaced = False
    merged_rows: List[Dict[str, str]] = []
    for existing in existing_rows:
        if target_key and business_key(existing) == target_key:
            if not replaced:
                merged_rows.append(normalized_row)
                replaced = True
            continue

        merged_rows.append(
            {
                field: safe(existing.get(field, ""))
                for field in OUTPUT_FIELDS
            }
        )

    if not replaced:
        merged_rows.append(normalized_row)

    temp_path = path.with_suffix(path.suffix + ".tmp")
    with temp_path.open(
        "w",
        encoding="utf-8-sig",
        newline="",
    ) as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=OUTPUT_FIELDS,
            extrasaction="ignore",
        )
        writer.writeheader()
        writer.writerows(merged_rows)
        handle.flush()

    temp_path.replace(path)

def rewrite_maps_csv(
    input_csv: str,
    rows: List[Dict[str, str]],
) -> None:
    if not rows:
        return

    fields = list(
        rows[0].keys()
    )

    for field in [
        "linkedin_url",
        "linkedin_company_url",
        "enrichment_status",
        "enrichment_state",
    ]:
        if field not in fields:
            fields.append(field)

    temp_path = Path(
        input_csv + ".tmp"
    )

    with temp_path.open(
        "w",
        encoding="utf-8-sig",
        newline="",
    ) as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=fields,
            extrasaction="ignore",
        )

        writer.writeheader()
        writer.writerows(rows)

    temp_path.replace(
        input_csv
    )


def enrich_csv(
    input_csv: str = (
        "google_maps_data.csv"
    ),
    output_csv: str = (
        "enriched_leads.csv"
    ),
    limit: int = 50,
    headless: bool = False,
    website_delay: float = 0.25,
    google_delay: float = 2,
    resume: bool = True,
    google_open_limit: int = 0,
    google_query_limit: int = 3,
    browser_context: Optional[
        BrowserContext
    ] = None,
    google_page: Optional[
        Page
    ] = None,
    state_path: str = (
        "pipeline_state.sqlite3"
    ),
    force_refresh: bool = False,
    retry_failed: bool = False,
) -> str:
    input_path = Path(input_csv)

    if not input_path.exists():
        raise FileNotFoundError(
            "Input CSV not found: "
            f"{input_csv}"
        )

    Path(output_csv).parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    if (
        not resume
        and Path(output_csv).exists()
    ):
        Path(output_csv).unlink()

    ensure_output_schema(output_csv)

    with input_path.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as handle:
        rows = list(csv.DictReader(handle))

    (
        queue,
        pending_count,
        retry_count,
        already_saved_count,
    ) = build_enrichment_queue(
        rows,
        output_csv,
        resume=resume,
        force_refresh=force_refresh,
        retry_failed=retry_failed,
    )

    print(
        "Enrichment queue: "
        f"{pending_count} pending/new, "
        f"{already_saved_count} already in enriched CSV, "
        f"{retry_count} explicit retries."
    )

    if retry_count and pending_count:
        print(
            "Pending/new records will run before failed-record retries."
        )

    if not queue:
        print(
            "No pending enrichment records. "
            "Use --retry-failed true for needs-review rows "
            "or --force-refresh true for a full rerun."
        )
        print(
            "\nEnrichment complete: "
            f"{output_csv}"
        )
        return output_csv

    contactout = ContactOutClient()
    processed_now = 0

    with PipelineState(state_path) as state:
        try:
            for row, is_retry in queue:
                if processed_now >= limit:
                    break

                state.register_business(row)

                # New/pending rows may reuse a valid cache when their CSV row
                # is missing. Explicit retries must bypass the old cache.
                cached = (
                    None
                    if (force_refresh or is_retry)
                    else state.get_cached_enrichment(row)
                )

                cached_reusable = bool(
                    cached
                    and safe(cached.get("Enrichment Version"))
                    == ENRICHMENT_VERSION
                )

                if cached_reusable:
                    append_output(output_csv, cached)
                    processed_now += 1

                    row["linkedin_url"] = safe(
                        cached.get("LinkedIn URL")
                    )
                    row["linkedin_company_url"] = safe(
                        cached.get("LinkedIn Company URL")
                    )
                    row["enrichment_status"] = safe(
                        cached.get("Lead Status")
                    )
                    row["enrichment_state"] = enrichment_workflow_state(
                        cached
                    )
                    rewrite_maps_csv(input_csv, rows)

                    print(
                        "Cached enrichment restored: "
                        f"{cached.get('Company') or row.get('name', '')} | "
                        "no ContactOut credit was used."
                    )
                    continue

                print(
                    ("\n--- Retrying: " if is_retry else "\n--- Enriching pending: ")
                    + f"{row.get('name', '')} | "
                    + f"{clean_domain(row.get('website_link', '')) or 'no domain'} ---"
                )

                enriched = enrich_one_business(
                    row=row,
                    contactout=contactout,
                    website_delay=website_delay,
                    google_delay=google_delay,
                    headless=headless,
                    google_open_limit=google_open_limit,
                    google_query_limit=google_query_limit,
                    browser_context=browser_context,
                    google_page=google_page,
                )

                append_output(output_csv, enriched)
                state.save_enrichment(
                    row,
                    enriched,
                    output_csv,
                )

                processed_now += 1
                row["linkedin_url"] = safe(
                    enriched.get("LinkedIn URL")
                )
                row["linkedin_company_url"] = safe(
                    enriched.get("LinkedIn Company URL")
                )
                row["enrichment_status"] = safe(
                    enriched.get("Lead Status")
                )
                row["enrichment_state"] = enrichment_workflow_state(
                    enriched
                )
                rewrite_maps_csv(input_csv, rows)

                print(
                    "Saved enriched lead: "
                    f"{enriched['Company']} | "
                    f"{enriched['Email'] or 'no email'} | "
                    f"{enriched['Email Source']}"
                )

        except QuitPipeline:
            print(
                "Safe stop requested. Current progress saved."
            )
            rewrite_maps_csv(input_csv, rows)

    remaining_pending = max(0, pending_count - processed_now)
    if remaining_pending:
        print(
            f"Pending backlog remaining after this run: {remaining_pending}."
        )

    print(
        "\nEnrichment complete: "
        f"{output_csv}"
    )
    return output_csv


if __name__ == "__main__":
    enrich_csv()