from __future__ import annotations

import csv
import re
import time
from difflib import SequenceMatcher
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple
from urllib.parse import (
    parse_qs,
    quote_plus,
    unquote,
    unquote_plus,
    urlencode,
    urlparse,
)

from bs4 import BeautifulSoup, Tag

from pipeline_state import PipelineState, business_aliases
from playwright.sync_api import (
    BrowserContext,
    Locator,
    Page,
    TimeoutError as PlaywrightTimeoutError,
    sync_playwright,
)


CSV_FIELDS = [
    "name",
    "address",
    "city",
    "state",
    "postal_code",
    "country",
    "phone",
    "website_link",
    "google_maps_url",
    "facebook",
    "instagram",
    "linkedin_url",
    "linkedin_company_url",
    "all_found_links",
    "search_term",
    "industry",
    "enrichment_status",
]

PHONE_RE = re.compile(
    r"(?:\+?1[\s.\-]?)?"
    r"\(?\d{3}\)?[\s.\-]"
    r"\d{3}[\s.\-]\d{4}"
)

STATE_ZIP_RE = re.compile(
    r"\b([A-Z]{2})\s+(\d{5}(?:-\d{4})?)\b",
    re.I,
)

STREET_RE = re.compile(
    r"\b\d{1,6}\s+"
    r"(?:[NSEW]\.?(?:\s+|$))?"
    r"[A-Za-z0-9][A-Za-z0-9 .#'’\-]{1,}"
    r"(?:Street|St|Avenue|Ave|Road|Rd|Boulevard|Blvd|"
    r"Drive|Dr|Lane|Ln|Court|Ct|Highway|Hwy|Parkway|"
    r"Pkwy|Place|Pl|Way|Terrace|Ter|Circle|Cir|"
    r"Trail|Trl|Plaza|Square|Sq)\b",
    re.I,
)

DIRECTORY_HOSTS = {
    "yelp.com",
    "yellowpages.com",
    "mapquest.com",
    "healthgrades.com",
    "zocdoc.com",
    "vitals.com",
    "bbb.org",
    "chamberofcommerce.com",
    "foursquare.com",
    "manta.com",
    "angi.com",
    "thumbtack.com",
    "nextdoor.com",
}

SOCIAL_HOSTS = {
    "facebook.com",
    "instagram.com",
    "linkedin.com",
    "youtube.com",
    "twitter.com",
    "x.com",
    "tiktok.com",
}

COUNTRY_NAMES = {
    "united states": "United States",
    "usa": "United States",
    "us": "United States",
}


# ---------------------------------------------------------------------------
# Generic helpers
# ---------------------------------------------------------------------------


def safe(value: object) -> str:
    return str(value or "").strip()


def normalize_spaces(value: object) -> str:
    return re.sub(
        r"\s+",
        " ",
        safe(value),
    ).strip()


def normalize_key(value: object) -> str:
    return re.sub(
        r"[^a-z0-9]+",
        "",
        safe(value).lower(),
    )


def host_matches(host: str, domain: str) -> bool:
    host = host.lower().split(":", 1)[0]
    domain = domain.lower()
    return host == domain or host.endswith("." + domain)


def strip_tracking(url: str) -> str:
    url = resolve_google_target(url)

    if not url:
        return ""

    parsed = urlparse(url)

    if not parsed.scheme:
        return url

    blocked = {
        "utm_source",
        "utm_medium",
        "utm_campaign",
        "utm_term",
        "utm_content",
        "gclid",
        "gbraid",
        "wbraid",
        "rwg_token",
    }

    query = parse_qs(
        parsed.query,
        keep_blank_values=True,
    )
    kept = {
        key: values
        for key, values in query.items()
        if key.lower() not in blocked
    }

    return parsed._replace(
        query=urlencode(kept, doseq=True),
        fragment="",
    ).geturl()


def resolve_google_target(
    raw_url: str,
    ping: str = "",
) -> str:
    """Resolve Google redirect URLs without returning ad click URLs."""
    candidates = [safe(raw_url)]

    if ping:
        ping_query = parse_qs(
            urlparse(safe(ping)).query
        )
        candidates.extend(ping_query.get("url", []))
        candidates.extend(ping_query.get("q", []))
        candidates.extend(ping_query.get("adurl", []))

    for candidate in candidates:
        if not candidate:
            continue

        candidate = unquote(candidate)

        if candidate.startswith("//"):
            candidate = "https:" + candidate

        if candidate.startswith("/maps/"):
            return "https://www.google.com" + candidate

        if candidate.startswith("/url?"):
            query = parse_qs(
                urlparse(candidate).query
            )
            target = safe(
                (
                    query.get("url")
                    or query.get("q")
                    or query.get("adurl")
                    or [""]
                )[0]
            )
            if target:
                return unquote(target)
            continue

        parsed = urlparse(candidate)
        host = parsed.netloc.lower()
        path = parsed.path.lower()

        if "google." in host:
            query = parse_qs(parsed.query)
            target = safe(
                (
                    query.get("url")
                    or query.get("q")
                    or query.get("adurl")
                    or [""]
                )[0]
            )
            if target:
                return unquote(target)

            if path.startswith("/maps/"):
                return candidate

            # Never save /aclk or other Google ad/tracking URLs.
            continue

        if parsed.scheme in {"http", "https"}:
            return candidate

    return ""


def is_google_maps_url(url: str) -> bool:
    parsed = urlparse(url)
    host = parsed.netloc.lower()
    path = parsed.path.lower()

    return (
        ("google." in host or "maps.google." in host)
        and (
            path.startswith("/maps")
            or "/maps/" in path
        )
    )


def is_external_business_website(
    url: str,
    *,
    allow_directory: bool = False,
) -> bool:
    if not url.startswith(("http://", "https://")):
        return False

    parsed = urlparse(url)
    host = parsed.netloc.lower().replace("www.", "")

    if not host:
        return False

    if (
        "google." in host
        or "gstatic." in host
        or "googleusercontent." in host
        or host in SOCIAL_HOSTS
        or any(host_matches(host, item) for item in SOCIAL_HOSTS)
    ):
        return False

    if not allow_directory and any(
        host_matches(host, domain)
        for domain in DIRECTORY_HOSTS
    ):
        return False

    return True


def business_similarity(
    business_name: str,
    text: str,
) -> float:
    left = normalize_spaces(
        business_name.split("|", 1)[0]
    ).lower()
    right = normalize_spaces(text).lower()

    if not left or not right:
        return 0.0

    if left in right or right in left:
        return 1.0

    return SequenceMatcher(
        None,
        left,
        right,
    ).ratio()


def normalize_social_url(
    url: str,
    network: str,
) -> str:
    url = strip_tracking(url)

    if not url:
        return ""

    parsed = urlparse(url)
    host = parsed.netloc.lower().replace("www.", "")
    path = parsed.path.rstrip("/")

    if network == "facebook":
        if not host_matches(host, "facebook.com"):
            return ""
        if any(
            token in path.lower()
            for token in (
                "/posts/",
                "/photo",
                "/videos/",
                "/reel/",
                "/share/",
            )
        ):
            return ""
        if path and path.count("/") <= 2:
            return "https://www.facebook.com" + path + "/"

    if network == "instagram":
        if not host_matches(host, "instagram.com"):
            return ""
        if any(
            token in path.lower()
            for token in (
                "/p/",
                "/reel/",
                "/stories/",
            )
        ):
            return ""
        if path and path.count("/") <= 1:
            return "https://www.instagram.com" + path + "/"

    return ""


def clean_full_address(value: str) -> str:
    parts = [
        normalize_spaces(part)
        for part in safe(value).split(",")
        if normalize_spaces(part)
    ]

    if parts and parts[-1].lower() in COUNTRY_NAMES:
        parts[-1] = COUNTRY_NAMES[parts[-1].lower()]

    return ", ".join(parts)


def city_from_search_term(search_term: str) -> str:
    value = normalize_spaces(search_term)

    match = re.search(
        r"\bin\s+([A-Za-z .'-]+?)(?:,\s*|\s+)([A-Z]{2})\b",
        value,
        re.I,
    )
    if match:
        return normalize_spaces(match.group(1)).title()

    return ""


def parse_address_components(
    address: str,
    search_term: str = "",
) -> Dict[str, str]:
    cleaned = clean_full_address(address)
    parts = [
        normalize_spaces(part)
        for part in cleaned.split(",")
        if normalize_spaces(part)
    ]

    country = ""
    if parts and parts[-1].lower() in COUNTRY_NAMES:
        country = COUNTRY_NAMES[parts.pop().lower()]

    city = ""
    state = ""
    postal_code = ""

    if parts:
        state_zip = STATE_ZIP_RE.search(parts[-1])
        if state_zip:
            state = state_zip.group(1).upper()
            postal_code = state_zip.group(2)
            if len(parts) >= 2:
                city = parts[-2]

    if not city:
        city = city_from_search_term(search_term)

    return {
        "address": cleaned,
        "city": city,
        "state": state,
        "postal_code": postal_code,
        "country": country,
    }


def infer_industry(search_term: str) -> str:
    value = normalize_spaces(search_term)
    value = re.split(
        r"\s+in\s+",
        value,
        maxsplit=1,
        flags=re.I,
    )[0]
    return value.strip()


def build_maps_search_url(
    business_name: str,
    address: str,
) -> str:
    query = normalize_spaces(
        f"{business_name} {address}"
    )
    if not query:
        return ""
    return (
        "https://www.google.com/maps/search/"
        "?api=1&query=" + quote_plus(query)
    )


def address_from_directions_url(
    directions_url: str,
    business_name: str,
) -> str:
    if "/maps/dir//" not in directions_url:
        return ""

    try:
        segment = directions_url.split(
            "/maps/dir//",
            1,
        )[1]
        segment = segment.split("/data=", 1)[0]
        segment = segment.split("?", 1)[0]
        decoded = normalize_spaces(
            unquote_plus(segment).replace("+", " ")
        )

        parts = [
            normalize_spaces(part)
            for part in decoded.split(",")
            if normalize_spaces(part)
        ]

        if (
            parts
            and business_similarity(
                business_name,
                parts[0],
            ) >= 0.82
        ):
            parts.pop(0)

        candidate = ", ".join(parts)
        if STREET_RE.search(candidate) or STATE_ZIP_RE.search(candidate):
            return clean_full_address(candidate)
    except Exception:
        pass

    return ""


def short_address_from_text(
    text: str,
    business_name: str,
    phone: str,
) -> str:
    for line in safe(text).splitlines():
        line = normalize_spaces(line)
        if not line:
            continue
        if normalize_key(line) in {
            normalize_key(business_name),
            normalize_key(phone),
        }:
            continue

        for piece in re.split(r"\s*[·•|]\s*", line):
            candidate = normalize_spaces(
                PHONE_RE.sub("", piece)
            ).strip(" ,;:-")
            if STREET_RE.search(candidate):
                return candidate

    return ""


# ---------------------------------------------------------------------------
# BeautifulSoup extraction using the current Google tags supplied by user
# ---------------------------------------------------------------------------


def tag_text(tag: Optional[Tag]) -> str:
    if tag is None:
        return ""
    return normalize_spaces(
        tag.get_text(" ", strip=True)
    )


def anchor_target(anchor: Optional[Tag]) -> str:
    if anchor is None:
        return ""
    return strip_tracking(
        resolve_google_target(
            safe(anchor.get("href")),
            safe(anchor.get("ping")),
        )
        or resolve_google_target(
            safe(anchor.get("data-url")),
            safe(anchor.get("ping")),
        )
    )


def soup_anchor_records(
    root: BeautifulSoup | Tag,
) -> List[Dict[str, str]]:
    records: List[Dict[str, str]] = []

    for anchor in root.select("a[href], a[data-url]"):
        url = anchor_target(anchor)
        if not url:
            continue

        records.append(
            {
                "url": url,
                "text": tag_text(anchor),
                "title": safe(anchor.get("title")),
                "aria": safe(anchor.get("aria-label")),
                "cite": tag_text(anchor.select_one("cite")),
                "heading": tag_text(anchor.select_one("h3")),
            }
        )

    return records


def choose_official_website(
    records: Iterable[Dict[str, str]],
    business_name: str,
) -> str:
    records = list(records)

    # Current local card/detail button: descendant text is exactly Website.
    for record in records:
        label = normalize_spaces(
            " ".join(
                [
                    record.get("text", ""),
                    record.get("aria", ""),
                    record.get("title", ""),
                ]
            )
        ).lower()
        if "website" not in label:
            continue

        url = record.get("url", "")
        if is_external_business_website(
            url,
            allow_directory=False,
        ):
            return url

    ranked: List[Tuple[int, str]] = []
    seen: Set[str] = set()

    for record in records:
        url = record.get("url", "")
        if url in seen or not is_external_business_website(url):
            continue
        seen.add(url)

        label = normalize_spaces(
            " ".join(
                [
                    record.get("heading", ""),
                    record.get("text", ""),
                    record.get("cite", ""),
                ]
            )
        )

        similarity = business_similarity(
            business_name,
            label,
        )
        score = int(similarity * 100)

        if record.get("heading"):
            score += 25
        if record.get("cite"):
            score += 20
        if urlparse(url).path in {"", "/"}:
            score += 15

        ranked.append((score, url))

    if not ranked:
        return ""

    ranked.sort(reverse=True)

    # Do not guess a random directory/unrelated URL.
    return ranked[0][1] if ranked[0][0] >= 65 else ""


def social_links_from_records(
    records: Iterable[Dict[str, str]],
    business_name: str,
) -> Tuple[str, str, str, str]:
    facebook = ""
    instagram = ""
    linkedin_person = ""
    linkedin_company = ""

    for record in records:
        url = record.get("url", "")
        label = normalize_spaces(
            " ".join(
                [
                    record.get("heading", ""),
                    record.get("text", ""),
                    record.get("cite", ""),
                ]
            )
        )

        # Bottom web results must belong to the selected business.
        if (
            record.get("heading")
            and business_similarity(
                business_name,
                label,
            ) < 0.55
        ):
            continue

        lower = url.lower()

        if not facebook:
            facebook = normalize_social_url(
                url,
                "facebook",
            )

        if not instagram:
            instagram = normalize_social_url(
                url,
                "instagram",
            )

        if (
            not linkedin_person
            and (
                "linkedin.com/in/" in lower
                or "linkedin.com/pub/" in lower
            )
        ):
            parsed = urlparse(url)
            linkedin_person = (
                "https://www.linkedin.com"
                + parsed.path.rstrip("/")
            )

        if (
            not linkedin_company
            and "linkedin.com/company/" in lower
        ):
            parsed = urlparse(url)
            linkedin_company = (
                "https://www.linkedin.com"
                + parsed.path.rstrip("/")
            )

    return (
        facebook,
        instagram,
        linkedin_person,
        linkedin_company,
    )


def all_links_string(
    records: Iterable[Dict[str, str]],
) -> str:
    output: List[str] = []

    for record in records:
        url = record.get("url", "")
        if (
            url.startswith(("http://", "https://"))
            and url not in output
            and "/aclk" not in url.lower()
        ):
            output.append(url)

    return " | ".join(output)


def parse_listing_card_html(
    html: str,
    search_term: str,
) -> Dict[str, str]:
    soup = BeautifulSoup(html, "html.parser")

    name = tag_text(
        soup.select_one(
            ".rllt__details .dbg0pd .OSrXXb, "
            ".rllt__details .dbg0pd, "
            ".rllt__details [role='heading'], "
            ".dbg0pd .OSrXXb, .dbg0pd"
        )
    )

    card_text = soup.get_text("\n", strip=True)
    phone_match = PHONE_RE.search(card_text)
    phone = normalize_spaces(
        phone_match.group(0)
        if phone_match
        else ""
    )

    records = soup_anchor_records(soup)
    website = choose_official_website(
        records,
        name,
    )

    directions_url = ""
    maps_url = ""

    for record in records:
        url = record["url"]
        label = normalize_spaces(
            record.get("text", "")
            + " "
            + record.get("aria", "")
        ).lower()

        if (
            not directions_url
            and (
                "/maps/dir/" in url.lower()
                or "directions" in label
            )
        ):
            directions_url = url

        if (
            not maps_url
            and is_google_maps_url(url)
            and "/maps/dir/" not in url.lower()
        ):
            maps_url = url

    address = address_from_directions_url(
        directions_url,
        name,
    )

    if not address:
        details = soup.select_one(".rllt__details")
        address = short_address_from_text(
            details.get_text("\n", strip=True)
            if details
            else card_text,
            name,
            phone,
        )

    (
        facebook,
        instagram,
        linkedin_person,
        linkedin_company,
    ) = social_links_from_records(
        records,
        name,
    )

    components = parse_address_components(
        address,
        search_term,
    )

    return {
        "name": name,
        **components,
        "phone": phone,
        "website_link": website,
        "google_maps_url": (
            directions_url
            or maps_url
            or build_maps_search_url(
                name,
                address,
            )
        ),
        "facebook": facebook,
        "instagram": instagram,
        "linkedin_url": linkedin_person,
        "linkedin_company_url": linkedin_company,
        "all_found_links": all_links_string(records),
        "search_term": search_term,
        "industry": infer_industry(search_term),
        "enrichment_status": "",
    }


def parse_detail_html(
    html: str,
    business_name: str,
    search_term: str,
) -> Dict[str, str]:
    soup = BeautifulSoup(html, "html.parser")
    records = soup_anchor_records(soup)

    address = ""
    google_maps_url = ""

    address_anchor = soup.select_one("a.zfFVc")
    if address_anchor is not None:
        address = clean_full_address(
            tag_text(address_anchor)
        )
        google_maps_url = anchor_target(
            address_anchor
        )

    # Exact current Website button and then current bottom web result tags.
    website = choose_official_website(
        records,
        business_name,
    )

    phone = ""
    call_anchor = soup.select_one(
        "a[aria-label^='Call '], "
        "a[aria-label='Call']"
    )
    if call_anchor is not None:
        phone_source = normalize_spaces(
            safe(call_anchor.get("aria-label"))
            + " "
            + tag_text(call_anchor)
        )
        match = PHONE_RE.search(phone_source)
        if match:
            phone = normalize_spaces(match.group(0))

    if not phone:
        match = PHONE_RE.search(
            soup.get_text(" ", strip=True)
        )
        if match:
            phone = normalize_spaces(match.group(0))

    (
        facebook,
        instagram,
        linkedin_person,
        linkedin_company,
    ) = social_links_from_records(
        records,
        business_name,
    )

    directions_url = ""
    maps_url = ""

    for record in records:
        url = record["url"]
        if not directions_url and "/maps/dir/" in url.lower():
            directions_url = url
        if (
            not maps_url
            and is_google_maps_url(url)
            and "/maps/dir/" not in url.lower()
        ):
            maps_url = url

    if not address:
        address = address_from_directions_url(
            directions_url,
            business_name,
        )

    components = parse_address_components(
        address,
        search_term,
    )

    return {
        **components,
        "phone": phone,
        "website_link": website,
        "google_maps_url": (
            google_maps_url
            or directions_url
            or maps_url
        ),
        "facebook": facebook,
        "instagram": instagram,
        "linkedin_url": linkedin_person,
        "linkedin_company_url": linkedin_company,
        "all_found_links": all_links_string(records),
    }


# ---------------------------------------------------------------------------
# Playwright interaction: old running scraper's click-first logic, updated tags
# ---------------------------------------------------------------------------


def handle_google_consent(page: Page) -> None:
    for selector in (
        "button:has-text('Accept all')",
        "button:has-text('I agree')",
        "button:has-text('Accept')",
    ):
        try:
            button = page.locator(selector).first
            if button.count() and button.is_visible():
                button.click(timeout=2500)
                page.wait_for_timeout(1000)
                return
        except Exception:
            continue


def wait_for_listing_details(page: Page) -> Locator:
    page.wait_for_selector(
        "div.rllt__details",
        timeout=60000,
    )
    return page.locator("div.rllt__details:visible")


def wrapper_for_details(details: Locator) -> Locator:
    return details.locator(
        "xpath=ancestor::div["
        "contains(concat(' ',normalize-space(@class),' '),' uMdZh ')"
        "][1]"
    )


def is_sponsored_wrapper(wrapper: Locator) -> bool:
    try:
        text = normalize_spaces(
            wrapper.inner_text(timeout=1500)
        ).lower()
        html = wrapper.evaluate(
            "element => element.outerHTML"
        ).lower()
    except Exception:
        return False

    return (
        "sponsored" in text
        or "aria-label=\"ad\"" in html
        or "data-text-ad" in html
    )


def visible_detail_root(page: Page) -> Optional[Locator]:
    for selector in (
        "#local-place-viewer",
        "div[id='local-place-viewer']",
    ):
        roots = page.locator(selector)
        try:
            for index in range(roots.count()):
                root = roots.nth(index)
                if root.is_visible():
                    return root
        except Exception:
            continue

    # Current full address tag supplied by user.
    addresses = page.locator("a.zfFVc")
    try:
        for index in range(addresses.count()):
            address = addresses.nth(index)
            if not address.is_visible():
                continue

            for xpath in (
                "xpath=ancestor::div[@id='local-place-viewer'][1]",
                "xpath=ancestor::div[contains(@class,'kp-wholepage')][1]",
                "xpath=ancestor::div[@data-attrid][1]",
            ):
                root = address.locator(xpath)
                if root.count() > 0:
                    return root.first
    except Exception:
        pass

    return None


def click_listing_and_wait(
    page: Page,
    details: Locator,
    expected_name: str,
) -> Optional[Locator]:
    wrapper = wrapper_for_details(details)

    click_targets = (
        "div[jsname='ZfUr6c'][role='button']",
        ".rllt__details",
    )

    clicked = False
    for selector in click_targets:
        try:
            target = wrapper.locator(selector).first
            if target.count() and target.is_visible():
                target.click(
                    timeout=6000,
                    force=True,
                )
                clicked = True
                break
        except Exception:
            continue

    if not clicked:
        try:
            details.click(timeout=6000, force=True)
            clicked = True
        except Exception:
            return None

    expected = normalize_key(
        expected_name.split("|", 1)[0]
    )

    for _ in range(40):
        page.wait_for_timeout(250)
        root = visible_detail_root(page)
        if root is None:
            continue

        try:
            root_text = normalize_key(
                root.inner_text(timeout=1200)
            )
        except Exception:
            root_text = ""

        if expected and expected in root_text:
            return root

        try:
            if root.locator("a.zfFVc").count() > 0:
                return root
        except Exception:
            pass

    return visible_detail_root(page)


def scroll_detail_panel(
    page: Page,
    root: Optional[Locator],
) -> None:
    if root is None:
        return

    # Web/social results are often lazy-loaded near the bottom of the panel.
    last_height = -1
    stable_rounds = 0

    for _ in range(8):
        try:
            height = int(
                root.evaluate(
                    "element => element.scrollHeight"
                )
            )
            root.evaluate(
                "element => { element.scrollTop = element.scrollHeight; }"
            )
            page.wait_for_timeout(450)

            if height == last_height:
                stable_rounds += 1
            else:
                stable_rounds = 0
            last_height = height

            if stable_rounds >= 2:
                break
        except Exception:
            break



def detail_html_for_business(
    page: Page,
    root: Locator,
    business_name: str,
) -> str:
    chunks: List[str] = []

    try:
        chunks.append(
            root.evaluate(
                "element => element.outerHTML"
            )
        )
    except Exception:
        pass

    # Current bottom web-result wrapper supplied by user:
    # div.Y8vXRe[data-query="Business Name ..."]
    result_groups = page.locator(
        "div.Y8vXRe[data-query]"
    )

    try:
        count = result_groups.count()
    except Exception:
        count = 0

    for index in range(min(count, 20)):
        group = result_groups.nth(index)

        try:
            data_query = normalize_spaces(
                group.get_attribute(
                    "data-query"
                )
            )

            if business_similarity(
                business_name,
                data_query,
            ) < 0.55:
                continue

            chunks.append(
                group.evaluate(
                    "element => element.outerHTML"
                )
            )
        except Exception:
            continue

    return "\n".join(chunks)

def merge_rows(
    base: Dict[str, str],
    detail: Dict[str, str],
) -> Dict[str, str]:
    output = dict(base)

    for field in (
        "address",
        "city",
        "state",
        "postal_code",
        "country",
        "phone",
        "website_link",
        "google_maps_url",
        "facebook",
        "instagram",
        "linkedin_url",
        "linkedin_company_url",
    ):
        if detail.get(field):
            output[field] = detail[field]

    base_links = [
        item.strip()
        for item in safe(
            base.get("all_found_links")
        ).split("|")
        if item.strip()
    ]
    detail_links = [
        item.strip()
        for item in safe(
            detail.get("all_found_links")
        ).split("|")
        if item.strip()
    ]

    combined: List[str] = []
    for item in base_links + detail_links:
        if item not in combined:
            combined.append(item)
    output["all_found_links"] = " | ".join(combined)

    return output


def extract_listing(
    page: Page,
    details: Locator,
    search_term: str,
) -> Dict[str, str]:
    wrapper = wrapper_for_details(details)

    wrapper_html = wrapper.evaluate(
        "element => element.outerHTML"
    )
    row = parse_listing_card_html(
        wrapper_html,
        search_term,
    )

    if not row["name"]:
        return row

    detail_root = click_listing_and_wait(
        page,
        details,
        row["name"],
    )

    if detail_root is not None:
        scroll_detail_panel(page, detail_root)

        # Parse only the selected detail panel plus matching Y8vXRe web
        # results. Parsing the whole page can steal another card's Website.
        detail = parse_detail_html(
            detail_html_for_business(
                page,
                detail_root,
                row["name"],
            ),
            row["name"],
            search_term,
        )
        row = merge_rows(row, detail)

    if not row.get("city"):
        row["city"] = city_from_search_term(
            search_term
        )

    if not row.get("google_maps_url"):
        row["google_maps_url"] = (
            build_maps_search_url(
                row["name"],
                row.get("address", ""),
            )
        )

    return row


# ---------------------------------------------------------------------------
# CSV and orchestration
# ---------------------------------------------------------------------------


def business_key(row: Dict[str, str]) -> str:
    name = normalize_key(row.get("name", ""))
    phone = normalize_key(row.get("phone", ""))
    address = normalize_key(row.get("address", ""))
    website = normalize_key(row.get("website_link", ""))
    return "|".join(
        [name, phone or address or website]
    )


def ensure_csv_schema(output_csv: str) -> None:
    path = Path(output_csv)
    if not path.exists() or path.stat().st_size == 0:
        return

    with path.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as handle:
        reader = csv.DictReader(handle)
        old_fields = reader.fieldnames or []
        rows = list(reader)

    if old_fields == CSV_FIELDS:
        return

    with path.open(
        "w",
        encoding="utf-8-sig",
        newline="",
    ) as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=CSV_FIELDS,
            extrasaction="ignore",
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    field: safe(row.get(field, ""))
                    for field in CSV_FIELDS
                }
            )


def load_existing_keys(output_csv: str) -> Set[str]:
    """Load every strong identity alias already saved in this CSV."""
    path = Path(output_csv)
    if not path.exists():
        return set()

    ensure_csv_schema(output_csv)
    keys: Set[str] = set()

    with path.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as handle:
        for row in csv.DictReader(handle):
            keys.update(business_aliases(row))

    return keys


def append_row(
    output_csv: str,
    row: Dict[str, str],
) -> None:
    path = Path(output_csv)
    exists = path.exists() and path.stat().st_size > 0

    if exists:
        ensure_csv_schema(output_csv)

    with path.open(
        "a",
        encoding="utf-8-sig",
        newline="",
    ) as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=CSV_FIELDS,
            extrasaction="ignore",
        )
        if not exists:
            writer.writeheader()
        writer.writerow(
            {
                field: safe(row.get(field, ""))
                for field in CSV_FIELDS
            }
        )


LOCAL_RESULTS_PAGE_SIZE = 20


def local_results_page_url(
    search_term: str,
    start: int = 0,
) -> str:
    """Build a deterministic Google local-results page URL.

    Google sometimes omits or hides the visual Next button. The ``start``
    parameter is therefore also used as a safe pagination fallback.
    """
    params = {
        "q": search_term,
        "tbm": "lcl",
    }
    if start > 0:
        params["start"] = str(start)

    return (
        "https://www.google.com/search?"
        + urlencode(params)
    )


def page_start_offset(url: str) -> int:
    try:
        value = (
            parse_qs(
                urlparse(url).query
            )
            .get("start", ["0"])[0]
        )
        return max(int(value), 0)
    except (TypeError, ValueError):
        return 0


def listing_page_signature(page: Page) -> str:
    """Return a stable signature for the currently visible result page."""
    details = page.locator(
        "div.rllt__details:visible"
    )
    names: List[str] = []

    try:
        count = details.count()
    except Exception:
        count = 0

    for index in range(min(count, 30)):
        item = details.nth(index)
        name = ""

        for selector in (
            ".dbg0pd .OSrXXb",
            ".dbg0pd",
            "[role='heading']",
        ):
            try:
                locator = item.locator(
                    selector
                ).first
                if locator.count() > 0:
                    name = normalize_spaces(
                        locator.inner_text(
                            timeout=900
                        )
                    )
                    if name:
                        break
            except Exception:
                continue

        if not name:
            try:
                raw_text = item.inner_text(
                    timeout=900
                )
                name = next(
                    (
                        normalize_spaces(line)
                        for line in raw_text.splitlines()
                        if normalize_spaces(line)
                    ),
                    "",
                )
            except Exception:
                name = ""

        if name:
            names.append(
                normalize_key(name)
            )

    return "|".join(names)


def wait_for_new_local_page(
    page: Page,
    previous_signature: str,
    timeout_ms: int = 25000,
) -> bool:
    deadline = time.monotonic() + (
        max(timeout_ms, 1000) / 1000
    )

    while time.monotonic() < deadline:
        try:
            page.wait_for_selector(
                "div.rllt__details",
                timeout=1500,
            )
        except PlaywrightTimeoutError:
            continue

        signature = listing_page_signature(
            page
        )
        if (
            signature
            and signature != previous_signature
        ):
            return True

        page.wait_for_timeout(350)

    return False


def next_button_candidates(
    page: Page,
    current_start: int,
) -> List[Locator]:
    """Find only forward-pagination controls, never previous/page-number links."""
    candidates: List[Locator] = []

    for selector in (
        "a#pnnext",
        "a[aria-label='Next page']",
        "a[aria-label='Next']",
        "[role='link'][aria-label='Next page']",
        "[role='link'][aria-label='Next']",
        "a:has-text('Next')",
    ):
        locator = page.locator(selector)
        try:
            for index in range(locator.count()):
                candidates.append(
                    locator.nth(index)
                )
        except Exception:
            continue

    # Google can render pagination without #pnnext. In that case choose the
    # smallest href start offset that is greater than the current page.
    href_links = page.locator(
        "a[href*='start=']"
    )
    forward_links: List[Tuple[int, Locator]] = []

    try:
        href_count = href_links.count()
    except Exception:
        href_count = 0

    for index in range(href_count):
        link = href_links.nth(index)
        try:
            href = safe(
                link.get_attribute("href")
            )
            start = page_start_offset(href)
            if start > current_start:
                forward_links.append(
                    (start, link)
                )
        except Exception:
            continue

    forward_links.sort(
        key=lambda item: item[0]
    )
    candidates.extend(
        link
        for _, link in forward_links
    )

    return candidates


def go_to_next_local_results_page(
    *,
    page: Page,
    search_term: str,
    current_start: int,
    previous_signature: str,
) -> Tuple[bool, int]:
    """Advance to the next local-result page.

    First use Google's real Next control when available. If Google hides the
    control, navigate with the local-search ``start`` offset. A signature
    check prevents an infinite loop when Google returns the same first page.
    """
    try:
        page.evaluate(
            "window.scrollTo(0, document.body.scrollHeight)"
        )
        page.wait_for_timeout(700)
    except Exception:
        pass

    for candidate in next_button_candidates(
        page,
        current_start,
    ):
        try:
            if not candidate.is_visible():
                continue

            href = safe(
                candidate.get_attribute("href")
            )
            expected_start = page_start_offset(
                href
            )
            if expected_start <= current_start:
                expected_start = (
                    current_start
                    + LOCAL_RESULTS_PAGE_SIZE
                )

            candidate.scroll_into_view_if_needed(
                timeout=3000
            )
            candidate.click(
                timeout=6000,
                force=True,
            )

            if wait_for_new_local_page(
                page,
                previous_signature,
            ):
                return (
                    True,
                    page_start_offset(
                        page.url
                    ) or expected_start,
                )
        except Exception:
            continue

    # Reliable fallback when the visual Next button is absent/not clickable.
    next_start = (
        current_start
        + LOCAL_RESULTS_PAGE_SIZE
    )
    next_url = local_results_page_url(
        search_term,
        next_start,
    )

    try:
        page.goto(
            next_url,
            wait_until="domcontentloaded",
            timeout=60000,
        )
        handle_google_consent(page)

        if wait_for_new_local_page(
            page,
            previous_signature,
        ):
            return True, next_start
    except Exception:
        pass

    return False, current_start


def scrape_search_term(
    *,
    page: Page,
    search_term: str,
    target_new_records: int,
    output_csv: str,
    maps_delay_seconds: float,
    existing_keys: Set[str],
    state: PipelineState,
) -> int:
    page.goto(
        local_results_page_url(
            search_term,
            0,
        ),
        wait_until="domcontentloaded",
        timeout=60000,
    )
    handle_google_consent(page)

    saved_this_run = 0
    current_start = page_start_offset(
        page.url
    )
    page_number = (
        current_start
        // LOCAL_RESULTS_PAGE_SIZE
    ) + 1
    visited_signatures: Set[str] = set()

    while saved_this_run < target_new_records:
        try:
            details_list = wait_for_listing_details(
                page
            )
        except PlaywrightTimeoutError:
            print(
                "Google local listings did "
                "not load."
            )
            break

        count = details_list.count()
        current_signature = (
            listing_page_signature(page)
        )

        if (
            current_signature
            and current_signature
            in visited_signatures
        ):
            print(
                "Google returned the same local-results "
                "page again; stopping pagination."
            )
            break

        if current_signature:
            visited_signatures.add(
                current_signature
            )

        print(
            f"Local page {page_number} "
            f"(start={current_start}): "
            f"found {count} listings."
        )

        for index in range(count):
            if (
                saved_this_run
                >= target_new_records
            ):
                break

            try:
                # Google mutates the DOM after the detail panel opens,
                # so query the listings again.
                details_list = page.locator(
                    "div.rllt__details:visible"
                )

                if index >= details_list.count():
                    break

                details = details_list.nth(index)
                wrapper = wrapper_for_details(
                    details
                )

                if is_sponsored_wrapper(wrapper):
                    print(
                        "Sponsored listing skip: "
                        f"{index + 1}"
                    )
                    continue

                details.scroll_into_view_if_needed(
                    timeout=4000
                )
                page.wait_for_timeout(350)

                row = extract_listing(
                    page,
                    details,
                    search_term,
                )

                if not row.get("name"):
                    continue

                row_aliases = business_aliases(
                    row
                )

                duplicate_in_file = bool(
                    row_aliases & existing_keys
                )
                duplicate_in_history = (
                    state.is_known_business(row)
                )

                if (
                    duplicate_in_file
                    or duplicate_in_history
                ):
                    print(
                        "Duplicate/history skip: "
                        f"{row['name']}"
                    )

                    # Naye phone/address/place aliases old record se bind ho jayein.
                    state.register_business(row)
                    existing_keys.update(
                        row_aliases
                    )
                    continue

                append_row(
                    output_csv,
                    row,
                )
                state.register_business(row)
                existing_keys.update(
                    row_aliases
                )
                saved_this_run += 1

                print(
                    "Saved new "
                    f"{saved_this_run}/"
                    f"{target_new_records}: "
                    f"{row['name']} | "
                    f"{row.get('website_link') or 'no website'} | "
                    f"{row.get('address') or 'no address'}"
                )

                time.sleep(
                    max(
                        maps_delay_seconds,
                        0,
                    )
                )

            except Exception as exc:
                print(
                    f"Listing {index + 1} "
                    f"error: {exc}"
                )

        if (
            saved_this_run
            >= target_new_records
        ):
            break

        moved, new_start = (
            go_to_next_local_results_page(
                page=page,
                search_term=search_term,
                current_start=current_start,
                previous_signature=(
                    current_signature
                ),
            )
        )

        if not moved:
            print(
                "No more local pages, or Google "
                "did not expose a next page."
            )
            break

        current_start = new_start
        page_number = (
            current_start
            // LOCAL_RESULTS_PAGE_SIZE
        ) + 1
        print(
            "Next local results page open: "
            f"page {page_number}, "
            f"start={current_start}"
        )

    return saved_this_run


def run_maps_scraper(
    input_file: str = "input.txt",
    output_csv: str = "google_maps_data.csv",
    total_results_to_scrape: int = 10,
    headless: bool = False,
    maps_delay_seconds: float = 2,
    resume: bool = True,
    page: Optional[Page] = None,
    browser_context: Optional[
        BrowserContext
    ] = None,
    search_terms: Optional[
        List[str]
    ] = None,
    state_path: str = (
        "pipeline_state.sqlite3"
    ),
) -> str:
    if search_terms is None:
        with open(
            input_file,
            "r",
            encoding="utf-8-sig",
        ) as handle:
            search_terms = [
                normalize_spaces(line)
                for line in handle
                if normalize_spaces(line)
            ]
    else:
        search_terms = [
            normalize_spaces(term)
            for term in search_terms
            if normalize_spaces(term)
        ]

    Path(output_csv).parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    if (
        not resume
        and Path(output_csv).exists()
    ):
        Path(output_csv).unlink()

    existing_keys = (
        load_existing_keys(output_csv)
        if resume
        else set()
    )

    own_playwright = None
    own_browser = None
    own_context = None
    own_page = False

    try:
        if page is None:
            if browser_context is not None:
                page = (
                    browser_context
                    .new_page()
                )
                own_page = True
            else:
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
                page = own_context.new_page()

        with PipelineState(
            state_path
        ) as state:
            for search_term in search_terms:
                print(
                    "\nGoogle Maps/local "
                    "search: "
                    f"{search_term}"
                )

                saved = scrape_search_term(
                    page=page,
                    search_term=search_term,
                    target_new_records=(
                        total_results_to_scrape
                    ),
                    output_csv=output_csv,
                    maps_delay_seconds=(
                        maps_delay_seconds
                    ),
                    existing_keys=(
                        existing_keys
                    ),
                    state=state,
                )

                print(
                    "New unique records saved "
                    "for this term: "
                    f"{saved}"
                )

    finally:
        if (
            own_page
            and page is not None
        ):
            page.close()
        if own_context is not None:
            own_context.close()
        if own_browser is not None:
            own_browser.close()
        if own_playwright is not None:
            own_playwright.stop()

    print(
        "\nGoogle Maps stage "
        f"complete: {output_csv}"
    )

    return output_csv


if __name__ == "__main__":
    run_maps_scraper()
