from __future__ import annotations

import os
import re
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import requests
from dotenv import load_dotenv


load_dotenv()


EMAIL_RE = re.compile(
    r"^[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}$",
    re.I,
)

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


def env_flag(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


@dataclass
class ApiResult:
    ok: bool
    status_code: int = 0
    data: Optional[Dict[str, Any]] = None
    error: str = ""
    inaccessible: bool = False
    rate_limited: bool = False


class ContactOutClient:
    BASE_URL = "https://api.contactout.com"

    def __init__(
        self,
        api_key: Optional[str] = None,
        timeout: int = 45,
    ):
        raw_key = (api_key or os.getenv("CONTACTOUT_API_KEY", ""))
        # Windows .env files and copy/paste can leave invisible CR/TAB/space
        # characters around the token. ContactOut treats that as a bad token.
        self.api_key = re.sub(r"[\r\n\t ]+", "", str(raw_key or ""))

        self.timeout = timeout

        self.capabilities = {
            "decision_makers": True,
            "linkedin_contact": True,
            # Kept separate from "linkedin_contact": these are distinct
            # ContactOut endpoints and can have different plan access. A 403
            # on one must never disable the others (see linkedin_email_status
            # and linkedin_enrich below, and the note on _request()).
            "linkedin_email_status": True,
            "linkedin_enrich": True,
            "people_enrich": True,
            "people_search": True,
            "domain_enrich": True,
        }

        self.session = requests.Session()

        self.session.headers.update(
            {
                "Accept": "application/json",
                "Content-Type": "application/json",
                # ContactOut docs show both token and authorization: basic.
                # The token header is still the actual API key; this header
                # keeps requests aligned with paid-only checker/stats endpoints.
                "authorization": "basic",
                "token": self.api_key,
            }
        )

    @property
    def configured(self) -> bool:
        return bool(self.api_key)

    def _request(
        self,
        method: str,
        path: str,
        capability: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Dict[str, Any]] = None,
    ) -> ApiResult:
        if not self.configured:
            return ApiResult(
                ok=False,
                error="ContactOut API key missing.",
            )

        if not self.capabilities.get(capability, False):
            return ApiResult(
                ok=False,
                error=(
                    "ContactOut capability disabled "
                    f"for this run: {capability}"
                ),
                inaccessible=True,
            )

        response = None
        last_error = ""

        for attempt in range(1, 3):
            try:
                response = self.session.request(
                    method=method,
                    url=f"{self.BASE_URL}{path}",
                    params=params,
                    json=json_body,
                    timeout=self.timeout,
                )
                break
            except requests.RequestException as exc:
                last_error = str(exc)
                if attempt < 2:
                    time.sleep(1.25)

        if response is None:
            return ApiResult(
                ok=False,
                error=(
                    "ContactOut connection error after retry: "
                    f"{last_error}"
                ),
            )

        try:
            payload = response.json()
        except ValueError:
            payload = {
                "raw": response.text[:1500]
            }

        message = str(
            payload.get("message")
            or payload.get("error")
            or payload.get("error_message")
            or response.text[:500]
        )

        if response.status_code in (401, 403):
            # This disables ONLY the specific capability key passed in for
            # this call, not sibling endpoints. Each distinct ContactOut
            # endpoint must use its own capability key (see __init__) so a
            # plan-access 403 on one endpoint (e.g. the free
            # personal_email_status/work_email_status checkers) can't shadow
            # a different endpoint that the API key genuinely has access to
            # (e.g. /v1/people/linkedin, the main email-lookup endpoint).
            self.capabilities[capability] = False

            return ApiResult(
                ok=False,
                status_code=response.status_code,
                data=payload,
                error=message,
                inaccessible=True,
            )

        if response.status_code == 429:
            return ApiResult(
                ok=False,
                status_code=429,
                data=payload,
                error=(
                    message
                    or "ContactOut rate/credit limit reached."
                ),
                rate_limited=True,
            )

        if response.status_code >= 400:
            return ApiResult(
                ok=False,
                status_code=response.status_code,
                data=payload,
                error=message,
            )

        return ApiResult(
            ok=True,
            status_code=response.status_code,
            data=payload,
        )

    def domain_enrich(
        self,
        domain: str,
    ) -> ApiResult:
        if not domain:
            return ApiResult(
                ok=False,
                error="ContactOut domain missing.",
            )

        return self._request(
            "POST",
            "/v1/domain/enrich",
            "domain_enrich",
            json_body={
                "domains": [domain],
            },
        )

    def decision_makers(
        self,
        *,
        domain: str = "",
        company_name: str = "",
        company_linkedin_url: str = "",
    ) -> ApiResult:
        params: Dict[str, Any] = {
            "page": 1,
            "reveal_info": "false",
        }

        if domain:
            params["domain"] = domain

        if company_name:
            params["name"] = company_name

        if company_linkedin_url:
            params["linkedin_url"] = company_linkedin_url

        if not any(
            [
                domain,
                company_name,
                company_linkedin_url,
            ]
        ):
            return ApiResult(
                ok=False,
                error=(
                    "ContactOut Decision Makers "
                    "requires company data."
                ),
            )

        return self._request(
            "GET",
            "/v1/people/decision-makers",
            "decision_makers",
            params=params,
        )

    def linkedin_emails(
        self,
        linkedin_url: str,
    ) -> ApiResult:
        """Return all available personal and work emails for one profile."""
        lower = linkedin_url.lower()
        if (
            "linkedin.com/in/" not in lower
            and "linkedin.com/pub/" not in lower
        ):
            return ApiResult(
                ok=False,
                error=(
                    "Regular LinkedIn person URL required."
                ),
            )

        requested_type = os.getenv(
            "CONTACTOUT_EMAIL_TYPE",
            "personal,work",
        ).strip().lower()

        if requested_type not in {
            "personal",
            "work",
            "personal,work",
            "work,personal",
        }:
            requested_type = "personal,work"

        return self._request(
            "GET",
            "/v1/people/linkedin",
            "linkedin_contact",
            params={
                "profile": linkedin_url,
                "include_phone": "false",
                "email_type": requested_type,
            },
        )

    def linkedin_work_email(
        self,
        linkedin_url: str,
    ) -> ApiResult:
        """Backward-compatible alias. It now requests personal + work."""
        return self.linkedin_emails(linkedin_url)

    def people_enrich(
        self,
        *,
        full_name: str = "",
        first_name: str = "",
        last_name: str = "",
        company_name: str = "",
        company_domain: str = "",
        job_title: str = "",
        location: str = "",
        linkedin_url: str = "",
    ) -> ApiResult:
        payload: Dict[str, Any] = {
            "include": [
                "work_email",
                "personal_email",
            ],
        }

        if full_name:
            payload["full_name"] = full_name

        if first_name:
            payload["first_name"] = first_name

        if last_name:
            payload["last_name"] = last_name

        if company_name:
            payload["company"] = [company_name]

        if company_domain:
            payload["company_domain"] = [company_domain]

        if job_title:
            payload["job_title"] = job_title

        if location:
            payload["location"] = location

        if linkedin_url:
            payload["linkedin_url"] = linkedin_url

        if not any(
            [
                full_name,
                first_name,
                last_name,
                linkedin_url,
            ]
        ):
            return ApiResult(
                ok=False,
                error=(
                    "ContactOut People Enrich "
                    "requires person data."
                ),
            )

        return self._request(
            "POST",
            "/v1/people/enrich",
            "people_enrich",
            json_body=payload,
        )



    def _availability_summary(
        self,
        result: ApiResult,
    ) -> Dict[str, Any]:
        profile = (result.data or {}).get("profile") if isinstance(result.data, dict) else None
        output: Dict[str, Any] = {}
        if isinstance(profile, dict):
            for key in ["email", "email_status", "phone"]:
                if key in profile and isinstance(profile.get(key), (bool, str, int, type(None))):
                    output[key] = profile.get(key)
        return output

    def _attempt_summary(
        self,
        name: str,
        result: ApiResult,
    ) -> Dict[str, Any]:
        parsed = extract_contactout_emails(result)
        item: Dict[str, Any] = {
            "name": name,
            "ok": bool(result.ok),
            "status_code": int(result.status_code or 0),
            "emails": len(parsed.get("all_emails", []) or []),
            "error": (result.error or "")[:220],
        }
        availability = self._availability_summary(result)
        if availability:
            item["availability"] = availability
        return item

    def contact_status(
        self,
        linkedin_url: str,
        kind: str,
    ) -> ApiResult:
        """Check whether ContactOut says email exists without consuming credits."""
        if kind not in {"personal", "work"}:
            return ApiResult(ok=False, error="kind must be personal or work")
        path = f"/v1/people/linkedin/{kind}_email_status"
        return self._request(
            "GET",
            path,
            "linkedin_email_status",
            params={"profile": linkedin_url},
        )

    def api_stats(
        self,
        period: str = "",
    ) -> ApiResult:
        params = {"period": period} if period else None
        return self._request(
            "GET",
            "/v1/stats",
            "linkedin_contact",
            params=params,
        )

    def _merge_email_result(
        self,
        target: Dict[str, Any],
        result: ApiResult,
    ) -> None:
        parsed = extract_contactout_emails(result)
        profile = target.setdefault("profile", {})
        for key, parsed_key in [
            ("work_email", "work_emails"),
            ("personal_email", "personal_emails"),
            ("email", "all_emails"),
        ]:
            existing = profile.setdefault(key, [])
            for email in parsed.get(parsed_key, []) or []:
                if email not in existing:
                    existing.append(email)
        statuses = parsed.get("statuses", {}) or {}
        if statuses:
            current = profile.setdefault("work_email_status", {})
            if isinstance(current, dict):
                current.update(statuses)

    def _result_email_count(self, result: ApiResult) -> int:
        return len(extract_contactout_emails(result).get("all_emails", []) or [])

    def linkedin_profile_emails(
        self,
        linkedin_url: str,
        *,
        full_name: str = "",
        company_name: str = "",
        company_domain: str = "",
    ) -> ApiResult:
        """
        Exhaustive but safe ContactOut API lookup for one verified LinkedIn URL.

        The old working endpoint is tried first. If it returns an empty 200,
        official ContactOut fallbacks are tried and all email fields are merged.
        This function does not read Facebook, website, or browser-extension data.
        """
        lower = linkedin_url.lower()
        if (
            "linkedin.com/in/" not in lower
            and "linkedin.com/pub/" not in lower
        ):
            return ApiResult(ok=False, error="Regular LinkedIn person URL required.")

        requested_type = os.getenv("CONTACTOUT_EMAIL_TYPE", "personal,work").strip().lower()
        if requested_type not in {"personal", "work", "personal,work", "work,personal"}:
            requested_type = "personal,work"

        merged: Dict[str, Any] = {
            "status_code": 200,
            "profile": {
                "url": linkedin_url,
                "email": [],
                "work_email": [],
                "personal_email": [],
                "work_email_status": {},
            },
            "attempts": [],
        }
        attempts: List[Dict[str, Any]] = merged["attempts"]
        last_error = ""
        last_status = 0

        def record(name: str, result: ApiResult) -> bool:
            nonlocal last_error, last_status
            attempts.append(self._attempt_summary(name, result))
            last_status = result.status_code or last_status
            if result.error:
                last_error = result.error
            if result.ok:
                self._merge_email_result(merged, result)
            return self._result_email_count(ApiResult(ok=True, status_code=200, data=merged)) > 0

        # 0) Free availability checks. These do not return the address, but
        # they prove whether ContactOut API says the profile has an email. If
        # these are false while the Chrome extension shows an email, the issue
        # is account/API dataset mismatch, not CSV/Facebook overwrite.
        personal_status = self.contact_status(linkedin_url, "personal")
        record("personal_email_status", personal_status)
        work_status = self.contact_status(linkedin_url, "work")
        record("work_email_status", work_status)

        # 1) Old working direct endpoint, exactly first.
        direct = self._request(
            "GET",
            "/v1/people/linkedin",
            "linkedin_contact",
            params={
                "profile": linkedin_url,
                "include_phone": "false",
                "email_type": requested_type,
            },
        )
        if record("people_linkedin_legacy", direct):
            return ApiResult(ok=True, status_code=direct.status_code, data=merged)

        if not env_flag("CONTACTOUT_EXHAUSTIVE_LOOKUP", True):
            return ApiResult(ok=True, status_code=last_status or 200, data=merged, error=last_error)

        # 2) Same endpoint without email_type. Docs say default returns both.
        default = self._request(
            "GET",
            "/v1/people/linkedin",
            "linkedin_contact",
            params={
                "profile": linkedin_url,
                "include_phone": "false",
            },
        )
        if record("people_linkedin_default", default):
            return ApiResult(ok=True, status_code=default.status_code, data=merged)

        # 3) Work-only can trigger real-time work email verification.
        work = self._request(
            "GET",
            "/v1/people/linkedin",
            "linkedin_contact",
            params={
                "profile": linkedin_url,
                "include_phone": "false",
                "email_type": "work",
            },
        )
        if record("people_linkedin_work_realtime", work):
            return ApiResult(ok=True, status_code=work.status_code, data=merged)

        # 4) Personal-only can expose personal emails separately on some accounts.
        personal = self._request(
            "GET",
            "/v1/people/linkedin",
            "linkedin_contact",
            params={
                "profile": linkedin_url,
                "include_phone": "false",
                "email_type": "personal",
            },
        )
        if record("people_linkedin_personal", personal):
            return ApiResult(ok=True, status_code=personal.status_code, data=merged)

        # 5) LinkedIn Profile API may return email/work_email/personal_email fields.
        enrich = self._request(
            "GET",
            "/v1/linkedin/enrich",
            "linkedin_enrich",
            params={
                "profile": linkedin_url,
                "profile_only": "false",
            },
        )
        if record("linkedin_enrich", enrich):
            return ApiResult(ok=True, status_code=enrich.status_code, data=merged)

        # 6) People Enrich endpoint with LinkedIn URL and include flags.
        people_enrich_result = self.people_enrich(
            full_name=full_name,
            company_name=company_name,
            company_domain=company_domain,
            linkedin_url=linkedin_url,
        )
        if record("people_enrich", people_enrich_result):
            return ApiResult(ok=True, status_code=people_enrich_result.status_code, data=merged)

        # 7) V1 bulk endpoint can return a different email-only shape.
        if env_flag("CONTACTOUT_V1_BATCH_FALLBACK", True):
            batch = self._request(
                "POST",
                "/v1/people/linkedin/batch",
                "linkedin_contact",
                json_body={
                    "profiles": [linkedin_url],
                    "include_phone": False,
                    "email_type": requested_type,
                },
            )
            if record("people_linkedin_batch_v1", batch):
                return ApiResult(ok=True, status_code=batch.status_code, data=merged)

        # 8) People Search fallback with name + company/domain, strict identity.
        if env_flag("CONTACTOUT_PEOPLE_SEARCH_FALLBACK", True) and full_name:
            search_result = self.people_search(
                full_name=full_name,
                company_name=company_name,
                company_domain=company_domain,
                page_size=int(os.getenv("CONTACTOUT_SEARCH_PAGE_SIZE", "5") or "5"),
            )
            if search_result.ok:
                selected = pick_search_profile(
                    search_result.data or {},
                    linkedin_url=linkedin_url,
                    full_name=full_name,
                    company_name=company_name,
                    company_domain=company_domain,
                )
                if selected:
                    search_result = ApiResult(
                        ok=True,
                        status_code=search_result.status_code,
                        data={"profile": selected},
                    )
            if record("people_search_identity", search_result):
                return ApiResult(ok=True, status_code=search_result.status_code, data=merged)

        return ApiResult(ok=True, status_code=last_status or 200, data=merged, error=last_error)

    def people_search(
        self,
        *,
        full_name: str,
        company_name: str = "",
        company_domain: str = "",
        page_size: int = 5,
    ) -> ApiResult:
        if not full_name:
            return ApiResult(ok=False, error="ContactOut People Search requires a name.")

        body: Dict[str, Any] = {
            "page": 1,
            "page_size": max(1, min(page_size, 25)),
            "name": full_name,
            "data_types": ["personal_email", "work_email"],
            "reveal_info": True,
            "detailed_experience": True,
            "detailed_education": False,
        }
        if company_name:
            body["company"] = [company_name]
            body["company_filter"] = "current"
        if company_domain:
            domain = company_domain.strip().lower()
            if domain:
                body["domain"] = [domain]

        return self._request(
            "POST",
            "/v1/people/search",
            "people_search",
            json_body=body,
        )


def extract_contactout_profiles(
    result: ApiResult,
) -> List[Dict[str, Any]]:
    if not result.ok or not result.data:
        return []

    profiles = result.data.get("profiles") or {}

    if isinstance(profiles, list):
        return [
            profile
            for profile in profiles
            if isinstance(profile, dict)
        ]

    output: List[Dict[str, Any]] = []

    if isinstance(profiles, dict):
        for linkedin_url, profile in profiles.items():
            if not isinstance(profile, dict):
                continue

            item = dict(profile)
            item.setdefault("linkedin_url", linkedin_url)
            output.append(item)

    return output



def _norm_text(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", " ", str(value or "").lower()).strip()


def _norm_linkedin(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    if raw.startswith("//"):
        raw = "https:" + raw
    if not raw.startswith(("http://", "https://")):
        raw = "https://" + raw
    raw = raw.split("?", 1)[0].rstrip("/")
    match = re.search(r"linkedin\.com/(?:in|pub)/([^/?#]+)", raw, flags=re.I)
    if not match:
        return ""
    slug = match.group(1).strip().lower()
    return "https://www.linkedin.com/in/" + slug


def _text_match_ratio(a: str, b: str) -> float:
    from difflib import SequenceMatcher
    a_norm = _norm_text(a)
    b_norm = _norm_text(b)
    if not a_norm or not b_norm:
        return 0.0
    if a_norm == b_norm:
        return 1.0
    return SequenceMatcher(None, a_norm, b_norm).ratio()


def _profile_company_text(profile: Dict[str, Any]) -> str:
    pieces: List[str] = []
    for key in ["headline", "title", "company_name", "companyName", "location"]:
        if profile.get(key):
            pieces.append(str(profile.get(key)))
    company = profile.get("company")
    if isinstance(company, dict):
        for key in ["name", "domain", "email_domain", "website", "url"]:
            if company.get(key):
                pieces.append(str(company.get(key)))
    elif isinstance(company, str):
        pieces.append(company)
    exp = profile.get("experience") or profile.get("experiences")
    if isinstance(exp, list):
        for item in exp[:5]:
            if isinstance(item, dict):
                for key in ["company_name", "companyName", "domain", "title", "summary"]:
                    if item.get(key):
                        pieces.append(str(item.get(key)))
            else:
                pieces.append(str(item))
    return " ".join(pieces)


def _company_matches(profile: Dict[str, Any], company_name: str, company_domain: str) -> bool:
    text = _norm_text(_profile_company_text(profile))
    company_norm = _norm_text(company_name)
    domain_norm = _norm_text(str(company_domain or "").replace("www.", ""))
    if company_norm and (company_norm in text or _text_match_ratio(company_norm, text) >= 0.72):
        return True
    if domain_norm and domain_norm in text:
        return True
    # Use distinctive company/domain token fallback.
    tokens = [t for t in company_norm.split() if len(t) >= 5 and t not in {"company", "service", "services", "repair", "auto", "truck", "llc", "inc"}]
    if tokens and any(t in text for t in tokens):
        return True
    return False


def pick_search_profile(
    data: Dict[str, Any],
    *,
    linkedin_url: str,
    full_name: str,
    company_name: str,
    company_domain: str,
) -> Optional[Dict[str, Any]]:
    """Pick only a strict identity match from ContactOut People Search."""
    target_url = _norm_linkedin(linkedin_url)
    profiles = data.get("profiles") if isinstance(data, dict) else None
    candidates: List[Dict[str, Any]] = []
    if isinstance(profiles, dict):
        for url, profile in profiles.items():
            if not isinstance(profile, dict):
                continue
            item = dict(profile)
            item.setdefault("linkedin_url", url)
            item.setdefault("url", url)
            candidates.append(item)
    elif isinstance(profiles, list):
        candidates = [p for p in profiles if isinstance(p, dict)]

    # Exact LinkedIn URL match is safest and does not need company proof.
    for profile in candidates:
        possible_urls = [
            profile.get("linkedin_url"), profile.get("linkedinUrl"),
            profile.get("url"), profile.get("li_vanity"),
        ]
        for raw in possible_urls:
            if _norm_linkedin(raw) and _norm_linkedin(raw) == target_url:
                return profile

    # Otherwise require strong name + company/domain evidence.
    for profile in candidates:
        name = profile.get("full_name") or profile.get("fullName") or profile.get("name") or ""
        if _text_match_ratio(str(name), full_name) < 0.88:
            continue
        if _company_matches(profile, company_name, company_domain):
            return profile
    return None


def _normalize_email(value: Any) -> str:
    email = str(value or "").strip().lower().strip(
        ".,;:()[]{}<>\\\"'"
    )

    if not EMAIL_RE.fullmatch(email):
        return ""

    local_part, domain = email.rsplit("@", 1)

    if (
        email in PLACEHOLDER_EMAILS
        or PLACEHOLDER_LOCAL_RE.fullmatch(local_part)
        or domain in {"domain.com", "yourdomain.com"}
        or "example" in domain
        or "placeholder" in email
    ):
        return ""

    return email


def _dedupe(values: List[str]) -> List[str]:
    output: List[str] = []
    seen = set()

    for value in values:
        normalized = _normalize_email(value)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        output.append(normalized)

    return output


def _emails_from_value(value: Any) -> List[str]:
    """Normalize ContactOut string/list/dict email response variants."""
    if value is None:
        return []

    if isinstance(value, str):
        return [value]

    if isinstance(value, (list, tuple, set)):
        output: List[str] = []
        for item in value:
            output.extend(_emails_from_value(item))
        return output

    if isinstance(value, dict):
        output: List[str] = []

        for key in (
            "email",
            "address",
            "value",
            "emails",
        ):
            if key in value:
                output.extend(_emails_from_value(value.get(key)))

        # Some ContactOut shapes use email addresses as dictionary keys,
        # for example {"person@company.com": "Verified"}.
        for key in value.keys():
            if _normalize_email(key):
                output.append(str(key))

        return output

    return []


def _profile_containers(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Return only known response wrappers; do not scan company/experience."""
    output: List[Dict[str, Any]] = []
    seen = set()

    def add(value: Any) -> None:
        if not isinstance(value, dict):
            return
        marker = id(value)
        if marker in seen:
            return
        seen.add(marker)
        output.append(value)

    add(data)
    add(data.get("profile"))
    add(data.get("data"))

    nested_data = data.get("data")
    if isinstance(nested_data, dict):
        add(nested_data.get("profile"))
        add(nested_data.get("data"))

    return output


def _values_for_keys(
    containers: List[Dict[str, Any]],
    keys: set[str],
) -> List[Any]:
    values: List[Any] = []

    for container in containers:
        for key, value in container.items():
            if str(key).lower() in keys:
                values.append(value)

    return values


def extract_contactout_emails(
    result: ApiResult,
) -> Dict[str, Any]:
    """
    Parse personal, work, and combined email fields from every official
    ContactOut response shape used by:
    - /v1/people/linkedin
    - /v1/linkedin/enrich
    - /v1/people/enrich
    - /v1/people/linkedin/batch
    - /v2/people/linkedin/batch
    - /v1/people/search contact_info
    """
    empty: Dict[str, Any] = {
        "primary_email": "",
        "primary_type": "",
        "primary_status": "",
        "work_emails": [],
        "personal_emails": [],
        "other_emails": [],
        "all_emails": [],
        "statuses": {},
    }

    if not result.ok or not result.data:
        return empty

    work_keys = {
        "work_email", "work_emails", "workemail", "workemails",
    }
    personal_keys = {
        "personal_email", "personal_emails", "personalemail", "personalemails",
    }
    combined_keys = {
        "email", "emails", "all_email", "all_emails", "allemail", "allemails",
    }
    status_keys = {
        "work_email_status", "work_email_statuses", "workemailstatus",
        "workemailstatuses", "email_status", "email_statuses", "emailstatus",
        "emailstatuses", "workEmailStatus",
    }

    work_raw: List[str] = []
    personal_raw: List[str] = []
    combined_raw: List[str] = []
    statuses: Dict[str, str] = {}
    seen: set[int] = set()

    def walk(value: Any, parent_key: str = "") -> None:
        marker = id(value)
        if isinstance(value, (dict, list, tuple, set)):
            if marker in seen:
                return
            seen.add(marker)

        key_norm = str(parent_key or "").lower()
        if key_norm in work_keys:
            work_raw.extend(_emails_from_value(value))
        elif key_norm in personal_keys:
            personal_raw.extend(_emails_from_value(value))
        elif key_norm in combined_keys:
            combined_raw.extend(_emails_from_value(value))
        elif key_norm in status_keys:
            if isinstance(value, dict):
                for raw_email, raw_status in value.items():
                    email = _normalize_email(raw_email)
                    if email:
                        statuses[email] = str(raw_status or "").strip()
            elif isinstance(value, str):
                # Applied later if exactly one work email exists.
                statuses.setdefault("__single_status__", value.strip())

        # V1 bulk can return profiles: {url: ["email@..."]}; parent key is URL.
        if isinstance(parent_key, str) and "linkedin.com/" in parent_key.lower():
            combined_raw.extend(_emails_from_value(value))

        if isinstance(value, dict):
            # Dictionaries can use emails as keys: {"person@company.com": "Verified"}.
            for raw_key, child in value.items():
                email_key = _normalize_email(raw_key)
                if email_key:
                    combined_raw.append(email_key)
                    if isinstance(child, str):
                        statuses[email_key] = child.strip()
                walk(child, str(raw_key))
        elif isinstance(value, (list, tuple, set)):
            for item in value:
                walk(item, parent_key)
        elif isinstance(value, str):
            # Only extract bare strings from explicit email-ish parents, not from
            # arbitrary summaries/headlines where false positives are possible.
            if key_norm in work_keys | personal_keys | combined_keys:
                combined_raw.extend(_emails_from_value(value))

    walk(result.data)

    work_emails = _dedupe(work_raw)
    personal_emails = _dedupe(personal_raw)
    combined_emails = _dedupe(combined_raw)

    classified = set(work_emails) | set(personal_emails)
    other_emails = [
        email for email in combined_emails if email not in classified
    ]

    all_emails = _dedupe(work_emails + personal_emails + combined_emails)

    if "__single_status__" in statuses and len(work_emails) == 1:
        statuses[work_emails[0]] = statuses.pop("__single_status__")
    else:
        statuses.pop("__single_status__", None)

    primary_email = ""
    primary_type = ""
    if work_emails:
        primary_email = work_emails[0]
        primary_type = "Work"
    elif personal_emails:
        primary_email = personal_emails[0]
        primary_type = "Personal"
    elif other_emails:
        primary_email = other_emails[0]
        primary_type = "Other"

    return {
        "primary_email": primary_email,
        "primary_type": primary_type,
        "primary_status": statuses.get(primary_email, ""),
        "work_emails": work_emails,
        "personal_emails": personal_emails,
        "other_emails": other_emails,
        "all_emails": all_emails,
        "statuses": statuses,
    }


def extract_contactout_work_email(
    result: ApiResult,
) -> Dict[str, str]:
    """Backward-compatible wrapper around the complete email parser."""
    parsed = extract_contactout_emails(result)

    email = (
        parsed["work_emails"][0]
        if parsed["work_emails"]
        else parsed["primary_email"]
    )

    return {
        "email": email,
        "status": parsed["statuses"].get(
            email,
            parsed["primary_status"],
        ),
    }
