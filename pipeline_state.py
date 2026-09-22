from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from contextlib import nullcontext
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Set
from urllib.parse import parse_qs, urlparse


def safe(value: object) -> str:
    return str(value or "").strip()


def normalize_key(value: object) -> str:
    return re.sub(r"[^a-z0-9]+", "", safe(value).lower())


def normalize_phone(value: object) -> str:
    digits = re.sub(r"\D+", "", safe(value))
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    return digits if len(digits) >= 10 else ""


def clean_domain(value: object) -> str:
    url = safe(value)
    if not url:
        return ""
    if "://" not in url:
        url = "https://" + url
    host = urlparse(url).netloc.lower().split(":", 1)[0]
    return host[4:] if host.startswith("www.") else host


def maps_place_id(row: Dict[str, Any]) -> str:
    text = " | ".join(
        [
            safe(row.get("google_maps_url")),
            safe(row.get("Google Maps URL")),
            safe(row.get("all_found_links")),
        ]
    )

    for pattern in [
        r"[?&]ftid=([^&|\s]+)",
        r"!1s(0x[0-9a-f]+:0x[0-9a-f]+)",
        r"\b(0x[0-9a-f]+:0x[0-9a-f]+)\b",
    ]:
        match = re.search(pattern, text, re.I)
        if match:
            return match.group(1).lower()

    for raw in [
        safe(row.get("google_maps_url")),
        safe(row.get("Google Maps URL")),
    ]:
        if not raw:
            continue
        try:
            query = parse_qs(urlparse(raw).query)
            for key in ("ftid", "query_place_id", "place_id"):
                if query.get(key):
                    return safe(query[key][0]).lower()
        except Exception:
            continue

    return ""


def business_aliases(row: Dict[str, Any]) -> Set[str]:
    name = normalize_key(row.get("name") or row.get("Company"))
    address = normalize_key(row.get("address") or row.get("Business Address"))
    city = normalize_key(row.get("city") or row.get("City"))
    phone = normalize_phone(row.get("phone") or row.get("Phone"))
    domain = clean_domain(row.get("website_link") or row.get("Website"))
    place_id = maps_place_id(row)

    aliases: Set[str] = set()

    if place_id:
        aliases.add("place:" + place_id)
    if phone:
        aliases.add("phone:" + phone)
    if name and address:
        aliases.add(f"name_address:{name}|{address}")
    if name and domain and city:
        aliases.add(f"name_domain_city:{name}|{domain}|{city}")
    if name and domain and not address:
        aliases.add(f"name_domain:{name}|{domain}")
    if name and city and not any([phone, domain, address, place_id]):
        aliases.add(f"name_city:{name}|{city}")
    if name and not aliases:
        aliases.add("name:" + name)

    return aliases


def canonical_id_for(row: Dict[str, Any]) -> str:
    aliases = sorted(business_aliases(row))
    raw = aliases[0] if aliases else json.dumps(
        row,
        sort_keys=True,
        ensure_ascii=False,
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class PipelineState:
    """Persistent duplicate tracking and enrichment cache."""

    def __init__(
        self,
        path: str = "pipeline_state.sqlite3",
    ) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(str(self.path))
        self.connection.row_factory = sqlite3.Row
        # While True, writes share one transaction instead of one commit
        # (disk sync) per row. Used for the CSV history import.
        self._batching = False
        self._initialize()

    def _tx(self):
        return nullcontext() if self._batching else self.connection

    def _initialize(self) -> None:
        with self.connection:
            self.connection.execute(
                """
                CREATE TABLE IF NOT EXISTS businesses (
                    canonical_id TEXT PRIMARY KEY,
                    name TEXT,
                    phone TEXT,
                    domain TEXT,
                    address TEXT,
                    city TEXT,
                    search_term TEXT,
                    first_seen_at TEXT NOT NULL,
                    last_seen_at TEXT NOT NULL
                )
                """
            )
            self.connection.execute(
                """
                CREATE TABLE IF NOT EXISTS business_aliases (
                    alias TEXT PRIMARY KEY,
                    canonical_id TEXT NOT NULL,
                    FOREIGN KEY(canonical_id)
                        REFERENCES businesses(canonical_id)
                )
                """
            )
            self.connection.execute(
                """
                CREATE TABLE IF NOT EXISTS enrichment_cache (
                    canonical_id TEXT PRIMARY KEY,
                    row_json TEXT NOT NULL,
                    output_csv TEXT,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY(canonical_id)
                        REFERENCES businesses(canonical_id)
                )
                """
            )

    def close(self) -> None:
        self.connection.close()

    def __enter__(self) -> "PipelineState":
        return self

    def __exit__(
        self,
        exc_type,
        exc,
        traceback,
    ) -> None:
        self.close()

    def find_business_id(
        self,
        row: Dict[str, Any],
    ) -> Optional[str]:
        aliases = business_aliases(row)
        if not aliases:
            return None

        placeholders = ",".join("?" for _ in aliases)
        found = self.connection.execute(
            (
                "SELECT canonical_id "
                "FROM business_aliases "
                f"WHERE alias IN ({placeholders}) "
                "LIMIT 1"
            ),
            tuple(aliases),
        ).fetchone()

        return (
            safe(found["canonical_id"])
            if found
            else None
        )

    def register_business(
        self,
        row: Dict[str, Any],
    ) -> str:
        aliases = business_aliases(row)
        existing_id = self.find_business_id(row)
        canonical_id = (
            existing_id
            or canonical_id_for(row)
        )
        now = utc_now()

        with self._tx():
            self.connection.execute(
                """
                INSERT INTO businesses (
                    canonical_id,
                    name,
                    phone,
                    domain,
                    address,
                    city,
                    search_term,
                    first_seen_at,
                    last_seen_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(canonical_id)
                DO UPDATE SET
                    name=excluded.name,
                    phone=excluded.phone,
                    domain=excluded.domain,
                    address=excluded.address,
                    city=excluded.city,
                    search_term=excluded.search_term,
                    last_seen_at=excluded.last_seen_at
                """,
                (
                    canonical_id,
                    safe(
                        row.get("name")
                        or row.get("Company")
                    ),
                    safe(
                        row.get("phone")
                        or row.get("Phone")
                    ),
                    clean_domain(
                        row.get("website_link")
                        or row.get("Website")
                    ),
                    safe(
                        row.get("address")
                        or row.get("Business Address")
                    ),
                    safe(
                        row.get("city")
                        or row.get("City")
                    ),
                    safe(
                        row.get("search_term")
                        or row.get("Industry")
                    ),
                    now,
                    now,
                ),
            )

            for alias in aliases:
                self.connection.execute(
                    """
                    INSERT OR IGNORE INTO business_aliases(
                        alias,
                        canonical_id
                    )
                    VALUES (?, ?)
                    """,
                    (alias, canonical_id),
                )

        return canonical_id

    def is_known_business(
        self,
        row: Dict[str, Any],
    ) -> bool:
        return self.find_business_id(row) is not None

    def get_cached_enrichment(
        self,
        row: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        canonical_id = self.find_business_id(row)
        if not canonical_id:
            return None

        found = self.connection.execute(
            """
            SELECT row_json
            FROM enrichment_cache
            WHERE canonical_id = ?
            """,
            (canonical_id,),
        ).fetchone()

        if not found:
            return None

        try:
            data = json.loads(found["row_json"])
            return data if isinstance(data, dict) else None
        except (TypeError, json.JSONDecodeError):
            return None

    def save_enrichment(
        self,
        business_row: Dict[str, Any],
        enriched_row: Dict[str, Any],
        output_csv: str = "",
        overwrite: bool = True,
    ) -> None:
        canonical_id = self.register_business(
            business_row
        )
        payload = json.dumps(
            enriched_row,
            ensure_ascii=False,
            sort_keys=True,
        )

        with self._tx():
            if overwrite:
                self.connection.execute(
                    """
                    INSERT INTO enrichment_cache(
                        canonical_id,
                        row_json,
                        output_csv,
                        updated_at
                    )
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(canonical_id)
                    DO UPDATE SET
                        row_json=excluded.row_json,
                        output_csv=excluded.output_csv,
                        updated_at=excluded.updated_at
                    """,
                    (
                        canonical_id,
                        payload,
                        output_csv,
                        utc_now(),
                    ),
                )
            else:
                self.connection.execute(
                    """
                    INSERT OR IGNORE INTO enrichment_cache(
                        canonical_id,
                        row_json,
                        output_csv,
                        updated_at
                    )
                    VALUES (?, ?, ?, ?)
                    """,
                    (
                        canonical_id,
                        payload,
                        output_csv,
                        utc_now(),
                    ),
                )


    def bootstrap_csv(
        self,
        csv_path: str,
    ) -> int:
        """Import legacy/current CSV rows into duplicate history and cache."""
        import csv

        path = Path(csv_path)
        if (
            not path.exists()
            or path.stat().st_size == 0
        ):
            return 0

        imported = 0
        self._batching = True

        try:
            with path.open(
                "r",
                encoding="utf-8-sig",
                newline="",
            ) as handle:
                reader = csv.DictReader(
                    handle
                )
                fields = set(
                    reader.fieldnames
                    or []
                )
                enriched_file = bool(
                    {
                        "Company",
                        "Email",
                        "Lead Status",
                    }
                    & fields
                )

                for row in reader:
                    if not (
                        safe(
                            row.get("name")
                            or row.get("Company")
                        )
                    ):
                        continue

                    self.register_business(
                        row
                    )

                    if enriched_file:
                        self.save_enrichment(
                            row,
                            row,
                            str(path),
                            overwrite=False,
                        )

                    imported += 1

        except (OSError, csv.Error):
            return imported
        finally:
            self._batching = False
            self.connection.commit()

        return imported
