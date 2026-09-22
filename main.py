from __future__ import annotations

import argparse
import csv
import os
import re
import sys
import unicodedata
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Sequence, Tuple

from dotenv import load_dotenv

from browser_session import SharedChromiumSession
from google_maps_scraper import run_maps_scraper
from lead_enricher import enrich_csv
from pipeline_state import PipelineState


DEFAULT_MAPS_OUTPUT = "google_maps_data.csv"
DEFAULT_ENRICHED_OUTPUT = "enriched_leads.csv"
DEFAULT_OUTPUT_DIR = "outputs"


TRUTHY = {"1", "true", "yes", "y", "on"}
FALSY = {"0", "false", "no", "n", "off"}


def str_to_bool(value: object) -> bool:
    return str(value).strip().lower() in TRUTHY


def env_int(name: str, default: Optional[int] = None) -> Optional[int]:
    value = os.getenv(name)
    if value is None or str(value).strip() == "":
        return default
    try:
        return int(str(value).strip())
    except ValueError:
        return default


def read_search_terms(input_file: str) -> List[str]:
    with open(input_file, "r", encoding="utf-8-sig") as handle:
        raw_terms = [re.sub(r"\s+", " ", line).strip() for line in handle]

    # Preserve order but ignore blank/repeated lines.
    return list(dict.fromkeys(term for term in raw_terms if term))


def read_search_terms_or_prompt(input_file: str, interactive: bool) -> List[str]:
    path = Path(input_file)
    if path.exists():
        terms = read_search_terms(input_file)
        if terms:
            return terms

    if not interactive:
        if not path.exists():
            raise FileNotFoundError(f"Input file not found: {input_file}")
        raise ValueError("input.txt has no search terms.")

    print(f"\n{input_file} is missing or empty.")
    print("Enter one or more search terms. Press Enter on a blank line to finish.")
    terms: List[str] = []
    while True:
        value = input("Search term: ").strip()
        if not value:
            break
        if value not in terms:
            terms.append(value)

    if not terms:
        raise ValueError("No search term was entered.")

    should_save = prompt_yes_no(f"Save these terms to {input_file}?", True)
    if should_save:
        path.write_text("\n".join(terms) + "\n", encoding="utf-8")

    return terms


def slugify_search_term(value: str) -> str:
    normalized = (
        unicodedata.normalize("NFKD", value)
        .encode("ascii", "ignore")
        .decode("ascii")
        .lower()
    )
    slug = re.sub(r"[^a-z0-9]+", "_", normalized).strip("_")
    return slug[:90] or "search_results"


def output_paths_for_term(
    *,
    search_term: str,
    output_dir: str,
    explicit_maps_output: str,
    explicit_enriched_output: str,
    single_term: bool,
) -> Tuple[str, str]:
    """Legacy helper kept for older tests/imports.

    The current runner intentionally writes all input.txt terms into one Maps
    CSV and one enriched CSV. This helper still returns the old per-term paths
    if external code imports it.
    """
    output_root = Path(output_dir)
    output_root.mkdir(parents=True, exist_ok=True)

    slug = slugify_search_term(search_term)
    maps_output = (
        explicit_maps_output
        if (single_term and explicit_maps_output)
        else str(output_root / f"{slug}_google_maps.csv")
    )
    enriched_output = (
        explicit_enriched_output
        if (single_term and explicit_enriched_output)
        else str(output_root / f"{slug}_enriched_leads.csv")
    )
    return maps_output, enriched_output


def default_output_path(explicit_value: str, fallback_filename: str) -> str:
    return explicit_value.strip() if explicit_value and explicit_value.strip() else fallback_filename


def timestamped_csv_path(path_value: str) -> str:
    path = Path(path_value)
    suffix = path.suffix or ".csv"
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return str(path.with_name(f"{path.stem}_{stamp}{suffix}"))


def count_csv_rows(path_value: str) -> int:
    path = Path(path_value)
    if not path.exists():
        return 0
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            return sum(1 for _ in csv.DictReader(handle))
    except Exception:
        return 0


def prompt_text(message: str, default: str = "") -> str:
    suffix = f" [{default}]" if default else ""
    value = input(f"{message}{suffix}: ").strip()
    return value or default


def prompt_int(message: str, default: int, minimum: int = 1) -> int:
    while True:
        value = input(f"{message} [{default}]: ").strip()
        if not value:
            return default
        try:
            number = int(value)
        except ValueError:
            print("Enter a number, for example: 100")
            continue
        if number < minimum:
            print(f"The minimum is {minimum}.")
            continue
        return number


def prompt_yes_no(message: str, default: bool = True) -> bool:
    default_label = "Y/n" if default else "y/N"
    while True:
        value = input(f"{message} [{default_label}]: ").strip().lower()
        if not value:
            return default
        if value in TRUTHY:
            return True
        if value in FALSY:
            return False
        print("Please enter Y or N.")


def prompt_choice(message: str, choices: Sequence[Tuple[str, str]], default: str) -> str:
    valid = {key for key, _ in choices}
    print(message)
    for key, label in choices:
        marker = " (default)" if key == default else ""
        print(f"  {key}) {label}{marker}")
    while True:
        value = input("Choice: ").strip().lower() or default
        if value in valid:
            return value
        print("Please choose a valid option.")


def choose_csv_file(label: str, default_path: str, *, must_read_existing: bool = False) -> str:
    path = Path(default_path)

    if path.exists():
        existing_rows = count_csv_rows(default_path)
        choice = prompt_choice(
            f"\n{label} file already exists: {default_path} ({existing_rows} rows)\nWhere should it be saved/used?",
            [
                ("1", "Continue/update this existing file — recommended"),
                ("2", "Create a new timestamped CSV file"),
                ("3", "Enter a custom CSV file path"),
            ],
            "1",
        )
        if choice == "1":
            return default_path
        if choice == "2":
            new_path = timestamped_csv_path(default_path)
            print(f"The new file will be used: {new_path}")
            return new_path
        return prompt_text("Custom CSV file path", default_path)

    if must_read_existing:
        print(f"\n{label} file not found: {default_path}")
        while True:
            selected = prompt_text("Enter the path of an existing Google Maps CSV", default_path)
            if Path(selected).exists():
                return selected
            print("That file does not exist. Enter a valid CSV path, or press Ctrl+C to stop.")

    print(f"\n{label} file not found. A new file will be created: {default_path}")
    return default_path


def show_input_summary(search_terms: Sequence[str]) -> None:
    print("\nInput terms detected from input.txt:")
    for index, term in enumerate(search_terms[:12], start=1):
        print(f"  {index}. {term}")
    if len(search_terms) > 12:
        print(f"  ... and {len(search_terms) - 12} more terms")
    print(
        "\nWhether input.txt has one line or many, the data for all terms is "
        "saved to the same Google Maps CSV and the same enriched leads CSV."
    )


def print_limit_explanation(term_count: int, maps_per_term: int, enrich_limit: int) -> None:
    possible_maps = term_count * maps_per_term
    print("\nRecord calculation:")
    print(f"  Search terms: {term_count}")
    print(f"  Google Maps records per term: {maps_per_term}")
    print(f"  Possible new Maps records this run: {term_count} × {maps_per_term} = {possible_maps}")
    print(f"  Enrichment records to process: {enrich_limit} total combined records")
    if enrich_limit < possible_maps:
        print(
            "  Note: the enrichment limit is lower than the Maps total, so some "
            "records may stay pending. You can run only the Enricher later."
        )


def apply_interactive_setup(args: argparse.Namespace, search_terms: Sequence[str]) -> None:
    print("\n==============================")
    print("AI Detriots Lead Scraper")
    print("Easy / non-technical mode")
    print("==============================")

    show_input_summary(search_terms)

    default_maps = default_output_path(args.maps_output, DEFAULT_MAPS_OUTPUT)
    default_enriched = default_output_path(args.enriched_output, DEFAULT_ENRICHED_OUTPUT)
    maps_exists = Path(default_maps).exists()
    enriched_exists = Path(default_enriched).exists()

    mode_choices: List[Tuple[str, str]] = [
        ("1", "Run Google Maps + Lead Enrichment"),
        ("2", "Run only the Google Maps scraper"),
        ("3", "Run only the Lead Enricher on an existing Google Maps CSV"),
        ("4", "Retry only missing/no-email records from an existing Google Maps CSV"),
    ]

    if maps_exists or enriched_exists:
        print("\nExisting CSV files were detected. You can keep using them or create new files.")
    else:
        print("\nIf you already have a Google Maps CSV, choose option 3 or 4 and enter its path.")

    selected = prompt_choice("\nWhat do you want to run?", mode_choices, "1")

    if selected == "1":
        args.mode = "all"
        args.retry_failed = "false"
    elif selected == "2":
        args.mode = "maps"
        args.retry_failed = "false"
    elif selected == "3":
        args.mode = "enrich"
        args.retry_failed = "false"
    else:
        args.mode = "enrich"
        args.retry_failed = "true"

    args.maps_output = choose_csv_file(
        "Google Maps CSV",
        default_maps,
        must_read_existing=(args.mode == "enrich"),
    )

    if args.mode in {"enrich", "all"}:
        args.enriched_output = choose_csv_file(
            "Enriched Leads CSV",
            default_enriched,
            must_read_existing=False,
        )
    else:
        args.enriched_output = default_enriched

    term_count = len(search_terms)

    if args.mode in {"maps", "all"}:
        args.total = prompt_int(
            "How many NEW Google Maps records should be collected for each input.txt search term?",
            args.total,
        )

    if args.mode in {"enrich", "all"}:
        if args.mode == "all":
            default_limit = term_count * args.total
            print(
                f"\nYour {term_count} search terms × {args.total} records per term = "
                f"up to {default_limit} Google Maps records."
            )
        else:
            rows_in_maps = count_csv_rows(args.maps_output)
            default_limit = rows_in_maps or args.limit or 100
            print(f"\nThe selected Google Maps CSV has about {rows_in_maps} rows.")
            print("In this mode Google Maps scraping will not run again; only this CSV will be enriched.")

        if str_to_bool(args.retry_failed):
            args.limit = prompt_int(
                "How many pending + missing/no-email records should be retried/enriched?",
                default_limit,
            )
        else:
            args.limit = prompt_int(
                "How many pending Google Maps records should be enriched?",
                default_limit,
            )

    visible_browser = prompt_yes_no(
        "Keep the browser visible? Recommended so you can solve CAPTCHAs",
        True,
    )
    args.headless = "false" if visible_browser else "true"
    args.resume = "true"

    print("\nFinal settings:")
    print(f"  Mode: {args.mode}")
    print(f"  Search terms: {term_count}")
    if args.mode in {"maps", "all"}:
        print(f"  Maps new records: {args.total} per term")
    if args.mode in {"enrich", "all"}:
        print(f"  Enrichment limit: {args.limit} total combined records")
        print(f"  Retry missing/no-email: {str_to_bool(args.retry_failed)}")
    print(f"  Maps CSV: {args.maps_output}")
    if args.mode in {"enrich", "all"}:
        print(f"  Enriched CSV: {args.enriched_output}")
    print(f"  Browser visible: {not str_to_bool(args.headless)}")

    if args.mode == "all":
        print_limit_explanation(term_count, args.total, args.limit)

    if not prompt_yes_no("Start now?", True):
        print("Cancelled.")
        raise SystemExit(0)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="AI Detriots Google Maps + ContactOut enrichment pipeline"
    )
    parser.add_argument("--mode", choices=["maps", "enrich", "all"], default="all")
    parser.add_argument("--input", default=os.getenv("INPUT_TERMS_FILE", "input.txt"))
    parser.add_argument("--output-dir", default=os.getenv("OUTPUT_DIR", DEFAULT_OUTPUT_DIR))
    parser.add_argument("--maps-output", default=os.getenv("GOOGLE_MAPS_OUTPUT", ""))
    parser.add_argument("--enriched-output", default=os.getenv("ENRICHED_OUTPUT", ""))
    parser.add_argument("--state-file", default=os.getenv("PIPELINE_STATE_FILE", ""))
    parser.add_argument(
        "--total",
        type=int,
        default=env_int("TOTAL_MAP_RESULTS", 3),
        help="New unique Maps records to save per search term during this run.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=env_int("ENRICH_LIMIT", None),
        help="Total combined records to enrich. If omitted in all-mode, defaults to total × search terms.",
    )
    parser.add_argument("--headless", default=os.getenv("HEADLESS", "false"))
    parser.add_argument("--resume", default=os.getenv("RESUME", "true"))
    parser.add_argument(
        "--retry-failed",
        default=os.getenv("RETRY_FAILED_ENRICHMENTS", "false"),
        help=(
            "After all never-enriched Maps rows, retry existing needs-review/not-found rows. "
            "Default false protects ContactOut credits."
        ),
    )
    parser.add_argument(
        "--force-refresh",
        default=os.getenv("FORCE_REFRESH", "false"),
        help="Ignore cached enrichment and spend provider credits again.",
    )
    parser.add_argument("--maps-delay", type=float, default=float(os.getenv("MAPS_DELAY_SECONDS", "1")))
    parser.add_argument("--website-delay", type=float, default=float(os.getenv("WEBSITE_DELAY_SECONDS", "0.25")))
    parser.add_argument("--google-delay", type=float, default=float(os.getenv("GOOGLE_SEARCH_DELAY_SECONDS", "1")))
    parser.add_argument("--google-open-limit", type=int, default=int(os.getenv("GOOGLE_OPEN_RESULT_LIMIT", "0")))
    parser.add_argument("--google-query-limit", type=int, default=int(os.getenv("GOOGLE_QUERY_LIMIT_PER_BUSINESS", "3")))
    parser.add_argument("--browser-profile", default=os.getenv("BROWSER_PROFILE_DIR", ".chromium_profile"))
    parser.add_argument(
        "--easy",
        default="false",
        help="Show the non-technical guided prompt even when command options are provided.",
    )
    parser.add_argument(
        "--non-interactive",
        default="false",
        help="Never show prompts. Useful for scheduled/automated runs.",
    )
    return parser


def bootstrap_history(state_file: str, maps_output: str, enriched_output: str, output_dir: str) -> int:
    bootstrap_candidates = [
        Path(DEFAULT_MAPS_OUTPUT),
        Path(DEFAULT_ENRICHED_OUTPUT),
        Path(maps_output),
        Path(enriched_output),
    ]
    output_root = Path(output_dir)
    if output_root.exists():
        bootstrap_candidates.extend(output_root.glob("*.csv"))

    imported_total = 0
    with PipelineState(state_file) as state:
        for candidate in dict.fromkeys(bootstrap_candidates):
            imported_total += state.bootstrap_csv(str(candidate))
    return imported_total


def main() -> None:
    load_dotenv()
    parser = build_parser()
    args = parser.parse_args()

    wants_interactive = not str_to_bool(args.non_interactive) and (
        len(sys.argv) == 1 or str_to_bool(args.easy)
    )

    search_terms = read_search_terms_or_prompt(args.input, wants_interactive)
    if not search_terms:
        raise ValueError("input.txt has no search terms.")

    if args.limit is None:
        # Clear default: all-mode enriches the same number of total rows that
        # Maps may collect across all search terms.
        args.limit = args.total * len(search_terms) if args.mode == "all" else 100

    if wants_interactive:
        apply_interactive_setup(args, search_terms)

    headless = str_to_bool(args.headless)
    resume = str_to_bool(args.resume)
    force_refresh = str_to_bool(args.force_refresh)
    retry_failed = str_to_bool(args.retry_failed)

    # Current UX intentionally uses one combined output CSV for every line in input.txt.
    maps_output = default_output_path(args.maps_output, DEFAULT_MAPS_OUTPUT)
    enriched_output = default_output_path(args.enriched_output, DEFAULT_ENRICHED_OUTPUT)

    Path(maps_output).parent.mkdir(parents=True, exist_ok=True)
    Path(enriched_output).parent.mkdir(parents=True, exist_ok=True)

    state_file = args.state_file or str(Path(args.output_dir) / "pipeline_state.sqlite3")
    Path(state_file).parent.mkdir(parents=True, exist_ok=True)

    imported_total = bootstrap_history(state_file, maps_output, enriched_output, args.output_dir)
    if imported_total:
        print(f"Duplicate/cache history loaded from {imported_total} existing CSV rows.")

    print("\nInput terms:")
    for index, term in enumerate(search_terms, start=1):
        print(f"  {index}. {term}")
    print(f"Maps CSV: {maps_output}")
    if args.mode in {"enrich", "all"}:
        print(f"Enriched CSV: {enriched_output}")

    if args.mode in {"maps", "all"}:
        print(
            f"Maps target: {args.total} new unique records per term × "
            f"{len(search_terms)} terms = up to {args.total * len(search_terms)} new Maps rows."
        )
    if args.mode in {"enrich", "all"}:
        print(f"Enrichment target: up to {args.limit} total combined pending rows.")

    with SharedChromiumSession(headless=headless, profile_dir=args.browser_profile) as session:
        assert session.context is not None
        assert session.primary_page is not None

        maps_page = session.primary_page
        enrichment_page = None

        if args.mode in {"maps", "all"}:
            print("\n=== Step 1: Google Maps/local scraper ===")
            print(f"All input.txt terms will be written into one Maps CSV: {maps_output}")

            run_maps_scraper(
                input_file=args.input,
                output_csv=maps_output,
                total_results_to_scrape=args.total,
                headless=headless,
                maps_delay_seconds=args.maps_delay,
                resume=resume,
                page=maps_page,
                browser_context=session.context,
                search_terms=list(search_terms),
                state_path=state_file,
            )

        if args.mode in {"enrich", "all"}:
            print("\n=== Step 2: Website + Google Owner/Founder/CEO + ContactOut ===")
            print(f"Enrichment will read from {maps_output} and write/update {enriched_output}")

            enrichment_page = session.new_page() if args.mode == "all" else maps_page

            enrich_csv(
                input_csv=maps_output,
                output_csv=enriched_output,
                limit=args.limit,
                headless=headless,
                website_delay=args.website_delay,
                google_delay=args.google_delay,
                resume=resume,
                google_open_limit=args.google_open_limit,
                google_query_limit=args.google_query_limit,
                browser_context=session.context,
                google_page=enrichment_page,
                state_path=state_file,
                force_refresh=force_refresh,
                retry_failed=retry_failed,
            )

    print("\nPipeline complete.")


if __name__ == "__main__":
    main()
