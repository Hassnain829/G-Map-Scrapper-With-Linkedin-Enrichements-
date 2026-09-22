"""Web UI for the AI Detriots lead pipeline.

Run this file (RUN_PIPELINE.bat does) and it opens the dashboard in the
user's default browser. The pipeline itself is still main.py: the dashboard
starts it as a subprocess, streams its output, answers CAPTCHA prompts over
stdin, and keeps a permanent record of every search term that was run.
"""
from __future__ import annotations

import atexit
import csv
import json
import logging
import os
import re
import socket
import subprocess
import sys
import threading
import time
import uuid
import webbrowser
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from dotenv import load_dotenv
from flask import Flask, abort, jsonify, render_template, request, send_file

BASE_DIR = Path(__file__).resolve().parent
os.chdir(BASE_DIR)
load_dotenv(BASE_DIR / ".env")

MAPS_CSV = Path(os.getenv("GOOGLE_MAPS_OUTPUT") or "google_maps_data.csv")
ENRICHED_CSV = Path(os.getenv("ENRICHED_OUTPUT") or "enriched_leads.csv")
INPUT_FILE = Path(os.getenv("INPUT_TERMS_FILE") or "input.txt")
OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR") or "outputs")
RUN_INPUT_DIR = OUTPUT_DIR / "run_inputs"
HISTORY_FILE = OUTPUT_DIR / "run_history.json"

DEFAULT_PORT = 5077
MAX_LOG_LINES = 6000
MODES = {"all", "maps", "enrich", "retry"}

app = Flask(__name__)


# ---------------------------------------------------------------------------
# small helpers
# ---------------------------------------------------------------------------

def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def term_key(term: str) -> str:
    return re.sub(r"\s+", " ", str(term or "")).strip().lower()


def clean_term(term: str) -> str:
    return re.sub(r"\s+", " ", str(term or "")).strip()


def read_csv(path: Path) -> Tuple[List[str], List[Dict[str, str]]]:
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            rows = list(reader)
            return list(reader.fieldnames or []), rows
    except Exception:
        return [], []


def business_key(name: str, phone: str) -> Tuple[str, str]:
    return (
        re.sub(r"\W+", "", str(name or "").lower()),
        re.sub(r"\D", "", str(phone or ""))[-10:],
    )


def file_sig(path: Path) -> Tuple[float, int]:
    try:
        stat = path.stat()
        return (stat.st_mtime, stat.st_size)
    except OSError:
        return (0.0, 0)


# ---------------------------------------------------------------------------
# saved search terms (input.txt)
# ---------------------------------------------------------------------------

def read_saved_terms() -> List[str]:
    try:
        raw = INPUT_FILE.read_text(encoding="utf-8-sig").splitlines()
    except FileNotFoundError:
        return []
    terms = [clean_term(line) for line in raw]
    return list(dict.fromkeys(term for term in terms if term))


def write_saved_terms(terms: List[str]) -> List[str]:
    unique = list(
        dict.fromkeys(clean_term(term) for term in terms if clean_term(term))
    )
    INPUT_FILE.write_text(
        "\n".join(unique) + ("\n" if unique else ""), encoding="utf-8"
    )
    return unique


# ---------------------------------------------------------------------------
# per-term lead statistics (computed from the CSV files, cached by mtime)
# ---------------------------------------------------------------------------

_stats_lock = threading.Lock()
_stats_cache: Dict[str, Any] = {"sig": None, "value": {}}
# str(path) -> (file signature, info dict, parsed rows)
_file_cache: Dict[str, Tuple[Tuple[float, int], Dict[str, Any], List[Dict[str, str]]]] = {}

RESERVED_NAMES = {"CON", "PRN", "AUX", "NUL", *(f"COM{i}" for i in range(1, 10)),
                  *(f"LPT{i}" for i in range(1, 10))}


def rel_path(path: Path) -> str:
    try:
        return path.resolve().relative_to(BASE_DIR).as_posix()
    except ValueError:
        return str(path)


def is_done_row(row: Dict[str, str]) -> bool:
    return bool(
        (row.get("enrichment_state") or row.get("enrichment_status") or "").strip()
    )


def inspect_csv(path: Path) -> Optional[Tuple[Dict[str, Any], List[Dict[str, str]]]]:
    """Classify a CSV as a Maps file or an enriched-leads file (cached by mtime)."""
    sig = file_sig(path)
    cached = _file_cache.get(str(path))
    if cached and cached[0] == sig:
        return (cached[1], cached[2]) if cached[1] else None

    fields, rows = read_csv(path)
    field_set = set(fields)
    if {"search_term", "name"} <= field_set:
        kind = "maps"
    elif "Company" in field_set and ({"Email", "Lead Status"} & field_set):
        kind = "enriched"
    else:
        _file_cache[str(path)] = (sig, {}, [])
        return None

    info = {
        "path": rel_path(path),
        "name": path.name,
        "kind": kind,
        "rows": len(rows),
        "pending": sum(1 for row in rows if not is_done_row(row)) if kind == "maps" else 0,
        "modified": datetime.fromtimestamp(sig[0]).isoformat(timespec="seconds"),
        "size": sig[1],
    }
    _file_cache[str(path)] = (sig, info, rows)
    return info, rows


def discover_files() -> List[Tuple[Dict[str, Any], List[Dict[str, str]]]]:
    """Every lead CSV in the project folder and in outputs/."""
    candidates: Dict[str, Path] = {}
    for folder in (BASE_DIR, OUTPUT_DIR):
        if folder.exists():
            for path in folder.glob("*.csv"):
                candidates[str(path.resolve())] = path
    for path in (MAPS_CSV, ENRICHED_CSV):
        if path.exists():
            candidates[str(path.resolve())] = path

    found = []
    for path in sorted(candidates.values(), key=lambda p: p.name.lower()):
        result = inspect_csv(path)
        if result:
            found.append(result)
    return found


def compute_stats() -> Dict[str, Any]:
    """Per-search-term counts, combined across every Maps/enriched CSV."""
    with _stats_lock:
        files = discover_files()
        sig = tuple((info["path"], info["size"], info["modified"]) for info, _ in files)
        if _stats_cache["sig"] == sig:
            return _stats_cache

        email_keys = {
            business_key(
                row.get("Company") or row.get("Name", ""),
                row.get("Phone", ""),
            )
            for info, rows in files if info["kind"] == "enriched"
            for row in rows
            if (row.get("Email") or "").strip()
        }

        stats: Dict[str, Dict[str, Any]] = {}
        seen = set()

        for info, rows in files:
            if info["kind"] != "maps":
                continue
            for row in rows:
                term = clean_term(row.get("search_term")) or "(no search term)"
                business = business_key(row.get("name", ""), row.get("phone", ""))
                # A business kept in two files is still one lead for that search.
                if (term_key(term), business) in seen:
                    continue
                seen.add((term_key(term), business))

                entry = stats.setdefault(
                    term_key(term),
                    {"term": term, "scraped": 0, "enriched": 0, "with_email": 0},
                )
                entry["scraped"] += 1
                if is_done_row(row):
                    entry["enriched"] += 1
                if business in email_keys:
                    entry["with_email"] += 1

        _stats_cache.update(sig=sig, value=stats)
        return _stats_cache


def resolve_file_spec(spec: Any, kind: str, allow_new: bool) -> Tuple[str, str]:
    """Turn the page's file choice into a safe project-relative path.

    Returns (path, error). Existing files must be ones discover_files() found,
    and new files are always created inside the outputs folder.
    """
    if not isinstance(spec, dict):
        return "", "Choose a file to use."

    if spec.get("mode") == "new":
        if not allow_new:
            return "", "This file must already exist."
        name = str(spec.get("name", "")).strip()
        name = re.sub(r"\.csv$", "", name, flags=re.IGNORECASE).strip(" .")
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9 _.\-]{0,79}", name) or name.upper() in RESERVED_NAMES:
            return "", "File names can use letters, numbers, spaces and - _ . only."
        target = OUTPUT_DIR / f"{name}.csv"
        if target.exists():
            return "", f'"{target.name}" already exists in the outputs folder. Pick it from the list or use another name.'
        return rel_path(target), ""

    wanted = str(spec.get("path", ""))
    for info, _ in discover_files():
        if info["path"] == wanted and info["kind"] == kind:
            return wanted, ""
    return "", "That file is no longer available. Refresh the page."


def stats_snapshot() -> Dict[str, Dict[str, int]]:
    return {
        key: {
            "scraped": item["scraped"],
            "enriched": item["enriched"],
            "with_email": item["with_email"],
        }
        for key, item in compute_stats()["value"].items()
    }


def diff_snapshots(
    before: Dict[str, Dict[str, int]], after: Dict[str, Dict[str, int]]
) -> Dict[str, Dict[str, int]]:
    delta: Dict[str, Dict[str, int]] = {}
    for key, now in after.items():
        old = before.get(key, {"scraped": 0, "enriched": 0, "with_email": 0})
        change = {
            field: now[field] - old.get(field, 0)
            for field in ("scraped", "enriched", "with_email")
        }
        if any(change.values()):
            delta[key] = change
    return delta


# ---------------------------------------------------------------------------
# permanent run history
# ---------------------------------------------------------------------------

_history_lock = threading.Lock()


def load_history() -> List[Dict[str, Any]]:
    try:
        return json.loads(HISTORY_FILE.read_text(encoding="utf-8"))
    except (FileNotFoundError, ValueError):
        return []


def save_history(history: List[Dict[str, Any]]) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    temp = HISTORY_FILE.with_suffix(".tmp")
    temp.write_text(json.dumps(history, indent=2), encoding="utf-8")
    temp.replace(HISTORY_FILE)


def upsert_run(record: Dict[str, Any]) -> None:
    with _history_lock:
        history = load_history()
        for index, existing in enumerate(history):
            if existing.get("id") == record["id"]:
                history[index] = record
                break
        else:
            history.append(record)
        save_history(history)


def mark_interrupted_runs() -> None:
    with _history_lock:
        history = load_history()
        changed = False
        for record in history:
            if record.get("status") == "running":
                record["status"] = "interrupted"
                record["ended"] = record.get("ended") or now_iso()
                changed = True
        if changed:
            save_history(history)


# ---------------------------------------------------------------------------
# pipeline runner (main.py as a subprocess)
# ---------------------------------------------------------------------------

class Runner:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.proc: Optional[subprocess.Popen] = None
        self.reset_state()

    def reset_state(self) -> None:
        self.lines: List[str] = []
        self.line_base = 0  # absolute index of self.lines[0]
        self.status = "idle"
        self.phase = ""
        self.current = ""
        self.processed = 0
        self.captcha_pending = False
        self.stop_requested = False
        # term_key -> "queued" | "running" | "done" for the active Maps run
        self.term_states: Dict[str, str] = {}
        self.run: Optional[Dict[str, Any]] = None
        self.before: Dict[str, Dict[str, int]] = {}
        self.started_at = 0.0

    # -- logging ----------------------------------------------------------
    def _log(self, line: str) -> None:
        self.lines.append(line)
        overflow = len(self.lines) - MAX_LOG_LINES
        if overflow > 0:
            del self.lines[:overflow]
            self.line_base += overflow

    # -- lifecycle --------------------------------------------------------
    @property
    def running(self) -> bool:
        return self.proc is not None and self.proc.poll() is None

    def start(self, cfg: Dict[str, Any]) -> Tuple[bool, str]:
        with self.lock:
            if self.running:
                return False, "A run is already in progress."

            mode = cfg["mode"]
            terms: List[str] = cfg["terms"]

            RUN_INPUT_DIR.mkdir(parents=True, exist_ok=True)
            run_id = datetime.now().strftime("%Y%m%d_%H%M%S") + "_" + uuid.uuid4().hex[:4]
            input_path = RUN_INPUT_DIR / f"{run_id}_input.txt"

            # main.py needs a non-empty input file even for enrichment-only runs.
            file_terms = terms or read_saved_terms() or ["enrichment only"]
            input_path.write_text("\n".join(file_terms) + "\n", encoding="utf-8")

            main_mode = "all" if mode == "all" else "maps" if mode == "maps" else "enrich"
            command = [
                sys.executable, "-u", "main.py",
                "--mode", main_mode,
                "--input", str(input_path),
                "--total", str(cfg["total"]),
                "--limit", str(cfg["limit"]),
                "--headless", "true" if cfg["headless"] else "false",
                "--resume", "true",
                "--retry-failed", "true" if mode == "retry" else "false",
                "--force-refresh", "true" if cfg["force_refresh"] else "false",
                "--non-interactive", "true",
                "--maps-output", cfg["maps_file"],
                "--enriched-output", cfg["enriched_file"],
            ]

            env = os.environ.copy()
            env["PYTHONIOENCODING"] = "utf-8"
            env["PYTHONUNBUFFERED"] = "1"
            env["PIPELINE_WEB"] = "1"

            self.reset_state()
            self.before = stats_snapshot()
            self.started_at = time.time()
            self.status = "running"
            if mode in {"all", "maps"}:
                self.term_states = {term_key(term): "queued" for term in terms}
            self.run = {
                "id": run_id,
                "started": now_iso(),
                "ended": None,
                "status": "running",
                "mode": mode,
                "terms": terms if mode in {"all", "maps"} else [],
                "total_per_term": cfg["total"] if mode in {"all", "maps"} else None,
                "enrich_limit": cfg["limit"] if mode != "maps" else None,
                "maps_file": cfg["maps_file"],
                "enriched_file": cfg["enriched_file"] if mode != "maps" else None,
                "processed": 0,
                "delta": {},
                "totals": {"scraped": 0, "enriched": 0, "with_email": 0},
            }
            upsert_run(self.run)
            self._log(f"$ python main.py --mode {main_mode} ...")

            try:
                self.proc = subprocess.Popen(
                    command,
                    cwd=str(BASE_DIR),
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                    bufsize=1,
                    env=env,
                )
            except Exception as error:
                self.status = "failed"
                self.run["status"] = "failed"
                self.run["ended"] = now_iso()
                upsert_run(self.run)
                return False, f"Could not start the pipeline: {error}"

            threading.Thread(target=self._reader, daemon=True).start()
            return True, run_id

    def _reader(self) -> None:
        proc = self.proc
        assert proc is not None and proc.stdout is not None

        for raw in proc.stdout:
            line = raw.rstrip("\r\n")
            # The CAPTCHA prompt has no trailing newline, so it prefixes the next line.
            if line.startswith("Choice: "):
                line = line[len("Choice: "):]

            with self.lock:
                if "@@CAPTCHA_WAIT@@" in line:
                    self.captcha_pending = True
                    continue
                if line.startswith("=== Step 1"):
                    self.phase = "Scraping Google Maps"
                elif line.startswith("=== Step 2"):
                    self.phase = "Enriching leads"
                    self._finish_running_terms()
                elif line.startswith("Google Maps/local search:"):
                    term = line.split(":", 1)[1].strip()
                    self._finish_running_terms()
                    self.term_states[term_key(term)] = "running"
                    self.current = term
                elif line.startswith("--- Enriching:"):
                    self.processed += 1
                    self.current = line.replace("--- Enriching:", "").strip(" -")
                if line.strip():
                    self._log(line)

        code = proc.wait()
        self._finish(code)

    def _finish_running_terms(self) -> None:
        for key, value in self.term_states.items():
            if value == "running":
                self.term_states[key] = "done"

    def _finish(self, code: int) -> None:
        with self.lock:
            self._finish_running_terms()
            after = stats_snapshot()
            delta = diff_snapshots(self.before, after)
            names = {key: item["term"] for key, item in compute_stats()["value"].items()}

            totals = {"scraped": 0, "enriched": 0, "with_email": 0}
            named: Dict[str, Dict[str, int]] = {}
            for key, change in delta.items():
                named[names.get(key, key)] = change
                for field in totals:
                    totals[field] += change[field]

            if self.stop_requested:
                status = "stopped"
            elif code == 0:
                status = "completed"
            else:
                status = "failed"

            self.status = status
            self.captcha_pending = False
            self.phase = ""
            self.current = ""
            if self.run is not None:
                self.run.update(
                    status=status,
                    ended=now_iso(),
                    processed=self.processed,
                    delta=named,
                    totals=totals,
                )
                upsert_run(self.run)
            self._log(f"--- Run {status} (exit code {code}) ---")

    def stop(self) -> bool:
        with self.lock:
            if not self.running:
                return False
            self.stop_requested = True
            proc = self.proc

        assert proc is not None
        try:
            if os.name == "nt":
                # /T also closes the browser windows the pipeline opened.
                subprocess.run(
                    ["taskkill", "/PID", str(proc.pid), "/T", "/F"],
                    capture_output=True,
                )
            else:
                proc.terminate()
        except Exception:
            proc.kill()
        return True

    def answer_captcha(self, action: str) -> bool:
        with self.lock:
            if not (self.running and self.captcha_pending):
                return False
            self.captcha_pending = False
            proc = self.proc

        assert proc is not None and proc.stdin is not None
        try:
            proc.stdin.write(action + "\n")
            proc.stdin.flush()
        except Exception:
            return False
        return True

    # -- status for the UI ------------------------------------------------
    def snapshot(self, since: int) -> Dict[str, Any]:
        with self.lock:
            start = max(since - self.line_base, 0)
            lines = self.lines[start:]
            next_index = self.line_base + len(self.lines)
            run = dict(self.run) if self.run else None
            limit = (run or {}).get("enrich_limit")
            before = self.before
            base = {
                "status": self.status,
                "running": self.running,
                "phase": self.phase,
                "current": self.current,
                "processed": self.processed,
                "limit": limit,
                "captcha": self.captcha_pending,
                "term_states": dict(self.term_states),
                "elapsed": int(time.time() - self.started_at) if self.running else 0,
                "run": run,
                "lines": lines,
                "next": next_index,
            }

        live = {"scraped": 0, "enriched": 0, "with_email": 0}
        if base["running"]:
            for change in diff_snapshots(before, stats_snapshot()).values():
                for field in live:
                    live[field] += change[field]
        base["live"] = live
        return base


runner = Runner()


# ---------------------------------------------------------------------------
# request guard: local-only, JSON-only for writes
# ---------------------------------------------------------------------------

@app.before_request
def guard() -> None:
    host = (request.host or "").rsplit(":", 1)[0].strip("[]").lower()
    if host not in {"127.0.0.1", "localhost"}:
        abort(403)
    if request.method == "POST" and not request.is_json:
        abort(415)


@app.after_request
def no_cache(response):
    response.headers["Cache-Control"] = "no-store"
    return response


# ---------------------------------------------------------------------------
# routes
# ---------------------------------------------------------------------------

@app.get("/")
def index():
    return render_template("index.html")


@app.get("/favicon.ico")
def favicon():
    return ("", 204)


@app.get("/api/ping")
def ping():
    return jsonify(app="lead-pipeline-ui")


@app.get("/api/config")
def config():
    return jsonify(
        saved_terms=read_saved_terms(),
        contactout_configured=bool((os.getenv("CONTACTOUT_API_KEY") or "").strip()),
        default_total=int(os.getenv("TOTAL_MAP_RESULTS", "3") or 3),
    )


@app.get("/api/files")
def files():
    """Lead CSVs the user can save to / read from, plus sensible defaults."""
    found = [info for info, _ in discover_files()]
    history = load_history()
    history.sort(key=lambda r: r.get("started", ""), reverse=True)

    def default_for(kind: str, field: str, fallback: Path) -> str:
        available = {f["path"] for f in found if f["kind"] == kind}
        for record in history:  # prefer whatever the last run used
            if record.get(field) in available:
                return record[field]
        fallback_path = rel_path(fallback)
        return fallback_path if fallback_path in available else ""

    stamp = datetime.now().strftime("%Y%m%d_%H%M")
    return jsonify(
        files=found,
        defaults={
            "maps": default_for("maps", "maps_file", MAPS_CSV),
            "enriched": default_for("enriched", "enriched_file", ENRICHED_CSV),
        },
        suggested={"maps": f"google_maps_{stamp}", "enriched": f"enriched_leads_{stamp}"},
        output_dir=rel_path(OUTPUT_DIR),
    )


@app.post("/api/saved-terms")
def set_saved_terms():
    payload = request.get_json(silent=True) or {}
    terms = payload.get("terms")
    if not isinstance(terms, list):
        return jsonify(error="terms must be a list"), 400
    return jsonify(saved_terms=write_saved_terms([str(t) for t in terms]))


@app.get("/api/terms")
def terms():
    """Every search term ever used, with its lead counts and run count."""
    stats = compute_stats()["value"]
    history = load_history()

    rows: Dict[str, Dict[str, Any]] = {}

    def row_for(term: str) -> Dict[str, Any]:
        return rows.setdefault(
            term_key(term),
            {
                "term": clean_term(term),
                "scraped": 0,
                "enriched": 0,
                "with_email": 0,
                "runs": 0,
                "last_run": None,
                "last_status": None,
                "saved": False,
                "running": False,
                "run_state": None,
            },
        )

    for key, item in stats.items():
        entry = row_for(item["term"])
        entry.update(
            scraped=item["scraped"],
            enriched=item["enriched"],
            with_email=item["with_email"],
        )

    for record in history:
        for term in record.get("terms") or []:
            entry = row_for(term)
            entry["runs"] += 1
            if not entry["last_run"] or record["started"] > entry["last_run"]:
                entry["last_run"] = record["started"]
                entry["last_status"] = record.get("status")

    for term in read_saved_terms():
        row_for(term)["saved"] = True

    if runner.running:
        with runner.lock:
            states = dict(runner.term_states)
        for term in (runner.run or {}).get("terms") or []:
            entry = row_for(term)
            state = states.get(term_key(term), "queued")
            entry["run_state"] = state
            entry["running"] = state == "running"

    # running first, then most recently run, then biggest
    ordered = sorted(
        rows.values(),
        key=lambda r: (r["running"], r["last_run"] or "", r["scraped"]),
        reverse=True,
    )
    return jsonify(terms=ordered)


@app.get("/api/runs")
def runs():
    history = load_history()
    history.sort(key=lambda r: r.get("started", ""), reverse=True)
    return jsonify(runs=history[:100])


@app.get("/api/state")
def state():
    try:
        since = int(request.args.get("since", "0"))
    except ValueError:
        since = 0
    return jsonify(runner.snapshot(since))


@app.post("/api/run")
def start_run():
    payload = request.get_json(silent=True) or {}

    mode = str(payload.get("mode", "all"))
    if mode not in MODES:
        return jsonify(error="Unknown mode."), 400

    selected = list(
        dict.fromkeys(
            clean_term(t) for t in (payload.get("terms") or []) if clean_term(t)
        )
    )

    try:
        total = max(int(payload.get("total") or 0), 0)
        limit = max(int(payload.get("limit") or 0), 0)
    except (TypeError, ValueError):
        return jsonify(error="Numbers are invalid."), 400

    if mode in {"all", "maps"}:
        if not selected:
            return jsonify(error="Select at least one search term."), 400
        if total < 1:
            return jsonify(error="Enter how many new leads to scrape per term."), 400
        # Terms typed into the page are remembered in input.txt.
        write_saved_terms(list(dict.fromkeys(read_saved_terms() + selected)))

    # Where the results go. Enrichment-only modes read an existing Maps file.
    maps_file, error = resolve_file_spec(
        payload.get("maps_file"), "maps", allow_new=mode in {"all", "maps"}
    )
    if error:
        return jsonify(error="Businesses file: " + error), 400

    enriched_file = rel_path(ENRICHED_CSV)
    if mode != "maps":
        enriched_file, error = resolve_file_spec(
            payload.get("enriched_file"), "enriched", allow_new=True
        )
        if error:
            return jsonify(error="Leads file: " + error), 400
        if enriched_file == maps_file:
            return jsonify(error="The two files must be different."), 400

    if mode == "all" and limit < 1:
        limit = total * len(selected)
    if mode in {"enrich", "retry"} and limit < 1:
        pending = next(
            (info["pending"] for info, _ in discover_files() if info["path"] == maps_file),
            0,
        )
        limit = max(pending, 1)
    if mode == "maps":
        limit = limit or 100

    ok, info = runner.start(
        {
            "mode": mode,
            "terms": selected,
            "total": total or 1,
            "limit": limit,
            "headless": bool(payload.get("headless", False)),
            "force_refresh": bool(payload.get("force_refresh", False)),
            "maps_file": maps_file,
            "enriched_file": enriched_file,
        }
    )
    if not ok:
        return jsonify(error=info), 409
    return jsonify(run_id=info)


@app.post("/api/stop")
def stop_run():
    return jsonify(stopped=runner.stop())


@app.post("/api/captcha")
def captcha():
    action = str((request.get_json(silent=True) or {}).get("action", "")).lower()
    if action not in {"c", "s", "b", "q"}:
        return jsonify(error="Invalid action."), 400
    return jsonify(sent=runner.answer_captcha(action))


@app.get("/download")
def download():
    """Download a lead CSV. Only files found by discover_files() are allowed."""
    wanted = request.args.get("path", "")
    for info, _ in discover_files():
        if info["path"] == wanted:
            path = (BASE_DIR / wanted).resolve()
            return send_file(path, as_attachment=True, download_name=path.name)
    abort(404)


# ---------------------------------------------------------------------------
# startup
# ---------------------------------------------------------------------------

def port_is_free(port: int) -> bool:
    # Binding is instant. Connecting to a closed port on Windows takes ~2s each,
    # which made starting the dashboard take 20+ seconds.
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        try:
            sock.bind(("127.0.0.1", port))
            return True
        except OSError:
            return False


def existing_dashboard_port() -> Optional[int]:
    import urllib.request

    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    for port in range(DEFAULT_PORT, DEFAULT_PORT + 10):
        if port_is_free(port):
            continue
        try:
            with opener.open(f"http://127.0.0.1:{port}/api/ping", timeout=1.5) as reply:
                if json.loads(reply.read()).get("app") == "lead-pipeline-ui":
                    return port
        except Exception:
            continue
    return None


def main() -> None:
    running_port = existing_dashboard_port()
    if running_port:
        url = f"http://127.0.0.1:{running_port}/"
        print(f"The dashboard is already running. Opening {url}")
        webbrowser.open(url)
        return

    port = next(
        (p for p in range(DEFAULT_PORT, DEFAULT_PORT + 10) if port_is_free(p)),
        None,
    )
    if port is None:
        print("No free port found between 5077 and 5086. Close other programs and retry.")
        return

    mark_interrupted_runs()
    atexit.register(runner.stop)
    logging.getLogger("werkzeug").setLevel(logging.ERROR)

    url = f"http://127.0.0.1:{port}/"
    print("=" * 60)
    print(" AI Detriots Lead Pipeline")
    print(f" Dashboard: {url}")
    print(" Your default browser will open automatically.")
    print(" Keep this window open while you work; close it to stop.")
    print("=" * 60)

    # The socket is already listening once make_server returns, so the browser
    # can open immediately (it just waits for serve_forever to answer).
    from werkzeug.serving import make_server

    server = make_server("127.0.0.1", port, app, threaded=True)
    threading.Thread(target=webbrowser.open, args=(url,), daemon=True).start()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
