# Architecture

## Overview

```
                 +-------------------------+
 Browser  <----> |  app.py (Flask, :5077)  |   local only
                 +-----------+-------------+
                             | starts / streams / answers via stdin
                             v
                 +-------------------------+        +--------------------+
                 |  main.py (subprocess)   | -----> | pipeline_state.py  |
                 +----+-------------+------+        |  SQLite: dedupe +  |
                      |             |               |  enrichment cache  |
                      v             v               +--------------------+
        google_maps_scraper.py   lead_enricher.py
                      |             |
                      |             +--> business website / Facebook (requests)
                      |             +--> Bing / Google search (Playwright)
                      |             +--> provider_clients.py --> ContactOut API
                      v
              shared Chrome session (browser_session.py)
```

The dashboard never imports the pipeline. It runs `main.py` as a subprocess, so the pipeline behaves exactly the same from the dashboard and from the command line.

## Pipeline stages

1. **Maps scrape** (`google_maps_scraper.py`). For each search term, in the order given, pages through Google's local results and saves new businesses (name, address, phone, website, social links). Businesses already in the duplicate history are skipped, and `--total` counts only *new* businesses.
2. **Enrichment** (`lead_enricher.py`), per business:
   1. Crawl the business website (and its Facebook page) for public emails and LinkedIn links.
   2. Search the web for the owner, founder or CEO (see below).
   3. Accept only a LinkedIn person profile that matches the company.
   4. Ask ContactOut for that person's email, with several fallbacks including a company-domain "decision makers" search.
   5. Write one row to the enriched CSV and store it in the cache so the same business never costs credits twice.

### Owner search and CAPTCHAs

Queries are plain text (`<company> owner LinkedIn`, `... founder ...`, `... CEO ...`). Each query goes to the first available engine in `SEARCH_ENGINE_ORDER`:

- Bing runs in its own tab; Google reuses the pipeline's search tab.
- If an engine shows a CAPTCHA it is marked blocked for `SEARCH_ENGINE_COOLDOWN_MINUTES` and the same query is sent to the next engine.
- Only when every engine is blocked is the user asked to solve a CAPTCHA.
- Delays between queries are randomised.
- A Google AI Overview name is only a hint. A matching LinkedIn result is always required.

## Duplicate history (`pipeline_state.py`)

`outputs/pipeline_state.sqlite3` maps each business to a canonical id using its strongest identifiers (Maps place id, phone, name + address, name + domain + city). It holds:

- `businesses` and `business_aliases`: what has been seen.
- `enrichment_cache`: finished enrichment rows, reused instead of calling ContactOut again.

On every run, existing CSV rows are imported into this history in a single transaction. The history is global, so a *new* output file only ever receives businesses that have not been collected before.

## Dashboard (`app.py`)

| Concern | How it works |
| --- | --- |
| Running the pipeline | `Runner` starts `main.py --non-interactive true` with the chosen files and terms, and reads its output on a thread. |
| Live status | The page polls `/api/state`; the runner parses a few log markers (`=== Step 1`, `Google Maps/local search:`, `--- Enriching:`) for the current phase and search term. |
| CAPTCHA prompts | With `PIPELINE_WEB=1`, `lead_enricher.captcha_action` prints `@@CAPTCHA_WAIT@@` and blocks on stdin. The dashboard shows a banner and writes the user's choice to the process's stdin. |
| Statistics | Per-term counts are computed from every Maps and enriched CSV found in the project folder and `outputs/`, cached by modification time. |
| Files | The page can only use CSVs discovered by the app, or create new files inside `outputs/` with a validated name. Downloads are limited to those files. |
| History | `outputs/run_history.json` stores one record per run, including the files it used. |
| Safety | Binds to `127.0.0.1` only, rejects other `Host` headers, and requires JSON bodies on `POST` requests so other websites cannot drive it. |

## Data and privacy

Generated data (CSVs, SQLite history, run history, the Chrome profile) and `.env` are excluded by `.gitignore`. Only source code, templates and the `*.example` files belong in version control.
