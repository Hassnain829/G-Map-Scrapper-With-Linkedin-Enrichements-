# AI Detriots Lead Pipeline

Find local businesses on Google Maps, identify their owner, founder or CEO, and look up a work email. Everything is driven from a local web dashboard.

```
Google Maps  ->  business website & Facebook  ->  owner / founder / CEO  ->  ContactOut email  ->  CSV
 (scrape)            (public emails)              (Bing + Google search)      (API lookup)
```

## Features

- **Web dashboard.** Start, stop and monitor runs from your browser. There is no terminal interaction.
- **Ordered search queue.** Drag search terms to control which one runs first.
- **Choose where results go.** Add to an existing CSV or create a new one.
- **Owner discovery.** Searches Bing, then Google, for the owner's LinkedIn profile and accepts only a profile that matches the company. If one engine shows a CAPTCHA it is paused and the other is used.
- **Email lookup.** Uses the ContactOut API, plus public emails found on the business website and Facebook page.
- **Duplicate protection.** A local SQLite history means a business is never scraped or paid for twice, even across different output files.
- **Run history.** Every search term and run is recorded with its number of businesses, lookups and emails found.

## Requirements

- Windows, macOS or Linux (the one-click launcher `RUN_PIPELINE.bat` is Windows-only)
- Python 3.10 or newer
- Google Chrome, Microsoft Edge, or the Playwright Chromium build (see step 3)
- A [ContactOut](https://contactout.com) API key (only needed for the email lookup step)

## Quick start

```bash
# 1. Create a virtual environment and install dependencies
python -m venv venv
venv\Scripts\activate            # macOS/Linux: source venv/bin/activate
pip install -r requirements.txt

# 2. Add your settings
copy .env.example .env           # macOS/Linux: cp .env.example .env
#    then open .env and set CONTACTOUT_API_KEY

# 3. (Optional) install Playwright's own Chromium if you do not have Chrome
playwright install chromium

# 4. Start the dashboard
python app.py
```

On Windows you can also double-click **`RUN_PIPELINE.bat`**. The dashboard opens in your default browser at <http://127.0.0.1:5000/>. Keep the console window open while you work; closing it stops the dashboard and any run in progress.

## Using the dashboard

1. **Pick what to do.**
   - *Find & enrich*: collect new businesses, then look up each owner's email.
   - *Collect businesses only*: gather businesses from Google Maps and enrich later.
   - *Look up emails only*: enrich businesses you already collected.
   - *Retry the misses*: enrich waiting businesses, then retry those where no email was found.
2. **Choose search terms.** Tick the searches to run and drag them into the order you want. New terms are saved to `input.txt`.
3. **Set how many** new businesses to collect for each search.
4. **Choose the files.** Add to existing CSV files or create new ones (saved in `outputs/`).
5. **Press Start.** Progress, counts and logs update live.

### CAPTCHAs

Keep **Show the browser window** ticked. If Google or Bing asks you to prove you are human, the dashboard shows a banner. Solve the puzzle in the browser window the pipeline opened, then choose *Continue*, *Skip this search*, *Skip this business* or *Save & quit*. CAPTCHA solving is deliberately not automated.

### Command line (optional)

The pipeline can also run without the dashboard:

```bash
python main.py --mode all --total 25 --headless false        # maps + enrichment
python main.py --mode maps --total 50                        # maps only
python main.py --mode enrich --limit 100                     # enrichment only
python main.py --easy true                                   # guided terminal prompts
```

Run `python main.py --help` for every option.

## Configuration

All settings live in `.env` (copy `.env.example` to get started). The most useful ones:

| Setting | Purpose |
| --- | --- |
| `CONTACTOUT_API_KEY` | Your ContactOut key. Never commit it. |
| `SEARCH_ENGINE_ORDER` | Engines used for owner lookup, e.g. `bing,google`. |
| `SEARCH_ENGINE_COOLDOWN_MINUTES` | How long an engine is paused after a CAPTCHA. |
| `GOOGLE_SEARCH_DELAY_SECONDS`, `BING_SEARCH_DELAY_SECONDS` | Base pause between searches (randomised). |
| `BROWSER_CHANNEL` | `chrome` (default), `msedge`, or empty for bundled Chromium. |
| `CONTACTOUT_DECISION_MAKERS_FALLBACK` | Find owners by company domain without using email-reveal quota. |

## Output

| File | Contents |
| --- | --- |
| `google_maps_data.csv` (or your chosen file) | Businesses scraped from Google Maps. |
| `enriched_leads.csv` (or your chosen file) | Owner name, title, LinkedIn, emails, status. |
| `outputs/pipeline_state.sqlite3` | Duplicate history and ContactOut cache. **Do not delete** unless you want the pipeline to forget what it has already collected. |
| `outputs/run_history.json` | The run and search-term history shown in the dashboard. |

These files contain personal data and are excluded from Git by `.gitignore`.

## Project layout

| Path | Role |
| --- | --- |
| `app.py`, `templates/index.html` | Flask dashboard and its page. |
| `main.py` | Pipeline entry point (used by the dashboard and the CLI). |
| `google_maps_scraper.py` | Google Maps local-results scraper. |
| `lead_enricher.py` | Website crawl, owner search, verification, CSV output. |
| `provider_clients.py` | ContactOut API client. |
| `pipeline_state.py` | SQLite duplicate history and cache. |
| `browser_session.py` | Shared persistent Chrome/Chromium session. |
| `self_test.py` | Offline self-test (no network or credits needed). |

See [ARCHITECTURE.md](ARCHITECTURE.md) for how the pieces fit together.

## Testing

```bash
python self_test.py
```

## Troubleshooting

- **The dashboard says the ContactOut key is missing.** Set `CONTACTOUT_API_KEY` in `.env` and restart.
- **Chrome does not open.** Run `playwright install chromium`, or set `BROWSER_CHANNEL=msedge`.
- **Too many CAPTCHAs.** Raise the search delays, keep `SEARCH_ENGINE_ORDER=bing,google`, and collect smaller batches.
- **A new file has fewer businesses than expected.** Businesses already collected are skipped everywhere. Removing that history means deleting `outputs/pipeline_state.sqlite3`.
- **Port 5077 is busy.** The dashboard automatically uses the next free port (5078-5086) and prints the address.

## Responsible use

This tool automates search engines and processes personal data (names and business emails). You are responsible for complying with the terms of service of Google, Bing, LinkedIn and ContactOut, and with the privacy and anti-spam laws that apply to you (for example GDPR, CAN-SPAM). Use conservative request rates and only contact people you have a lawful basis to contact.
