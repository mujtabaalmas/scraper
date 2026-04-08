# Yelp Profile Scraper (Playwright Edition )

A fast async Python scraper that enriches business records by finding matching **Yelp** profile URLs, then writing results back to CSV.

It uses multiple search strategies (direct site search, DuckDuckGo, Bing, and website backlink checks), plus verification logic (website domain match + name similarity) to improve accuracy.

---

## Table of Contents

- [What this script does](#what-this-script-does)
- [How it works (high level)](#how-it-works-high-level)
- [Requirements](#requirements)
- [Installation](#installation)
- [How to run](#how-to-run)
- [Input and output format](#input-and-output-format)
- [Configuration](#configuration)
- [Step-by-step code explanation](#step-by-step-code-explanation)
  - [Imports and constants](#1-imports-and-constants)
  - [Utility functions](#2-utility-functions)
  - [Stealth browser setup](#3-stealth-browser-setup)
  - [BBB scraper](#4-bbb-scraper)
  - [Yelp scraper](#5-yelp-scraper)
  - [Proxy helper](#6-proxy-helper)
  - [Per-record processing](#7-per-record-processing)
  - [Main workflow](#8-main-workflow)
- [Logging](#logging)
- [Troubleshooting](#troubleshooting)
- [Notes and limitations](#notes-and-limitations)
- [Suggested improvements](#suggested-improvements)

---

## What this script does

Given an input CSV of businesses, the script:

1. Reads each business record (`name`, `city`, `state`, `website`, etc.).
2. Checks if `bbb` and/or `yelp` columns are missing.
3. Searches for missing profiles using several strategies.
4. Verifies likely matches using:
   - website domain matching, and/or
   - company name similarity scoring.
5. Writes updated records to an output CSV.

It is optimized to search **BBB and Yelp concurrently per record** when both are missing.

---

## How it works (high level)

For each business row:

- If BBB missing → run BBB search pipeline.
- If Yelp missing → run Yelp search pipeline.
- If both missing → run both in parallel (`asyncio.gather`) using separate browser pages.
- Save found URLs in `bbb` and `yelp` columns.
- Keep existing URLs unchanged if already present.

---

## Requirements

- Python 3.9+ (recommended)
- Internet connection
- Playwright Chromium browser binaries

Python packages used:

- `playwright`
- `beautifulsoup4`
- `lxml`

---

## Installation

### 1) Clone and enter project directory

```bash
git clone https://github.com/mujtabaalmas/scraper.git
cd scraper
```

### 2) (Recommended) Create virtual environment

**Windows (PowerShell):**
```bash
python -m venv .venv
.venv\Scripts\Activate.ps1
```

**macOS/Linux:**
```bash
python3 -m venv .venv
source .venv/bin/activate
```

### 3) Install Python dependencies

```bash
pip install playwright beautifulsoup4 lxml
```

### 4) Install Playwright Chromium

```bash
playwright install chromium
```

---

## How to run

Make sure the input file exists (default: `bussiness_records.csv`), then run:

```bash
python yelp_bbb_scraper.py
```

The script writes results to:

- `bussiness_records_yelp_bbb.csv`
- and logs to `yelp_bbb_scraper.log`

---

## Input and output format

### Expected input CSV (`bussiness_records.csv`)

Must include at least:

- `name`
- `city`
- `state`
- `website`
- `bbb` (can be empty/null/none)
- `yelp` (can be empty/null/none)

Example:

```csv
name,city,state,website,bbb,yelp
Acme Plumbing,Houston,TX,acmeplumbing.com,,
Best Dental Clinic,Dallas,TX,bestdental.com,null,none
```

### Output CSV (`bussiness_records_yelp_bbb.csv`)

Same columns as input, with `bbb` and/or `yelp` populated where found.

---

## Configuration

Top-of-file constants control behavior:

- `INPUT_CSV = "bussiness_records.csv"`
- `OUTPUT_CSV = "bussiness_records_yelp_bbb.csv"`
- `TEST_LIMIT = 10`  
  - Only first 10 records processed by default.
- `HEADLESS = True`  
  - Set `False` to watch browser.
- `NAVIGATION_TIMEOUT = 25000`
- `MIN_DELAY = 0.5`, `MAX_DELAY = 1.5`
- `MAX_CANDIDATES_TO_CHECK = 5`
- `PROXIES = []`  
  - Add proxy URLs if needed.

---

## Step-by-step code explanation

## 1) Imports and constants

The script imports async/browser tools, HTML parsing, URL parsing, regex, logging, CSV, and matching helpers.

Important modules:
- `playwright.async_api` for browser automation
- `bs4.BeautifulSoup` for parsing HTML
- `SequenceMatcher` for fuzzy name similarity

Constants define:
- input/output files
- speed, delays, timeout
- headless mode
- max candidate URLs checked per search

---

## 2) Utility functions

These are foundational helpers used everywhere:

- `extract_domain(url)`  
  Normalizes URL to domain (`www.` removed, lowercased, port stripped).

- `domains_match(url1, url2)`  
  Returns true if normalized domains are equal.

- `similarity(a, b)`  
  Computes fuzzy ratio after alphanumeric cleanup.

- `clean_company_name(name)`  
  Removes suffixes like Inc, LLC, Corp, `.com` to improve matching/search.

- `is_valid_bbb_profile(url)`  
  True if URL contains `bbb.org` and `/profile/`.

- `is_valid_yelp_profile(url)`  
  True if URL contains `yelp.com/biz/`.

- `clean_yelp_url(url)`  
  Drops query string (`?` params).

- `clean_bbb_url(url)`  
  Drops hash fragment (`#...`).

- `decode_bing_url(bing_href)`  
  Decodes Bing redirect wrappers (`bing.com/ck/`, `aclick`) to actual destination.

- `name_in_url(company_name, url)`  
  Checks whether most business name words appear in URL slug.

- `random_delay(min_s, max_s)`  
  Adds randomized sleep for anti-bot pacing.

---

## 3) Stealth browser setup

### `STEALTH_SCRIPT`
Injects JS properties to reduce automation fingerprints:
- hides `navigator.webdriver`
- sets fake `navigator.plugins`
- sets `navigator.languages`
- patches permission query behavior

### `create_stealth_context(...)`
- chooses random user-agent
- launches Chromium with anti-automation flags
- optionally applies proxy config
- creates browser context with locale/timezone

### `setup_page(context)`
Creates page and blocks heavy resource files (images/fonts/video) for speed.

---

## 4) BBB scraper

Class: `BBBScraper`

### Main method: `search(...)`
Tries strategies in order:

1. DuckDuckGo HTML search (direct links)
2. Direct BBB site search
3. Bing search (with redirect decode)
4. Company website backlink check

Returns first verified BBB URL or `None`.

### Candidate extraction methods
- `_search_duckduckgo` parses DDG results and redirect params (`uddg`)
- `_search_bbb_direct` parses page links, API responses, and Next.js hydration JSON
- `_search_bing` parses normal links + `<cite>` url text
- `_check_website` scans company website for BBB links via regex and anchors

### Verification methods
- `_verify_candidates`:
  - Fast path: URL slug contains company name
  - Strong path: visit profile page, extract business website, compare domain
  - Fallback: compare profile business name to company name (threshold `> 0.55`)

### Parsing helpers
- `_extract_website_from_bbb_profile` finds business site from page links/text/embedded JSON
- `_extract_name_from_bbb_profile` gets business name from `<h1>` or `og:title`
- `_extract_bbb_links`, `_extract_from_api`, `_extract_from_nextjs_data`, `_find_profile_urls`
  recursively harvest candidate URLs

---

## 5) Yelp scraper

Class: `YelpScraper`

### Main method: `search(...)`
Tries strategies in order:

1. Direct Yelp search page
2. DuckDuckGo search
3. Bing search
4. Company website backlink check

### Candidate extraction
- `_search_yelp_direct`: parse `/biz/` links from Yelp search result page
- `_search_duckduckgo`: parse DDG links and decode `uddg`
- `_search_bing`: parse Bing links + decode redirect + parse `<cite>` URLs
- `_check_website`: scrape company website for yelp.com/biz links
- `_extract_yelp_links`: normalize and filter invalid/non-profile Yelp URLs

### Verification
`_verify_candidates` visits candidate pages and validates using:
1. website domain match (best signal), otherwise
2. profile name similarity score (`> 0.6`)

### Yelp profile parsers
- `_extract_website_from_yelp_profile`:
  - handles `/biz_redir?url=...` extraction
  - checks nearby “business website” labels
  - checks JSON-LD scripts
  - checks raw JSON-like patterns
- `_extract_name_from_yelp_profile`: from `<h1>` or `og:title`

---

## 6) Proxy helper

`get_proxy_config(proxy_url)` converts a proxy URL into Playwright proxy dict:
- `server`
- optional `username`
- optional `password`

If `PROXIES` has entries, first one is used.

---

## 7) Per-record processing

Function: `process_record(...)`

For each CSV row:
- reads name/location/website
- decides if BBB/Yelp are needed (`empty`, `null`, `none`)
- if both needed:
  - creates 2 pages
  - runs BBB + Yelp searches concurrently via `asyncio.gather`
- if only one needed:
  - runs only that scraper
- closes pages in `finally`
- returns tuple `(found_bbb, found_yelp)` as counters

---

## 8) Main workflow

Function: `main()`

1. Start timer and log header.
2. Read input CSV via `csv.DictReader`.
3. Apply `TEST_LIMIT` to number of rows processed.
4. Count already-existing BBB/Yelp values.
5. Create scraper instances.
6. Start Playwright context (with optional proxy + stealth setup).
7. Iterate records:
   - call `process_record`
   - accumulate found counts
   - delay between records
8. Close browser/context.
9. Write all rows to output CSV.
10. Print summary (processed, found, totals, elapsed time, output file).

Entrypoint:
```python
if __name__ == "__main__":
    asyncio.run(main())
```

---

## Logging

Logs are written to:

- Console (stdout)
- File: `yelp_bbb_scraper.log`

Log includes:
- per-record status
- search strategy used
- candidate verification details
- domain mismatches
- summary stats

---

## Troubleshooting

### 1) `ModuleNotFoundError`
Install dependencies again:
```bash
pip install playwright beautifulsoup4 lxml
playwright install chromium
```

### 2) Browser fails to launch
Try:
- updating Playwright
- reinstalling Chromium binaries
- setting `HEADLESS = False` to inspect behavior

### 3) Very few matches found
- Increase `MAX_CANDIDATES_TO_CHECK` (e.g., 10)
- Raise `NAVIGATION_TIMEOUT`
- Set `TEST_LIMIT = None` to process full file
- Ensure website domains in input are correct

### 4) Blocked/challenged pages
- Add valid proxies in `PROXIES`
- Increase delays (`MIN_DELAY`, `MAX_DELAY`)
- Rotate user agents (already enabled)

---

## Notes and limitations

- Search engines and target sites can change markup, which may break selectors/patterns.
- Aggressive scraping may trigger anti-bot protections.
- Domain matching depends on clean/accurate website values in CSV.
- `TEST_LIMIT` defaults to 10, so full dataset is not processed unless you change it.

---
## DEVELOPER NOTE 
- THIS IS CURRENTLY EXTRACTING YELP PROFILE ONLY BBB IS NOT FETECHING RIGHT NOW 
