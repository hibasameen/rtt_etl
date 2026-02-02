# RTT waiting times ETL pipeline (2014/15–2024/25)

This project builds a repeatable ETL pipeline for NHS England Consultant-led **Referral to Treatment (RTT)** waiting times.

It scrapes the NHS England RTT *financial-year* pages, downloads the monthly publication files, converts legacy Excel formats where needed, and extracts (by trust and treatment function / specialty):

- **Incomplete pathways** (waiting list position at month-end)
  - Total waiting: `Total number of incomplete pathways`
  - Median wait (weeks): `Average (median) waiting time (in weeks)`

- **Completed pathways** (activity in the month)
  - **Admitted** (inpatient/day case): total completed + median wait
  - **Non-admitted**: total completed + median wait

## Datasets supported

The pipeline can extract one of the following datasets:

### provider_trust_all (trust + specialty + pathway type)

- Monthly **Incomplete Provider**, **Admitted Provider**, and **Non(-)Admitted Provider** publication files
- Extracts the **Provider** worksheet (provider / trust level)
- Output grain (long): `period_end × provider_code × treatment_function_code × pathway_type`
- Also outputs a **wide** CSV with separate columns for:
  - incomplete (waiting)
  - admitted completed
  - non-admitted completed

### provider_trust_incomplete (trust + specialty)

- Monthly **Incomplete Provider** publication files only

### provider_trust_admitted (trust + specialty)

- Monthly **Admitted Provider** publication files only

### provider_trust_nonadmitted (trust + specialty)

- Monthly **Non(-)Admitted Provider** publication files only

### commissioner_national_incomplete (England national + specialty)

- Monthly **Incomplete Commissioner** publication files
- Extracts the **National** worksheet
- Output grain: `period_end × treatment_function_code`

## Outputs

When you run the pipeline it writes:

- `data/bronze/`  
  Raw downloads organised by financial year and a `manifest_<dataset>.csv` with URL + checksum metadata.
- `data/silver/`  
  Parquet output for the chosen dataset.
- `data/gold/`  
  One collated CSV for the chosen dataset.

File names (data/gold):

- `provider_trust_all`
  - Long CSV: `rtt_provider_trust_specialty_all_2014_2019_long.csv`
  - Wide CSV: `rtt_provider_trust_specialty_all_2014_2019_wide.csv`

- `provider_trust_incomplete`
  - Long CSV: `rtt_provider_trust_specialty_incomplete_2014_2019_long.csv`

- `provider_trust_admitted`
  - Long CSV: `rtt_provider_trust_specialty_admitted_2014_2019_long.csv`

- `provider_trust_nonadmitted`
  - Long CSV: `rtt_provider_trust_specialty_nonadmitted_2014_2019_long.csv`

- `commissioner_national_incomplete`
  - Long CSV: `rtt_commissioner_national_specialty_incomplete_2014_2019_long.csv`

## Requirements

Python packages (see `requirements.txt`):

- pandas
- requests
- beautifulsoup4
- openpyxl
- pyarrow (optional, for parquet output)
- python-dateutil

System requirement:

- LibreOffice (`soffice`) on PATH for converting `.xls` → `.xlsx`.

Notes (macOS/Windows):

- The pipeline runs LibreOffice in headless mode and uses a **temporary per-conversion user profile** to avoid stale locks.
- If conversion is slow or appears to hang, you can set:
  - `RTT_LIBREOFFICE_TIMEOUT_S` (default `300`) to increase/decrease the conversion timeout
  - `RTT_SOFFICE_PATH` if `soffice` is not on your PATH (e.g. `/Applications/LibreOffice.app/Contents/MacOS/soffice`)
  - `RTT_KEEP_LO_PROFILE=1` to keep LibreOffice profile folders for debugging



## Run

### Online mode (scrape + download)

```bash
pip install -r requirements.txt
python -m src.rtt_etl --out data --start-fy 2014 --end-fy 2018 --dataset provider_trust_all
```

#### Re-running without re-downloading

Downloads are cached under `data/bronze/`.

On subsequent runs with the same `--out` directory, the pipeline will **only download files that are not already present** (or that are clearly invalid/empty).

Useful flags:

- `--verbose-download` — print messages when skipping/resuming downloads
- `--force-download` — re-download everything even if already on disk

**Tip:** use an *absolute* `--out` path if you want to update/replace the code folder without
re-downloading everything. For example:

```bash
python -m src.rtt_etl --out "/Users/<you>/rtt_etl_data" --start-fy 2014 --end-fy 2018 --dataset provider_trust_all
```

This ensures all runs (even from different working directories or new copies of the project) reuse
the same `bronze/` download cache.

### Offline mode (use files you have downloaded)

If you cannot download the monthly XLS/XLSX/ZIP files programmatically (for example, because the website requires an interactive browser step),
download the monthly publication files in your browser and point the pipeline at the folder.

1) Download the following files for every month you need:

- Incomplete Provider
- Admitted Provider
- NonAdmitted Provider (sometimes labelled Non-admitted or Non Admitted)

2) Put all downloaded files anywhere under a single folder (the pipeline scans recursively), e.g. `input/`.

3) Run:

```bash
python -m src.rtt_etl --out data --start-fy 2014 --end-fy 2018 --dataset provider_trust_all --input-dir input
```

The pipeline reads the period from the workbook header, and then filters to the requested FY window.

## Notes

- NHS England publishes periodic revisions; the year pages typically link to the most recent revised file for each month.
- Treatment Function Code formatting changes around March 2018 (e.g. `IP100` → `C_100`). The pipeline outputs both:
  - `tfc_code_raw` (as published)
  - `tfc` (normalised) for joining across years


## Download caching and retries

- The pipeline skips downloading files that already exist under `out/bronze/`.
- To see skip/resume messages, run with `--verbose-download`.
- Transient download errors are retried automatically. You can tune via environment variables:
  - `RTT_DOWNLOAD_RETRIES` (default 5)
  - `RTT_DOWNLOAD_BACKOFF_S` (default 1.0)
  - `RTT_REQUEST_DELAY_S` (default 0.0)
