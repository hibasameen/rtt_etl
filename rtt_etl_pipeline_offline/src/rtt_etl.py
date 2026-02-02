"""RTT waiting times ETL pipeline.

This project builds a repeatable ETL pipeline for NHS England Consultant-led Referral to Treatment (RTT)
waiting times.

It scrapes the NHS England RTT waiting times *financial-year* pages, downloads the monthly publication
files, converts legacy Excel formats where required, and extracts (by Treatment Function / specialty):

* **Incomplete pathways (waiting list at month-end)**
  * Total waiting: "Total number of incomplete pathways"
  * Median wait: "Average (median) waiting time (in weeks)"

* **Completed pathways**
  * Admitted (inpatient/day case): total completed + median wait
  * Non-admitted: total completed + median wait

Datasets supported (2014/15 onwards; depends on source file schema):

* commissioner_national_incomplete
  * Monthly "Incomplete Commissioner" files
  * Extract the **National** worksheet (England level)
  * Output: England × specialty

* provider_trust_all
  * Monthly "Incomplete Provider", "Admitted Provider", and "Non(-)Admitted Provider" files
  * Extract the **Provider** worksheet (trust/provider level)
  * Output: month × trust × specialty × pathway_type

The main data sources are the NHS England RTT waiting times year pages.
"""

from __future__ import annotations

import argparse
import hashlib
import os
import shutil
import time
import re
import subprocess
import sys
from dataclasses import dataclass
import csv
from datetime import date, datetime
from pathlib import Path
from typing import Iterable, List, Optional, Sequence
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup
from dateutil.relativedelta import relativedelta


# Use a shared session for keep-alive, browser-like headers, and to avoid hanging on misconfigured
# proxy environment variables.
SESSION = requests.Session()
SESSION.headers.update(
    {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/121.0 Safari/537.36"
        ),
        "Accept": "*/*",
        "Accept-Language": "en-GB,en;q=0.9",
    }
)
# Ignore HTTP(S)_PROXY env vars by default to reduce 'hung download' behaviour.
SESSION.trust_env = False

# Project root used for path normalisation.
#
# Many users run this ETL from Jupyter notebooks where the current working directory (CWD)
# can change between runs. If `--out` is provided as a relative path (e.g. `--out data`),
# resolving relative to CWD can accidentally create a *new* data directory and re-download
# everything. To keep reruns idempotent, we anchor relative `--out` paths to the folder
# containing this project (parent of `src/`).
PROJECT_ROOT = Path(__file__).resolve().parent.parent


YEAR_PAGES = {
    "2014-15": "https://www.england.nhs.uk/statistics/statistical-work-areas/rtt-waiting-times/rtt-data-2014-15/",
    "2015-16": "https://www.england.nhs.uk/statistics/statistical-work-areas/rtt-waiting-times/rtt-data-2015-16/",
    "2016-17": "https://www.england.nhs.uk/statistics/statistical-work-areas/rtt-waiting-times/rtt-data-2016-17/",
    "2017-18": "https://www.england.nhs.uk/statistics/statistical-work-areas/rtt-waiting-times/rtt-data-2017-18/",
    "2018-19": "https://www.england.nhs.uk/statistics/statistical-work-areas/rtt-waiting-times/rtt-data-2018-19/",
}


@dataclass(frozen=True)
class DownloadedFile:
    url: str
    raw_path: Path
    sha256: str
    downloaded_at_utc: datetime


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def fetch_year_page(url: str, timeout_s: int = 60) -> str:
    r = SESSION.get(url, timeout=timeout_s)
    r.raise_for_status()
    return r.text


def _looks_like_html(path: Path, max_bytes: int = 2048) -> bool:
    """Heuristic: detect when a downloaded "Excel" file is actually HTML.

    This can happen if a server returns an interstitial page or error HTML while keeping the
    original .xls filename.
    """
    try:
        with path.open("rb") as f:
            head = f.read(max_bytes)
    except Exception:
        return False
    if not head:
        return False
    h = head.lstrip().lower()
    return h.startswith(b"<html") or h.startswith(b"<!doctype") or b"<head" in h[:200]


def _load_existing_manifest(manifest_path: Path) -> dict:
    """Load an existing manifest_{dataset}.csv into a dict keyed by raw_path.

    This lets reruns avoid re-hashing already-downloaded files.
    """
    if not manifest_path.exists():
        return {}
    out: dict = {}
    try:
        with manifest_path.open("r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                rp = row.get("raw_path")
                if not rp:
                    continue
                out[rp] = {
                    "sha256": row.get("sha256") or "",
                    "downloaded_at_utc": row.get("downloaded_at_utc") or "",
                    "source_url": row.get("source_url") or "",
                }
    except Exception:
        return {}
    return out


def extract_monthly_links(
    html: str,
    base_url: str,
    include_needles: Sequence[str],
    exclude_needles: Optional[Sequence[str]] = None,
) -> List[str]:
    """Extract monthly download links from a FY page.

    Parameters
    ----------
    include_needles:
        One or more *case-insensitive* substrings to match in anchor text.
        Example: ["Incomplete Provider"].
    exclude_needles:
        Optional list of substrings; any anchor whose text contains one of these
        will be ignored.

    Returns
    -------
    List[str]
        De-duplicated list of download URLs.
    """
    soup = BeautifulSoup(html, "html.parser")

    urls: List[str] = []
    include_lc = [n.strip().lower() for n in include_needles if n and n.strip()]
    if not include_lc:
        raise ValueError("include_needles cannot be empty")
    exclude_lc = [n.strip().lower() for n in (exclude_needles or []) if n and n.strip()]

    for a in soup.find_all("a"):
        text = (a.get_text() or "").strip().lower()
        href = a.get("href")
        if not href:
            continue

        if not any(n in text for n in include_lc):
            continue

        if exclude_lc and any(n in text for n in exclude_lc):
            continue

        full = urljoin(base_url, href)
        if re.search(r"\.(xls|xlsx|zip)(\?|$)", full, flags=re.I):
            urls.append(full)

    # de-dupe preserve order
    dedup: List[str] = []
    seen = set()
    for u in urls:
        if u in seen:
            continue
        seen.add(u)
        dedup.append(u)

    return dedup



def _norm_for_match(s: str) -> str:
    """Normalise strings for filename matching (lowercase and replace -/_ with spaces)."""
    return re.sub(r"[\-_]+", " ", (s or "").strip().lower())


def list_input_files(input_dir: Path) -> List[Path]:
    """Recursively list supported input files from a local directory.

    Supported:
      - .xls, .xlsx, .zip (containing xls/xlsx)
    """
    exts = {".xls", ".xlsx", ".zip"}
    paths = [p for p in input_dir.rglob("*") if p.is_file() and p.suffix.lower() in exts]
    # Sort by modified time so that later revisions (downloaded later) are processed last.
    paths.sort(key=lambda p: p.stat().st_mtime)
    return paths


def filter_input_files(
    files: Sequence[Path],
    include_needles: Sequence[str],
    exclude_needles: Optional[Sequence[str]] = None,
) -> List[Path]:
    """Filter local files using the same include/exclude needles as web link extraction."""
    include_lc = [_norm_for_match(n) for n in include_needles if n and n.strip()]
    if not include_lc:
        raise ValueError("include_needles cannot be empty")

    exclude_lc = [_norm_for_match(n) for n in (exclude_needles or []) if n and n.strip()]

    out: List[Path] = []
    for p in files:
        name = _norm_for_match(p.name)
        if not any(n in name for n in include_lc):
            continue
        if exclude_lc and any(n in name for n in exclude_lc):
            continue
        out.append(p)
    return out



def download(
    url: str,
    dest: Path,
    timeout_s: int = 180,
    *,
    skip_if_exists: bool = True,
    force: bool = False,
    resume_partial: bool = True,
    manifest_cache: Optional[dict] = None,
    verbose: bool = False,
) -> DownloadedFile:
    """Download a URL to disk.

    Behaviour (default):
      * If `dest` already exists (and is non-empty and not HTML), **do not download again**.
      * If a partial `.part` file exists, try to resume using HTTP Range requests.
      * Retries transient connection errors (server resets, timeouts) with exponential backoff.

    Environment variables (optional):
      * RTT_DOWNLOAD_RETRIES (default: 5)
      * RTT_DOWNLOAD_BACKOFF_S (default: 1.0)
      * RTT_REQUEST_DELAY_S (default: 0.0)  # polite delay after each successful download
    """
    ensure_dir(dest.parent)

    # Cache hit: return existing file metadata without touching the network.
    if not force and skip_if_exists and dest.exists() and dest.stat().st_size > 0 and not _looks_like_html(dest):
        if verbose:
            print(f"Skipping (already downloaded): {dest.name}")

        cached = (manifest_cache or {}).get(str(dest), {})
        sha = (cached.get("sha256") or "").strip()
        ts = (cached.get("downloaded_at_utc") or "").strip()
        if not sha:
            sha = sha256_file(dest)
        if ts:
            try:
                dt = datetime.fromisoformat(ts.replace("Z", "+00:00")).replace(tzinfo=None)
            except Exception:
                dt = datetime.utcfromtimestamp(dest.stat().st_mtime)
        else:
            dt = datetime.utcfromtimestamp(dest.stat().st_mtime)

        return DownloadedFile(url=url, raw_path=dest, sha256=sha, downloaded_at_utc=dt)

    # Retry settings
    max_retries = int(os.environ.get("RTT_DOWNLOAD_RETRIES", "5"))
    backoff_s = float(os.environ.get("RTT_DOWNLOAD_BACKOFF_S", "1.0"))
    delay_s = float(os.environ.get("RTT_REQUEST_DELAY_S", "0.0"))

    tmp = dest.with_suffix(dest.suffix + ".part")

    # Use a connect/read timeout pair to reduce hanging connections.
    timeout = (20, timeout_s)

    last_err: Optional[BaseException] = None
    for attempt in range(1, max_retries + 1):
        # Determine whether to resume a previous partial.
        start_at = 0
        mode = "wb"
        headers = {}

        if resume_partial and tmp.exists() and tmp.stat().st_size > 0 and not force:
            start_at = tmp.stat().st_size
            headers = {"Range": f"bytes={start_at}-"}
            mode = "ab"
            if verbose and attempt == 1:
                print(f"Resuming partial download ({start_at} bytes): {dest.name}")
        else:
            # Ensure old partials don't interfere
            if tmp.exists() and (force or tmp.stat().st_size == 0):
                tmp.unlink(missing_ok=True)

        # Log only when we are actually attempting a network download.
        if attempt == 1:
            print(f"Downloading {url}")
        else:
            wait = backoff_s * (2 ** (attempt - 2))
            print(f"Retrying download (attempt {attempt}/{max_retries}) after {wait:.1f}s: {dest.name}")
            time.sleep(wait)

        try:
            with SESSION.get(url, stream=True, timeout=timeout, headers=headers, allow_redirects=True) as r:
                # If server doesn't honour Range, restart the download.
                if start_at > 0 and r.status_code == 200:
                    start_at = 0
                    mode = "wb"
                    headers = {}
                    if tmp.exists():
                        tmp.unlink(missing_ok=True)

                r.raise_for_status()

                # Fail fast if we accidentally got HTML.
                ctype = (r.headers.get("Content-Type") or "").lower()
                if "text/html" in ctype:
                    raise RuntimeError(f"Download returned HTML, not an Excel/ZIP file: {url}")

                with tmp.open(mode) as f:
                    for chunk in r.iter_content(chunk_size=1024 * 256):
                        if chunk:
                            f.write(chunk)

            # If the completed file is HTML, force a failure to avoid downstream conversion/parsing hangs.
            if _looks_like_html(tmp):
                raise RuntimeError(f"Downloaded content looks like HTML (blocked/interstitial): {url}")

            tmp.replace(dest)

            if delay_s > 0:
                time.sleep(delay_s)

            return DownloadedFile(
                url=url,
                raw_path=dest,
                sha256=sha256_file(dest),
                downloaded_at_utc=datetime.utcnow(),
            )

        except RuntimeError:
            # HTML/interstitial pages are not transient; don't retry.
            raise
        except (requests.exceptions.RequestException, OSError) as e:
            last_err = e
            # Keep partial file so next attempt can resume.
            if attempt >= max_retries:
                break
            if verbose:
                print(f"Download error ({type(e).__name__}): {e}")

    assert last_err is not None
    raise requests.exceptions.ConnectionError(
        f"Failed to download after {max_retries} attempts: {url}"
    ) from last_err


def _soffice_user_profile_dir(base: Path) -> Path:
    """Create a unique LibreOffice user profile directory.

    LibreOffice headless conversions can hang if a previous run left a lock behind, or if the
    profile is specified using a malformed file:// URI (common when relative paths are used on macOS).
    We therefore:
      * resolve to an absolute path
      * create a unique subdirectory per conversion run
    """
    base = base.resolve()
    root = base / "_lo_profile"
    ensure_dir(root)
    run_dir = root / f"run_{os.getpid()}_{int(time.time() * 1000)}"
    ensure_dir(run_dir)
    return run_dir


def libreoffice_convert_xls_to_xlsx(xls_path: Path, out_dir: Path) -> Path:
    """Convert legacy .xls into .xlsx using LibreOffice headless conversion.

    Fixes common macOS issues where LibreOffice is invoked with a malformed file:// URI when relative
    paths are used (e.g. `file://data/...`), which can cause conversions to hang indefinitely.
    """
    out_dir = out_dir.resolve()
    xls_path = xls_path.resolve()
    ensure_dir(out_dir)

    # Reuse an existing conversion if present (speeds up reruns and avoids repeated LibreOffice calls).
    expected = out_dir / (xls_path.stem + ".xlsx")
    if expected.exists() and expected.stat().st_size > 0:
        return expected
    cands = sorted(out_dir.glob(f"{xls_path.stem}*.xlsx"))
    if cands and cands[0].stat().st_size > 0:
        return cands[0]

    soffice = os.environ.get("RTT_SOFFICE_PATH", "soffice")
    timeout_s = int(os.environ.get("RTT_LIBREOFFICE_TIMEOUT_S", "300"))

    profile = _soffice_user_profile_dir(out_dir)
    profile_uri = profile.resolve().as_uri()  # -> file:///... (always absolute)

    cmd = [
        soffice,
        "--headless",
        "--nologo",
        "--nofirststartwizard",
        "--norestore",
        f"-env:UserInstallation={profile_uri}",
        "--convert-to",
        "xlsx",
        "--outdir",
        str(out_dir),
        str(xls_path),
    ]

    print(f"Converting (LibreOffice): {xls_path.name}")
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout_s)
    except subprocess.TimeoutExpired as e:
        raise RuntimeError(
            f"LibreOffice conversion timed out after {timeout_s}s for {xls_path}. "
            f"Increase RTT_LIBREOFFICE_TIMEOUT_S if needed. Command: {' '.join(cmd)}"
        ) from e

    if proc.returncode != 0:
        raise RuntimeError(
            f"LibreOffice conversion failed for {xls_path}:\nSTDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}"
        )

    out_path = out_dir / (xls_path.stem + ".xlsx")
    if not out_path.exists():
        # Fallback: LibreOffice occasionally tweaks output names
        cands = sorted(out_dir.glob(f"{xls_path.stem}*.xlsx"))
        if cands:
            out_path = cands[0]
        else:
            raise FileNotFoundError(f"Expected converted file not found: {out_path}")

    # Clean up per-run profile dir unless explicitly kept for debugging
    if os.environ.get("RTT_KEEP_LO_PROFILE", "0") not in ("1", "true", "True"):
        shutil.rmtree(profile, ignore_errors=True)

    return out_path


def parse_period_from_header(sheet_df: pd.DataFrame) -> Optional[date]:
    """Parse the "Period:" header to a month-end date."""
    top = sheet_df.iloc[:30, :10].copy()
    for r in range(top.shape[0]):
        row = [str(x).strip() if pd.notna(x) else "" for x in top.iloc[r].tolist()]
        for c, val in enumerate(row):
            if val.lower() == "period:":
                if c + 1 < len(row) and row[c + 1]:
                    s = row[c + 1]
                    try:
                        dt = pd.to_datetime(s, dayfirst=True, errors="raise")
                        month_start = date(dt.year, dt.month, 1)
                        next_month = month_start + relativedelta(months=1)
                        return next_month - relativedelta(days=1)
                    except Exception:
                        return None
    return None


def normalise_tfc(code_raw: str) -> str:
    """Normalise published Treatment Function Codes across known format changes."""
    code_raw = (code_raw or "").strip()
    code = re.sub(r"^(IP|AP|NP)", "", code_raw, flags=re.I)
    code = re.sub(r"^C_", "", code, flags=re.I)
    return code.upper()


def tfc_to_numeric(tfc: str) -> Optional[int]:
    """Convert a normalised Treatment Function Code to a numeric code.

    Notes
    -----
    NHS England publication files sometimes use:
    - Digits, e.g. "100"
    - "X01" for the "Other" category
    - "999" for totals

    This function maps "X01" → 1 to align with historic outputs.
    """
    if tfc is None:
        return None
    t = str(tfc).strip().upper()
    if not t:
        return None
    if t == "X01":
        return 1
    if t.isdigit():
        try:
            return int(t)
        except Exception:
            return None
    # fallback: extract first digit run
    m = re.search(r"\d+", t)
    if m:
        try:
            return int(m.group(0))
        except Exception:
            return None
    return None


def _canon_col(c) -> str:
    return re.sub(r"\s+", " ", str(c)).strip().lower()


def _find_col_by_regex(columns: Iterable, patterns: Sequence[str]) -> Optional[str]:
    """Find first column whose *canonicalised* name matches any regex in patterns."""
    pats = [re.compile(p, flags=re.I) for p in patterns]
    for c in columns:
        cc = _canon_col(c)
        if any(p.search(cc) for p in pats):
            return c
    return None


def compute_financial_year(period_end: date) -> str:
    if period_end.month >= 4:
        start = period_end.year
        end = period_end.year + 1
    else:
        start = period_end.year - 1
        end = period_end.year
    return f"{start}-{str(end)[-2:]}"


def _find_header_row(raw: pd.DataFrame, required_tokens: Iterable[str], max_scan_rows: int = 250) -> int:
    tokens = [t.lower() for t in required_tokens]
    for i in range(min(len(raw), max_scan_rows)):
        row_lc = raw.iloc[i].astype(str).str.lower()
        if all(row_lc.str.contains(tok).any() for tok in tokens):
            return i
    raise ValueError("Could not locate header row")


def parse_incomplete_commissioner_national(xlsx_path: Path) -> pd.DataFrame:
    """Extract National-level totals/median wait by specialty from an Incomplete Commissioner file."""
    xl = pd.ExcelFile(xlsx_path, engine="openpyxl")
    nat_sheet = next((s for s in xl.sheet_names if s.lower().startswith("national")), None)
    if nat_sheet is None:
        raise ValueError(f"No 'National' sheet found in {xlsx_path.name}")

    raw = pd.read_excel(xlsx_path, sheet_name=nat_sheet, header=None, engine="openpyxl")
    period_end = parse_period_from_header(raw)

    header_row = _find_header_row(raw, required_tokens=["treatment function code"], max_scan_rows=250)

    df = raw.iloc[header_row + 1 :].copy()
    df.columns = raw.iloc[header_row].tolist()
    df = df.loc[:, [c for c in df.columns if pd.notna(c)]]

    col_code = "Treatment Function Code"
    col_name = "Treatment Function"
    col_total = "Total number of incomplete pathways"
    col_median = "Average (median) waiting time (in weeks)"
    for c in [col_code, col_name, col_total, col_median]:
        if c not in df.columns:
            raise ValueError(f"Missing expected column '{c}' in {xlsx_path.name}")

    out = df[[col_code, col_name, col_total, col_median]].rename(
        columns={
            col_code: "tfc_code_raw",
            col_name: "specialty",
            col_total: "total_waiting",
            col_median: "median_wait_weeks",
        }
    )

    out = out[out["tfc_code_raw"].notna()].copy()
    out["tfc_code_raw"] = out["tfc_code_raw"].astype(str).str.strip()
    out["tfc"] = out["tfc_code_raw"].map(normalise_tfc)
    out["total_waiting"] = pd.to_numeric(out["total_waiting"], errors="coerce").astype("Int64")
    out["median_wait_weeks"] = pd.to_numeric(out["median_wait_weeks"], errors="coerce")
    out["period_end"] = pd.to_datetime(period_end) if period_end else pd.NaT
    out["is_total_row"] = out["tfc"].eq("999") | out["specialty"].astype(str).str.lower().eq("total")

    return out


def parse_incomplete_provider_trust_specialty(xlsx_path: Path) -> pd.DataFrame:
    """Extract Provider (trust) totals/median wait by specialty from an Incomplete Provider file."""
    xl = pd.ExcelFile(xlsx_path, engine="openpyxl")

    # Prefer the sheet called exactly "Provider". Some workbooks include "Provider with DTA" etc.
    provider_sheet = None
    for s in xl.sheet_names:
        if s.strip().lower() == "provider":
            provider_sheet = s
            break
    if provider_sheet is None:
        # fallback: first sheet that starts with Provider but isn't IS Provider
        provider_sheet = next(
            (
                s
                for s in xl.sheet_names
                if s.strip().lower().startswith("provider") and not s.strip().lower().startswith("is provider")
            ),
            None,
        )
    if provider_sheet is None:
        raise ValueError(f"No 'Provider' sheet found in {xlsx_path.name}")

    raw = pd.read_excel(xlsx_path, sheet_name=provider_sheet, header=None, engine="openpyxl")
    period_end = parse_period_from_header(raw)

    header_row = _find_header_row(
        raw,
        required_tokens=["provider code", "treatment function code"],
        max_scan_rows=400,
    )

    df = raw.iloc[header_row + 1 :].copy()
    df.columns = raw.iloc[header_row].tolist()
    df = df.loc[:, [c for c in df.columns if pd.notna(c)]]

    col_provider_code = "Provider Code"
    col_provider_name = "Provider Name"
    col_code = "Treatment Function Code"
    col_name = "Treatment Function"
    col_total = "Total number of incomplete pathways"
    col_median = "Average (median) waiting time (in weeks)"

    for c in [col_provider_code, col_provider_name, col_code, col_name, col_total, col_median]:
        if c not in df.columns:
            raise ValueError(f"Missing expected column '{c}' in {xlsx_path.name}")

    out = df[[col_provider_code, col_provider_name, col_code, col_name, col_total, col_median]].rename(
        columns={
            col_provider_code: "provider_code",
            col_provider_name: "provider_name",
            col_code: "tfc_code_raw",
            col_name: "specialty",
            col_total: "total_waiting",
            col_median: "median_wait_weeks",
        }
    )

    out = out[out["provider_code"].notna() & out["tfc_code_raw"].notna()].copy()

    out["provider_code"] = out["provider_code"].astype(str).str.strip()
    out["provider_name"] = out["provider_name"].astype(str).str.strip()
    out["tfc_code_raw"] = out["tfc_code_raw"].astype(str).str.strip()
    out["tfc"] = out["tfc_code_raw"].map(normalise_tfc)

    out["total_waiting"] = pd.to_numeric(out["total_waiting"], errors="coerce").astype("Int64")
    out["median_wait_weeks"] = pd.to_numeric(out["median_wait_weeks"], errors="coerce")
    out["period_end"] = pd.to_datetime(period_end) if period_end else pd.NaT
    out["is_total_row"] = out["tfc"].eq("999") | out["specialty"].astype(str).str.lower().eq("total")

    return out


def parse_provider_trust_specialty_part(xlsx_path: Path, pathway_type: str) -> pd.DataFrame:
    """Extract Provider (trust) totals/median wait by specialty for a given RTT pathway type.

    Parameters
    ----------
    pathway_type:
        One of: "incomplete", "admitted", "nonadmitted".

    Returns
    -------
    pd.DataFrame
        Columns:
        - provider_code, provider_name
        - tfc_code_raw, specialty
        - total_pathways, median_wait_weeks
        - pathway_type
        - period_end, tfc, is_total_row
    """
    pt = pathway_type.strip().lower()
    if pt not in {"incomplete", "admitted", "nonadmitted"}:
        raise ValueError("pathway_type must be one of: incomplete, admitted, nonadmitted")

    xl = pd.ExcelFile(xlsx_path, engine="openpyxl")

    # Prefer the sheet called exactly "Provider". Some workbooks include "Provider with DTA" etc.
    provider_sheet = None
    for s in xl.sheet_names:
        if s.strip().lower() == "provider":
            provider_sheet = s
            break
    if provider_sheet is None:
        provider_sheet = next(
            (
                s
                for s in xl.sheet_names
                if s.strip().lower().startswith("provider") and not s.strip().lower().startswith("is provider")
            ),
            None,
        )
    if provider_sheet is None:
        raise ValueError(f"No 'Provider' sheet found in {xlsx_path.name}")

    raw = pd.read_excel(xlsx_path, sheet_name=provider_sheet, header=None, engine="openpyxl")
    period_end = parse_period_from_header(raw)

    header_row = _find_header_row(
        raw,
        required_tokens=["provider code", "treatment function code"],
        max_scan_rows=400,
    )

    df = raw.iloc[header_row + 1 :].copy()
    df.columns = raw.iloc[header_row].tolist()
    df = df.loc[:, [c for c in df.columns if pd.notna(c)]]

    # Core columns
    col_provider_code = "Provider Code" if "Provider Code" in df.columns else _find_col_by_regex(
        df.columns,
        [r"^provider\s*code$", r"provider.*code"],
    )
    col_provider_name = "Provider Name" if "Provider Name" in df.columns else _find_col_by_regex(
        df.columns,
        [r"^provider\s*name$", r"provider.*name"],
    )
    col_code = "Treatment Function Code" if "Treatment Function Code" in df.columns else _find_col_by_regex(
        df.columns,
        [r"treatment\s*function\s*code", r"tfc.*code"],
    )
    col_name = "Treatment Function" if "Treatment Function" in df.columns else _find_col_by_regex(
        df.columns,
        [r"^treatment\s*function$", r"treatment\s*function(?!.*code)"],
    )
    if not all([col_provider_code, col_provider_name, col_code, col_name]):
        raise ValueError(f"Missing key columns in {xlsx_path.name} (provider_code/name, treatment function code/name)")

    # Median wait column
    col_median = (
        "Average (median) waiting time (in weeks)"
        if "Average (median) waiting time (in weeks)" in df.columns
        else _find_col_by_regex(
            df.columns,
            [
                r"average.*\(median\).*waiting.*weeks",
                r"median.*waiting.*weeks",
            ],
        )
    )
    if not col_median:
        raise ValueError(f"Could not find a median-wait column in {xlsx_path.name}")

        # Total/completed-count column varies by pathway type and year; column names are not fully consistent.
    # We first try to find a "total/number of pathways" column. If absent (rare), we infer totals from
    # mutually-exclusive wait-time band columns (e.g. 0-1, 1-2, ..., 52+).
    exclude_direct = [
        r"%", r"percent", r"percentage",
        r"median", r"average", r"waiting\s*time",
        r"within", r"\bover\b", r"breach",
    ]

    def _find_count_col(patterns: Sequence[str], extra_exclude: Sequence[str] = ()) -> Optional[str]:
        pats = [re.compile(p, flags=re.I) for p in patterns]
        exs = [re.compile(p, flags=re.I) for p in list(exclude_direct) + list(extra_exclude)]
        for pat in pats:
            for c in df.columns:
                cc = _canon_col(c)
                if any(ex.search(cc) for ex in exs):
                    continue
                if pat.search(cc):
                    return c
        return None

    col_total = None
    if pt == "incomplete":
        col_total = (
            "Total number of incomplete pathways"
            if "Total number of incomplete pathways" in df.columns
            else _find_count_col(
                [
                    r"(total|number).*(incomplete).*pathways?",
                    r"incomplete.*pathways?.*(total|number)",
                ]
            )
        )
    elif pt == "nonadmitted":
        col_total = _find_count_col(
            [
                r"(total|number).*(completed\s+)?non[-\s]?admitted.*pathways?",
                r"(total|number).*non[-\s]?admitted.*pathways?",
                r"(total|number).*(completed\s+)?pathways?",
                r"^(completed\s+)?pathways?$",
            ]
        )
        # Safety: if we fell back to a generic column that mentions "admitted" but not "non", reject it.
        if col_total:
            cc = _canon_col(col_total)
            if ("admitted" in cc) and ("non" not in cc):
                col_total = None

    else:  # admitted
        col_total = _find_count_col(
            [
                r"(total|number).*(completed\s+)?admitted.*pathways?",
                r"(total|number).*admitted.*pathways?",
                r"(total|number).*(completed\s+)?pathways?",
                r"^(completed\s+)?pathways?$",
            ],
            extra_exclude=[r"non[-\s]?admitted"],
        )

    # Build output skeleton first; attach total_pathways via either a column or a fallback inference.
    out = df[[col_provider_code, col_provider_name, col_code, col_name, col_median]].rename(
        columns={
            col_provider_code: "provider_code",
            col_provider_name: "provider_name",
            col_code: "tfc_code_raw",
            col_name: "specialty",
            col_median: "median_wait_weeks",
        }
    )

    total_series = None
    if col_total:
        total_series = df[col_total]
    else:
        # Fallback: infer totals from wait-time band columns (mutually exclusive bands).
        band_exclude = [
            r"%", r"percent", r"percentage",
            r"median", r"average", r"waiting\s*time",
            r"within", r"\bover\b", r"breach",
        ]
        band_patterns = [
            r"\b\d+\s*[-–]\s*\d+\b",        # 0-1, 18-25
            r"\b\d+\s*to\s*\d+\b",          # 0 to 1
            r"\b\d+\s*\+\b",                 # 52+
            r">\s*\d+\b",                      # >52
        ]
        band_cols = []
        for c in df.columns:
            if c in {col_provider_code, col_provider_name, col_code, col_name, col_median}:
                continue
            cc = _canon_col(c)
            if any(re.search(p, cc, flags=re.I) for p in band_exclude):
                continue
            if any(re.search(p, cc, flags=re.I) for p in band_patterns):
                band_cols.append(c)

        if band_cols:
            tmp = df[band_cols].apply(pd.to_numeric, errors="coerce")
            total_series = tmp.sum(axis=1, min_count=1)
        else:
            cols_preview = ", ".join([_canon_col(c) for c in list(df.columns)[:40]])
            raise ValueError(
                f"Could not find a total-count column for pathway_type={pt} in {xlsx_path.name}. "
                f"Columns (first 40): {cols_preview}"
            )

    out["total_pathways"] = pd.to_numeric(total_series, errors="coerce").astype("Int64")

    out = out[out["provider_code"].notna() & out["tfc_code_raw"].notna()].copy()

    out["provider_code"] = out["provider_code"].astype(str).str.strip()
    out["provider_name"] = out["provider_name"].astype(str).str.strip()
    out["tfc_code_raw"] = out["tfc_code_raw"].astype(str).str.strip()
    out["tfc"] = out["tfc_code_raw"].map(normalise_tfc)

    out["total_pathways"] = pd.to_numeric(out["total_pathways"], errors="coerce").astype("Int64")
    out["median_wait_weeks"] = pd.to_numeric(out["median_wait_weeks"], errors="coerce")

    out["pathway_type"] = pt
    out["period_end"] = pd.to_datetime(period_end) if period_end else pd.NaT
    out["is_total_row"] = out["tfc"].eq("999") | out["specialty"].astype(str).str.lower().eq("total")

    return out


def run_etl(
    out_dir: Path,
    start_fy: int = 2014,
    end_fy: int = 2018,
    dataset: str = "provider_trust_all",
    input_dir: Optional[Path] = None,
    *,
    force_download: bool = False,
    verbose_download: bool = False,
) -> None:
    # Normalise output directory.
    #
    # If a user passes a relative path (e.g. `--out data`) we anchor it to PROJECT_ROOT,
    # not the current working directory, to avoid accidental re-downloads when running
    # from different folders / notebook CWDs.
    out_dir = Path(out_dir)
    if not out_dir.is_absolute():
        out_dir = (PROJECT_ROOT / out_dir).resolve()
    else:
        out_dir = out_dir.expanduser().resolve()

    print(f"== Using output directory: {out_dir}")

    bronze = out_dir / "bronze"
    silver = out_dir / "silver"
    gold = out_dir / "gold"
    ensure_dir(bronze)
    ensure_dir(silver)
    ensure_dir(gold)

    # Helpful diagnostics: confirm whether we are reusing an existing cache.
    try:
        existing_raw = (
            sum(1 for _ in bronze.rglob("*.xls"))
            + sum(1 for _ in bronze.rglob("*.xlsx"))
            + sum(1 for _ in bronze.rglob("*.zip"))
        )
        if existing_raw > 0:
            print(f"== Found existing source files under bronze/: {existing_raw}")
    except Exception:
        pass

    # Small sanity signal on reruns: how many raw source files already exist?
    if verbose_download:
        existing = (
            list(bronze.rglob("*.xls"))
            + list(bronze.rglob("*.xlsx"))
            + list(bronze.rglob("*.zip"))
        )
        print(f"== Existing source files under bronze: {len(existing)}")

    # Backwards compatible aliases
    dataset = dataset.strip().lower()
    if dataset == "commissioner_national":
        dataset = "commissioner_national_incomplete"
    if dataset == "provider_trust":
        dataset = "provider_trust_incomplete"

    valid = {
        "commissioner_national_incomplete",
        "provider_trust_incomplete",
        "provider_trust_admitted",
        "provider_trust_nonadmitted",
        "provider_trust_all",
    }
    if dataset not in valid:
        raise ValueError(f"dataset must be one of: {sorted(valid)}")

    # Load any prior manifest so reruns can re-use SHA/timestamps without re-hashing every file.
    # (Downloads themselves are skipped based on file existence.)
    manifest_path = bronze / f"manifest_{dataset}.csv"
    manifest_cache = _load_existing_manifest(manifest_path)

    # Configure which monthly publication files to pull and how to parse them.
    if dataset == "commissioner_national_incomplete":
        parts = [
            {
                "pathway_type": "incomplete",
                "include": ["Incomplete Commissioner"],
                "exclude": [],
                "parser": lambda p: parse_incomplete_commissioner_national(p).rename(
                    columns={"total_waiting": "total_pathways"}
                ),
                "grain": "commissioner_national",
            }
        ]
        parquet_name = "rtt_commissioner_national_specialty_incomplete.parquet"
        csv_name_long = f"rtt_commissioner_national_specialty_incomplete_{start_fy}_{end_fy+1}_long.csv"
        csv_name_wide = None
        sort_cols_long = ["period_end", "tfc"]
        dedup_cols = ["period_end", "tfc", "pathway_type"]
    else:
        wanted_parts = {
            "provider_trust_incomplete": ["incomplete"],
            "provider_trust_admitted": ["admitted"],
            "provider_trust_nonadmitted": ["nonadmitted"],
            "provider_trust_all": ["incomplete", "admitted", "nonadmitted"],
        }[dataset]

        provider_parts = [
            {
                "pathway_type": "incomplete",
                "include": ["Incomplete Provider"],
                "exclude": [],
                "parser": lambda p: parse_provider_trust_specialty_part(p, "incomplete"),
                "grain": "provider_trust",
            },
            {
                "pathway_type": "admitted",
                "include": ["Admitted Provider"],
                # Some years use "Non Admitted" (space) which would also match "Admitted Provider".
                "exclude": ["NonAdmitted Provider", "Non Admitted Provider", "Non-admitted Provider"],
                "parser": lambda p: parse_provider_trust_specialty_part(p, "admitted"),
                "grain": "provider_trust",
            },
            {
                "pathway_type": "nonadmitted",
                "include": ["NonAdmitted Provider", "Non Admitted Provider", "Non-admitted Provider"],
                "exclude": [],
                "parser": lambda p: parse_provider_trust_specialty_part(p, "nonadmitted"),
                "grain": "provider_trust",
            },
        ]
        parts = [p for p in provider_parts if p["pathway_type"] in wanted_parts]

        parquet_name = f"rtt_provider_trust_specialty_{dataset.replace('provider_trust_', '')}.parquet"
        csv_name_long = f"rtt_provider_trust_specialty_{dataset.replace('provider_trust_', '')}_{start_fy}_{end_fy+1}_long.csv"
        csv_name_wide = (
            f"rtt_provider_trust_specialty_all_{start_fy}_{end_fy+1}_wide.csv" if dataset == "provider_trust_all" else None
        )
        sort_cols_long = ["period_end", "provider_code", "tfc", "pathway_type"]
        dedup_cols = ["period_end", "provider_code", "tfc", "pathway_type"]

    manifest_rows: List[dict] = []
    all_rows: List[pd.DataFrame] = []

    offline_done = False
    if input_dir is not None:
        input_dir = Path(input_dir)
        if not input_dir.exists():
            raise FileNotFoundError(f"input_dir not found: {input_dir}")
        print(f"== Offline mode: scanning local files under {input_dir}")
        local_files = list_input_files(input_dir)

        for part in parts:
            matched_files = filter_input_files(local_files, part["include"], part["exclude"])
            if not matched_files:
                print(
                    f"WARNING: no local files matched pathway_type={part['pathway_type']} include={part['include']}"
                )

            for raw_path in matched_files:
                filename = raw_path.name

                manifest_rows.append(
                    {
                        "financial_year_page": "",
                        "dataset": dataset,
                        "pathway_type": part["pathway_type"],
                        "source_url": "",
                        "raw_path": str(raw_path),
                        "sha256": sha256_file(raw_path),
                        "downloaded_at_utc": datetime.utcnow().isoformat(),
                        "mode": "offline",
                    }
                )

                # Convert/read (store artefacts under data/bronze/_local)
                local_root = bronze / "_local" / part["pathway_type"]
                ensure_dir(local_root)

                if raw_path.suffix.lower() == ".xlsx":
                    xlsx_path = raw_path
                elif raw_path.suffix.lower() == ".xls":
                    converted_dir = local_root / "_converted"
                    xlsx_path = libreoffice_convert_xls_to_xlsx(raw_path, converted_dir)
                elif raw_path.suffix.lower() == ".zip":
                    import zipfile

                    tmpdir = local_root / "_unzipped" / raw_path.stem
                    ensure_dir(tmpdir)
                    with zipfile.ZipFile(raw_path, "r") as z:
                        z.extractall(tmpdir)
                    candidates = list(tmpdir.glob("*.xls")) + list(tmpdir.glob("*.xlsx"))
                    if not candidates:
                        raise FileNotFoundError(f"No Excel files found inside {raw_path}")
                    cand = candidates[0]
                    if cand.suffix.lower() == ".xlsx":
                        xlsx_path = cand
                    else:
                        xlsx_path = libreoffice_convert_xls_to_xlsx(cand, tmpdir / "_converted")
                else:
                    raise ValueError(f"Unsupported file type: {raw_path}")

                df = part["parser"](xlsx_path)
                df["pathway_type"] = part["pathway_type"]
                df["source_url"] = ""
                df["source_file"] = filename
                df["source_path"] = str(raw_path)

                if df["period_end"].notna().any():
                    pe = pd.to_datetime(df["period_end"].dropna().iloc[0]).date()
                    df["financial_year"] = compute_financial_year(pe)
                else:
                    df["financial_year"] = ""

                all_rows.append(df)

        offline_done = True

    for fy_start in range(start_fy, end_fy + 1):
        if offline_done:
            continue
        key = f"{fy_start}-{str(fy_start + 1)[-2:]}"
        # NHS England FY pages follow a consistent URL pattern; support newer years without
        # hard-coding them in YEAR_PAGES.
        year_url = YEAR_PAGES.get(key) or (
            "https://www.england.nhs.uk/statistics/statistical-work-areas/rtt-waiting-times/"
            f"rtt-data-{key}/"
        )

        print(f"== FY {key}: scraping {year_url}")
        try:
            html = fetch_year_page(year_url)
        except requests.HTTPError as e:
            # If a FY page doesn't exist (e.g., future year), skip gracefully.
            resp = getattr(e, "response", None)
            if resp is not None and getattr(resp, "status_code", None) == 404:
                print(f"   FY page not found (404), skipping: {year_url}")
                continue
            raise

        for part in parts:
            file_urls = extract_monthly_links(
                html,
                year_url,
                include_needles=part["include"],
                exclude_needles=part["exclude"],
            )

            for url in file_urls:
                filename = url.split("/")[-1].split("?")[0]
                raw_path = bronze / key / part["pathway_type"] / filename

                # Download (cached). Reruns should skip files already on disk.

                info = download(
                    url,
                    raw_path,
                    skip_if_exists=True,
                    force=force_download,
                    resume_partial=True,
                    manifest_cache=manifest_cache,
                    verbose=verbose_download,
                )

                manifest_rows.append(
                    {
                        "financial_year_page": key,
                        "dataset": dataset,
                        "pathway_type": part["pathway_type"],
                        "source_url": info.url,
                        "raw_path": str(info.raw_path),
                        "sha256": info.sha256,
                        "downloaded_at_utc": info.downloaded_at_utc.isoformat(),
                        "mode": "online",
                    }
                )

                # Convert/read
                if raw_path.suffix.lower() == ".xlsx":
                    xlsx_path = raw_path
                elif raw_path.suffix.lower() == ".xls":
                    converted_dir = bronze / key / part["pathway_type"] / "_converted"
                    xlsx_path = libreoffice_convert_xls_to_xlsx(raw_path, converted_dir)
                elif raw_path.suffix.lower() == ".zip":
                    import zipfile

                    tmpdir = bronze / key / part["pathway_type"] / "_unzipped" / raw_path.stem
                    ensure_dir(tmpdir)
                    with zipfile.ZipFile(raw_path, "r") as z:
                        z.extractall(tmpdir)
                    candidates = list(tmpdir.glob("*.xls")) + list(tmpdir.glob("*.xlsx"))
                    if not candidates:
                        raise FileNotFoundError(f"No Excel files found inside {raw_path}")
                    cand = candidates[0]
                    if cand.suffix.lower() == ".xlsx":
                        xlsx_path = cand
                    else:
                        xlsx_path = libreoffice_convert_xls_to_xlsx(cand, tmpdir / "_converted")
                else:
                    raise ValueError(f"Unsupported file type: {raw_path}")

                df = part["parser"](xlsx_path)
                # Ensure pathway_type is present and consistent.
                df["pathway_type"] = part["pathway_type"]
                df["source_url"] = url
                df["source_file"] = filename

                if df["period_end"].notna().any():
                    pe = pd.to_datetime(df["period_end"].dropna().iloc[0]).date()
                    df["financial_year"] = compute_financial_year(pe)
                else:
                    df["financial_year"] = key

                all_rows.append(df)

    # Persist a manifest of all downloaded/processed source files.
    # On rerun, merge with any prior manifest so resume runs don't lose earlier entries.
    manifest_df = pd.DataFrame(manifest_rows)
    if manifest_path.exists():
        try:
            prior = pd.read_csv(manifest_path)
            manifest_df = pd.concat([prior, manifest_df], ignore_index=True)
        except Exception:
            pass
    if not manifest_df.empty and "raw_path" in manifest_df.columns:
        manifest_df = manifest_df.drop_duplicates(subset=["raw_path"], keep="last")
    else:
        manifest_df = manifest_df.drop_duplicates(keep="last")
    manifest_df.to_csv(manifest_path, index=False)

    if not all_rows:
        raise RuntimeError("No data extracted.")

    full = pd.concat(all_rows, ignore_index=True)

    # If running in offline mode, restrict to the requested FY window.
    if input_dir is not None:
        start_dt = pd.Timestamp(date(start_fy, 4, 1))
        end_dt = pd.Timestamp(date(end_fy + 1, 3, 31))
        pe = pd.to_datetime(full["period_end"], errors="coerce")
        full = full[(pe >= start_dt) & (pe <= end_dt)].copy()


    # De-duplicate in case a page links multiple variants of the same month (revisions)
    full = full.drop_duplicates(subset=dedup_cols, keep="last")

    # Enhance: add period string and numeric TFC
    full["period"] = pd.to_datetime(full["period_end"], errors="coerce").dt.strftime("%Y-%m")
    full["tfc_numeric"] = full["tfc"].map(tfc_to_numeric).astype("Int64")

    # Provider/trust-friendly naming (keep originals too)
    if "provider_code" in full.columns:
        full["trust_code"] = full["provider_code"]
        full["trust_name"] = full.get("provider_name")
    full["specialty_code"] = full.get("tfc_code_raw")
    full["specialty_code_norm"] = full.get("tfc_numeric")
    full["specialty_name"] = full.get("specialty")
    full["is_total_specialty"] = full.get("is_total_row")

    # Outputs
    try:
        full.to_parquet(silver / parquet_name, index=False)
    except Exception as e:
        print(f"Could not write Parquet: {e}", file=sys.stderr)

    csv_path_long = gold / csv_name_long
    full.sort_values(sort_cols_long, inplace=True)
    full.to_csv(csv_path_long, index=False)

    csv_paths = [csv_path_long]

    # Optional: wide file for provider_trust_all
    if csv_name_wide and dataset.startswith("provider_trust"):
        id_cols = [
            "period_end",
            "period",
            "financial_year",
            "trust_code",
            "trust_name",
            "specialty_code_norm",
            "specialty_name",
            "is_total_specialty",
        ]
        base = full.copy()
        # Drop rows without key identifiers
        base = base[base["trust_code"].notna() & base["specialty_code_norm"].notna()].copy()

        wide = (
            base.pivot_table(
                index=id_cols,
                columns="pathway_type",
                values=["total_pathways", "median_wait_weeks"],
                aggfunc="first",
            )
            .reset_index()
        )

        # Flatten column index
        wide.columns = [
            "_".join([c for c in col if c]) if isinstance(col, tuple) else col  # type: ignore
            for col in wide.columns
        ]

        # Rename to user-facing measure names
        wide = wide.rename(
            columns={
                "total_pathways_incomplete": "incomplete_total_waiting",
                "median_wait_weeks_incomplete": "incomplete_median_wait_weeks",
                "total_pathways_admitted": "admitted_completed_pathways",
                "median_wait_weeks_admitted": "admitted_median_wait_weeks",
                "total_pathways_nonadmitted": "nonadmitted_completed_pathways",
                "median_wait_weeks_nonadmitted": "nonadmitted_median_wait_weeks",
            }
        )

        csv_path_wide = gold / csv_name_wide
        wide.sort_values(["period_end", "trust_code", "specialty_code_norm"], inplace=True)
        wide.to_csv(csv_path_wide, index=False)
        csv_paths.append(csv_path_wide)

    print(
        "Done. Outputs:\n"
        + "\n".join(f"- {p}" for p in csv_paths)
        + f"\n- {silver / parquet_name}\n- {manifest_path}"
    )


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--out", type=str, default="data")
    p.add_argument("--start-fy", type=int, default=2014)
    p.add_argument("--end-fy", type=int, default=2018)
    p.add_argument(
        "--input-dir",
        type=str,
        default=None,
        help=(
            "Optional local directory containing downloaded monthly Excel/ZIP files. "
            "If set, the pipeline runs in offline mode (no scraping/downloading)."
        ),
    )
    p.add_argument(
        "--dataset",
        type=str,
        default="provider_trust_all",
        choices=[
            # Provider/trust outputs
            "provider_trust_all",
            "provider_trust_incomplete",
            "provider_trust_admitted",
            "provider_trust_nonadmitted",
            # Commissioner (England) outputs
            "commissioner_national_incomplete",
            # Backwards-compatible aliases
            "provider_trust",
            "commissioner_national",
        ],
        help="Which dataset to extract (see README for details).",
    )
    p.add_argument(
        "--force-download",
        action="store_true",
        help="Re-download source files even if they already exist on disk.",
    )
    p.add_argument(
        "--verbose-download",
        action="store_true",
        help="Print messages when skipping/resuming downloads.",
    )
    args = p.parse_args()

    run_etl(
        Path(args.out),
        start_fy=args.start_fy,
        end_fy=args.end_fy,
        dataset=args.dataset,
        input_dir=Path(args.input_dir) if args.input_dir else None,
        force_download=args.force_download,
        verbose_download=args.verbose_download,
    )


if __name__ == "__main__":
    main()
