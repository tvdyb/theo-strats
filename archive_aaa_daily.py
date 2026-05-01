#!/usr/bin/env python3
"""Daily AAA archiver. Run ONCE per day after AAA's morning publish window.

Recommended cron (UTC):
    30 14 * * * cd /Users/wilsonw/mm-setup && python3 auto_theo/archive_aaa_daily.py >> auto_theo/archive_aaa_daily.log 2>&1

That fires at 14:30 UTC = 10:30 ET, after AAA's typical 06:00-10:00 ET refresh window.

To install via launchd on macOS, see `auto_theo/archive_aaa_daily.plist` (created alongside).

Behavior:
  - Scrapes https://gasprices.aaa.com/ (failover ?state=US) using the same regex
    + sanity guards as theo_refresh.fetch_aaa(). Hard-fails on any guard miss
    (does NOT corrupt the append-only archive).
  - Writes to auto_theo/archive/aaa/national_regular/<UTC_DATE>.json.
  - Idempotent: skips silently if today's file already exists with
    value_origin == "live_scrape".
  - EIA-fwd-fill clobber: if today's file exists with value_origin != "live_scrape"
    (synthetic), OVERWRITES with the live scrape (live data is authoritative).
  - Atomic write (tempfile + os.replace).
  - Stdlib only (urllib.request, no `requests`) so it works from cron without
    a venv.
"""
import datetime as dt
import json
import os
import re
import sys
import tempfile
import urllib.error
import urllib.request
from pathlib import Path

# --- canonical scraper constants, copied verbatim from theo_refresh.py ---
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36")

AAA_PRICE_MIN = 2.00      # below this is implausible (US national avg has not been < $2 since 2016)
AAA_PRICE_MAX = 7.00      # above this is implausible (peak-shock ceiling — US has never crossed $5.10)
AAA_MAX_DAILY_MOVE = 0.30 # |today - yesterday| must be ≤ this; AAA daily moves > 30¢ are unheard of

ARCHIVE_DIR = Path("/Users/wilsonw/mm-setup/auto_theo/archive/aaa/national_regular")
HOMEPAGE_URL = "https://gasprices.aaa.com/"
FAILOVER_URL = "https://gasprices.aaa.com/?state=US"


def _now_utc():
    return dt.datetime.now(dt.timezone.utc)


def _iso_z(t):
    return t.strftime("%Y-%m-%dT%H:%M:%SZ")


def _http_get(url, timeout=10):
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        body = resp.read()
        # AAA homepage is utf-8; fall back gracefully.
        try:
            return body.decode("utf-8")
        except UnicodeDecodeError:
            return body.decode("utf-8", errors="replace")


def _parse_aaa_html(html):
    """Same algorithm as theo_refresh.fetch_aaa(): find <tr>s where the row has
    4-6 $X.XXX prices, take the first column. First five qualifying rows are
    [today, yesterday, week, month, year]."""
    rows = []
    for tr in re.finditer(r"<tr[^>]*>(.*?)</tr>", html, re.S):
        prices = re.findall(r"\$\d\.\d{3}", tr.group(1))
        if 4 <= len(prices) <= 6:
            rows.append(float(prices[0].lstrip("$")))
        if len(rows) == 5:
            break
    return rows


def fetch_aaa():
    """Return (raw_dict, source_url_used). Tries homepage then ?state=US failover.
    Applies sanity guards verbatim from theo_refresh.fetch_aaa(); raises
    RuntimeError on any miss so we don't corrupt the archive."""
    last_exc = None
    chosen_url = None
    html = None
    for url in (HOMEPAGE_URL, FAILOVER_URL):
        try:
            html = _http_get(url)
            chosen_url = url
            break
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, OSError) as e:
            last_exc = e
            continue
    if html is None:
        raise RuntimeError(f"AAA fetch failed for both {HOMEPAGE_URL} and {FAILOVER_URL}: {last_exc!r}")

    rows = _parse_aaa_html(html)
    # Guard 1: <5 valid rows
    if len(rows) < 5:
        raise RuntimeError(f"AAA parse: only {len(rows)} rows found")
    today, yest = rows[0], rows[1]
    # Guard 2: today in plausible range
    if not (AAA_PRICE_MIN <= today <= AAA_PRICE_MAX):
        raise RuntimeError(
            f"AAA today=${today:.3f} outside plausible range "
            f"[${AAA_PRICE_MIN:.2f}, ${AAA_PRICE_MAX:.2f}] -- rejecting scrape"
        )
    # Guard 2b: yesterday in plausible range
    if not (AAA_PRICE_MIN <= yest <= AAA_PRICE_MAX):
        raise RuntimeError(f"AAA yesterday=${yest:.3f} outside plausible range -- rejecting scrape")
    # Guard 3: daily move
    if abs(today - yest) > AAA_MAX_DAILY_MOVE:
        raise RuntimeError(
            f"AAA daily move |today - yest| = ${abs(today - yest):.3f} > "
            f"${AAA_MAX_DAILY_MOVE:.2f} max -- rejecting scrape (likely stale page or parse error)"
        )
    return (
        {
            "today": today,
            "yesterday": yest,
            "week_ago": rows[2],
            "month_ago": rows[3],
            "year_ago": rows[4],
            "source_url_used": chosen_url,
        },
        chosen_url,
    )


def _atomic_write(path: Path, payload: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(payload, f, separators=(",", ":"))
        os.replace(tmp_path, path)
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def main():
    now = _now_utc()
    today_str = now.strftime("%Y-%m-%d")
    out_path = ARCHIVE_DIR / f"{today_str}.json"

    # Idempotency / clobber decision: read current file (if any) first.
    prior_value = None
    prior_origin = None
    if out_path.exists():
        try:
            prior = json.loads(out_path.read_text())
            prior_origin = prior.get("value_origin")
            prior_value = prior.get("value_usd_per_gal")
        except (json.JSONDecodeError, OSError) as e:
            # Don't auto-clobber a corrupted file; surface and exit.
            print(f"ERROR: existing {out_path} unreadable: {e!r}", file=sys.stderr)
            sys.exit(1)
        if prior_origin == "live_scrape":
            # Append-only: today's live scrape already exists. Skip silently.
            sys.exit(0)
        # else: fall through and clobber the synthetic placeholder.

    # Scrape (will raise if any guard fires).
    try:
        raw, source_url_used = fetch_aaa()
    except Exception as e:
        print(f"ERROR: AAA scrape failed: {e}", file=sys.stderr)
        sys.exit(1)

    fetched_iso = _iso_z(now)
    payload = {
        "fetched_at": fetched_iso,
        "source_url": HOMEPAGE_URL,
        "publication_timestamp": fetched_iso,
        "effective_date": today_str,
        "value_usd_per_gal": raw["today"],
        "value_origin": "live_scrape",
        "raw": {
            "today": raw["today"],
            "yesterday": raw["yesterday"],
            "week_ago": raw["week_ago"],
            "month_ago": raw["month_ago"],
            "year_ago": raw["year_ago"],
            "source_url_used": source_url_used,
        },
    }

    _atomic_write(out_path, payload)

    today_val = raw["today"]
    if prior_origin is None:
        print(f"archived AAA {today_str} = ${today_val:.3f} (new)")
    else:
        prior_disp = f"${float(prior_value):.3f}" if prior_value is not None else "?"
        print(
            f"archived AAA {today_str} = ${today_val:.3f} "
            f"(was: {prior_disp} via {prior_origin})"
        )


if __name__ == "__main__":
    main()
