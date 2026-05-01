#!/usr/bin/env python3
"""Daily archiver for the Truflation EV Commodity Index reconstruction.

Run ONCE per day after the major US futures session closes (e.g. 22:00 UTC
which is 18:00 ET — after NYMEX close so PA/PL settle prices are stable).

Recommended cron (UTC):
    0 22 * * * cd /Users/wilsonw/mm-setup && python3 auto_theo/archive_truflation_ev_daily.py >> auto_theo/archive_truflation_ev_daily.log 2>&1

Behavior:
  - Fetches Cu / Pd / Pt from Yahoo v8/finance/chart/{HG=F, PA=F, PL=F}.
  - Fetches LIT (lithium ETF proxy) and NICK.L (WisdomTree nickel ETC) from
    the same Yahoo endpoint.
  - Cobalt has no free daily feed — uses a hand-coded sparse anchor from the
    last known TradingEconomics print (~$56,290/MT as of 2026-04-30).
  - Writes append-only to auto_theo/archive/truflation_ev/<metal>/<UTC_DATE>.json
    with the standard schema (publication_timestamp, value, source_url,
    value_origin).
  - Idempotent: skips silently if today's file already exists with
    value_origin == "live_scrape".
  - Atomic write (tempfile + os.replace).
  - Stdlib only (urllib.request) so it works from cron without a venv.
"""
import datetime as dt
import json
import os
import sys
import tempfile
import urllib.error
import urllib.request
from pathlib import Path

UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
)

ARCHIVE_ROOT = Path("/Users/wilsonw/mm-setup/auto_theo/archive/truflation_ev")

# Yahoo v8/finance/chart fetch spec for each metal.
# value_unit notes: HG=F COMEX copper $/lb; PA=F NYMEX palladium $/oz; PL=F
# NYMEX platinum $/oz; LIT lithium ETF $/share (rescaled at use-site to ~USD/MT
# anchor — the PIPELINE uses the same series so the units cancel in the ratio);
# NICK.L WisdomTree nickel ETC GBp/share (rescaled to LME nickel USD/T anchor).
YAHOO_SYMBOLS = {
    "copper":    {"symbol": "HG=F", "unit": "USD/lb"},
    "palladium": {"symbol": "PA=F", "unit": "USD/oz"},
    "platinum":  {"symbol": "PL=F", "unit": "USD/oz"},
    "lithium":   {"symbol": "LIT",  "unit": "USD/share (proxy, rescaled at use)"},
    "nickel":    {"symbol": "NICK.L", "unit": "GBp/share (proxy, rescaled at use)"},
}

# Cobalt has no free daily feed; hand-coded sparse anchor. Update this when a
# fresh TradingEconomics print becomes available.
COBALT_SPARSE_ANCHOR = {
    "value": 56290.0,
    "unit": "USD/MT",
    "anchor_date": "2026-04-26",
    "anchor_source": "TradingEconomics LME cobalt print, hand-coded",
}

# Lithium and nickel are PROXIES. We rescale them at theo time inside the
# pipeline; here we just archive the raw close. The pipeline's anchor table
# (ANCHOR_PRICES_2025_10_01) handles the scaling via the chain-link ratio.
TIMEOUT_S = 15


def _now_utc() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def _iso_z(t: dt.datetime) -> str:
    return t.strftime("%Y-%m-%dT%H:%M:%SZ")


def _http_get(url: str, timeout: int = TIMEOUT_S) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        body = resp.read()
        try:
            return body.decode("utf-8")
        except UnicodeDecodeError:
            return body.decode("utf-8", errors="replace")


def _fetch_yahoo_close(symbol: str) -> tuple[float, str, str]:
    """Return (close_value, source_url, publication_timestamp_iso).

    Hits v8/finance/chart with range=5d, interval=1d. The most recent valid
    (timestamp, close) pair is the daily settle. Raises RuntimeError on any
    parse failure (we never silently archive a bad value).
    """
    url = (
        f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
        f"?range=5d&interval=1d"
    )
    raw = _http_get(url)
    try:
        j = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"yahoo {symbol}: JSON decode failed: {exc}")
    chart = j.get("chart") or {}
    err = chart.get("error")
    if err:
        raise RuntimeError(f"yahoo {symbol}: API error: {err}")
    results = chart.get("result") or []
    if not results:
        raise RuntimeError(f"yahoo {symbol}: empty result list")
    r0 = results[0]
    timestamps = r0.get("timestamp") or []
    indicators = r0.get("indicators") or {}
    quote = (indicators.get("quote") or [{}])[0]
    closes = quote.get("close") or []
    if not timestamps or not closes:
        raise RuntimeError(f"yahoo {symbol}: missing timestamp/close arrays")
    # Walk backwards to find the last non-null close.
    for i in range(len(closes) - 1, -1, -1):
        if closes[i] is None:
            continue
        ts = timestamps[i]
        try:
            value = float(closes[i])
        except (TypeError, ValueError):
            continue
        if value <= 0:
            continue
        pub_ts = dt.datetime.fromtimestamp(ts, tz=dt.timezone.utc)
        return value, url, _iso_z(pub_ts)
    raise RuntimeError(f"yahoo {symbol}: no valid close in last 5d")


def _atomic_write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(payload, f, separators=(",", ":"))
        os.replace(tmp, path)
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def _skip_if_live_already(out_path: Path) -> bool:
    """Return True iff out_path exists and has value_origin == 'live_scrape'."""
    if not out_path.exists():
        return False
    try:
        prior = json.loads(out_path.read_text())
    except (json.JSONDecodeError, OSError):
        return False
    return prior.get("value_origin") == "live_scrape"


def archive_yahoo_metal(metal: str, today_str: str, today_now: dt.datetime) -> tuple[str, str]:
    """Archive one Yahoo-driven metal. Returns (status, msg)."""
    info = YAHOO_SYMBOLS[metal]
    out_path = ARCHIVE_ROOT / metal / f"{today_str}.json"
    if _skip_if_live_already(out_path):
        return "skip", f"{metal} {today_str} already archived (live_scrape)"
    try:
        value, source_url, pub_ts_iso = _fetch_yahoo_close(info["symbol"])
    except (RuntimeError, urllib.error.URLError, urllib.error.HTTPError, OSError) as exc:
        return "error", f"{metal}: fetch failed: {exc!r}"
    payload = {
        "fetched_at": _iso_z(today_now),
        "source_url": source_url,
        "publication_timestamp": pub_ts_iso,
        "effective_date": today_str,
        "value": value,
        "value_unit": info["unit"],
        "value_origin": "live_scrape",
        "raw": {
            "symbol": info["symbol"],
            "metal": metal,
        },
    }
    _atomic_write(out_path, payload)
    return "ok", f"{metal} {today_str} = {value} {info['unit']}"


def archive_cobalt(today_str: str, today_now: dt.datetime) -> tuple[str, str]:
    """Archive cobalt sparse anchor (forward-fill from hand-coded value)."""
    out_path = ARCHIVE_ROOT / "cobalt" / f"{today_str}.json"
    if _skip_if_live_already(out_path):
        return "skip", f"cobalt {today_str} already archived"
    payload = {
        "fetched_at": _iso_z(today_now),
        "source_url": COBALT_SPARSE_ANCHOR["anchor_source"],
        "publication_timestamp": COBALT_SPARSE_ANCHOR["anchor_date"] + "T00:00:00Z",
        "effective_date": today_str,
        "value": COBALT_SPARSE_ANCHOR["value"],
        "value_unit": COBALT_SPARSE_ANCHOR["unit"],
        "value_origin": "sparse_anchor_forward_fill",
        "raw": dict(COBALT_SPARSE_ANCHOR),
    }
    _atomic_write(out_path, payload)
    return "ok", (
        f"cobalt {today_str} = {COBALT_SPARSE_ANCHOR['value']} "
        f"{COBALT_SPARSE_ANCHOR['unit']} (anchor {COBALT_SPARSE_ANCHOR['anchor_date']})"
    )


def main() -> int:
    today_now = _now_utc()
    today_str = today_now.strftime("%Y-%m-%d")
    rc = 0
    statuses: list[tuple[str, str, str]] = []
    for metal in YAHOO_SYMBOLS:
        status, msg = archive_yahoo_metal(metal, today_str, today_now)
        statuses.append((metal, status, msg))
        if status == "error":
            rc = 1
    cstatus, cmsg = archive_cobalt(today_str, today_now)
    statuses.append(("cobalt", cstatus, cmsg))
    for metal, status, msg in statuses:
        print(f"[{status}] {msg}")
    return rc


if __name__ == "__main__":
    sys.exit(main())
