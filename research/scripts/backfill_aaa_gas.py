#!/usr/bin/env python3
"""Archive backfiller for the AAA gas auto-theo system.

Two parallel backfills:
  A. Kalshi resolved daily-gas events (KXAAAGASD family) — markets + candlesticks.
  B. AAA national-regular daily history via:
       B.1 EIA weekly CSV (cross-check series).
       B.3 KXAAAGASD resolutions -> implied AAA daily bounds.

Hard rules:
  - Append-only archive; never overwrite existing files (idempotent skip).
  - Conservative pacing to avoid Kalshi 429 storms.
  - Writes only under /Users/wilsonw/mm-setup/auto_theo/archive/.
"""
import datetime as dt
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request

ROOT = "/Users/wilsonw/mm-setup/auto_theo"
ARCHIVE = os.path.join(ROOT, "archive")
KALSHI_MARKETS_DIR = os.path.join(ARCHIVE, "kalshi/markets")
KALSHI_CANDLES_DIR = os.path.join(ARCHIVE, "kalshi/candles")
AAA_EIA_DIR = os.path.join(ARCHIVE, "aaa/national_regular_eia_weekly")
AAA_IMPLIED_DIR = os.path.join(ARCHIVE, "aaa/national_regular_implied")

for d in (KALSHI_MARKETS_DIR, KALSHI_CANDLES_DIR, AAA_EIA_DIR, AAA_IMPLIED_DIR):
    os.makedirs(d, exist_ok=True)

KALSHI_BASE = "https://api.elections.kalshi.com/trade-api/v2"
SERIES_TICKER = "KXAAAGASD"
EIA_XLS_URL = "https://www.eia.gov/dnav/pet/hist_xls/EMM_EPMR_PTE_NUS_DPGw.xls"
USER_AGENT = "mm-setup-backfill/1.0"

INTER_CALL_SLEEP = 6.5            # seconds between successful Kalshi calls
BACKOFF_START = 30.0              # initial sleep on 429
BACKOFF_STEP = 20.0               # increment per retry
MAX_RETRIES = 12
CANDLE_MAX_RETRIES = 4            # candles are best-effort; keep budget tight

# Limit candidate event scan to most-recent N to bound run time on heavy 429.
MAX_EVENTS_TARGET = 200           # tighten to e.g. 20 if rate-limited heavily.

# Candles are highest-volume and least essential. Allow caller to disable via env.
SKIP_CANDLES = os.environ.get("BACKFILL_SKIP_CANDLES", "0") == "1"

stats = {
    "kalshi_requests": 0,
    "kalshi_429s": 0,
    "kalshi_other_errors": 0,
    "events_listed": 0,
    "events_processed": 0,
    "markets_written": 0,
    "markets_skipped_existing": 0,
    "candles_written": 0,
    "candles_skipped_existing": 0,
    "candles_failed": 0,
    "skipped_events": [],
    "eia_weekly_written": 0,
    "eia_weekly_skipped_existing": 0,
    "eia_first_date": None,
    "eia_last_date": None,
    "implied_daily_written": 0,
    "implied_daily_skipped_existing": 0,
    "started_at": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
}

def utcnow_iso():
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def kalshi_get(url, max_retries=MAX_RETRIES):
    """GET with exponential backoff on 429 / transient failures.

    Returns parsed JSON on success or raises RuntimeError on retry exhaustion.
    """
    last_err = None
    for i in range(max_retries):
        stats["kalshi_requests"] += 1
        try:
            req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(req, timeout=30) as r:
                data = r.read()
            j = json.loads(data)
            # Some endpoints return error envelopes with 200.
            if isinstance(j, dict) and "error" in j and isinstance(j["error"], dict):
                code = j["error"].get("code")
                if code == "too_many_requests":
                    stats["kalshi_429s"] += 1
                    wait = BACKOFF_START + BACKOFF_STEP * i
                    print(f"  [429-body] sleep {wait:.0f}s -- {url[:90]}", flush=True)
                    time.sleep(wait)
                    continue
                # other error envelope -> raise
                raise RuntimeError(f"kalshi error: {j['error']}")
            return j
        except urllib.error.HTTPError as e:
            if e.code == 429:
                stats["kalshi_429s"] += 1
                wait = BACKOFF_START + BACKOFF_STEP * i
                print(f"  [429] sleep {wait:.0f}s -- {url[:90]}", flush=True)
                time.sleep(wait)
                last_err = e
                continue
            stats["kalshi_other_errors"] += 1
            wait = BACKOFF_START + BACKOFF_STEP * i
            print(f"  [http {e.code}] sleep {wait:.0f}s -- {url[:90]}", flush=True)
            time.sleep(wait)
            last_err = e
        except Exception as e:
            stats["kalshi_other_errors"] += 1
            wait = BACKOFF_START + BACKOFF_STEP * i
            print(f"  [err {type(e).__name__}: {e}] sleep {wait:.0f}s", flush=True)
            time.sleep(wait)
            last_err = e
    raise RuntimeError(f"retries exhausted for {url}: {last_err}")


# -------------------- A. Kalshi KXAAAGASD --------------------

def list_settled_events():
    cursor = None
    out = []
    while True:
        url = f"{KALSHI_BASE}/events?series_ticker={SERIES_TICKER}&status=settled&limit=200"
        if cursor:
            url += f"&cursor={cursor}"
        j = kalshi_get(url)
        evs = j.get("events", [])
        out.extend(evs)
        cursor = j.get("cursor")
        print(f"  listed +{len(evs)} (total {len(out)}) cursor={'y' if cursor else 'n'}", flush=True)
        time.sleep(INTER_CALL_SLEEP)
        if not cursor or not evs:
            break
        if len(out) >= MAX_EVENTS_TARGET:
            break
    return out


def list_markets_for_event(et):
    cursor = None
    mkts = []
    while True:
        url = f"{KALSHI_BASE}/markets?event_ticker={et}&limit=200"
        if cursor:
            url += f"&cursor={cursor}"
        j = kalshi_get(url)
        m = j.get("markets", [])
        mkts.extend(m)
        cursor = j.get("cursor")
        time.sleep(INTER_CALL_SLEEP)
        if not cursor or not m:
            break
    return mkts


def write_market(market):
    tk = market.get("ticker")
    if not tk:
        return False
    p = os.path.join(KALSHI_MARKETS_DIR, f"{tk}.json")
    if os.path.exists(p):
        stats["markets_skipped_existing"] += 1
        return False
    src = f"{KALSHI_BASE}/markets?event_ticker={market.get('event_ticker','?')}&limit=200"
    payload = {
        "fetched_at": utcnow_iso(),
        "source_url": src,
        "raw": market,
    }
    tmp = p + ".tmp"
    with open(tmp, "w") as f:
        json.dump(payload, f)
    os.replace(tmp, p)
    stats["markets_written"] += 1
    return True


def write_candles(market):
    tk = market.get("ticker")
    if not tk:
        return False
    p = os.path.join(KALSHI_CANDLES_DIR, f"{tk}.json")
    if os.path.exists(p):
        stats["candles_skipped_existing"] += 1
        return False
    # Determine window from creation/open and close.
    open_ts = market.get("open_time") or market.get("open_ts")
    close_ts = market.get("close_time") or market.get("close_ts")
    def to_epoch(x):
        if x is None:
            return None
        if isinstance(x, (int, float)):
            return int(x)
        try:
            return int(dt.datetime.fromisoformat(str(x).replace("Z", "+00:00")).timestamp())
        except Exception:
            return None
    start = to_epoch(open_ts)
    end = to_epoch(close_ts)
    if start is None or end is None or end <= start:
        stats["candles_failed"] += 1
        return False
    # Kalshi caps candlestick range; for 60-min interval cap at ~5000 candles.
    # 1d horizons: window typically <48h; safe.
    series_tk = market.get("series_ticker") or SERIES_TICKER
    url = (
        f"{KALSHI_BASE}/series/{series_tk}/markets/{tk}/candlesticks"
        f"?start_ts={start}&end_ts={end}&period_interval=60"
    )
    try:
        j = kalshi_get(url, max_retries=CANDLE_MAX_RETRIES)
    except Exception as e:
        stats["candles_failed"] += 1
        print(f"  candles FAIL {tk}: {e}", flush=True)
        return False
    payload = {
        "fetched_at": utcnow_iso(),
        "source_url": url,
        "raw": j,
    }
    tmp = p + ".tmp"
    with open(tmp, "w") as f:
        json.dump(payload, f)
    os.replace(tmp, p)
    stats["candles_written"] += 1
    return True


def parse_strike_from_ticker(tk):
    """KXAAAGASD-25APR15-T3.45 or -B3.45 etc. -- extract trailing float."""
    m = re.search(r"-[A-Z]?(\d+\.\d+)$", tk)
    if m:
        try:
            return float(m.group(1))
        except Exception:
            return None
    return None


def parse_event_resolution_date(et):
    """KXAAAGASD-25APR15 -> date(2025,4,15)."""
    m = re.match(r"^[A-Z]+-(\d{2})([A-Z]{3})(\d{2})$", et)
    if not m:
        return None
    yy, mon, dd = m.groups()
    months = {"JAN":1,"FEB":2,"MAR":3,"APR":4,"MAY":5,"JUN":6,
              "JUL":7,"AUG":8,"SEP":9,"OCT":10,"NOV":11,"DEC":12}
    if mon not in months:
        return None
    try:
        return dt.date(2000 + int(yy), months[mon], int(dd))
    except Exception:
        return None


def implied_aaa_bound(markets, event_ticker):
    """Given markets for one settled event, derive AAA daily bound.

    For 'above strike' (KXAAAGASD: 'price > strike on day X'):
      - YES strikes: AAA > strike  -> AAA > max(yes_strikes)  => low bound
      - NO strikes : AAA <= strike -> AAA <= min(no_strikes)  => high bound
    Returns (low, high, count_yes, count_no) or None.
    """
    yes_strikes = []
    no_strikes = []
    for m in markets:
        res = (m.get("result") or m.get("settlement_value") or m.get("status") or "").lower()
        s = m.get("floor_strike")
        if s is None:
            s = parse_strike_from_ticker(m.get("ticker", ""))
        if s is None:
            continue
        if res in ("yes",):
            yes_strikes.append(float(s))
        elif res in ("no",):
            no_strikes.append(float(s))
    if not yes_strikes and not no_strikes:
        return None
    low = max(yes_strikes) if yes_strikes else None   # AAA strictly > low
    high = min(no_strikes) if no_strikes else None    # AAA <= high
    return {
        "low": low,
        "high": high,
        "n_yes": len(yes_strikes),
        "n_no": len(no_strikes),
    }


def write_implied(et, bound):
    d = parse_event_resolution_date(et)
    if d is None:
        return False
    p = os.path.join(AAA_IMPLIED_DIR, f"{d.isoformat()}.json")
    if os.path.exists(p):
        stats["implied_daily_skipped_existing"] += 1
        return False
    payload = {
        "fetched_at": utcnow_iso(),
        "source": "Kalshi KXAAAGASD settled markets",
        "event_ticker": et,
        "effective_date": d.isoformat(),
        "precision": "bound",
        "low": bound["low"],
        "high": bound["high"],
        "n_yes": bound["n_yes"],
        "n_no": bound["n_no"],
        "note": (
            "Bound implied by YES/NO resolutions on KXAAAGASD strikes for that day. "
            "AAA strictly greater than 'low' (max YES strike) and at most 'high' (min NO strike). "
            "Derived from append-only Kalshi market archive; cross-references AAA daily print."
        ),
    }
    tmp = p + ".tmp"
    with open(tmp, "w") as f:
        json.dump(payload, f, indent=2)
    os.replace(tmp, p)
    stats["implied_daily_written"] += 1
    return True


def backfill_kalshi():
    print(f"[A] listing settled {SERIES_TICKER} events...", flush=True)
    try:
        events = list_settled_events()
    except Exception as e:
        print(f"[A] FAIL listing events: {e}", flush=True)
        stats["skipped_events"].append({"event": "<listing>", "reason": f"list_failed: {e}"})
        return
    stats["events_listed"] = len(events)
    print(f"[A] {len(events)} settled events to process", flush=True)
    # Sort newest first by close_time so a partial run still gets the most recent N.
    def close_key(e):
        c = e.get("close_time") or e.get("strike_date") or ""
        return c
    events.sort(key=close_key, reverse=True)
    # Pass 1: markets + implied bound for ALL events (cheap, essential).
    all_markets_by_event = {}
    for idx, ev in enumerate(events):
        et = ev.get("event_ticker")
        if not et:
            continue
        print(f"[A.1] ({idx+1}/{len(events)}) {et}", flush=True)
        try:
            mkts = list_markets_for_event(et)
        except Exception as e:
            print(f"  SKIP {et}: list markets failed: {e}", flush=True)
            stats["skipped_events"].append({"event": et, "reason": f"list_markets: {e}"})
            continue
        print(f"  {len(mkts)} markets", flush=True)
        all_markets_by_event[et] = mkts
        for m in mkts:
            try:
                write_market(m)
            except Exception as e:
                print(f"  market write fail {m.get('ticker')}: {e}", flush=True)
        # Implied AAA bound from resolutions
        try:
            bound = implied_aaa_bound(mkts, et)
            if bound and (bound["low"] is not None or bound["high"] is not None):
                write_implied(et, bound)
        except Exception as e:
            print(f"  implied derive fail {et}: {e}", flush=True)
        stats["events_processed"] += 1

    # Pass 2: candles, best-effort. Skip whole pass if env says so.
    if SKIP_CANDLES:
        print(f"[A.2] BACKFILL_SKIP_CANDLES=1 -> skipping candles", flush=True)
        return
    print(f"[A.2] candles for {sum(len(v) for v in all_markets_by_event.values())} markets", flush=True)
    n_consec_fail = 0
    for et, mkts in all_markets_by_event.items():
        for m in mkts:
            tk = m.get("ticker")
            # Pre-check existence to avoid wasting API budget on cached.
            p = os.path.join(KALSHI_CANDLES_DIR, f"{tk}.json")
            if os.path.exists(p):
                stats["candles_skipped_existing"] += 1
                continue
            ok = False
            try:
                ok = write_candles(m)
            except Exception as e:
                stats["candles_failed"] += 1
                print(f"  candles raised {tk}: {e}", flush=True)
            time.sleep(INTER_CALL_SLEEP)
            if ok:
                n_consec_fail = 0
            else:
                n_consec_fail += 1
                # If we've taken 25 consecutive candle failures, give up on the rest.
                if n_consec_fail >= 25:
                    print(f"[A.2] {n_consec_fail} consecutive candle failures; abandoning candles pass", flush=True)
                    return


# -------------------- B. EIA weekly --------------------

def excel_serial_to_date(serial):
    # Excel "1900 leap-year bug" baseline: serial 1 == 1900-01-01 but Excel
    # incorrectly treats 1900 as a leap year, so for serials >=60 the offset is
    # the standard (1899-12-30) origin.
    base = dt.date(1899, 12, 30)
    return base + dt.timedelta(days=int(serial))


def backfill_eia_weekly():
    print(f"[B.1] downloading EIA weekly xls...", flush=True)
    try:
        req = urllib.request.Request(
            EIA_XLS_URL,
            headers={"User-Agent": "Mozilla/5.0 mm-setup-backfill/1.0"},
        )
        with urllib.request.urlopen(req, timeout=60) as r:
            data = r.read()
        path = "/tmp/eia_emm_epmr.xls"
        with open(path, "wb") as f:
            f.write(data)
    except Exception as e:
        print(f"[B.1] FAIL download: {e}", flush=True)
        return
    try:
        import xlrd
    except Exception as e:
        print(f"[B.1] xlrd not available: {e}; skipping", flush=True)
        return
    try:
        wb = xlrd.open_workbook(path)
        sh = wb.sheet_by_name("Data 1") if "Data 1" in wb.sheet_names() else wb.sheet_by_index(1)
    except Exception as e:
        print(f"[B.1] FAIL parse: {e}", flush=True)
        return
    rows = []
    for r in range(3, sh.nrows):
        vals = sh.row_values(r)
        if not vals or len(vals) < 2:
            continue
        serial, val = vals[0], vals[1]
        if not isinstance(serial, (int, float)) or serial <= 0:
            continue
        if not isinstance(val, (int, float)):
            continue
        try:
            d = excel_serial_to_date(serial)
        except Exception:
            continue
        rows.append((d, float(val)))
    print(f"[B.1] parsed {len(rows)} weekly observations", flush=True)
    if rows:
        rows.sort()
        stats["eia_first_date"] = rows[0][0].isoformat()
        stats["eia_last_date"] = rows[-1][0].isoformat()
    for d, v in rows:
        p = os.path.join(AAA_EIA_DIR, f"{d.isoformat()}.json")
        if os.path.exists(p):
            stats["eia_weekly_skipped_existing"] += 1
            continue
        # Effective Mon morning; EIA publishes Mon ~17:00 ET. Approximate
        # publication_timestamp as 21:00Z on the same date.
        pub_ts = dt.datetime.combine(d, dt.time(21, 0), tzinfo=dt.timezone.utc)
        payload = {
            "fetched_at": utcnow_iso(),
            "source": "EIA EMM_EPMR_PTE_NUS_DPGw",
            "source_url": EIA_XLS_URL,
            "publication_timestamp": pub_ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "effective_date": d.isoformat(),
            "value_usd_per_gal": round(v, 4),
            "note": (
                "EIA weekly survey, used as fallback when daily AAA archive is unavailable. "
                "AAA-EIA spread typically <30c; can be <10c in calm regimes."
            ),
        }
        tmp = p + ".tmp"
        with open(tmp, "w") as f:
            json.dump(payload, f, indent=2)
        os.replace(tmp, p)
        stats["eia_weekly_written"] += 1


# -------------------- main --------------------

def main():
    t0 = time.time()
    # B first (cheap, no Kalshi pressure, gives some data even if A fails).
    try:
        backfill_eia_weekly()
    except Exception as e:
        print(f"[B.1] uncaught: {e}", flush=True)
    # A (the heavy bit; produces both market archive and implied AAA bounds).
    try:
        backfill_kalshi()
    except Exception as e:
        print(f"[A] uncaught: {e}", flush=True)
    elapsed = time.time() - t0
    stats["wall_clock_s"] = round(elapsed, 1)
    stats["finished_at"] = utcnow_iso()
    summary_path = os.path.join(
        ARCHIVE,
        f"_backfill_aaa_gas_{stats['started_at'].replace(':','').replace('-','')}.json",
    )
    with open(summary_path, "w") as f:
        json.dump(stats, f, indent=2)
    print(f"\nSUMMARY -> {summary_path}", flush=True)
    print(json.dumps(stats, indent=2), flush=True)


if __name__ == "__main__":
    main()
