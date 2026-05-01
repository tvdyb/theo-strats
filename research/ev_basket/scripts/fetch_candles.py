#!/usr/bin/env python3
"""Fetch candlesticks for the boundary markets (one above, one below midpoint) of each event,
to evaluate basket-implied vs kalshi-implied at lookback windows.

Conservative pacing — Kalshi rate-limits aggressively.
"""
import json, os, glob, time, urllib.request, datetime as dt

BASE = "https://api.elections.kalshi.com/trade-api/v2"
ROOT = "/Users/wilsonw/mm-setup/auto_theo"
ARCHIVE_C = os.path.join(ROOT, "archive/kalshi/candlesticks")
RESEARCH = os.path.join(ROOT, "research/ev_basket")

def get(url, retries=10, base_sleep=20):
    for i in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent":"mm-setup-research/1.0"})
            with urllib.request.urlopen(req, timeout=30) as r:
                data = r.read()
            j = json.loads(data)
            if "error" in j and j["error"].get("code") == "too_many_requests":
                wait = base_sleep + 15*i
                print(f"  rate-limited, sleep {wait}s", flush=True)
                time.sleep(wait)
                continue
            return j
        except Exception as e:
            wait = base_sleep + 15*i
            print(f"  err: {e} sleep {wait}s", flush=True)
            time.sleep(wait)
    raise RuntimeError(f"failed: {url}")

with open(os.path.join(RESEARCH,"kalshi_history.json")) as f:
    history = json.load(f)

# For each event, pick the two boundary tickers: largest YES and smallest NO
targets = []
for ev in history:
    if ev["midpoint"] is None: continue
    s = ev["strikes"]; r = ev["resolutions"]
    yes_tk = sorted([(s[t],t) for t,res in r.items() if res=="yes" and s[t] is not None])
    no_tk  = sorted([(s[t],t) for t,res in r.items() if res=="no"  and s[t] is not None])
    if yes_tk: targets.append((ev["event_ticker"], ev["close_time"], yes_tk[-1][1]))
    if no_tk:  targets.append((ev["event_ticker"], ev["close_time"], no_tk[0][1]))

print(f"{len(targets)} candle targets", flush=True)

# Fetch each: 7 days ending close_time, 60-min interval
for et, close_time, tk in targets:
    out_dir = os.path.join(ARCHIVE_C, tk)
    os.makedirs(out_dir, exist_ok=True)
    out_file = os.path.join(out_dir, "close_window.json")
    if os.path.exists(out_file):
        print(f"skip {tk} (cached)", flush=True)
        continue
    ct = dt.datetime.fromisoformat(close_time.replace("Z","+00:00"))
    end_ts = int(ct.timestamp())
    start_ts = end_ts - 7*24*3600
    # Need also series ticker
    series_tk = "KXTRUEV"
    url = f"{BASE}/series/{series_tk}/markets/{tk}/candlesticks?start_ts={start_ts}&end_ts={end_ts}&period_interval=60"
    try:
        j = get(url, base_sleep=15)
    except Exception as e:
        print(f"FAIL {tk}: {e}", flush=True)
        continue
    with open(out_file,"w") as f:
        json.dump({
            "fetched_at": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "source_url": url,
            "raw": j,
        }, f)
    n = len(j.get("candlesticks",[]))
    print(f"OK  {tk}: {n} candles", flush=True)
    time.sleep(15)
print("done", flush=True)
