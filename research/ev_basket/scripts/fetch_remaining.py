#!/usr/bin/env python3
"""Fetch ONLY the remaining KXTRUEV events not yet archived."""
import json, os, glob, time, urllib.request, datetime as dt, sys

BASE = "https://api.elections.kalshi.com/trade-api/v2"
ROOT = "/Users/wilsonw/mm-setup/auto_theo"
ARCHIVE_M = os.path.join(ROOT, "archive/kalshi/markets")

def get(url, retries=12, base_sleep=20):
    for i in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent":"mm-setup-research/1.0"})
            with urllib.request.urlopen(req, timeout=30) as r:
                data = r.read()
            j = json.loads(data)
            if "error" in j and j["error"].get("code") == "too_many_requests":
                wait = base_sleep + 20*i
                print(f"  rate-limited, sleep {wait}s", flush=True)
                time.sleep(wait)
                continue
            return j
        except Exception as e:
            wait = base_sleep + 20*i
            print(f"  err: {e} sleep {wait}s", flush=True)
            time.sleep(wait)
    raise RuntimeError(f"failed: {url}")

with open("/tmp/kxtruev_settled.json") as f:
    events = json.load(f)["events"]

archived = set()
for path in glob.glob(os.path.join(ARCHIVE_M, "KXTRUEV-*.json")):
    fn = os.path.basename(path).replace(".json","")
    archived.add(fn.split("-T")[0])

missing = [ev for ev in events if ev["event_ticker"] not in archived]
print(f"Archived: {sorted(archived)}", flush=True)
print(f"Missing: {[m['event_ticker'] for m in missing]}", flush=True)

# Slow pace: 30s between events
for ev in missing:
    et = ev["event_ticker"]
    print(f"-> {et}", flush=True)
    j = get(f"{BASE}/markets?event_ticker={et}&limit=200", base_sleep=30)
    markets = j.get("markets", [])
    for m in markets:
        path = os.path.join(ARCHIVE_M, f"{m['ticker']}.json")
        with open(path,"w") as f:
            json.dump({
                "fetched_at": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "source_url": f"{BASE}/markets?event_ticker={et}",
                "raw": m,
            }, f, indent=2)
    print(f"   fetched {len(markets)} markets", flush=True)
    time.sleep(30)
print("done", flush=True)
