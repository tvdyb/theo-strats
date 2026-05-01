#!/usr/bin/env python3
"""Build kalshi_history.json from archived markets, fetching any missing events."""
import json, os, glob, time, urllib.request, datetime as dt

BASE = "https://api.elections.kalshi.com/trade-api/v2"
ROOT = "/Users/wilsonw/mm-setup/auto_theo"
ARCHIVE_M = os.path.join(ROOT, "archive/kalshi/markets")
RESEARCH = os.path.join(ROOT, "research/ev_basket")

def get(url, retries=10):
    for i in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent":"mm-setup-research/1.0"})
            with urllib.request.urlopen(req, timeout=30) as r:
                data = r.read()
            j = json.loads(data)
            if "error" in j and j["error"].get("code") == "too_many_requests":
                wait = 15 + 15*i
                print(f"  rate-limited, sleep {wait}s")
                time.sleep(wait)
                continue
            return j
        except Exception as e:
            wait = 15 + 15*i
            print(f"  err: {e} sleep {wait}s")
            time.sleep(wait)
    raise RuntimeError(f"failed: {url}")

# Load events list
with open("/tmp/kxtruev_settled.json") as f:
    events = json.load(f)["events"]

# Group archived markets by event ticker
archived = {}
for path in glob.glob(os.path.join(ARCHIVE_M, "KXTRUEV-*.json")):
    fn = os.path.basename(path).replace(".json","")
    # KXTRUEV-26APR21-T1150.40 -> event KXTRUEV-26APR21
    parts = fn.split("-T")
    et = parts[0]
    archived.setdefault(et, []).append(path)

print(f"Archived events: {sorted(archived.keys())}")

# For each event from settled list: build summary; fetch if not archived
results = []
for ev in events:
    et = ev["event_ticker"]
    if et in archived and len(archived[et]) >= 10:
        # Build from archive
        markets = []
        for p in archived[et]:
            with open(p) as f:
                rec = json.load(f)
            markets.append(rec["raw"])
        print(f"-> {et}: {len(markets)} markets from archive")
    else:
        print(f"-> {et}: fetching")
        j = get(f"{BASE}/markets?event_ticker={et}&limit=200")
        time.sleep(10.0)
        markets = j.get("markets", [])
        # Save raw
        for m in markets:
            path = os.path.join(ARCHIVE_M, f"{m['ticker']}.json")
            with open(path,"w") as f:
                json.dump({
                    "fetched_at": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "source_url": f"{BASE}/markets?event_ticker={et}",
                    "raw": m,
                }, f, indent=2)
        print(f"   fetched {len(markets)} markets")

    strikes = {}
    resolutions = {}
    close_time = None
    for m in markets:
        t = m["ticker"]
        try:
            strike = float(t.split("-T")[-1])
        except:
            strike = None
        strikes[t] = strike
        resolutions[t] = m.get("result")
        ct = m.get("close_time")
        if ct: close_time = ct
    yes_strikes = sorted([strikes[t] for t,r in resolutions.items() if r=="yes" and strikes[t] is not None])
    no_strikes  = sorted([strikes[t] for t,r in resolutions.items() if r=="no"  and strikes[t] is not None])
    low  = max(yes_strikes) if yes_strikes else None
    high = min(no_strikes)  if no_strikes  else None
    midpoint = (low+high)/2.0 if (low is not None and high is not None) else None
    results.append({
        "event_ticker": et,
        "close_time": close_time,
        "strikes": strikes,
        "resolutions": resolutions,
        "low": low, "high": high, "midpoint": midpoint,
        "n_yes": len(yes_strikes), "n_no": len(no_strikes),
    })
    print(f"   close={close_time} low={low} high={high} mid={midpoint}")

with open(os.path.join(RESEARCH,"kalshi_history.json"),"w") as f:
    json.dump(results, f, indent=2)
print(f"\nWrote {len(results)} events to kalshi_history.json")
