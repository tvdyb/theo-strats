#!/usr/bin/env python3
"""Fetch settled KXTRUEV events: markets (strikes+resolutions) and candlesticks."""
import json, os, time, urllib.request, urllib.parse, datetime as dt

BASE = "https://api.elections.kalshi.com/trade-api/v2"
ROOT = "/Users/wilsonw/mm-setup/auto_theo"
ARCHIVE_M = os.path.join(ROOT, "archive/kalshi/markets")
ARCHIVE_C = os.path.join(ROOT, "archive/kalshi/candlesticks")
RESEARCH = os.path.join(ROOT, "research/ev_basket")
os.makedirs(ARCHIVE_M, exist_ok=True)
os.makedirs(ARCHIVE_C, exist_ok=True)

def get(url, retries=8):
    for i in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent":"mm-setup-research/1.0"})
            with urllib.request.urlopen(req, timeout=30) as r:
                data = r.read()
            j = json.loads(data)
            if "error" in j and j["error"].get("code") == "too_many_requests":
                wait = 10 + 10*i
                print(f"  rate-limited json, sleep {wait}s")
                time.sleep(wait)
                continue
            return j
        except Exception as e:
            wait = 10 + 10*i
            print(f"  err: {e} retry {i+1}/{retries} sleep {wait}s")
            time.sleep(wait)
    raise RuntimeError(f"failed: {url}")

# Load events
with open("/tmp/kxtruev_settled.json") as f:
    events = json.load(f)["events"]

print(f"Have {len(events)} settled events")

results = []
# Resume support: skip events that already have all archived markets
for ev in events:
    et = ev["event_ticker"]
    print(f"-> {et}")
    # Try cached fetch first
    cache = os.path.join("/tmp", f"_evfetch_{et}.json")
    if os.path.exists(cache):
        try:
            with open(cache) as f: j = json.load(f)
        except:
            j = get(f"{BASE}/markets?event_ticker={et}&limit=200")
    else:
        j = get(f"{BASE}/markets?event_ticker={et}&limit=200")
        with open(cache,"w") as f: json.dump(j, f)
    time.sleep(8.0)
    if "markets" not in j:
        print(f"   no markets: {j}")
        continue
    markets = j["markets"]
    # Save raw markets metadata
    for m in markets:
        path = os.path.join(ARCHIVE_M, f"{m['ticker']}.json")
        with open(path,"w") as f:
            json.dump({
                "fetched_at": dt.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
                "source_url": f"{BASE}/markets?event_ticker={et}",
                "raw": m,
            }, f, indent=2)
    # Build strikes + resolutions
    strikes = {}
    resolutions = {}
    close_time = None
    for m in markets:
        t = m["ticker"]
        # Strike: parse from ticker after T
        try:
            strike = float(t.split("-T")[-1])
        except:
            strike = None
        strikes[t] = strike
        resolutions[t] = m.get("result")  # "yes"/"no"/"" until settled
        ct = m.get("close_time")
        if ct: close_time = ct
    # Infer realized bounds: largest YES strike (lower bound), smallest NO strike (upper bound)
    yes_strikes = sorted([strikes[t] for t,r in resolutions.items() if r=="yes" and strikes[t] is not None])
    no_strikes  = sorted([strikes[t] for t,r in resolutions.items() if r=="no"  and strikes[t] is not None])
    # "above strike" => YES => realized > strike. So the LARGEST yes-strike is the lower bound.
    # NO => realized <= strike. So the SMALLEST no-strike is the upper bound.
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
    print(f"   close={close_time} low={low} high={high} mid={midpoint} (Y={len(yes_strikes)}/N={len(no_strikes)})")

with open(os.path.join(RESEARCH,"kalshi_history.json"),"w") as f:
    json.dump(results, f, indent=2)
print(f"Wrote {len(results)} events to kalshi_history.json")
