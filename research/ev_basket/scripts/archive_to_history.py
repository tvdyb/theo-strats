#!/usr/bin/env python3
"""Build kalshi_history.json strictly from already-archived markets (no fetching)."""
import json, os, glob

ROOT = "/Users/wilsonw/mm-setup/auto_theo"
ARCHIVE_M = os.path.join(ROOT, "archive/kalshi/markets")
RESEARCH = os.path.join(ROOT, "research/ev_basket")

archived = {}
for path in glob.glob(os.path.join(ARCHIVE_M, "KXTRUEV-*.json")):
    fn = os.path.basename(path).replace(".json","")
    et = fn.split("-T")[0]
    archived.setdefault(et, []).append(path)

results = []
for et in sorted(archived.keys()):
    markets = []
    for p in archived[et]:
        with open(p) as f: rec = json.load(f)
        markets.append(rec["raw"])
    strikes = {}; resolutions = {}; close_time = None
    for m in markets:
        t = m["ticker"]
        try: strike = float(t.split("-T")[-1])
        except: strike = None
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
    print(f"{et}: close={close_time} mid={midpoint} (Y={len(yes_strikes)}/N={len(no_strikes)})")

with open(os.path.join(RESEARCH,"kalshi_history.json"),"w") as f:
    json.dump(results, f, indent=2)
print(f"\nWrote {len(results)} events to kalshi_history.json")
