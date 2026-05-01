#!/usr/bin/env python3
"""Fetch EV/battery ETF daily closes via Yahoo chart v8 JSON API.

Saves one CSV per symbol under components/<sym_lower>.csv with schema (date,close).
"""
import datetime as dt
import json
import os
import sys
import urllib.request

ROOT = "/Users/wilsonw/mm-setup/auto_theo/research/ev_basket"
COMP = os.path.join(ROOT, "components")
os.makedirs(COMP, exist_ok=True)

# Cover Apr 15 - Apr 29 2026 plus 30d lookback for stability and ~2y of training noise.
P1 = int(dt.datetime(2024, 1, 1).timestamp())
P2 = int(dt.datetime(2026, 5, 1).timestamp())

SYMS = ["LIT", "BATT", "DRIV", "IDRV", "KARS"]

def fetch(sym):
    url = (f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}"
           f"?period1={P1}&period2={P2}&interval=1d")
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        data = json.load(resp)
    res = data["chart"]["result"][0]
    ts = res["timestamp"]
    closes = res["indicators"]["quote"][0]["close"]
    rows = []
    for t, c in zip(ts, closes):
        if c is None:
            continue
        d = dt.datetime.utcfromtimestamp(t).date().isoformat()
        rows.append((d, float(c)))
    return rows

for sym in SYMS:
    try:
        rows = fetch(sym)
    except Exception as e:
        print(f"FAIL {sym}: {e}")
        continue
    out = os.path.join(COMP, f"{sym.lower()}.csv")
    with open(out, "w") as f:
        f.write("date,close\n")
        for d, c in rows:
            f.write(f"{d},{c}\n")
    print(f"wrote {out} rows={len(rows)} range={rows[0][0]}..{rows[-1][0]}")
