#!/usr/bin/env python3
"""Fetch component price series from Yahoo (v8 chart) + FRED."""
import os, json, urllib.request, datetime as dt, time

ROOT = "/Users/wilsonw/mm-setup/auto_theo"
OUT = os.path.join(ROOT, "research/ev_basket/components")
ARCH = os.path.join(ROOT, "archive")
os.makedirs(OUT, exist_ok=True)
os.makedirs(os.path.join(ARCH, "yahoo"), exist_ok=True)
os.makedirs(os.path.join(ARCH, "fred"), exist_ok=True)

# Yahoo continuous futures symbols
YH = {
    "copper":   "HG=F",  # CME copper, USD/lb
    "palladium":"PA=F",  # NYMEX palladium, USD/oz
    "platinum": "PL=F",  # NYMEX platinum, USD/oz
    # Try LME-style ETFs / proxies for nickel & cobalt
    "nickel_lme": "LN%3DF",  # may not exist
}
# We'll fall back to FRED for nickel/cobalt monthly
FRED = {
    "nickel_fred":  "PNICKUSDM",
    "cobalt_fred":  "PCOBAUSDM",
}

# 24mo window ending today
END = int(dt.datetime(2026,4,30,23,59,0).timestamp())
START = int(dt.datetime(2024,4,29,0,0,0).timestamp())

def get(url, retries=3):
    for i in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent":"Mozilla/5.0 mm-setup-research"})
            with urllib.request.urlopen(req, timeout=30) as r:
                return r.read().decode()
        except Exception as e:
            print(f"  err: {e} retry {i+1}")
            time.sleep(3+3*i)
    return None

def fetch_yahoo(name, sym):
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}?period1={START}&period2={END}&interval=1d"
    text = get(url)
    if not text: return None
    raw = os.path.join(ARCH,"yahoo", f"{sym.replace('=','_').replace('%3D','_')}.json")
    with open(raw,"w") as f: f.write(text)
    j = json.loads(text)
    if "chart" not in j or not j["chart"].get("result"):
        print(f"ERR yahoo {name} ({sym}): {j.get('chart',{}).get('error')}")
        return None
    r = j["chart"]["result"][0]
    ts = r["timestamp"]
    close = r["indicators"]["quote"][0]["close"]
    rows = []
    for t,c in zip(ts,close):
        if c is None: continue
        d = dt.datetime.utcfromtimestamp(t).strftime("%Y-%m-%d")
        rows.append((d,c))
    out = os.path.join(OUT, f"{name}.csv")
    with open(out,"w") as f:
        f.write("date,close\n")
        for d,c in rows: f.write(f"{d},{c}\n")
    print(f"OK  yahoo {name} ({sym}): {len(rows)} rows -> {out}")
    return rows

def fetch_fred(name, series):
    url = f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={series}"
    text = get(url)
    if not text:
        print(f"ERR fred {name}: no data")
        return None
    out = os.path.join(OUT, f"{name}.csv")
    with open(out,"w") as f: f.write(text)
    raw = os.path.join(ARCH,"fred", f"{series}.csv")
    with open(raw,"w") as f: f.write(text)
    n = len([l for l in text.splitlines() if l and "DATE" not in l.upper() and "observation_date" not in l])
    print(f"OK  fred  {name} ({series}): {n} rows -> {out}")
    return text

if __name__=="__main__":
    for name, sym in YH.items():
        fetch_yahoo(name, sym)
        time.sleep(0.5)
    for name, series in FRED.items():
        fetch_fred(name, series)
        time.sleep(0.5)
    print("done")
