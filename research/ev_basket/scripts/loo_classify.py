#!/usr/bin/env python3
"""Leave-one-out classification at boundary strikes.

For each held-out event:
  fit on the remaining N-1 events
  predict basket-implied TruEV value
  classify the boundary strikes (highest YES = `low`, lowest NO = `high`):
    low strike: predict YES iff predicted >= low
    high strike: predict YES iff predicted >= high
  Compare to actual resolutions (low resolved YES; high resolved NO).
Report hit rate over 2 * N predictions.
"""
import json, os, csv, datetime as dt
import numpy as np

ROOT = "/Users/wilsonw/mm-setup/auto_theo"
RES = os.path.join(ROOT, "research/ev_basket")

def load_csv(path):
    out = {}
    with open(path) as f:
        r = csv.DictReader(f)
        for row in r:
            d = row.get("date") or row.get("DATE")
            v = row.get("close") or row.get("value") or row.get("price")
            if not d or not v: continue
            try: out[d[:10]] = float(v)
            except: pass
    return out

cu_s = load_csv(os.path.join(RES,"components/copper.csv"))
pd_s = load_csv(os.path.join(RES,"components/palladium.csv"))
pt_s = load_csv(os.path.join(RES,"components/platinum.csv"))
ni_s = load_csv(os.path.join(RES,"components/nickel_wb.csv"))

def latest(series, target):
    d = dt.date.fromisoformat(target)
    best = None
    for k,v in series.items():
        kd = dt.date.fromisoformat(k)
        if kd <= d and (best is None or kd > best[0]):
            best = (kd,v)
    return best[1] if best else None

with open(os.path.join(RES,"kalshi_history.json")) as f:
    history = json.load(f)

obs = []
for ev in history:
    if ev.get("midpoint") is None: continue
    ct = ev["close_time"][:10]
    cu = latest(cu_s, ct); pd = latest(pd_s, ct); pt = latest(pt_s, ct); ni = latest(ni_s, ct)
    if None in (cu, pd, pt, ni): continue
    obs.append({"event": ev["event_ticker"], "y": ev["midpoint"], "low": ev["low"], "high": ev["high"],
                "x": [cu, pd, pt, ni]})

n = len(obs)
print(f"Observations: {n}")

# LOO with intercept, linear levels
hits = 0; total = 0
per_event = []
for i in range(n):
    train = [o for j,o in enumerate(obs) if j != i]
    test = obs[i]
    X = np.array([[1.0] + o["x"] for o in train])
    y = np.array([o["y"] for o in train])
    beta, *_ = np.linalg.lstsq(X, y, rcond=None)
    pred = float(np.array([1.0] + test["x"]) @ beta)
    # boundary low (YES truth) — predict YES iff pred >= low
    low_pred_yes = pred >= test["low"]
    low_correct = low_pred_yes is True   # truth = YES
    # boundary high (NO truth) — predict YES iff pred >= high
    high_pred_yes = pred >= test["high"]
    high_correct = high_pred_yes is False  # truth = NO
    hits += int(low_correct) + int(high_correct)
    total += 2
    per_event.append({
        "event": test["event"], "actual_mid": test["y"], "predicted": round(pred,2),
        "low": test["low"], "high": test["high"],
        "low_correct": low_correct, "high_correct": high_correct,
        "abs_error": round(abs(pred - test["y"]), 2),
    })
    print(f"  {test['event']}: actual={test['y']:.2f} pred={pred:.2f} err={pred-test['y']:+.2f}  low_OK={low_correct} high_OK={high_correct}")

rate = hits/total
print(f"\nLOO hit rate: {hits}/{total} = {rate:.3f}")

with open(os.path.join(RES, "loo_classification.json"), "w") as f:
    json.dump({"n_events": n, "predictions": 2*n, "hits": hits, "hit_rate": rate, "per_event": per_event}, f, indent=2)
print(f"Wrote loo_classification.json")
