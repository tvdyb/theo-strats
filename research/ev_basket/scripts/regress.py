#!/usr/bin/env python3
"""Regress inferred TruEV midpoints onto component prices."""
import csv, json, os, glob, datetime as dt
from collections import defaultdict

ROOT = "/Users/wilsonw/mm-setup/auto_theo"
RES  = os.path.join(ROOT, "research/ev_basket")

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

# Load components
copper = load_csv(os.path.join(RES,"components/copper.csv"))   # USD/lb daily
palladium = load_csv(os.path.join(RES,"components/palladium.csv"))  # USD/oz daily
platinum = load_csv(os.path.join(RES,"components/platinum.csv"))  # USD/oz daily
nickel_wb = load_csv(os.path.join(RES,"components/nickel_wb.csv"))  # USD/MT monthly mid-month

print(f"copper days: {len(copper)}, palladium: {len(palladium)}, platinum: {len(platinum)}, nickel(monthly): {len(nickel_wb)}")

# Build daily nickel via forward-fill from monthly mid-month points
def nickel_value(date_str):
    """Returns most recent nickel monthly point on or before date_str."""
    d = dt.date.fromisoformat(date_str)
    # find the latest nickel sample with date <= d
    best = None
    for k,v in nickel_wb.items():
        kd = dt.date.fromisoformat(k)
        if kd <= d and (best is None or kd > best[0]):
            best = (kd,v)
    return best[1] if best else None

# Load Kalshi history
with open(os.path.join(RES,"kalshi_history.json")) as f:
    history = json.load(f)

# Build observation set: (close_date, midpoint, low, high)
# Use the EOD price at close_date - 1 trading day (close_time is 23:59 UTC, so use the trading day OF close_date or prior)
# Yahoo close for "2026-04-29" corresponds to that trading session.
obs = []
for ev in history:
    if ev.get("midpoint") is None: continue
    ct = ev["close_time"][:10]  # YYYY-MM-DD
    # Use price as of close_date itself if available, else prior trading day
    def latest_on_or_before(series, target):
        d = dt.date.fromisoformat(target)
        best = None
        for k,v in series.items():
            kd = dt.date.fromisoformat(k)
            if kd <= d and (best is None or kd > best[0]):
                best = (kd,v)
        return best
    cu = latest_on_or_before(copper, ct)
    pd = latest_on_or_before(palladium, ct)
    pt = latest_on_or_before(platinum, ct)
    ni = nickel_value(ct)
    if not cu or not pd or not pt or ni is None:
        print(f"SKIP {ev['event_ticker']}: cu={cu}, pd={pd}, pt={pt}, ni={ni}")
        continue
    obs.append({
        "event": ev["event_ticker"],
        "close_date": ct,
        "y": ev["midpoint"],
        "low": ev["low"], "high": ev["high"],
        "cu_lb": cu[1], "pd_oz": pd[1], "pt_oz": pt[1], "ni_mt": ni,
        "as_of_cu": cu[0].isoformat(),
        "as_of_pd": pd[0].isoformat(),
        "as_of_pt": pt[0].isoformat(),
    })

print(f"\nObservations: {len(obs)}")
if not obs:
    print("INSUFFICIENT DATA")
    raise SystemExit(1)
for o in obs:
    print(f"  {o['close_date']} y={o['y']:.2f} cu={o['cu_lb']:.3f} pd={o['pd_oz']:.1f} pt={o['pt_oz']:.1f} ni={o['ni_mt']:.0f}")

# OLS: y = a*cu + b*pd + c*pt + d*ni  (no intercept)
# Manual via numpy
try:
    import numpy as np
except ImportError:
    print("numpy missing — using pure-python OLS via normal equations")
    np = None

def fit(obs, with_intercept):
    cols = ["cu_lb","pd_oz","pt_oz","ni_mt"]
    X = []
    y = []
    for o in obs:
        row = [o[c] for c in cols]
        if with_intercept: row = [1.0] + row
        X.append(row); y.append(o["y"])
    if np is not None:
        Xa = np.array(X); ya = np.array(y)
        # Least squares
        beta, residuals, rank, sv = np.linalg.lstsq(Xa, ya, rcond=None)
        yhat = Xa @ beta
        resid = ya - yhat
        ss_res = float((resid**2).sum())
        ss_tot = float(((ya - ya.mean())**2).sum())
        r2 = 1 - ss_res/ss_tot if ss_tot>0 else 0.0
        sigma = float(np.sqrt((resid**2).mean()))
        return {
            "with_intercept": with_intercept,
            "n": len(obs),
            "beta": beta.tolist(),
            "cols": (["intercept"] if with_intercept else []) + cols,
            "r2": r2,
            "residual_stdev": sigma,
            "yhat": yhat.tolist(),
            "resid": resid.tolist(),
        }

results = {}
for wi in [False, True]:
    res = fit(obs, with_intercept=wi)
    print(f"\n=== Fit (intercept={wi}) ===")
    print(f"  n={res['n']}, R²={res['r2']:.4f}, residual_stdev={res['residual_stdev']:.3f}")
    for c,b in zip(res["cols"], res["beta"]):
        print(f"  {c:>10s}: {b:+.6f}")
    results["with_intercept" if wi else "no_intercept"] = res

# Also log-prices regression with intercept
if np is not None:
    cols = ["cu_lb","pd_oz","pt_oz","ni_mt"]
    X = np.array([[1.0] + [np.log(o[c]) for c in cols] for o in obs])
    y = np.log(np.array([o["y"] for o in obs]))
    beta, *_ = np.linalg.lstsq(X, y, rcond=None)
    yhat = X @ beta
    resid = y - yhat
    r2 = 1 - (resid**2).sum() / ((y-y.mean())**2).sum() if y.var()>0 else 0.0
    sigma_log = float(np.sqrt((resid**2).mean()))
    # Translate residual sigma into index points using mean level
    mean_y = float(np.exp(y).mean())
    sigma_pts = sigma_log * mean_y
    print(f"\n=== Log-prices fit (intercept) ===")
    print(f"  n={len(obs)}, R²={r2:.4f}, residual_stdev_log={sigma_log:.4f} (~{sigma_pts:.2f} idx pts at mean {mean_y:.1f})")
    for c,b in zip(["intercept","ln_cu","ln_pd","ln_pt","ln_ni"], beta):
        print(f"  {c:>10s}: {b:+.6f}")
    results["log_prices"] = {
        "n": len(obs),
        "beta": beta.tolist(),
        "cols": ["intercept","ln_cu","ln_pd","ln_pt","ln_ni"],
        "r2": float(r2),
        "residual_stdev_log": sigma_log,
        "residual_stdev_points": sigma_pts,
        "mean_index_level": mean_y,
    }

# Save weights.json
strike_spacing = 10.0  # documented
chosen = results["with_intercept"]
out_w = {
    "fit_method": "OLS_with_intercept_linear_levels",
    "n_observations": chosen["n"],
    "r2": chosen["r2"],
    "residual_stdev_index_points": chosen["residual_stdev"],
    "residual_stdev_pct_of_strike_spacing": chosen["residual_stdev"]/strike_spacing,
    "weights": {c:b for c,b in zip(chosen["cols"], chosen["beta"])},
    "all_fits": results,
    "components": {
        "copper":   "Yahoo HG=F continuous, USD/lb, daily",
        "palladium":"Yahoo PA=F continuous, USD/oz, daily",
        "platinum": "Yahoo PL=F continuous, USD/oz, daily",
        "nickel":   "World Bank Pink Sheet, USD/MT, monthly (forward-filled to daily)",
        "cobalt":   "DROPPED - no free public daily/monthly source identified",
    },
    "as_of_alignment_rule": "Use latest component close on or before Kalshi event close_date.",
    "observations": obs,
}
with open(os.path.join(RES,"weights.json"),"w") as f:
    json.dump(out_w, f, indent=2)
print("\nWrote weights.json")
