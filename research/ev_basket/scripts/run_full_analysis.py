#!/usr/bin/env python3
"""End-to-end analysis: regression + classification + (where candles available) Kalshi divergence.

Outputs:
  weights.json (overwritten with full diagnostic)
  backtest.json
"""
import csv, json, os, glob, datetime as dt
import numpy as np

ROOT = "/Users/wilsonw/mm-setup/auto_theo"
RES  = os.path.join(ROOT, "research/ev_basket")
ARCHIVE_C = os.path.join(ROOT, "archive/kalshi/candlesticks")
STRIKE_SPACING = 10.0

def load_csv(path):
    out = {}
    with open(path) as f:
        r = csv.DictReader(f)
        for row in r:
            d = row.get("date") or row.get("DATE") or row.get("observation_date")
            v = row.get("close") or row.get("value") or row.get("price")
            if not d or not v: continue
            try: out[d[:10]] = float(v)
            except: pass
    return out

copper    = load_csv(os.path.join(RES,"components/copper.csv"))
palladium = load_csv(os.path.join(RES,"components/palladium.csv"))
platinum  = load_csv(os.path.join(RES,"components/platinum.csv"))
nickel_wb = load_csv(os.path.join(RES,"components/nickel_wb.csv"))

def latest(series, target):
    d = dt.date.fromisoformat(target)
    best = None
    for k,v in series.items():
        kd = dt.date.fromisoformat(k)
        if kd <= d and (best is None or kd > best[0]):
            best = (kd,v)
    return best

with open(os.path.join(RES,"kalshi_history.json")) as f:
    history = json.load(f)

# Build observation set, sorted by close_time
history.sort(key=lambda e: e["close_time"])
obs = []
for ev in history:
    if ev.get("midpoint") is None: continue
    ct = ev["close_time"][:10]
    cu = latest(copper, ct)
    pdv = latest(palladium, ct)
    pt = latest(platinum, ct)
    ni = latest(nickel_wb, ct)
    if not cu or not pdv or not pt or not ni:
        print(f"SKIP {ev['event_ticker']}: cu={cu} pd={pdv} pt={pt} ni={ni}")
        continue
    obs.append({
        "event": ev["event_ticker"],
        "close_date": ct,
        "close_time": ev["close_time"],
        "y": ev["midpoint"],
        "low": ev["low"], "high": ev["high"],
        "cu_lb": cu[1], "pd_oz": pdv[1], "pt_oz": pt[1], "ni_mt": ni[1],
        "as_of_cu": cu[0].isoformat(),
        "as_of_pd": pdv[0].isoformat(),
        "as_of_pt": pt[0].isoformat(),
        "as_of_ni": ni[0].isoformat(),
    })

n = len(obs)
print(f"Observations: {n}")

cols = ["cu_lb","pd_oz","pt_oz","ni_mt"]
y = np.array([o["y"] for o in obs])
X_no_int = np.array([[o[c] for c in cols] for o in obs])
X_int = np.column_stack([np.ones(n), X_no_int])

def fit(X, y):
    beta, *_ = np.linalg.lstsq(X, y, rcond=None)
    yhat = X @ beta
    resid = y - yhat
    ss_res = float((resid**2).sum())
    ss_tot = float(((y-y.mean())**2).sum())
    r2 = 1 - ss_res/ss_tot if ss_tot>0 else 0.0
    sigma = float(np.sqrt((resid**2).mean()))
    return beta.tolist(), float(r2), sigma, yhat.tolist(), resid.tolist()

beta_n, r2_n, sigma_n, yhat_n, resid_n = fit(X_no_int, y)
beta_i, r2_i, sigma_i, yhat_i, resid_i = fit(X_int, y)
print(f"\nLinear no-intercept: R²={r2_n:.4f} sigma={sigma_n:.3f}")
for c,b in zip(cols, beta_n): print(f"  {c}: {b:+.6f}")
print(f"Linear with-intercept: R²={r2_i:.4f} sigma={sigma_i:.3f}")
for c,b in zip(["intercept"]+cols, beta_i): print(f"  {c}: {b:+.6f}")

# Log fit
X_log = np.column_stack([np.ones(n)] + [np.log([o[c] for o in obs]) for c in cols])
beta_l, r2_l, sigma_l, yhat_l, resid_l = fit(X_log, np.log(y))
mean_y = float(y.mean())
sigma_l_pts = sigma_l * mean_y
print(f"\nLog-prices: R²={r2_l:.4f} sigma_log={sigma_l:.4f} (~{sigma_l_pts:.2f} idx pts)")
for c,b in zip(["intercept"]+["ln_"+c for c in cols], beta_l): print(f"  {c}: {b:+.6f}")

# Best fit (with intercept) — used for backtest
chosen_beta = beta_i
chosen_r2   = r2_i
chosen_sigma = sigma_i

# Leave-one-out classification at boundary strikes
loo_classifications = []
correct = 0; total = 0
for i in range(n):
    mask = np.ones(n, dtype=bool); mask[i]=False
    bi,*_ = np.linalg.lstsq(X_int[mask], y[mask], rcond=None)
    yhat_i_loo = float(X_int[i] @ bi)
    ev = obs[i]
    # at low (truth=YES => realized > low)
    pred_yes_low = yhat_i_loo > ev["low"]
    # at high (truth=NO => realized <= high)
    pred_yes_high = yhat_i_loo > ev["high"]
    truth_low_yes = True
    truth_high_yes = False
    c1 = pred_yes_low == truth_low_yes
    c2 = pred_yes_high == truth_high_yes
    if c1: correct += 1
    if c2: correct += 1
    total += 2
    loo_classifications.append({
        "event": ev["event"],
        "yhat_loo": yhat_i_loo,
        "truth_mid": ev["y"],
        "low_strike": ev["low"], "high_strike": ev["high"],
        "pred_yes_at_low": bool(pred_yes_low), "correct_at_low": bool(c1),
        "pred_yes_at_high": bool(pred_yes_high), "correct_at_high": bool(c2),
    })
hit_rate = correct/total
print(f"\nLOO classification at boundary strikes: {correct}/{total} = {hit_rate:.1%}")

# In-sample predictions for each event at multiple lookback windows
# We need component prices at close_time - 1h, 1d, 3d, 7d.
# Without intraday metals, "1h before close" = same EOD as close_date.
# 1d: prior trading day. 3d: 3 trading days prior. 7d: 7 calendar days prior.
backtest_rows = []
for o in obs:
    cd = dt.date.fromisoformat(o["close_date"])
    ev_close_dt = dt.datetime.fromisoformat(o["close_time"].replace("Z","+00:00"))
    row = {
        "event": o["event"],
        "close_time": o["close_time"],
        "realized_truEV_low": o["low"],
        "realized_truEV_high": o["high"],
        "realized_truEV_mid": o["y"],
    }
    for label, days_back, calendar in [("close_minus_1h", 0, False),
                                        ("close_minus_1d", 1, False),
                                        ("close_minus_3d", 3, False),
                                        ("close_minus_7d", 7, True)]:
        if calendar:
            target_date = (cd - dt.timedelta(days=days_back)).isoformat()
        else:
            # Find the trading day N business days prior using the copper series as calendar
            sorted_days = sorted([d for d in copper.keys() if dt.date.fromisoformat(d) <= cd])
            if days_back < len(sorted_days):
                target_date = sorted_days[-1-days_back]
            else:
                target_date = sorted_days[0]
        # Look up
        cu = latest(copper, target_date)
        pdv = latest(palladium, target_date)
        pt = latest(platinum, target_date)
        ni = latest(nickel_wb, target_date)
        if cu and pdv and pt and ni:
            x = np.array([1.0, cu[1], pdv[1], pt[1], ni[1]])
            yhat = float(x @ chosen_beta)
            row[label] = {
                "as_of_components": target_date,
                "cu_lb": cu[1], "pd_oz": pdv[1], "pt_oz": pt[1], "ni_mt": ni[1],
                "basket_implied_truEV": yhat,
                "abs_error_vs_realized_mid": abs(yhat - o["y"]),
            }
        else:
            row[label] = {"as_of_components": target_date, "error":"missing component"}

    backtest_rows.append(row)

# Try to load Kalshi candlesticks for boundary markets and compute kalshi-implied value at close-1h, etc.
def kalshi_implied_for_event(ev):
    """For each lookback window, compute Kalshi-implied truEV from BBO mid-prices.
    Method: at lookback T, find the smallest strike with YES mid < 50¢ and largest strike with YES mid > 50¢ —
    the implied value sits between. Simpler approach: at each strike, mid_yes = P(truEV > strike).
    Given a sorted list of (strike, P), the implied value is the strike at which P=0.5 by interpolation."""
    # Look up all candlestick files for this event
    et = ev["event"]
    cd = dt.datetime.fromisoformat(ev["close_time"].replace("Z","+00:00"))
    # Look for any candlestick file for this event's tickers
    candles = {}
    for path in glob.glob(os.path.join(ARCHIVE_C, f"{et}-T*", "close_window.json")):
        ticker = os.path.basename(os.path.dirname(path))
        try: strike = float(ticker.split("-T")[-1])
        except: continue
        with open(path) as f: data = json.load(f)
        candles[strike] = data.get("raw",{}).get("candlesticks",[])
    if not candles: return {"available": False, "note": "no candle files for this event"}
    # Also record raw BBO for documentation
    raw_bbo = {}
    for strike, candle_list in sorted(candles.items()):
        for c in candle_list[-3:]:
            yb = c.get("yes_bid",{}).get("close_dollars")
            ya = c.get("yes_ask",{}).get("close_dollars")
            raw_bbo.setdefault(strike, []).append({"ts": c.get("end_period_ts"), "yb": yb, "ya": ya})

    # For each lookback time, build (strike, mid_yes) and interpolate
    out = {"available": True, "strikes_with_candles": sorted(candles.keys()),
           "raw_bbo_last3": raw_bbo}
    for label, sec_back in [("close_minus_1h", 3600),
                             ("close_minus_1d", 86400),
                             ("close_minus_3d", 3*86400),
                             ("close_minus_7d", 7*86400)]:
        target_ts = int(cd.timestamp()) - sec_back
        # For each strike, find the candlestick at or before target_ts
        per_strike = []
        for strike, candle_list in sorted(candles.items()):
            cl = [c for c in candle_list if c.get("end_period_ts",0) <= target_ts]
            if not cl: continue
            last = cl[-1]
            # candlestick price fields: yes_bid.close, yes_ask.close, etc. Mid = (yb + ya)/2 if both present
            # Kalshi candlesticks use 'close_dollars' as a string fraction of $1.
            yb_str = last.get("yes_bid",{}).get("close_dollars") or last.get("yes_bid",{}).get("close")
            ya_str = last.get("yes_ask",{}).get("close_dollars") or last.get("yes_ask",{}).get("close")
            if yb_str is None or ya_str is None: continue
            try:
                yb = float(yb_str); ya = float(ya_str)
            except: continue
            mid_yes_prob = (yb + ya)/2.0
            # Tag wide spreads but don't drop — useful for documenting illiquidity
            spread = ya - yb
            per_strike.append((strike, mid_yes_prob))
        if len(per_strike) < 2:
            out[label] = {"target_ts": target_ts, "kalshi_implied_truEV": None, "n_strikes": len(per_strike)}
            continue
        # Interpolate strike at P=0.5
        per_strike.sort()
        # Find pair where P crosses 0.5
        implied = None
        for i in range(len(per_strike)-1):
            s1,p1 = per_strike[i]; s2,p2 = per_strike[i+1]
            if (p1 - 0.5) * (p2 - 0.5) <= 0:
                # Linear interp on strike given prob
                if abs(p1-p2) < 1e-9:
                    implied = (s1+s2)/2
                else:
                    implied = s1 + (0.5 - p1)*(s2-s1)/(p2-p1)
                break
        out[label] = {"target_ts": target_ts, "kalshi_implied_truEV": implied, "n_strikes": len(per_strike)}
    return out

for row in backtest_rows:
    ev_obj = next(o for o in obs if o["event"]==row["event"])
    row["kalshi"] = kalshi_implied_for_event(row)

# Compute basket–kalshi divergence
divergences = []
for row in backtest_rows:
    if not row.get("kalshi",{}).get("available"): continue
    for label in ["close_minus_1h","close_minus_1d","close_minus_3d","close_minus_7d"]:
        bk = row.get(label)
        ks = row.get("kalshi",{}).get(label)
        if bk and isinstance(bk, dict) and bk.get("basket_implied_truEV") is not None and ks and ks.get("kalshi_implied_truEV") is not None:
            divergences.append({
                "event": row["event"], "lookback": label,
                "basket": bk["basket_implied_truEV"],
                "kalshi": ks["kalshi_implied_truEV"],
                "diff": bk["basket_implied_truEV"] - ks["kalshi_implied_truEV"],
            })

# Top 3 outliers (in-sample fit residual)
outlier_events = sorted(zip([o["event"] for o in obs], resid_i, [o["y"] for o in obs]),
                        key=lambda x: -abs(x[1]))[:3]

# Save weights.json
out_w = {
    "fit_method_chosen": "OLS_with_intercept_linear_levels",
    "n_observations": n,
    "r2": chosen_r2,
    "residual_stdev_index_points": chosen_sigma,
    "residual_stdev_pct_of_strike_spacing": chosen_sigma / STRIKE_SPACING,
    "weights": {c:b for c,b in zip(["intercept"]+cols, chosen_beta)},
    "fits": {
        "no_intercept": {"r2": r2_n, "residual_stdev": sigma_n,
                         "weights": {c:b for c,b in zip(cols, beta_n)}},
        "with_intercept": {"r2": r2_i, "residual_stdev": sigma_i,
                           "weights": {c:b for c,b in zip(["intercept"]+cols, beta_i)}},
        "log_prices": {"r2": r2_l, "residual_stdev_log": sigma_l,
                       "residual_stdev_index_points_at_mean": sigma_l_pts,
                       "weights": {c:b for c,b in zip(["intercept"]+["ln_"+c for c in cols], beta_l)}},
    },
    "components": {
        "copper":   "Yahoo HG=F continuous, USD/lb, daily",
        "palladium":"Yahoo PA=F continuous, USD/oz, daily",
        "platinum": "Yahoo PL=F continuous, USD/oz, daily",
        "nickel":   "World Bank Pink Sheet, USD/MT, monthly forward-filled",
        "cobalt":   "DROPPED — no free public daily/monthly source identified (LME paywalled, Yahoo no symbol, IndexMundi has no cobalt page, FRED unreachable from this network for PCOBAUSDM)",
    },
    "as_of_alignment_rule": "Use latest component close on or before Kalshi event close_date.",
    "loo_classification": {
        "correct": correct, "total": total, "hit_rate": hit_rate,
        "details": loo_classifications,
    },
    "outlier_events_top3": [{"event": e, "residual": r, "truth_mid": t} for e,r,t in outlier_events],
    "observations": obs,
}
with open(os.path.join(RES,"weights.json"),"w") as f:
    json.dump(out_w, f, indent=2)

# Save backtest.json
out_b = {
    "n_events": n,
    "events": backtest_rows,
    "basket_kalshi_divergences": divergences,
}
with open(os.path.join(RES,"backtest.json"),"w") as f:
    json.dump(out_b, f, indent=2)

print(f"\nWrote weights.json (R²={chosen_r2:.3f}, sigma={chosen_sigma:.2f} pts, hit={hit_rate:.1%})")
print(f"Wrote backtest.json (n={n} events, {len(divergences)} basket-vs-kalshi divergences)")

# Print acceptance check
print("\n=== ACCEPTANCE CRITERION CHECK ===")
print(f"  R² >= 0.85: {chosen_r2:.4f} -> {'PASS' if chosen_r2 >= 0.85 else 'FAIL'}")
print(f"  residual_stdev < 5.0 pts: {chosen_sigma:.2f} -> {'PASS' if chosen_sigma < 5.0 else 'FAIL'}")
print(f"  classification hit rate >= 70%: {hit_rate:.1%} -> {'PASS' if hit_rate >= 0.70 else 'FAIL'}")
print(f"  >=50 component obs and >=5 events: cu={len(copper)}, n_events={n} -> {'PASS' if len(copper)>=50 and n>=5 else 'FAIL'}")

# Outliers
print("\n=== TOP 3 OUTLIER EVENTS ===")
for e,r,t in outlier_events:
    print(f"  {e}: residual={r:+.2f} truth={t:.2f}")

# Divergences
if divergences:
    diffs = [abs(d["diff"]) for d in divergences]
    print(f"\n=== BASKET vs KALSHI DIVERGENCE (abs) ===")
    print(f"  n={len(diffs)} mean={np.mean(diffs):.2f} max={max(diffs):.2f}")
else:
    print("\nNo basket-vs-kalshi divergences computed (no candlesticks fetched yet)")
