#!/usr/bin/env python3
"""V2 fit: expanded basket = original metals + EV/battery ETFs.

Tries OLS-levels, OLS log-returns, ridge on standardized features, and
forward-stepwise feature selection (cap=5). Writes weights_v2.json,
backtest_v2.json, loo_classification_v2.json. Reports acceptance.

n=13 is brutal — interpret R² gains skeptically.
"""
import csv, json, os, datetime as dt
import numpy as np

ROOT = "/Users/wilsonw/mm-setup/auto_theo"
RES  = os.path.join(ROOT, "research/ev_basket")
STRIKE_SPACING = 10.0

# ----- helpers -----
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

def latest(series, target):
    d = dt.date.fromisoformat(target)
    best = None
    for k,v in series.items():
        kd = dt.date.fromisoformat(k)
        if kd <= d and (best is None or kd > best[0]):
            best = (kd,v)
    return best[1] if best else None

# ----- load -----
COMPS = {
    "cu":   load_csv(os.path.join(RES,"components/copper.csv")),     # USD/lb daily
    "pd":   load_csv(os.path.join(RES,"components/palladium.csv")),  # USD/oz daily
    "pt":   load_csv(os.path.join(RES,"components/platinum.csv")),   # USD/oz daily
    "ni":   load_csv(os.path.join(RES,"components/nickel_wb.csv")),  # USD/MT monthly
    "lit":  load_csv(os.path.join(RES,"components/lit.csv")),        # ETF $
    "batt": load_csv(os.path.join(RES,"components/batt.csv")),
    "driv": load_csv(os.path.join(RES,"components/driv.csv")),
    "idrv": load_csv(os.path.join(RES,"components/idrv.csv")),
    "kars": load_csv(os.path.join(RES,"components/kars.csv")),
}
print({k: len(v) for k,v in COMPS.items()})

with open(os.path.join(RES,"kalshi_history.json")) as f:
    history = json.load(f)
history.sort(key=lambda e: e["close_time"])

obs = []
for ev in history:
    if ev.get("midpoint") is None: continue
    ct = ev["close_time"][:10]
    row = {"event": ev["event_ticker"], "close_date": ct,
           "y": ev["midpoint"], "low": ev["low"], "high": ev["high"]}
    ok = True
    for name, ser in COMPS.items():
        v = latest(ser, ct)
        if v is None:
            print(f"SKIP {ev['event_ticker']}: missing {name}")
            ok = False; break
        row[name] = v
    if ok:
        obs.append(row)

n = len(obs)
print(f"\nObservations: {n}")
ALL_FEATS = ["cu","pd","pt","ni","lit","batt","driv","idrv","kars"]
y = np.array([o["y"] for o in obs])
X_full = np.array([[o[c] for c in ALL_FEATS] for o in obs])
print(f"X shape: {X_full.shape}, y mean={y.mean():.2f}, y std={y.std():.2f}")

# ----- helpers -----
def ols_fit(X, y_):
    beta, *_ = np.linalg.lstsq(X, y_, rcond=None)
    yhat = X @ beta
    resid = y_ - yhat
    ss_res = float((resid**2).sum())
    ss_tot = float(((y_ - y_.mean())**2).sum())
    r2 = 1 - ss_res/ss_tot if ss_tot > 0 else 0.0
    sigma = float(np.sqrt((resid**2).mean()))
    return beta, r2, sigma, yhat, resid

def loo_classify(X_with_int, y_, lows, highs, fit_fn):
    """fit_fn takes (X_train, y_train) -> beta. Predict on row i with X[i]@beta.
    Returns (hits, total, per_event_list). 2 predictions per held-out event:
    low strike (truth=YES => predict YES iff yhat>=low),
    high strike (truth=NO => predict YES iff yhat>=high).
    """
    n_ = len(y_)
    hits = 0
    per = []
    for i in range(n_):
        mask = np.ones(n_, dtype=bool); mask[i] = False
        beta = fit_fn(X_with_int[mask], y_[mask])
        pred = float(X_with_int[i] @ beta)
        # truth at low strike = YES (mid > low); predict YES iff pred>=low
        # truth at high strike = NO (mid < high); predict YES iff pred>=high
        c1 = bool(pred >= lows[i])           # correct iff predicted YES
        c2 = (not bool(pred >= highs[i]))    # correct iff predicted NO
        hits += int(c1) + int(c2)
        per.append({"i": i, "pred": pred, "low": lows[i], "high": highs[i],
                    "low_correct": c1, "high_correct": c2,
                    "abs_err": abs(pred - y_[i])})
    return hits, 2*n_, per

lows  = np.array([o["low"]  for o in obs])
highs = np.array([o["high"] for o in obs])

# ===========================================================
# Strategy (a): OLS log-levels with intercept, all 9 features
# ===========================================================
X_log = np.log(X_full)
X_log_i = np.column_stack([np.ones(n), X_log])
y_log = np.log(y)
beta_a, r2_a_log, sigma_a_log, yhat_a_log, resid_a_log = ols_fit(X_log_i, y_log)
# Translate residual back to index points
mean_y = float(y.mean())
sigma_a_pts = sigma_a_log * mean_y
print(f"\n(a) OLS log-levels n={n} feats=9+intercept R²(log)={r2_a_log:.4f} sigma~{sigma_a_pts:.2f} pts")
# r2 in original units? Compute by exponentiating yhat
yhat_a_orig = np.exp(yhat_a_log)
ss_res = float(((y - yhat_a_orig)**2).sum())
ss_tot = float(((y - y.mean())**2).sum())
r2_a_orig = 1 - ss_res/ss_tot
sigma_a_orig = float(np.sqrt(((y - yhat_a_orig)**2).mean()))
print(f"    in-orig: R²={r2_a_orig:.4f} sigma={sigma_a_orig:.2f} pts")

# ============================================================================
# Strategy (b): OLS on log-returns (differenced) -- this targets day-over-day
# changes. Skipped for cross-event comparability: events are non-consecutive
# in trading days only mildly, but the y-values are levels of an index with
# meaningful trend, so log-returns of y across consecutive events are valid.
# ============================================================================
# Sort events by close_date and take diffs
sorted_obs = sorted(obs, key=lambda o: o["close_date"])
y_sorted = np.array([o["y"] for o in sorted_obs])
X_sorted = np.array([[o[c] for c in ALL_FEATS] for o in sorted_obs])
ret_y = np.diff(np.log(y_sorted))
ret_X = np.diff(np.log(X_sorted), axis=0)
ret_X_i = np.column_stack([np.ones(len(ret_y)), ret_X])
beta_b, r2_b, sigma_b_log, yhat_b, resid_b = ols_fit(ret_X_i, ret_y)
sigma_b_pts = sigma_b_log * mean_y
print(f"\n(b) OLS log-returns n={len(ret_y)} feats=9+intercept R²={r2_b:.4f} sigma~{sigma_b_pts:.2f} pts (idx)")

# ===========================================================
# Strategy (c): Ridge on standardized features (full set, with intercept handled by demean)
# Closed-form: beta = (X'X + alpha I)^-1 X'y
# ===========================================================
Xc = X_full - X_full.mean(0)
Xs = Xc / X_full.std(0)
yc = y - y.mean()
ridge_results = {}
best_ridge = None
for alpha in [0.1, 0.3, 1.0, 3.0, 10.0]:
    G = Xs.T @ Xs + alpha * np.eye(Xs.shape[1])
    beta_s = np.linalg.solve(G, Xs.T @ yc)
    yhat = Xs @ beta_s + y.mean()
    resid = y - yhat
    ss_res = float((resid**2).sum())
    ss_tot = float(((y - y.mean())**2).sum())
    r2 = 1 - ss_res/ss_tot
    sigma = float(np.sqrt((resid**2).mean()))
    # LOO predictions
    n_ = n
    pred_loo = np.zeros(n_)
    for i in range(n_):
        mask = np.ones(n_, dtype=bool); mask[i] = False
        Xs_i = (X_full[mask] - X_full[mask].mean(0)) / X_full[mask].std(0)
        yc_i = y[mask] - y[mask].mean()
        G_i = Xs_i.T @ Xs_i + alpha * np.eye(Xs_i.shape[1])
        b_i = np.linalg.solve(G_i, Xs_i.T @ yc_i)
        # standardize test row using TRAIN mean/std
        x_test = (X_full[i] - X_full[mask].mean(0)) / X_full[mask].std(0)
        pred_loo[i] = float(x_test @ b_i + y[mask].mean())
    hits = 0
    per = []
    for i in range(n_):
        cl = bool(pred_loo[i] >= lows[i])
        ch = (not bool(pred_loo[i] >= highs[i]))
        hits += int(cl) + int(ch)
        per.append({"i": i, "pred": pred_loo[i], "low": lows[i], "high": highs[i],
                    "low_correct": cl, "high_correct": ch})
    hit_rate = hits / (2*n_)
    ridge_results[alpha] = {"r2": r2, "sigma": sigma, "hit_rate": hit_rate,
                             "loo_hits": hits, "loo_total": 2*n_,
                             "beta_std": beta_s.tolist(), "per_event": per}
    print(f"(c) Ridge alpha={alpha}: R²={r2:.4f} sigma={sigma:.2f} pts LOO_hit={hit_rate:.1%}")
    if best_ridge is None or hit_rate > best_ridge[0] or (hit_rate == best_ridge[0] and r2 > best_ridge[1]):
        best_ridge = (hit_rate, r2, alpha)

# ===========================================================
# Strategy (d): Forward-stepwise OLS with intercept, cap=5
# ===========================================================
def ols_intercept_r2(X, y_):
    Xi = np.column_stack([np.ones(len(y_)), X])
    beta, *_ = np.linalg.lstsq(Xi, y_, rcond=None)
    yhat = Xi @ beta
    resid = y_ - yhat
    ss_res = float((resid**2).sum())
    ss_tot = float(((y_ - y_.mean())**2).sum())
    return 1 - ss_res/ss_tot if ss_tot > 0 else 0, float(np.sqrt((resid**2).mean())), beta

selected = []
remaining = list(range(len(ALL_FEATS)))
fwd_path = []
while len(selected) < 5 and remaining:
    best_idx = None
    best_r2 = -1
    best_sig = None
    best_beta = None
    for j in remaining:
        cols = selected + [j]
        Xs2 = X_full[:, cols]
        r2, sig, beta = ols_intercept_r2(Xs2, y)
        if r2 > best_r2:
            best_r2 = r2; best_idx = j; best_sig = sig; best_beta = beta
    selected.append(best_idx); remaining.remove(best_idx)
    fwd_path.append({"step": len(selected), "added": ALL_FEATS[best_idx],
                     "feats": [ALL_FEATS[k] for k in selected],
                     "r2": best_r2, "sigma": best_sig})
    print(f"(d) step {len(selected)}: add {ALL_FEATS[best_idx]} -> R²={best_r2:.4f} sigma={best_sig:.2f}")

# Pick best by adjusted criterion: choose smallest k such that R² increase < 0.02
# But also evaluate LOO at each k
fwd_loo = []
for step in fwd_path:
    cols_idx = [ALL_FEATS.index(c) for c in step["feats"]]
    X_sel = X_full[:, cols_idx]
    Xi = np.column_stack([np.ones(n), X_sel])
    def fit_fn(Xtr, ytr):
        b, *_ = np.linalg.lstsq(Xtr, ytr, rcond=None)
        return b
    hits, tot, per = loo_classify(Xi, y, lows, highs, fit_fn)
    # in-sample sigma
    beta, *_ = np.linalg.lstsq(Xi, y, rcond=None)
    yhat = Xi @ beta
    resid = y - yhat
    sigma = float(np.sqrt((resid**2).mean()))
    fwd_loo.append({"k": len(step["feats"]), "feats": step["feats"], "r2": step["r2"],
                    "sigma": sigma, "loo_hits": hits, "loo_total": tot,
                    "loo_hit_rate": hits/tot, "beta": beta.tolist()})
    print(f"(d) k={len(step['feats'])} feats={step['feats']} R²={step['r2']:.4f} sigma={sigma:.2f} LOO={hits}/{tot}={hits/tot:.1%}")

# ===========================================================
# Pick the winning fit. Acceptance: R²>=0.85, sigma<5, LOO>=80%, CI lower>70%.
# Among passers, prefer fewer features.
# ===========================================================
def binom_ci_lower(k, n_, z=1.96):
    """Wilson score lower bound."""
    if n_ == 0: return 0.0
    p = k/n_
    denom = 1 + z*z/n_
    centre = (p + z*z/(2*n_))/denom
    half = z*np.sqrt(p*(1-p)/n_ + z*z/(4*n_*n_))/denom
    return centre - half

candidates = []
# (a) full log-levels
candidates.append({
    "name": "ols_log_levels_full9",
    "r2": r2_a_orig, "sigma": sigma_a_orig,
    "feats": ALL_FEATS, "k": 9,
    "loo_hits": None, "loo_total": None, "loo_hit_rate": None,
    "beta": beta_a.tolist(), "form": "log-levels with intercept (9 feats)"
})
# Compute LOO for (a) too
X_log_full_i = np.column_stack([np.ones(n), np.log(X_full)])
def fit_loglevels(Xtr, ytr):
    b, *_ = np.linalg.lstsq(Xtr, np.log(ytr), rcond=None)
    return b
def loo_classify_log(X_with_int, y_, lows_, highs_):
    n_ = len(y_)
    hits = 0; per = []
    for i in range(n_):
        mask = np.ones(n_, dtype=bool); mask[i] = False
        beta, *_ = np.linalg.lstsq(X_with_int[mask], np.log(y_[mask]), rcond=None)
        pred = float(np.exp(X_with_int[i] @ beta))
        cl = bool(pred >= lows_[i])
        ch = (not bool(pred >= highs_[i]))
        hits += int(cl) + int(ch)
        per.append({"i": i, "pred": pred, "low": lows_[i], "high": highs_[i],
                    "low_correct": cl, "high_correct": ch})
    return hits, 2*n_, per
ha, ta, pera = loo_classify_log(X_log_full_i, y, lows, highs)
candidates[-1]["loo_hits"] = ha; candidates[-1]["loo_total"] = ta
candidates[-1]["loo_hit_rate"] = ha/ta
candidates[-1]["loo_per_event"] = pera

# (c) ridge results, all alphas
for alpha, r in ridge_results.items():
    candidates.append({
        "name": f"ridge_a{alpha}",
        "r2": r["r2"], "sigma": r["sigma"],
        "feats": ALL_FEATS, "k": 9,
        "loo_hits": r["loo_hits"], "loo_total": r["loo_total"],
        "loo_hit_rate": r["hit_rate"],
        "beta_std": r["beta_std"],
        "form": f"ridge alpha={alpha} on standardized 9 feats",
        "loo_per_event": r["per_event"],
    })

# (d) forward stepwise per k
for f in fwd_loo:
    candidates.append({
        "name": f"fwd_k{f['k']}",
        "r2": f["r2"], "sigma": f["sigma"],
        "feats": f["feats"], "k": f["k"],
        "loo_hits": f["loo_hits"], "loo_total": f["loo_total"],
        "loo_hit_rate": f["loo_hit_rate"],
        "beta": f["beta"],
        "form": f"forward-stepwise k={f['k']}",
    })

# Score and pick winner
def passes(c):
    return (c["r2"] >= 0.85
            and c["sigma"] < 5.0
            and c["loo_hit_rate"] is not None
            and c["loo_hit_rate"] >= 0.80
            and binom_ci_lower(c["loo_hits"], c["loo_total"]) > 0.70)

passers = [c for c in candidates if passes(c)]
print(f"\n=== {len(passers)} candidate(s) PASS all gates ===")

# Even if none pass, pick the best by composite score = LOO hit rate then R²
def score(c):
    return (c["loo_hit_rate"] or 0, c["r2"])
candidates_sorted = sorted(candidates, key=score, reverse=True)
winner = candidates_sorted[0] if not passers else min(passers, key=lambda c: c["k"])

print(f"\nWinner: {winner['name']} ({winner['form']})")
print(f"  R²={winner['r2']:.4f} sigma={winner['sigma']:.2f} pts")
print(f"  LOO={winner['loo_hits']}/{winner['loo_total']}={winner['loo_hit_rate']:.1%}"
      f" Wilson95%-LB={binom_ci_lower(winner['loo_hits'], winner['loo_total']):.3f}")
print(f"  feats={winner['feats']}")

# ===========================================================
# Write outputs
# ===========================================================
result = {
    "task": "v2 expanded basket re-fit",
    "n_observations": n,
    "all_candidates": candidates_sorted,
    "winner": winner,
    "passes_all_gates": bool(passers),
    "acceptance": {
        "r2_target": 0.85, "sigma_target": 5.0,
        "loo_target": 0.80, "wilson_lb_target": 0.70,
    },
    "components": {
        "cu":   "Yahoo HG=F continuous, USD/lb, daily",
        "pd":   "Yahoo PA=F continuous, USD/oz, daily",
        "pt":   "Yahoo PL=F continuous, USD/oz, daily",
        "ni":   "World Bank Pink Sheet, USD/MT, monthly forward-filled",
        "lit":  "Yahoo chart v8 LIT (Global X Lithium & Battery Tech ETF)",
        "batt": "Yahoo chart v8 BATT (Amplify Lithium & Battery Tech ETF)",
        "driv": "Yahoo chart v8 DRIV (Global X Autonomous & EV ETF)",
        "idrv": "Yahoo chart v8 IDRV (iShares Self-Driving EV & Tech ETF)",
        "kars": "Yahoo chart v8 KARS (KraneShares EV & Future Mobility ETF)",
    },
    "observations": obs,
}

with open(os.path.join(RES, "weights_v2.json"), "w") as f:
    json.dump(result, f, indent=2, default=lambda o: float(o) if hasattr(o, 'item') else str(o))
print("Wrote weights_v2.json")

with open(os.path.join(RES, "loo_classification_v2.json"), "w") as f:
    json.dump({
        "winner": winner["name"],
        "loo_hits": winner["loo_hits"],
        "loo_total": winner["loo_total"],
        "loo_hit_rate": winner["loo_hit_rate"],
        "wilson_95_lb": binom_ci_lower(winner["loo_hits"], winner["loo_total"]),
        "per_event": winner.get("loo_per_event"),
    }, f, indent=2, default=lambda o: float(o) if hasattr(o, 'item') else str(o))
print("Wrote loo_classification_v2.json")

# Backtest = in-sample fit per event
backtest_rows = []
if "beta" in winner and winner["beta"] is not None:
    cols_idx = [ALL_FEATS.index(c) for c in winner["feats"]]
    for o in obs:
        x_row = [1.0] + [o[c] for c in winner["feats"]]
        yhat = float(np.array(x_row) @ np.array(winner["beta"]))
        backtest_rows.append({
            "event": o["event"], "close_date": o["close_date"],
            "realized_truEV_low": o["low"], "realized_truEV_high": o["high"],
            "realized_truEV_mid": o["y"],
            "basket_implied_truEV": yhat,
            "abs_error_vs_realized_mid": abs(yhat - o["y"]),
        })
elif "beta_std" in winner:
    # ridge: reconstruct on standardized features
    Xs_full = (X_full - X_full.mean(0)) / X_full.std(0)
    bs = np.array(winner["beta_std"])
    for i, o in enumerate(obs):
        yhat = float(Xs_full[i] @ bs + y.mean())
        backtest_rows.append({
            "event": o["event"], "close_date": o["close_date"],
            "realized_truEV_low": o["low"], "realized_truEV_high": o["high"],
            "realized_truEV_mid": o["y"],
            "basket_implied_truEV": yhat,
            "abs_error_vs_realized_mid": abs(yhat - o["y"]),
        })

with open(os.path.join(RES, "backtest_v2.json"), "w") as f:
    json.dump({"n_events": n, "winner": winner["name"], "events": backtest_rows},
              f, indent=2, default=lambda o: float(o) if hasattr(o, 'item') else str(o))
print("Wrote backtest_v2.json")

# Acceptance summary
print("\n=== ACCEPTANCE GATES ===")
print(f"  R² >= 0.85:        {winner['r2']:.4f} -> {'PASS' if winner['r2']>=0.85 else 'FAIL'}")
print(f"  sigma < 5.0 pts:   {winner['sigma']:.2f}    -> {'PASS' if winner['sigma']<5.0 else 'FAIL'}")
print(f"  LOO hit >= 80%:    {winner['loo_hit_rate']:.1%}  -> {'PASS' if winner['loo_hit_rate']>=0.80 else 'FAIL'}")
lb = binom_ci_lower(winner["loo_hits"], winner["loo_total"])
print(f"  Wilson95%-LB>70%:  {lb:.3f}   -> {'PASS' if lb>0.70 else 'FAIL'}")
print(f"  OVERALL:           {'PASS' if passers else 'FAIL'}")
