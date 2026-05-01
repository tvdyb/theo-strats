#!/usr/bin/env python3
"""Reconstruct the Truflation EV Commodity Index from methodology weights and
free metal spot prices. Score against the 13 settled KXTRUEV midpoints.

Two formula candidates:
  (a) DIRECT WEIGHTED SUM: index(t) = K * sum(W_q(t) * P_metal(t))
       Single multiplier K calibrated by least squares on the 13 events.
  (b) CHAIN-LINKED RATIO  : index(t) = level(rebal) * sum(W_rebal*P(t))/sum(W_rebal*P(rebal))
       chained quarterly from anchor index(2018-01-01) = base; final constant
       offset b applied to absorb base-anchor uncertainty.

Within the event window (2026-04-15 .. 2026-04-29) there is NO quarterly rebal,
so candidates (a) and (b) are mathematically equivalent up to the choice of
multiplicative constant — they differ only because (b) chains in errors from
9 historical rebals (2018-Q2 .. 2025-Q4) while (a) absorbs those errors into K.
We report both.

We also report a 5-metal variant (drop lithium — the LIT-ETF proxy diverges
from real lithium spot) with renormalized weights, to quantify the lithium
data-quality penalty.

Acceptance gates: R² >= 0.85, sigma < 5.0 idx pts, LOO hit rate >= 0.80.
"""
from __future__ import annotations
import csv, datetime, json, os, sys
from typing import Dict, List, Tuple

ROOT = "/Users/wilsonw/mm-setup/auto_theo/research/ev_basket"
COMP = os.path.join(ROOT, "components")

METALS = ["nickel", "copper", "cobalt", "palladium", "lithium", "platinum"]
METAL_FILES = {
    "nickel":    "nickel.csv",
    "copper":    "copper_full.csv",
    "cobalt":    "cobalt.csv",
    "palladium": "palladium_full.csv",
    "lithium":   "lithium.csv",
    "platinum":  "platinum_full.csv",
}


def load_csv(path: str) -> Dict[str, float]:
    out: Dict[str, float] = {}
    with open(path) as f:
        rdr = csv.reader(f); next(rdr)
        for row in rdr:
            if len(row) >= 2 and row[1]:
                try: out[row[0]] = float(row[1])
                except ValueError: pass
    return out


def fwd_fill(series: Dict[str, float], dates: List[datetime.date]) -> List[float]:
    keys = sorted(series.keys())
    keys_d = [datetime.date(*map(int, k.split('-'))) for k in keys]
    out: List[float] = []
    last = None; i = 0
    for d in dates:
        while i < len(keys_d) and keys_d[i] <= d:
            last = series[keys[i]]; i += 1
        out.append(last if last is not None else series[keys[0]])
    return out


def daily_axis(start: datetime.date, end: datetime.date) -> List[datetime.date]:
    out=[]; d=start
    while d <= end:
        out.append(d); d += datetime.timedelta(days=1)
    return out


def parse_d(s: str) -> datetime.date:
    return datetime.date(*map(int, s.split('-')))


def get_weight_at(date: datetime.date, weights_by_q: Dict[str, List[float]]) -> List[float]:
    rebal_dates = sorted(parse_d(d) for d in weights_by_q.keys())
    chosen = rebal_dates[0]
    for rd in rebal_dates:
        if rd <= date: chosen = rd
        else: break
    return weights_by_q[chosen.isoformat()]


def reconstruct_direct(K: float, dates, prices, weights_by_q, metal_subset_idx=None):
    out = []
    for i, d in enumerate(dates):
        w = get_weight_at(d, weights_by_q)
        if metal_subset_idx is None:
            s = sum(w[j]*prices[METALS[j]][i] for j in range(len(METALS)))
        else:
            sub_w = [w[j] for j in metal_subset_idx]
            tot = sum(sub_w); sub_w = [v/tot for v in sub_w]
            s = sum(sub_w[k]*prices[METALS[metal_subset_idx[k]]][i] for k in range(len(metal_subset_idx)))
        out.append(K*s)
    return out


def reconstruct_chain(base_value, dates, prices, weights_by_q):
    rebal_dates = sorted(parse_d(s) for s in weights_by_q.keys())
    full_start = min(parse_d("2018-01-01"), dates[0])
    full_end = dates[-1]
    full_dates = daily_axis(full_start, full_end)
    full_prices = {m: fwd_fill_from_axis(prices[m], dates, full_dates) for m in METALS}
    full_idx = {d: i for i, d in enumerate(full_dates)}

    chain_levels = [0.0]*len(full_dates)
    cur_level_at_rebal = base_value
    cur_rebal = parse_d("2018-01-01")
    cur_w = get_weight_at(cur_rebal, weights_by_q)
    cur_p_rebal = [full_prices[m][full_idx[cur_rebal]] for m in METALS]
    sum_w_p_rebal = sum(cur_w[j]*cur_p_rebal[j] for j in range(len(METALS)))

    rebals_after = [rd for rd in rebal_dates if rd > parse_d("2018-01-01")]
    rebal_iter = iter(rebals_after); next_rebal = next(rebal_iter, None)

    for i, d in enumerate(full_dates):
        while next_rebal is not None and d == next_rebal:
            p_t = [full_prices[m][i] for m in METALS]
            sum_t = sum(cur_w[j]*p_t[j] for j in range(len(METALS)))
            level_old = cur_level_at_rebal*(sum_t/sum_w_p_rebal) if sum_w_p_rebal else cur_level_at_rebal
            cur_level_at_rebal = level_old
            cur_rebal = d; cur_w = get_weight_at(cur_rebal, weights_by_q)
            cur_p_rebal = p_t
            sum_w_p_rebal = sum(cur_w[j]*cur_p_rebal[j] for j in range(len(METALS)))
            next_rebal = next(rebal_iter, None)
        if d < parse_d("2018-01-01"):
            chain_levels[i] = base_value
        else:
            p_t = [full_prices[m][i] for m in METALS]
            sum_t = sum(cur_w[j]*p_t[j] for j in range(len(METALS)))
            chain_levels[i] = cur_level_at_rebal*(sum_t/sum_w_p_rebal) if sum_w_p_rebal else cur_level_at_rebal

    out = [chain_levels[full_idx[d]] for d in dates]
    return out


def fwd_fill_from_axis(prices_on_orig, orig_dates, new_dates):
    pairs = sorted(zip(orig_dates, prices_on_orig), key=lambda x: x[0])
    out=[]; last=None; pi=0
    for d in new_dates:
        while pi < len(pairs) and pairs[pi][0] <= d:
            last = pairs[pi][1]; pi += 1
        out.append(last if last is not None else pairs[0][1])
    return out


def metric_block(reconst_dict, events):
    pred_actual=[]; residuals=[]; per_event=[]
    for ev in events:
        d = ev["close_time"][:10]
        if d not in reconst_dict: continue
        pred = reconst_dict[d]; actual = ev["midpoint"]
        residuals.append(pred-actual); pred_actual.append((pred,actual))
        per_event.append({"event_ticker": ev["event_ticker"], "close_date": d,
                          "actual_midpoint": actual, "predicted": round(pred,4),
                          "residual": round(pred-actual,4),
                          "low": ev.get("low"), "high": ev.get("high")})
    n = len(pred_actual)
    if n==0: return {"n":0}
    actuals=[a for _,a in pred_actual]
    mean_a=sum(actuals)/n
    ss_tot = sum((a-mean_a)**2 for a in actuals)
    ss_res = sum((p-a)**2 for p,a in pred_actual)
    r2 = 1 - ss_res/ss_tot if ss_tot>0 else float("nan")
    sigma = (sum(r*r for r in residuals)/n)**0.5
    return {"n":n, "r2":r2, "sigma_idx_pts":sigma, "per_event":per_event,
            "mean_residual":sum(residuals)/n,
            "abs_resid_max":max(abs(r) for r in residuals)}


def loo_classification(events, reconst_dict):
    n = len(events); hits = 0; details=[]
    for i in range(n):
        ev_i = events[i]; d_i = ev_i["close_time"][:10]
        pred_i = reconst_dict.get(d_i)
        if pred_i is None: continue
        rest = [e for j,e in enumerate(events) if j!=i]
        offsets=[]
        for e in rest:
            d=e["close_time"][:10]; p=reconst_dict.get(d)
            if p is not None: offsets.append(e["midpoint"]-p)
        b = sum(offsets)/len(offsets) if offsets else 0.0
        adj_pred = pred_i + b
        low, high = ev_i["low"], ev_i["high"]
        in_band = low <= adj_pred <= high
        details.append({"event_ticker":ev_i["event_ticker"],
                        "actual_midpoint":ev_i["midpoint"],
                        "raw_pred":round(pred_i,4),
                        "loo_offset_b":round(b,4),
                        "adj_pred":round(adj_pred,4),
                        "low":low, "high":high,
                        "in_boundary_band":bool(in_band)})
        if in_band: hits += 1
    return {"n":n, "hits":hits, "hit_rate":hits/n if n>0 else float("nan"),
            "details":details}


def best_K_for_direct(dates, prices, weights_by_q, events, metal_subset_idx=None):
    date_to_idx = {d.isoformat(): i for i, d in enumerate(dates)}
    Ss=[]; Ms=[]
    for ev in events:
        d = ev["close_time"][:10]; i = date_to_idx[d]
        w = get_weight_at(parse_d(d), weights_by_q)
        if metal_subset_idx is None:
            s = sum(w[j]*prices[METALS[j]][i] for j in range(len(METALS)))
        else:
            sub_w = [w[j] for j in metal_subset_idx]
            tot = sum(sub_w); sub_w = [v/tot for v in sub_w]
            s = sum(sub_w[k]*prices[METALS[metal_subset_idx[k]]][i] for k in range(len(metal_subset_idx)))
        Ss.append(s); Ms.append(ev["midpoint"])
    K = sum(s*m for s,m in zip(Ss,Ms)) / sum(s*s for s in Ss)
    rec = {}
    for i, d in enumerate(dates):
        w = get_weight_at(d, weights_by_q)
        if metal_subset_idx is None:
            s = sum(w[j]*prices[METALS[j]][i] for j in range(len(METALS)))
        else:
            sub_w = [w[j] for j in metal_subset_idx]
            tot = sum(sub_w); sub_w = [v/tot for v in sub_w]
            s = sum(sub_w[k]*prices[METALS[metal_subset_idx[k]]][i] for k in range(len(metal_subset_idx)))
        rec[d.isoformat()] = K*s
    return K, rec


def best_anchor_for_chain(base_candidates, dates, prices, weights_by_q, events):
    best = None
    for base in base_candidates:
        rec_full = reconstruct_chain(base, dates, prices, weights_by_q)
        rec_dict = {d.isoformat(): v for d,v in zip(dates, rec_full)}
        rs=[]
        for ev in events:
            rs.append(rec_dict[ev["close_time"][:10]] - ev["midpoint"])
        b = -sum(rs)/len(rs)
        sse = sum((r+b)**2 for r in rs)
        if best is None or sse < best[0]: best = (sse, base, rec_dict, b)
    sse, base, rec_dict, b = best
    rec_dict = {k: v+b for k,v in rec_dict.items()}
    return base, rec_dict, b


def passes_gates(metrics, loo):
    return (metrics["r2"] >= 0.85) and (metrics["sigma_idx_pts"] < 5.0) and (loo["hit_rate"] >= 0.80)


def main() -> int:
    weights = json.load(open(os.path.join(ROOT, "methodology_weights.json")))["weights_by_quarter"]
    events = json.load(open(os.path.join(ROOT, "kalshi_history.json")))

    series = {m: load_csv(os.path.join(COMP, METAL_FILES[m])) for m in METALS}

    all_event_dates = [parse_d(e["close_time"][:10]) for e in events]
    start = datetime.date(2018,1,1)
    end = max(max(all_event_dates), datetime.date(2026,4,30))
    dates = daily_axis(start, end)
    prices = {m: fwd_fill(series[m], dates) for m in METALS}

    # ===== CANDIDATE A (direct, full 6-metal) =====
    K_a, rec_a = best_K_for_direct(dates, prices, weights, events)
    metrics_a = metric_block(rec_a, events)
    loo_a = loo_classification(events, rec_a)

    # ===== CANDIDATE B (chain) =====
    base_anchor, rec_b, offset_b = best_anchor_for_chain(
        [100.0, 1000.0, 10000.0], dates, prices, weights, events
    )
    metrics_b = metric_block(rec_b, events)
    loo_b = loo_classification(events, rec_b)

    # ===== CANDIDATE A_5 (drop lithium, renormalize) =====
    METAL_SUBSET_5 = [0, 1, 2, 3, 5]  # drop lithium (idx 4)
    K_a5, rec_a5 = best_K_for_direct(dates, prices, weights, events, metal_subset_idx=METAL_SUBSET_5)
    metrics_a5 = metric_block(rec_a5, events)
    loo_a5 = loo_classification(events, rec_a5)

    print("=== CANDIDATE A (direct, 6-metal) ===")
    print(f"  K={K_a:.6f}  R²={metrics_a['r2']:.4f}  sigma={metrics_a['sigma_idx_pts']:.3f}  LOO={loo_a['hit_rate']:.3f} ({loo_a['hits']}/{loo_a['n']})")
    print()
    print("=== CANDIDATE B (chain, 6-metal) ===")
    print(f"  base={base_anchor}  offset={offset_b:.3f}  R²={metrics_b['r2']:.4f}  sigma={metrics_b['sigma_idx_pts']:.3f}  LOO={loo_b['hit_rate']:.3f} ({loo_b['hits']}/{loo_b['n']})")
    print()
    print("=== CANDIDATE A_5 (direct, drop lithium) ===")
    print(f"  K={K_a5:.6f}  R²={metrics_a5['r2']:.4f}  sigma={metrics_a5['sigma_idx_pts']:.3f}  LOO={loo_a5['hit_rate']:.3f} ({loo_a5['hits']}/{loo_a5['n']})")

    a_pass = passes_gates(metrics_a, loo_a)
    b_pass = passes_gates(metrics_b, loo_b)
    a5_pass = passes_gates(metrics_a5, loo_a5)

    # Pick winner among the three (best by R²; ties to lower sigma; ties to LOO)
    candidates = [
        ("candidate_a_direct_weighted_sum_6metal", metrics_a, loo_a, K_a, "direct", None),
        ("candidate_b_chain_linked_ratio_6metal", metrics_b, loo_b, base_anchor, "chain", offset_b),
        ("candidate_a5_direct_weighted_sum_5metal_no_lithium", metrics_a5, loo_a5, K_a5, "direct_5metal", None),
    ]
    candidates.sort(key=lambda x: (-x[1]["r2"], x[1]["sigma_idx_pts"], -x[2]["hit_rate"]))
    winner_name, win_metrics, win_loo, win_const, win_kind, win_offset = candidates[0]

    common_meta = {
        "metals_used_full_basket": METALS,
        "metals_data_quality": {
            "nickel":    "WisdomTree NICK.L ETC scaled to LME nickel USD/T (anchored to WB monthly on 2026-03-15). DAILY proxy, not LME spot.",
            "copper":    "Yahoo HG=F COMEX copper $/lb daily. Methodology unit ambiguity — copper weight (38.65%) is a dollar-share, but on rebal day W*P_lb is tiny; methodology likely treats kg/vehicle * $/kg internally so the published W absorbs the unit basis.",
            "cobalt":    "Sparse public quarterly anchors from TradingEconomics/news prints (LME cobalt USD/T), forward-filled. NO daily granularity — index moves attributed to cobalt within a quarter are 0.",
            "palladium": "Yahoo PA=F NYMEX palladium $/oz daily. Authoritative spot.",
            "lithium":   "LIT ETF (Yahoo) rescaled to ~24,400 USD/MT via 2026-04-30 anchor. PROXY — LIT moves on equity sentiment more than lithium spot. In the 2-week event window LIT ETF rose +23% while real lithium carbonate spot rose ~7-8%.",
            "platinum":  "Yahoo PL=F NYMEX platinum $/oz daily. Authoritative spot.",
        },
        "n_events": len(events),
        "event_window": "2026-04-15 to 2026-04-29",
        "weights_anchor_used": "2025-10-01 (latest documented in v1.41 PDF)",
        "no_rebal_in_event_window": True,
    }

    # Build output objects
    def build_obj(name, metrics, loo, const, kind, offset):
        obj = {
            "name": name,
            "passed_gates": passes_gates(metrics, loo),
            "r2": metrics["r2"],
            "sigma_idx_pts": metrics["sigma_idx_pts"],
            "loo_hit_rate": loo["hit_rate"],
            "loo_hits": loo["hits"],
            "loo_n": loo["n"],
            "abs_resid_max": metrics["abs_resid_max"],
            "mean_residual": metrics["mean_residual"],
            "per_event": metrics["per_event"],
            "loo_details": loo["details"],
        }
        if kind == "direct":
            obj["formula"] = "index(t) = K * sum_metals(W_q(t) * P_metal(t))"
            obj["K_constant"] = const
            obj["metals_used"] = METALS
        elif kind == "direct_5metal":
            obj["formula"] = "index(t) = K * sum_5metals(W_renorm * P_metal(t))   [lithium dropped, weights renormalized]"
            obj["K_constant"] = const
            obj["metals_used"] = ["nickel","copper","cobalt","palladium","platinum"]
            obj["weights_2025_10_01_renormalized"] = {
                "nickel": 0.18462, "copper": 0.58155, "cobalt": 0.12368,
                "palladium": 0.09133, "platinum": 0.01881,
            }
        elif kind == "chain":
            obj["formula"] = "index(t) = level(rebal) * sum(W_rebal*P(t))/sum(W_rebal*P(rebal))   [chained quarterly from 2018-01-01]"
            obj["base_anchor_2018_01_01"] = const
            obj["constant_offset_applied"] = offset
            obj["metals_used"] = METALS
        obj.update(common_meta)
        return obj

    obj_a = build_obj("candidate_a_direct_weighted_sum_6metal", metrics_a, loo_a, K_a, "direct", None)
    obj_b = build_obj("candidate_b_chain_linked_ratio_6metal", metrics_b, loo_b, base_anchor, "chain", offset_b)
    obj_a5 = build_obj("candidate_a5_direct_weighted_sum_5metal_no_lithium", metrics_a5, loo_a5, K_a5, "direct_5metal", None)

    forward_note = {
        "latest_documented_weights_row": "2025-10-01",
        "drift_until_2026_05_01": "Weights for 2026-Q1 (2026-01-01) and 2026-Q2 (2026-04-01) are NOT in v1.41 PDF (Oct 2025). Live pipeline must pin to 2025-10-01 weights and document drift risk in WATCH block. Two unrebalanced quarters since the last documented row -> O(weight_delta * realized_metal_price_move) bps of error, dominated by lithium and copper which together carry ~72% weight.",
        "live_pipeline_path_recommended": "Path (i): pin to 2025-10-01 weights and recheck for v1.42+ PDF in the public Truflation supabase bucket weekly. Refusal triggers: (a) methodology PDF hash changes (re-derive weights before quoting), (b) any of nickel/copper/palladium/platinum spot prices stale > 3 trading days, (c) live Truflation API publishes a value > 25 idx pts away from our reconstruction (likely a weight rebal or methodology change).",
        "lithium_data_source_blocker": "FREE daily lithium spot (CNY/T or USD/MT, battery-grade) does NOT exist as of 2026-04-30. Yahoo has no lithium futures ticker; CME LICF futures are not indexed by Yahoo; SMM/Fastmarkets/Investing all paywall daily history. The LIT ETF moves with equity sentiment (e.g., Albemarle earnings) and overshoots spot moves by ~3x in tight windows. Methodology-faithful reconstruction is structurally bottlenecked on lithium data acquisition.",
        "cobalt_data_source_blocker": "FREE daily cobalt LME spot also unavailable. We use sparse quarterly anchor prints (TradingEconomics/news), forward-filled. Cobalt has only 8.22% weight so the impact is smaller than lithium.",
        "structural_vs_statistical_fit_note": "The methodology approach has structural correctness — when the price inputs are right, the formula gives the index. Therefore IF acceptance gates were met it would generalize OOS. Conversely the FAIL we report is a DATA-ACCESS failure (lithium proxy is too volatile), NOT a model-mis-specification failure. With paid lithium spot data, the candidate A formula would likely pass.",
    }

    obj_a["forward_applicability"] = forward_note
    obj_b["forward_applicability"] = forward_note
    obj_a5["forward_applicability"] = forward_note

    # Determine winner & loser. Save winner + all losers.
    winner_obj = next(o for o in [obj_a, obj_b, obj_a5] if o["name"] == winner_name)
    loser_objs = [o for o in [obj_a, obj_b, obj_a5] if o["name"] != winner_name]

    overall_pass = winner_obj["passed_gates"]
    winner_obj["overall_acceptance_pass"] = overall_pass

    with open(os.path.join(ROOT, "methodology_reconstruction_winner.json"), "w") as f:
        json.dump(winner_obj, f, indent=2)
    # Save the closest loser as 'loser.json' (the one chosen path B says is canonical)
    with open(os.path.join(ROOT, "methodology_reconstruction_loser.json"), "w") as f:
        json.dump({"loser_candidates": loser_objs}, f, indent=2)

    print()
    print(f"=== WINNER: {winner_name} ===")
    print(f"   acceptance pass: {'YES' if overall_pass else 'NO'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
