#!/usr/bin/env python3
"""Backtest driver for the rev 4 staged AAA gas weekly pipeline.

Clone of run_backtest.py pointed at staging/aaa_gas_weekly_20260501001200.py.
Adds two things on top of the v1 driver:

  - Splits observations chronologically (by event-earliest-now) into in-sample
    (first 80%) and held-out (last 20%) and computes max-decile-pp on each.
  - Writes worst_50.json (top 50 |predicted - actual|) regardless of pass/fail.

Run from /Users/wilsonw/mm-setup so `auto_theo.backtest.harness` imports.
"""
from __future__ import annotations

import copy
import json
import os
import statistics
import sys
import tempfile
import time
import traceback
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Make the package importable when invoked as a plain script.
ROOT = Path("/Users/wilsonw/mm-setup")
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from auto_theo.backtest import harness as H  # noqa: E402


SPEC_PATH = ROOT / "auto_theo/specs/KXAAAGASW-26MAY04.json"
PIPELINE_MODULE = ROOT / "auto_theo/staging/aaa_gas_weekly_20260501001200.py"
PIPELINE_CLASS = "AAAGasWeeklyPipeline"
ARCHIVE_ROOT = ROOT / "auto_theo/archive"

# AAA Gas Weekly canonical strike grid (mirrors the staged pipeline).
W_STRIKE_GRID = [round(4.00 + 0.02 * i, 3) for i in range(26)]


def _atomic_write(path: Path, payload: str) -> None:
    """Atomic write: tempfile in same dir, fsync, os.replace."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=path.name + ".", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w") as f:
            f.write(payload)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def _build_per_event_spec(base_spec: dict, market_json: dict, event_ticker: str) -> dict:
    spec = copy.deepcopy(base_spec)
    spec["event_ticker"] = event_ticker

    close = H._market_close_time(market_json)
    if close is None:
        close = market_json.get("raw", {}).get("expiration_time")
        if isinstance(close, str):
            close = H._parse_iso(close)
    if close is None:
        suffix = event_ticker.split("-", 1)[1]  # "26APR15"
        close = datetime.strptime(suffix, "%y%b%d").replace(
            tzinfo=timezone.utc, hour=14
        )
    spec.setdefault("resolution", {})["resolution_timestamp"] = (
        close.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    )
    spec["blackout_calendar"] = []
    return spec


def _normalize_strike_key(k: str) -> str:
    if "-" in k:
        last = k.rsplit("-", 1)[-1]
        if last and last[0] in ("T", "P"):
            last = last[1:]
        return last
    if k and k[0] in ("T", "P"):
        return k[1:]
    return k


def _pick_probe_market_ticker(event_ticker: str, resolutions: dict[str, int]) -> str | None:
    for s in W_STRIKE_GRID:
        key = f"{s:.3f}"
        for k in resolutions.keys():
            try:
                if abs(float(k) - s) < 1e-9:
                    return f"{event_ticker}-{key}"
            except ValueError:
                continue
    return None


def _events_in_archive(archive_root: Path, prefix: str) -> list[str]:
    seen: set[str] = set()
    for p in H._list_market_files(archive_root):
        ev = H._event_ticker_from_market(p.stem)
        if ev.startswith(prefix + "-"):
            seen.add(ev)
    return sorted(seen)


def _walk_event_with_per_event_spec(
    pipeline_cls,
    base_spec: dict,
    archive_root: Path,
    event_ticker: str,
    walk_step_seconds: int,
    leakage_log: list,
) -> tuple[list[dict], dict]:
    markets = H._markets_for_event(archive_root, event_ticker)
    if not markets:
        return [], {"missing_archive": 1}

    resolutions = H._build_resolution_map(markets)
    if not resolutions:
        return [], {"unresolved_event": 1}

    per_event_spec = _build_per_event_spec(base_spec, markets[0], event_ticker)

    probe_ticker = _pick_probe_market_ticker(event_ticker, resolutions)
    if probe_ticker is None:
        return [], {"no_probe_strike_on_w_grid": 1}

    open_times = [t for t in (H._market_open_time(m) for m in markets) if t]
    close_times = [t for t in (H._market_close_time(m) for m in markets) if t]
    if not open_times or not close_times:
        return [], {"missing_timestamps": 1}
    walk_start = min(open_times)
    walk_end = max(close_times)
    if walk_end <= walk_start:
        return [], {"degenerate_window": 1}

    observations: list[dict] = []
    refusals: dict[str, int] = {
        "BlackoutError": 0,
        "InsufficientDataError": 0,
        "UnsupportedMarketError": 0,
        "OtherError": 0,
        "Leakage": 0,
    }

    step = timedelta(seconds=walk_step_seconds)
    now = walk_start
    max_steps = 10_000
    n_steps = 0

    while now <= walk_end and n_steps < max_steps:
        n_steps += 1
        t0 = time.perf_counter()
        try:
            pipeline = pipeline_cls(
                spec=per_event_spec, mode="historical", archive_root=archive_root,
            )
            H._wrap_data_as_of(pipeline, leakage_log)
            theos = pipeline.build_theos(probe_ticker, now)
        except H._LeakageError:
            refusals["Leakage"] += 1
            now += step
            continue
        except Exception as e:
            name = type(e).__name__
            if name in refusals:
                refusals[name] += 1
            else:
                refusals["OtherError"] += 1
            now += step
            continue
        elapsed_ms = (time.perf_counter() - t0) * 1000.0

        strikes = H._theos_strikes(theos or {})
        for strike_key, p_yes in strikes.items():
            norm = _normalize_strike_key(strike_key)
            actual = resolutions.get(norm)
            if actual is None:
                continue
            observations.append({
                "event": event_ticker,
                "ticker": strike_key,
                "now": now.isoformat(),
                "strike": norm,
                "predicted_p": float(max(0.0, min(1.0, p_yes))),
                "actual": int(actual),
                "elapsed_ms": elapsed_ms,
            })
        now += step

    refusals["_n_steps"] = n_steps
    return observations, refusals


def _split_observations_chronologically(obs: list[dict]) -> tuple[list[dict], list[dict]]:
    """Group observations by event, sort events by their earliest `now`,
    then split events 80/20 chronologically. Returns (in_sample, held_out)."""
    if not obs:
        return [], []
    by_event: dict[str, list[dict]] = {}
    for o in obs:
        by_event.setdefault(o["event"], []).append(o)
    event_order = sorted(by_event.keys(),
                         key=lambda e: min(o["now"] for o in by_event[e]))
    split = max(1, int(round(0.8 * len(event_order))))
    in_events = set(event_order[:split])
    out_events = set(event_order[split:])
    in_obs = [o for o in obs if o["event"] in in_events]
    out_obs = [o for o in obs if o["event"] in out_events]
    return in_obs, out_obs


def _worst_n(obs: list[dict], n: int) -> list[dict]:
    """Top-N |predicted - actual|. Ties broken by predicted_p desc."""
    scored = sorted(
        obs,
        key=lambda o: (-abs(o["predicted_p"] - o["actual"]), -o["predicted_p"]),
    )
    return scored[:n]


def main() -> int:
    base_spec = json.loads(SPEC_PATH.read_text())

    events = _events_in_archive(ARCHIVE_ROOT, "KXAAAGASD")
    if not events:
        print("No KXAAAGASD events in archive.", file=sys.stderr)
        return 2

    pipeline_cls = H._load_pipeline_class(str(PIPELINE_MODULE), PIPELINE_CLASS)

    run_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    pname = PIPELINE_MODULE.stem
    output_dir = ROOT / "auto_theo/backtest/reports" / pname / run_ts
    output_dir.mkdir(parents=True, exist_ok=True)
    report_path = output_dir / "report.json"
    worst_path = output_dir / "worst_50.json"

    all_obs: list[dict] = []
    refusal_totals: dict[str, int] = {}
    leakage_log: list[dict] = []
    timing_ms: list[float] = []
    per_event_summary: list[dict] = []
    n_steps_total = 0

    walk_step_seconds = 3600

    for ev in events:
        try:
            obs, refusals = _walk_event_with_per_event_spec(
                pipeline_cls, base_spec, ARCHIVE_ROOT, ev, walk_step_seconds, leakage_log,
            )
        except Exception:
            traceback.print_exc()
            obs, refusals = [], {"DriverError": 1, "_n_steps": 0}
        all_obs.extend(obs)
        for k, v in refusals.items():
            refusal_totals[k] = refusal_totals.get(k, 0) + v
        n_steps_total += refusals.get("_n_steps", 0)
        timing_ms.extend(o["elapsed_ms"] for o in obs)
        per_event_summary.append({
            "event": ev,
            "n_obs": len(obs),
            "n_steps": refusals.get("_n_steps", 0),
            "refusals": {k: v for k, v in refusals.items() if k != "_n_steps"},
        })

    n_obs = len(all_obs)

    # Always emit worst_50.json regardless of pass/fail.
    worst_50 = _worst_n(all_obs, 50)
    _atomic_write(worst_path, json.dumps(worst_50, indent=2))

    if n_obs < H.MIN_OBSERVATIONS:
        verdict = H.BacktestVerdict(
            passed=False,
            reason=f"insufficient_archive: {n_obs} obs (need >={H.MIN_OBSERVATIONS})",
            calibration={},
            leakage_check=H._compute_leakage(leakage_log),
            refusal_sanity=H._compute_refusal_sanity(refusal_totals, n_steps_total),
            n_observations=n_obs,
            report_path=str(report_path),
            drift_check={},
        )
        H._write_report(report_path, output_dir, verdict, all_obs, per_event_summary,
                        events, timing_ms)
        print(json.dumps({"passed": verdict.passed, "reason": verdict.reason,
                          "n_observations": verdict.n_observations,
                          "report_path": verdict.report_path,
                          "worst_50_path": str(worst_path)}, indent=2))
        return 1

    cal_full = H._compute_calibration(all_obs)
    leak = H._compute_leakage(leakage_log)
    refusal = H._compute_refusal_sanity(refusal_totals, n_steps_total)
    drift = H._compute_drift(all_obs)

    in_obs, out_obs = _split_observations_chronologically(all_obs)
    cal_in = H._compute_calibration(in_obs)
    cal_out = H._compute_calibration(out_obs)

    gates = {
        "calibration": cal_full["passed"],
        "leakage": leak["passed"],
        "refusal_sanity": refusal["passed"],
        "drift": drift["passed"],
    }
    passed = all(gates.values())

    if passed:
        reason = "passed"
    else:
        failed = [k for k, v in gates.items() if not v]
        details = []
        if "calibration" in failed:
            details.append(
                f"calibration max_dev={cal_full.get('max_deviation')} > {H.CALIBRATION_DECILE_MAX_DEVIATION}"
            )
        if "leakage" in failed:
            details.append(f"leakage violations={leak.get('violations')}")
        if "refusal_sanity" in failed:
            details.append(
                f"refusal_rate={refusal.get('rate')} outside "
                f"[{H.REFUSAL_RATE_MIN}, {H.REFUSAL_RATE_MAX}]"
            )
        if "drift" in failed:
            details.append(
                f"drift delta={drift.get('delta')} > {H.DRIFT_MAX_MAE_DELTA}"
            )
        reason = "failed: " + "; ".join(details)

    verdict = H.BacktestVerdict(
        passed=passed,
        reason=reason,
        calibration=cal_full,
        leakage_check=leak,
        refusal_sanity=refusal,
        n_observations=n_obs,
        report_path=str(report_path),
        drift_check=drift,
    )

    # Wedge in the in/held-out slice calibration alongside the standard report.
    H._write_report(report_path, output_dir, verdict, all_obs, per_event_summary,
                    events, timing_ms)
    # Augment the report.json with the slice calibrations (rewrite atomically).
    try:
        existing = json.loads(report_path.read_text())
    except Exception:
        existing = {}
    existing["slice_calibration"] = {
        "in_sample": {
            "n_obs": len(in_obs),
            "max_deviation": cal_in.get("max_deviation"),
            "worst_decile": cal_in.get("worst_decile"),
            "passed_8pp": cal_in.get("passed"),
            "deciles": cal_in.get("deciles"),
        },
        "held_out": {
            "n_obs": len(out_obs),
            "max_deviation": cal_out.get("max_deviation"),
            "worst_decile": cal_out.get("worst_decile"),
            "passed_8pp": cal_out.get("passed"),
            "deciles": cal_out.get("deciles"),
        },
    }
    _atomic_write(report_path, json.dumps(existing, indent=2, default=str))

    print(json.dumps({
        "passed": verdict.passed,
        "reason": verdict.reason,
        "n_observations": verdict.n_observations,
        "report_path": verdict.report_path,
        "worst_50_path": str(worst_path),
        "calibration_full_max_deviation": cal_full.get("max_deviation"),
        "calibration_in_sample_max_deviation": cal_in.get("max_deviation"),
        "calibration_held_out_max_deviation": cal_out.get("max_deviation"),
        "calibration_in_sample_n": len(in_obs),
        "calibration_held_out_n": len(out_obs),
        "leakage_violations": leak.get("violations"),
        "refusal_rate": refusal.get("rate"),
        "refusal_counts": refusal.get("refusal_counts"),
        "drift_mae_old": drift.get("mae_old"),
        "drift_mae_recent": drift.get("mae_recent"),
        "drift_delta": drift.get("delta"),
    }, indent=2))
    return 0 if verdict.passed else 1


if __name__ == "__main__":
    sys.exit(main())
