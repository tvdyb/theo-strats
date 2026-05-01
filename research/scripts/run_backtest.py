#!/usr/bin/env python3
"""Backtest driver for the staged AAA gas weekly pipeline.

The harness's standard CLI assumes spec.event_ticker matches the historical
events. For KXAAAGASW the historical archive only has KXAAAGASD events (daily
sibling family with the same scraper, smaller strike spacing). This driver
synthesizes a per-event spec (overriding event_ticker, resolution_timestamp
from the market's close_time, and clearing blackouts), picks a probe market
ticker on the W-grid for each event, and runs the backtest manually, then
calls the harness's gate computations.

Run from /Users/wilsonw/mm-setup so `auto_theo.backtest.harness` imports.
"""
from __future__ import annotations

import copy
import json
import statistics
import sys
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
PIPELINE_MODULE = ROOT / "auto_theo/staging/aaa_gas_weekly_20260430201923.py"
PIPELINE_CLASS = "AAAGasWeeklyPipeline"
ARCHIVE_ROOT = ROOT / "auto_theo/archive"

# AAA Gas Weekly canonical strike grid (mirrors the staged pipeline).
W_STRIKE_GRID = [round(4.00 + 0.02 * i, 3) for i in range(26)]


def _build_per_event_spec(base_spec: dict, market_json: dict, event_ticker: str) -> dict:
    """Produce a thin override of the base spec for a historical event.

    Overrides:
      - event_ticker -> historical event_ticker (e.g. KXAAAGASD-26APR15)
      - resolution.resolution_timestamp -> archived market close_time
      - blackout_calendar -> [] (the spec's blackouts apply to a future date,
        which falls inside the historical walk window for unrelated reasons)
    Everything else (data_sources, vol_estimation_hints, model_family) carries
    over so the staged pipeline reads the same archive paths.
    """
    spec = copy.deepcopy(base_spec)
    spec["event_ticker"] = event_ticker

    close = H._market_close_time(market_json)
    if close is None:
        # Fall back to expiration_time
        close = market_json.get("raw", {}).get("expiration_time")
        if isinstance(close, str):
            close = H._parse_iso(close)
    if close is None:
        # As a last resort, derive from the event ticker date suffix.
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
    """Map both 'KXAAAGASD-26APR15-3.990' and '3.990' (and 'T1068.41') -> '3.990' (or '1068.41').

    Mirrors the live theo schema where strike keys are full market tickers,
    while the resolution map keys are just the trailing strike.
    """
    if "-" in k:
        last = k.rsplit("-", 1)[-1]
        if last and last[0] in ("T", "P"):
            last = last[1:]
        return last
    if k and k[0] in ("T", "P"):
        return k[1:]
    return k


def _pick_probe_market_ticker(event_ticker: str, resolutions: dict[str, int]) -> str | None:
    """Pick a market ticker whose strike sits on the W-grid (so the staged
    weekly pipeline accepts it and emits its full canonical 26-strike grid).
    """
    for s in W_STRIKE_GRID:
        key = f"{s:.3f}"
        # Match either the exact key in resolutions or any key that parses to s.
        for k in resolutions.keys():
            try:
                if abs(float(k) - s) < 1e-9:
                    return f"{event_ticker}-{key}"
            except ValueError:
                continue
    # No exact W-grid match for this event. Fall back to the nearest 2c-aligned
    # strike-string in the grid (the pipeline rejects off-grid tickers, so this
    # whole event will be skipped if no grid alignment exists).
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

    # Per-event spec from any one market (close_time should be identical across strikes).
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
                "now": now.isoformat(),
                "strike": norm,
                "predicted_p": float(max(0.0, min(1.0, p_yes))),
                "actual": int(actual),
                "elapsed_ms": elapsed_ms,
            })
        now += step

    refusals["_n_steps"] = n_steps
    return observations, refusals


def main() -> int:
    base_spec = json.loads(SPEC_PATH.read_text())

    # Discover historical events. The spec is for KXAAAGASW, but we use the
    # KXAAAGASD daily archive as the family-similar historical surrogate.
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
                          "report_path": verdict.report_path}, indent=2))
        return 1

    cal = H._compute_calibration(all_obs)
    leak = H._compute_leakage(leakage_log)
    refusal = H._compute_refusal_sanity(refusal_totals, n_steps_total)
    drift = H._compute_drift(all_obs)

    gates = {
        "calibration": cal["passed"],
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
                f"calibration max_dev={cal.get('max_deviation')} > {H.CALIBRATION_DECILE_MAX_DEVIATION}"
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
        calibration=cal,
        leakage_check=leak,
        refusal_sanity=refusal,
        n_observations=n_obs,
        report_path=str(report_path),
        drift_check=drift,
    )
    H._write_report(report_path, output_dir, verdict, all_obs, per_event_summary,
                    events, timing_ms)
    print(json.dumps({
        "passed": verdict.passed,
        "reason": verdict.reason,
        "n_observations": verdict.n_observations,
        "report_path": verdict.report_path,
        "calibration_max_deviation": cal.get("max_deviation"),
        "calibration_worst_decile": cal.get("worst_decile"),
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
