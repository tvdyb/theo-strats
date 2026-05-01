"""Generic walk-forward backtester for auto-theo pipelines.

This is the mechanical pass/fail gate sitting between Modeler and Integrator.
Per CLAUDE.md: "Backtest before live, but don't gold-plate the backtest." The
four mechanical gates encoded here (calibration, leakage, refusal sanity,
held-out drift) are the floor — pipelines that pass still have per-pipeline
PnL trip as the runtime safety net.

Hard rules:
- Reads ONLY from archive_root and the staged pipeline. No Kalshi/web fetches.
- Stdlib only (json, csv, dataclasses, importlib, datetime, pathlib, statistics).
- Must not mutate the pipeline file, the spec, or any archive entry.
"""
from __future__ import annotations

import csv
import importlib.util
import json
import statistics
import sys
import time
import traceback
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


# ---------------------------------------------------------------------------
# Pass/fail thresholds (per CLAUDE.md). Centralized so the Backtester role's
# expectations are visible at the top of the file.
# ---------------------------------------------------------------------------
CALIBRATION_DECILE_MAX_DEVIATION = 0.08      # 8 percentage points
REFUSAL_RATE_MIN = 0.0
REFUSAL_RATE_MAX = 0.50
DRIFT_MAX_MAE_DELTA = 0.03                   # 3 percentage points
MIN_OBSERVATIONS = 10                         # below this, refuse cleanly
N_DECILES = 10
WALK_SAMPLES_IN_REPORT = 50


# ---------------------------------------------------------------------------
# Result types
# ---------------------------------------------------------------------------
@dataclass
class BacktestVerdict:
    passed: bool
    reason: str
    calibration: dict
    leakage_check: dict
    refusal_sanity: dict
    n_observations: int
    report_path: str
    drift_check: dict = field(default_factory=dict)


# Internal exception used to flag leakage violations from the wrapped data_as_of.
class _LeakageError(Exception):
    def __init__(self, dp_publication: datetime, now: datetime, source_id: str):
        super().__init__(
            f"leakage: DataPoint pub_ts={dp_publication.isoformat()} > now={now.isoformat()} "
            f"(source={source_id})"
        )
        self.dp_publication = dp_publication
        self.now = now
        self.source_id = source_id


# ---------------------------------------------------------------------------
# Pipeline loading (staged files live off PYTHONPATH)
# ---------------------------------------------------------------------------
def _load_pipeline_class(module_path: str, class_name: str) -> type:
    """Load a class from a file path using importlib (the staging dir is not
    on PYTHONPATH and we must not put it there — staged code is untrusted)."""
    module_path = str(Path(module_path).resolve())
    spec = importlib.util.spec_from_file_location(
        f"_staged_pipeline_{abs(hash(module_path))}", module_path
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(f"could not load module spec from {module_path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # type: ignore[union-attr]
    if not hasattr(mod, class_name):
        raise AttributeError(f"{module_path} has no class {class_name!r}")
    return getattr(mod, class_name)


# ---------------------------------------------------------------------------
# Archive helpers (resolutions + family fallback)
# ---------------------------------------------------------------------------
def _kalshi_markets_dir(archive_root: Path) -> Path:
    return Path(archive_root) / "kalshi" / "markets"


def _list_market_files(archive_root: Path) -> list[Path]:
    d = _kalshi_markets_dir(archive_root)
    if not d.is_dir():
        return []
    return sorted(d.glob("*.json"))


def _event_ticker_from_market(market_ticker: str) -> str:
    # KXTRUEV-26APR15-T1068.41 -> KXTRUEV-26APR15
    parts = market_ticker.split("-")
    if len(parts) < 3:
        return market_ticker
    return "-".join(parts[:2])


def _load_market_json(path: Path) -> dict | None:
    try:
        with path.open("r") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return None


def _markets_for_event(archive_root: Path, event_ticker: str) -> list[dict]:
    out: list[dict] = []
    for p in _list_market_files(archive_root):
        if not p.stem.startswith(event_ticker + "-"):
            continue
        j = _load_market_json(p)
        if j is None:
            continue
        out.append(j)
    return out


def _market_close_time(market_json: dict) -> datetime | None:
    raw = market_json.get("raw", {})
    for key in ("close_time", "expiration_time", "latest_expiration_time"):
        v = raw.get(key)
        if v:
            try:
                return _parse_iso(v)
            except ValueError:
                continue
    return None


def _market_open_time(market_json: dict) -> datetime | None:
    raw = market_json.get("raw", {})
    for key in ("open_time", "created_time"):
        v = raw.get(key)
        if v:
            try:
                return _parse_iso(v)
            except ValueError:
                continue
    return None


def _market_result(market_json: dict) -> str | None:
    """Return 'yes' / 'no' / None (unresolved)."""
    raw = market_json.get("raw", {})
    r = raw.get("result")
    if r in ("yes", "no"):
        return r
    return None


def _parse_iso(s: str) -> datetime:
    """Parse ISO timestamps; tolerate trailing Z."""
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _spec_family_prefix(spec: dict) -> str | None:
    """Heuristic: extract the leading event-ticker family from the spec.

    The fallback for `historical_events=[]` is to scan all archived market
    files and group them by event-ticker family prefix that matches the spec's
    own event_ticker prefix. This is documented in the harness contract.
    """
    et = spec.get("event_ticker")
    if not isinstance(et, str) or "-" not in et:
        return None
    # KXTRUEV-26APR30 -> "KXTRUEV"
    return et.split("-", 1)[0]


def _discover_events_from_archive(archive_root: Path, spec: dict) -> list[str]:
    prefix = _spec_family_prefix(spec)
    if not prefix:
        return []
    seen: set[str] = set()
    for p in _list_market_files(archive_root):
        ev = _event_ticker_from_market(p.stem)
        if ev.startswith(prefix + "-"):
            seen.add(ev)
    # Skip the spec's own event — it's in-flight, not historical.
    seen.discard(spec.get("event_ticker", ""))
    return sorted(seen)


# ---------------------------------------------------------------------------
# Theos parsing — extract (strike, p_yes) pairs from the pipeline output
# ---------------------------------------------------------------------------
def _theos_strikes(theos: dict) -> dict[str, float]:
    """Pull p_yes per strike from the theos dict.

    The live-bot schema has a `strikes` map: `{<strike_str>: p_yes}` OR
    `{<strike_str>: {"p_yes": ..., ...}}`. Accept both.
    """
    out: dict[str, float] = {}
    raw = theos.get("strikes") or {}
    if not isinstance(raw, dict):
        return out
    for k, v in raw.items():
        if isinstance(v, (int, float)):
            out[str(k)] = float(v)
        elif isinstance(v, dict):
            for pkey in ("p_yes", "yes_prob", "probability"):
                if pkey in v and isinstance(v[pkey], (int, float)):
                    out[str(k)] = float(v[pkey])
                    break
    return out


def _strike_from_market_ticker(market_ticker: str) -> str | None:
    # KXTRUEV-26APR15-T1068.41 -> "1068.41"
    parts = market_ticker.split("-")
    if len(parts) < 3:
        return None
    last = parts[-1]
    if last.startswith("T"):
        return last[1:]
    return last


def _build_resolution_map(markets: list[dict]) -> dict[str, int]:
    """{strike_str: 1 if yes, 0 if no} for resolved markets only."""
    out: dict[str, int] = {}
    for m in markets:
        ticker = m.get("raw", {}).get("ticker", "")
        strike = _strike_from_market_ticker(ticker)
        if strike is None:
            continue
        res = _market_result(m)
        if res is None:
            continue
        out[strike] = 1 if res == "yes" else 0
    return out


# ---------------------------------------------------------------------------
# Walk-forward driver
# ---------------------------------------------------------------------------
def _wrap_data_as_of(pipeline, leakage_log: list[dict]):
    """Replace pipeline.data_as_of with a wrapper that asserts publication
    timestamps are <= now. Records violations in leakage_log AND raises so
    the harness records a refusal-style failure for that step (we don't want
    to silently accept leaked data)."""
    original = pipeline.data_as_of

    def wrapped(source, now):
        result = original(source, now)
        items = result if isinstance(result, list) else [result]
        for dp in items:
            pub = getattr(dp, "publication_timestamp", None)
            if pub is None:
                continue
            if pub.tzinfo is None:
                pub = pub.replace(tzinfo=timezone.utc)
            now_aware = now if now.tzinfo else now.replace(tzinfo=timezone.utc)
            if pub > now_aware:
                src_id = (source or {}).get("url") or (source or {}).get("purpose") or "?"
                leakage_log.append({
                    "now": now_aware.isoformat(),
                    "publication_timestamp": pub.isoformat(),
                    "source": src_id,
                })
                raise _LeakageError(pub, now_aware, src_id)
        return result

    pipeline.data_as_of = wrapped  # type: ignore[method-assign]


def _walk_event(
    pipeline_cls: type,
    spec: dict,
    archive_root: Path,
    event_ticker: str,
    walk_step_seconds: int,
    leakage_log: list[dict],
) -> tuple[list[dict], dict]:
    """Walk a single event end-to-end. Returns (observations, refusals_by_kind)."""
    markets = _markets_for_event(archive_root, event_ticker)
    if not markets:
        return [], {"missing_archive": 1}

    # Skip events where any market is unresolved (need ground truth for calibration).
    resolutions = _build_resolution_map(markets)
    if len(resolutions) == 0:
        return [], {"unresolved_event": 1}

    # Determine walk window.
    open_times = [t for t in (_market_open_time(m) for m in markets) if t]
    close_times = [t for t in (_market_close_time(m) for m in markets) if t]
    if not open_times or not close_times:
        return [], {"missing_timestamps": 1}
    walk_start = min(open_times)
    walk_end = max(close_times)
    if walk_end <= walk_start:
        return [], {"degenerate_window": 1}

    observations: list[dict] = []
    refusals: dict[str, int] = {"BlackoutError": 0, "InsufficientDataError": 0,
                                "UnsupportedMarketError": 0, "OtherError": 0,
                                "Leakage": 0}

    step = timedelta(seconds=walk_step_seconds)
    now = walk_start
    # Hard cap on iterations to prevent runaway on misconfigured walk_step.
    max_steps = 10_000
    n_steps = 0

    while now <= walk_end and n_steps < max_steps:
        n_steps += 1
        t0 = time.perf_counter()
        try:
            pipeline = pipeline_cls(spec=spec, mode="historical", archive_root=archive_root)
            _wrap_data_as_of(pipeline, leakage_log)
            theos = pipeline.build_theos(event_ticker, now)
        except _LeakageError:
            refusals["Leakage"] += 1
            now += step
            continue
        except Exception as e:  # catch refusal types by name to avoid importing
            name = type(e).__name__
            if name in refusals:
                refusals[name] += 1
            else:
                refusals["OtherError"] += 1
            now += step
            continue
        elapsed_ms = (time.perf_counter() - t0) * 1000.0

        strikes = _theos_strikes(theos or {})
        for strike, p_yes in strikes.items():
            actual = resolutions.get(strike)
            if actual is None:
                continue
            observations.append({
                "event": event_ticker,
                "now": now.isoformat(),
                "strike": strike,
                "predicted_p": float(max(0.0, min(1.0, p_yes))),
                "actual": int(actual),
                "elapsed_ms": elapsed_ms,
            })
        now += step

    refusals["_n_steps"] = n_steps
    return observations, refusals


# ---------------------------------------------------------------------------
# Gate computations
# ---------------------------------------------------------------------------
def _compute_calibration(obs: list[dict]) -> dict:
    """Bin (predicted_p, actual) into N_DECILES by predicted_p; return table."""
    if not obs:
        return {"passed": False, "reason": "no observations", "deciles": []}
    # Sort by predicted_p, equal-count bins.
    sorted_obs = sorted(obs, key=lambda o: o["predicted_p"])
    n = len(sorted_obs)
    bins: list[list[dict]] = [[] for _ in range(N_DECILES)]
    for i, o in enumerate(sorted_obs):
        # Map index to bin 0..N-1
        idx = min(N_DECILES - 1, (i * N_DECILES) // n)
        bins[idx].append(o)

    table = []
    max_dev = 0.0
    worst_decile = -1
    for i, b in enumerate(bins):
        if not b:
            table.append({"decile": i, "n": 0, "mean_pred": None, "mean_actual": None,
                          "deviation": None})
            continue
        mp = statistics.fmean(o["predicted_p"] for o in b)
        ma = statistics.fmean(o["actual"] for o in b)
        dev = abs(mp - ma)
        if dev > max_dev:
            max_dev = dev
            worst_decile = i
        table.append({
            "decile": i,
            "n": len(b),
            "mean_pred": round(mp, 6),
            "mean_actual": round(ma, 6),
            "deviation": round(dev, 6),
        })

    passed = max_dev <= CALIBRATION_DECILE_MAX_DEVIATION
    return {
        "passed": passed,
        "max_deviation": round(max_dev, 6),
        "worst_decile": worst_decile,
        "threshold": CALIBRATION_DECILE_MAX_DEVIATION,
        "deciles": table,
    }


def _compute_leakage(leakage_log: list[dict]) -> dict:
    return {
        "passed": len(leakage_log) == 0,
        "violations": len(leakage_log),
        "first_violations": leakage_log[:5],
    }


def _compute_refusal_sanity(refusal_counts: dict[str, int], n_steps_total: int) -> dict:
    refusal_total = sum(v for k, v in refusal_counts.items() if k != "_n_steps")
    rate = (refusal_total / n_steps_total) if n_steps_total > 0 else 1.0
    passed = REFUSAL_RATE_MIN <= rate <= REFUSAL_RATE_MAX
    return {
        "passed": passed,
        "rate": round(rate, 6),
        "lo_threshold": REFUSAL_RATE_MIN,
        "hi_threshold": REFUSAL_RATE_MAX,
        "refusal_counts": {k: v for k, v in refusal_counts.items() if k != "_n_steps"},
        "n_steps_total": n_steps_total,
    }


def _compute_drift(obs: list[dict]) -> dict:
    """Split events 80/20 chronologically; compare MAE on each split."""
    if len(obs) < 2:
        return {"passed": True, "reason": "too few observations to assess drift"}
    # Group by event, sort events by their earliest `now`.
    by_event: dict[str, list[dict]] = {}
    for o in obs:
        by_event.setdefault(o["event"], []).append(o)
    event_order = sorted(by_event.keys(),
                         key=lambda e: min(o["now"] for o in by_event[e]))
    split = max(1, int(round(0.8 * len(event_order))))
    old_events = event_order[:split]
    recent_events = event_order[split:]
    if not recent_events:
        # Single event — can't assess drift, treat as pass.
        return {"passed": True, "reason": "only one event; drift not assessable",
                "n_old_events": len(old_events), "n_recent_events": 0}

    def _mae(events: list[str]) -> float:
        pts = [o for e in events for o in by_event[e]]
        if not pts:
            return 0.0
        return statistics.fmean(abs(o["predicted_p"] - o["actual"]) for o in pts)

    mae_old = _mae(old_events)
    mae_recent = _mae(recent_events)
    delta = abs(mae_recent - mae_old)
    passed = delta <= DRIFT_MAX_MAE_DELTA
    return {
        "passed": passed,
        "mae_old": round(mae_old, 6),
        "mae_recent": round(mae_recent, 6),
        "delta": round(delta, 6),
        "threshold": DRIFT_MAX_MAE_DELTA,
        "n_old_events": len(old_events),
        "n_recent_events": len(recent_events),
    }


def _stratified_samples(obs: list[dict], k: int) -> list[dict]:
    if not obs:
        return []
    sorted_obs = sorted(obs, key=lambda o: o["predicted_p"])
    n = len(sorted_obs)
    if n <= k:
        return list(sorted_obs)
    step = n / k
    return [sorted_obs[int(i * step)] for i in range(k)]


# ---------------------------------------------------------------------------
# Public entrypoint
# ---------------------------------------------------------------------------
def run_backtest(
    pipeline_module_path: str,
    pipeline_class_name: str,
    spec_path: str,
    historical_events: list[str] | None = None,
    archive_root: str | Path = "auto_theo/archive",
    walk_step_seconds: int = 3600,
    output_dir: str | Path | None = None,
) -> BacktestVerdict:
    """Run the walk-forward backtest. See module docstring for contract."""
    archive_root = Path(archive_root)
    spec = json.loads(Path(spec_path).read_text())

    # Resolve the event list.
    events = list(historical_events or [])
    if not events:
        events = _discover_events_from_archive(archive_root, spec)

    # Output dir.
    if output_dir is None:
        run_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        pname = Path(pipeline_module_path).stem
        output_dir = Path("auto_theo/backtest/reports") / pname / run_ts
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    report_path = output_dir / "report.json"

    # Load pipeline class once; instantiated per step in _walk_event.
    try:
        pipeline_cls = _load_pipeline_class(pipeline_module_path, pipeline_class_name)
    except Exception as e:
        verdict = BacktestVerdict(
            passed=False,
            reason=f"pipeline_load_error: {type(e).__name__}: {e}",
            calibration={}, leakage_check={}, refusal_sanity={},
            n_observations=0, report_path=str(report_path),
            drift_check={},
        )
        report_path.write_text(json.dumps({
            "verdict": asdict(verdict),
            "traceback": traceback.format_exc(),
        }, indent=2))
        return verdict

    # Walk every event.
    all_obs: list[dict] = []
    refusal_totals: dict[str, int] = {}
    leakage_log: list[dict] = []
    timing_ms: list[float] = []
    per_event_summary: list[dict] = []
    n_steps_total = 0

    for ev in events:
        obs, refusals = _walk_event(
            pipeline_cls, spec, archive_root, ev, walk_step_seconds, leakage_log,
        )
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

    # Empty/sparse archive — refuse cleanly.
    if n_obs < MIN_OBSERVATIONS:
        verdict = BacktestVerdict(
            passed=False,
            reason=f"insufficient_archive: {n_obs} obs (need >={MIN_OBSERVATIONS})",
            calibration={}, leakage_check=_compute_leakage(leakage_log),
            refusal_sanity=_compute_refusal_sanity(refusal_totals, n_steps_total),
            n_observations=n_obs,
            report_path=str(report_path),
            drift_check={},
        )
        _write_report(report_path, output_dir, verdict, all_obs, per_event_summary,
                      events, timing_ms)
        return verdict

    # Compute the four gates.
    cal = _compute_calibration(all_obs)
    leak = _compute_leakage(leakage_log)
    refusal = _compute_refusal_sanity(refusal_totals, n_steps_total)
    drift = _compute_drift(all_obs)

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
                f"calibration max_dev={cal.get('max_deviation')} "
                f"> {CALIBRATION_DECILE_MAX_DEVIATION}"
            )
        if "leakage" in failed:
            details.append(f"leakage violations={leak.get('violations')}")
        if "refusal_sanity" in failed:
            details.append(f"refusal_rate={refusal.get('rate')} "
                           f"outside [{REFUSAL_RATE_MIN}, {REFUSAL_RATE_MAX}]")
        if "drift" in failed:
            details.append(f"drift delta={drift.get('delta')} > {DRIFT_MAX_MAE_DELTA}")
        reason = "failed: " + "; ".join(details)

    verdict = BacktestVerdict(
        passed=passed,
        reason=reason,
        calibration=cal,
        leakage_check=leak,
        refusal_sanity=refusal,
        n_observations=n_obs,
        report_path=str(report_path),
        drift_check=drift,
    )

    _write_report(report_path, output_dir, verdict, all_obs, per_event_summary,
                  events, timing_ms)
    return verdict


def _write_report(
    report_path: Path,
    output_dir: Path,
    verdict: BacktestVerdict,
    all_obs: list[dict],
    per_event_summary: list[dict],
    events: list[str],
    timing_ms: list[float],
) -> None:
    """Write report.json (machine-readable) and calibration.csv (human-readable)."""
    samples = _stratified_samples(all_obs, WALK_SAMPLES_IN_REPORT)

    if timing_ms:
        timing_stats = {
            "n": len(timing_ms),
            "mean_ms": round(statistics.fmean(timing_ms), 3),
            "median_ms": round(statistics.median(timing_ms), 3),
            "p95_ms": round(_percentile(timing_ms, 95), 3),
            "max_ms": round(max(timing_ms), 3),
        }
    else:
        timing_stats = {"n": 0}

    report = {
        "verdict": asdict(verdict),
        "events_walked": events,
        "per_event": per_event_summary,
        "walk_samples": samples,
        "timing_ms": timing_stats,
    }
    report_path.write_text(json.dumps(report, indent=2, default=str))

    # CSV for humans — only if we have a calibration table.
    cal = verdict.calibration or {}
    if cal.get("deciles"):
        csv_path = output_dir / "calibration.csv"
        with csv_path.open("w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["decile", "n", "mean_pred", "mean_actual", "deviation"])
            for row in cal["deciles"]:
                w.writerow([row["decile"], row["n"], row["mean_pred"],
                            row["mean_actual"], row["deviation"]])


def _percentile(xs: list[float], pct: float) -> float:
    if not xs:
        return 0.0
    s = sorted(xs)
    k = (len(s) - 1) * (pct / 100.0)
    lo = int(k)
    hi = min(lo + 1, len(s) - 1)
    if lo == hi:
        return s[lo]
    frac = k - lo
    return s[lo] * (1 - frac) + s[hi] * frac


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def _main(argv: list[str]) -> int:
    if len(argv) < 3:
        print("usage: python -m auto_theo.backtest.harness "
              "<spec.json> <pipeline_module.py> <PipelineClassName> "
              "[--archive-root DIR] [--step-seconds N] [--output-dir DIR] "
              "[--events EV1,EV2,...]",
              file=sys.stderr)
        return 2
    spec_path = argv[0]
    module_path = argv[1]
    class_name = argv[2]

    archive_root = "auto_theo/archive"
    step_seconds = 3600
    output_dir: str | None = None
    events: list[str] = []

    i = 3
    while i < len(argv):
        a = argv[i]
        if a == "--archive-root" and i + 1 < len(argv):
            archive_root = argv[i + 1]; i += 2
        elif a == "--step-seconds" and i + 1 < len(argv):
            step_seconds = int(argv[i + 1]); i += 2
        elif a == "--output-dir" and i + 1 < len(argv):
            output_dir = argv[i + 1]; i += 2
        elif a == "--events" and i + 1 < len(argv):
            events = [e.strip() for e in argv[i + 1].split(",") if e.strip()]; i += 2
        else:
            print(f"unknown arg: {a}", file=sys.stderr)
            return 2

    verdict = run_backtest(
        pipeline_module_path=module_path,
        pipeline_class_name=class_name,
        spec_path=spec_path,
        historical_events=events,
        archive_root=archive_root,
        walk_step_seconds=step_seconds,
        output_dir=output_dir,
    )
    print(json.dumps({
        "passed": verdict.passed,
        "reason": verdict.reason,
        "n_observations": verdict.n_observations,
        "report_path": verdict.report_path,
    }, indent=2))
    return 0 if verdict.passed else 1


if __name__ == "__main__":
    sys.exit(_main(sys.argv[1:]))
