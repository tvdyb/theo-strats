#!/usr/bin/env python3
"""Walk-forward selection of SIGMA_ROLLING_WEEKS (EIA-weekly window) for the
rev-2 AAA gas weekly pipeline.

Procedure:
1. Load resolved KXAAAGASD events from the archive (the family-similar
   historical surrogate).
2. Sort events chronologically by min(open_time). Split 80/20.
3. For each candidate window in [4, 8, 12, 16]:
   - Reuse the per-event-spec patching from run_backtest.py.
   - Import the new staging pipeline; monkey-patch the candidate value into
     the loaded module's `SIGMA_ROLLING_WEEKS` constant.
   - Run harness against in-sample 80% — record max-decile-deviation and mae.
   - Run against held-out 20% — record same.
4. Pick the candidate minimizing held-out max-decile-deviation. Tie-break:
   smaller window.
5. Overfit check: if held-out > in-sample by >8pp for the chosen window, STOP.
   Write sigma_window_selection.json with overfit_flag=True.
6. Write sigma_window_selection.json with the table + chosen window.

Stdlib-only.
"""
from __future__ import annotations

import copy
import importlib.util
import json
import statistics
import sys
import time
import traceback
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path("/Users/wilsonw/mm-setup")
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from auto_theo.backtest import harness as H  # noqa: E402

SPEC_PATH = ROOT / "auto_theo/specs/KXAAAGASW-26MAY04.json"
PIPELINE_MODULE = ROOT / "auto_theo/staging/aaa_gas_weekly_20260430234928.py"
PIPELINE_CLASS = "AAAGasWeeklyPipeline"
ARCHIVE_ROOT = ROOT / "auto_theo/archive"

OUTPUT_PATH = Path(__file__).resolve().parent / "sigma_window_selection.json"

CANDIDATE_WINDOWS = [4, 8, 12, 16]  # weekly observations
HELD_OUT_FRAC = 0.20
OVERFIT_DELTA_PP = 0.08

W_STRIKE_GRID = [round(4.00 + 0.02 * i, 3) for i in range(26)]


def _load_module(path: Path):
    """Load the staging pipeline as a fresh module each call so that monkey-
    patching SIGMA_ROLLING_WEEKS does not leak between candidates."""
    spec = importlib.util.spec_from_file_location(
        f"_sigma_v2_{abs(hash((str(path), time.perf_counter_ns())))}",
        str(path),
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _build_per_event_spec(base_spec: dict, market_json: dict, event_ticker: str) -> dict:
    spec = copy.deepcopy(base_spec)
    spec["event_ticker"] = event_ticker
    close = H._market_close_time(market_json)
    if close is None:
        close = market_json.get("raw", {}).get("expiration_time")
        if isinstance(close, str):
            close = H._parse_iso(close)
    if close is None:
        suffix = event_ticker.split("-", 1)[1]
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


def _pick_probe_market_ticker(event_ticker: str, resolutions: dict) -> str | None:
    """Pick a probe ticker for backtest. We use the EVENT-LEVEL ticker so the
    pipeline emits all 26 canonical strikes; the resolution map then matches
    by trailing strike key.
    """
    # Event-level ticker triggers full-grid emission in the rev-2 pipeline.
    return event_ticker


def _events_in_archive(archive_root: Path, prefix: str) -> list[str]:
    seen: set[str] = set()
    for p in H._list_market_files(archive_root):
        ev = H._event_ticker_from_market(p.stem)
        if ev.startswith(prefix + "-"):
            seen.add(ev)
    return sorted(seen)


def _walk_event(
    pipeline_cls,
    base_spec,
    archive_root,
    event_ticker,
    walk_step_seconds,
    leakage_log,
):
    markets = H._markets_for_event(archive_root, event_ticker)
    if not markets:
        return [], {"missing_archive": 1, "_n_steps": 0}

    resolutions = H._build_resolution_map(markets)
    if not resolutions:
        return [], {"unresolved_event": 1, "_n_steps": 0}

    per_event_spec = _build_per_event_spec(base_spec, markets[0], event_ticker)
    probe_ticker = _pick_probe_market_ticker(event_ticker, resolutions)
    if probe_ticker is None:
        return [], {"no_probe_strike_on_w_grid": 1, "_n_steps": 0}

    open_times = [t for t in (H._market_open_time(m) for m in markets) if t]
    close_times = [t for t in (H._market_close_time(m) for m in markets) if t]
    if not open_times or not close_times:
        return [], {"missing_timestamps": 1, "_n_steps": 0}
    walk_start = min(open_times)
    walk_end = max(close_times)
    if walk_end <= walk_start:
        return [], {"degenerate_window": 1, "_n_steps": 0}

    observations = []
    refusals = {
        "BlackoutError": 0, "InsufficientDataError": 0,
        "UnsupportedMarketError": 0, "OtherError": 0, "Leakage": 0,
    }

    step = timedelta(seconds=walk_step_seconds)
    now = walk_start
    max_steps = 10_000
    n_steps = 0

    while now <= walk_end and n_steps < max_steps:
        n_steps += 1
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
            })
        now += step

    refusals["_n_steps"] = n_steps
    return observations, refusals


def _earliest_open(archive_root: Path, event_ticker: str) -> datetime | None:
    markets = H._markets_for_event(archive_root, event_ticker)
    opens = [t for t in (H._market_open_time(m) for m in markets) if t]
    return min(opens) if opens else None


def _calibration(obs: list[dict]) -> tuple[float, float]:
    if not obs:
        return 0.0, 0.0
    sorted_obs = sorted(obs, key=lambda o: o["predicted_p"])
    n = len(sorted_obs)
    bins = [[] for _ in range(10)]
    for i, o in enumerate(sorted_obs):
        idx = min(9, (i * 10) // n)
        bins[idx].append(o)
    max_dev = 0.0
    for b in bins:
        if not b:
            continue
        mp = statistics.fmean(o["predicted_p"] for o in b)
        ma = statistics.fmean(o["actual"] for o in b)
        max_dev = max(max_dev, abs(mp - ma))
    mae = statistics.fmean(abs(o["predicted_p"] - o["actual"]) for o in obs)
    return max_dev, mae


def _atomic_write_json(path: Path, data: dict) -> None:
    """Atomic write per project convention."""
    import os
    import tempfile
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", dir=str(path.parent), delete=False
    ) as f:
        json.dump(data, f, indent=2, default=str)
        tmp = f.name
    os.replace(tmp, str(path))


def main() -> int:
    base_spec = json.loads(SPEC_PATH.read_text())
    events = _events_in_archive(ARCHIVE_ROOT, "KXAAAGASD")
    if not events:
        print("No KXAAAGASD events in archive.", file=sys.stderr)
        return 2

    def _event_sort_key(ev: str):
        t = _earliest_open(ARCHIVE_ROOT, ev)
        if t is not None:
            return t
        suffix = ev.split("-", 1)[1]
        return datetime.strptime(suffix, "%y%b%d").replace(tzinfo=timezone.utc)

    events_sorted = sorted(events, key=_event_sort_key)
    n_events = len(events_sorted)
    n_held_out = max(1, int(round(HELD_OUT_FRAC * n_events)))
    in_sample = events_sorted[: n_events - n_held_out]
    held_out = events_sorted[n_events - n_held_out :]

    print(
        f"Total events: {n_events}; in_sample: {len(in_sample)}; "
        f"held_out: {len(held_out)}",
        file=sys.stderr,
    )

    rows = []
    for w in CANDIDATE_WINDOWS:
        # Load a fresh module each candidate so SIGMA_ROLLING_WEEKS overrides
        # do not leak.
        mod = _load_module(PIPELINE_MODULE)
        mod.SIGMA_ROLLING_WEEKS = w
        pipeline_cls = getattr(mod, PIPELINE_CLASS)

        leakage_log: list = []
        in_obs, out_obs = [], []
        t0 = time.perf_counter()

        for ev in in_sample:
            try:
                obs, _ = _walk_event(
                    pipeline_cls, base_spec, ARCHIVE_ROOT, ev, 3600, leakage_log,
                )
                in_obs.extend(obs)
            except Exception:
                traceback.print_exc()

        for ev in held_out:
            try:
                obs, _ = _walk_event(
                    pipeline_cls, base_spec, ARCHIVE_ROOT, ev, 3600, leakage_log,
                )
                out_obs.extend(obs)
            except Exception:
                traceback.print_exc()

        in_dev, in_mae = _calibration(in_obs)
        out_dev, out_mae = _calibration(out_obs)
        elapsed = time.perf_counter() - t0
        row = {
            "window_weeks": w,
            "in_sample_max_decile_deviation": round(in_dev, 6),
            "held_out_max_decile_deviation": round(out_dev, 6),
            "in_sample_mae": round(in_mae, 6),
            "held_out_mae": round(out_mae, 6),
            "n_in_sample_obs": len(in_obs),
            "n_held_out_obs": len(out_obs),
            "elapsed_s": round(elapsed, 2),
        }
        rows.append(row)
        print(json.dumps(row, indent=2), file=sys.stderr)

    # Pick best held-out, tie-break smaller window.
    best = min(
        rows,
        key=lambda r: (r["held_out_max_decile_deviation"], r["window_weeks"]),
    )
    delta = (
        best["held_out_max_decile_deviation"]
        - best["in_sample_max_decile_deviation"]
    )
    overfit = delta > OVERFIT_DELTA_PP

    result = {
        "events_total": n_events,
        "events_in_sample": len(in_sample),
        "events_held_out": len(held_out),
        "in_sample_event_list": in_sample,
        "held_out_event_list": held_out,
        "candidate_windows_weeks": CANDIDATE_WINDOWS,
        "rows": rows,
        "selected_window_weeks": best["window_weeks"],
        "selected_held_out_max_decile_deviation": best["held_out_max_decile_deviation"],
        "selected_in_sample_max_decile_deviation": best["in_sample_max_decile_deviation"],
        "overfit_delta_pp": round(delta, 6),
        "overfit_threshold_pp": OVERFIT_DELTA_PP,
        "overfit_flag": overfit,
        "selected_at": datetime.now(timezone.utc).isoformat(),
        "pipeline_module": str(PIPELINE_MODULE),
    }
    _atomic_write_json(OUTPUT_PATH, result)
    print(
        json.dumps(
            {
                k: v
                for k, v in result.items()
                if k not in ("in_sample_event_list", "held_out_event_list")
            },
            indent=2,
        )
    )
    return 0 if not overfit else 3


if __name__ == "__main__":
    sys.exit(main())
