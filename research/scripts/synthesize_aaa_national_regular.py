#!/usr/bin/env python3
"""Synthesize archive/aaa/national_regular/<date>.json from EIA weekly + implied bounds.

The staged AAAGasWeeklyPipeline reads from `archive/aaa/national_regular/<date>.json`
(per ARCHIVE_SOURCE_DIR = "aaa/national_regular"). The backfiller wrote to
`national_regular_eia_weekly` (1857 weekly EIA files, 1990-08-20 -> 2026-04-27)
and `national_regular_implied` (36 one-sided AAA-daily lower bounds derived
from KXAAAGASD resolutions). This script merges them into the per-day directory
the pipeline expects.

Logic:
  1. For each EIA weekly file (with `value_usd_per_gal` for week starting on
     `effective_date`), forward-fill 7 daily files [week_start, week_start+6].
  2. Where an implied-bound file exists for that date with a `low` floor,
     use `max(low, eia_value)` -- the implied lower bound is a tighter floor
     on actual AAA, since AAA-EIA spread can be 10-30c during rally regimes.
     Mark `value_origin = "implied_floor_over_eia"`.
  3. Write to `archive/aaa/national_regular/<YYYY-MM-DD>.json` with the schema
     the pipeline's _fetch_historical reads (lines 229-285): the pipeline reads
     `value` (required), `publication_timestamp` (optional, falls back to
     midnight UTC of the file date), and tolerates everything else as `raw`.
  4. Append-only: skip dates whose target file already exists.

Stdlib only.
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path("/Users/wilsonw/mm-setup/auto_theo")
ARCHIVE = ROOT / "archive"
EIA_DIR = ARCHIVE / "aaa" / "national_regular_eia_weekly"
IMPLIED_DIR = ARCHIVE / "aaa" / "national_regular_implied"
OUT_DIR = ARCHIVE / "aaa" / "national_regular"

# Sanity range mirroring the pipeline (AAA_PRICE_MIN/MAX).
PRICE_MIN = 2.00
PRICE_MAX = 7.00


def _parse_date(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc)


def _read_json(p: Path) -> dict | None:
    try:
        with p.open("r") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return None


def load_eia_series() -> dict[str, dict]:
    """{date_str: payload} keyed by week_start effective_date."""
    out: dict[str, dict] = {}
    if not EIA_DIR.is_dir():
        return out
    for p in sorted(EIA_DIR.glob("*.json")):
        j = _read_json(p)
        if not j:
            continue
        d = j.get("effective_date") or p.stem
        out[d] = j
    return out


def load_implied() -> dict[str, dict]:
    """{date_str: payload} from implied bound files."""
    out: dict[str, dict] = {}
    if not IMPLIED_DIR.is_dir():
        return out
    for p in sorted(IMPLIED_DIR.glob("*.json")):
        j = _read_json(p)
        if not j:
            continue
        d = j.get("effective_date") or p.stem
        out[d] = j
    return out


def synthesize() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    eia = load_eia_series()
    implied = load_implied()

    # Build week_starts -> next_week_start so we know how to forward-fill (typically 7d).
    week_starts = sorted(eia.keys())
    if not week_starts:
        print("ERROR: no EIA weekly files found; cannot synthesize.", file=sys.stderr)
        return 0

    today = datetime.now(timezone.utc).date()
    today_str = today.isoformat()

    n_eia = len(eia)
    n_implied = len(implied)
    n_written = 0
    n_skipped_exists = 0
    n_skipped_range = 0
    n_implied_overrides = 0

    fetched_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    for i, ws_str in enumerate(week_starts):
        ws_dt = _parse_date(ws_str)
        ws_pub = eia[ws_str].get("publication_timestamp") or f"{ws_str}T21:00:00Z"
        eia_value = eia[ws_str].get("value_usd_per_gal")
        if eia_value is None:
            continue
        try:
            eia_value = float(eia_value)
        except (TypeError, ValueError):
            continue

        # Forward-fill window: [week_start, min(week_start+6, next_week_start-1, today)].
        if i + 1 < len(week_starts):
            next_ws_dt = _parse_date(week_starts[i + 1])
            ff_end = min(ws_dt + timedelta(days=6), next_ws_dt - timedelta(days=1))
        else:
            ff_end = ws_dt + timedelta(days=6)
        ff_end_date = ff_end.date()
        if ff_end_date > today:
            ff_end_date = today

        d = ws_dt.date()
        while d <= ff_end_date:
            d_str = d.isoformat()
            out_path = OUT_DIR / f"{d_str}.json"
            d += timedelta(days=1)

            if out_path.exists():
                n_skipped_exists += 1
                continue

            value = eia_value
            origin = "eia_weekly_forward_fill"
            implied_payload = implied.get(d_str.replace("-", "-"))  # noop, but explicit
            implied_payload = implied.get(d_str)
            if implied_payload is not None:
                low = implied_payload.get("low")
                if isinstance(low, (int, float)):
                    new_value = max(float(low), eia_value)
                    if new_value > eia_value + 1e-9:
                        n_implied_overrides += 1
                        value = new_value
                        origin = "implied_floor_over_eia"
                    else:
                        # Even when implied <= eia, mark that an implied bound
                        # was checked so we have full provenance.
                        origin = "eia_weekly_forward_fill_implied_below"

            if not (PRICE_MIN <= value <= PRICE_MAX):
                n_skipped_range += 1
                continue

            payload = {
                "fetched_at": fetched_at,
                "source": "synthesize_aaa_national_regular.py",
                "synthesizer": "eia_weekly_ffill_with_implied_floor",
                "value": round(float(value), 4),
                "value_usd_per_gal": round(float(value), 4),
                "publication_timestamp": ws_pub,
                "effective_date": d_str,
                "value_origin": origin,
                "source_url": eia[ws_str].get("source_url", ""),
                "raw": {
                    "eia_week_start": ws_str,
                    "eia_value_usd_per_gal": eia_value,
                    "implied": implied_payload,
                },
                "note": (
                    "Synthesized for the staged AAAGasWeeklyPipeline historical "
                    "mode. EIA weekly forward-filled to daily; implied lower "
                    "bounds (when present and tighter than EIA) override EIA. "
                    "AAA-EIA spread can be 10-30c during rally regimes, so "
                    "EIA-only ffill systematically under-prices AAA in rallies."
                ),
            }

            tmp = out_path.with_suffix(".json.tmp")
            try:
                with tmp.open("w") as f:
                    json.dump(payload, f, separators=(",", ":"))
                os.replace(tmp, out_path)
                n_written += 1
            except OSError as exc:
                print(f"WARN: failed to write {out_path}: {exc}", file=sys.stderr)
                try:
                    tmp.unlink()
                except OSError:
                    pass

    # Also: handle implied-only dates (i.e. dates after the last EIA week_end).
    # The most recent EIA week may be older than today; for trailing dates we
    # can still emit a file from implied bounds alone.
    if week_starts:
        last_ws = _parse_date(week_starts[-1])
        last_eia_value = float(eia[week_starts[-1]]["value_usd_per_gal"])
        last_eia_pub = eia[week_starts[-1]].get("publication_timestamp") or f"{week_starts[-1]}T21:00:00Z"
        # Already covered up to last_ws+6 above; extend implied-only past that.
        d_start = (last_ws + timedelta(days=7)).date()
        d = d_start
        while d <= today:
            d_str = d.isoformat()
            out_path = OUT_DIR / f"{d_str}.json"
            d_iter = d
            d = d + timedelta(days=1)

            if out_path.exists():
                n_skipped_exists += 1
                continue

            implied_payload = implied.get(d_str)
            if implied_payload is None:
                # Use the trailing EIA value as a last-resort ffill.
                value = last_eia_value
                origin = "eia_weekly_forward_fill_trailing"
            else:
                low = implied_payload.get("low")
                if isinstance(low, (int, float)):
                    value = max(float(low), last_eia_value)
                    if value > last_eia_value + 1e-9:
                        n_implied_overrides += 1
                        origin = "implied_floor_over_eia"
                    else:
                        origin = "eia_weekly_forward_fill_trailing_implied_below"
                else:
                    value = last_eia_value
                    origin = "eia_weekly_forward_fill_trailing"

            if not (PRICE_MIN <= value <= PRICE_MAX):
                n_skipped_range += 1
                continue

            payload = {
                "fetched_at": fetched_at,
                "source": "synthesize_aaa_national_regular.py",
                "synthesizer": "eia_weekly_ffill_with_implied_floor",
                "value": round(float(value), 4),
                "value_usd_per_gal": round(float(value), 4),
                "publication_timestamp": last_eia_pub,
                "effective_date": d_str,
                "value_origin": origin,
                "source_url": eia[week_starts[-1]].get("source_url", ""),
                "raw": {
                    "eia_week_start": week_starts[-1],
                    "eia_value_usd_per_gal": last_eia_value,
                    "implied": implied_payload,
                },
                "note": (
                    "Trailing synth past last EIA week_start. Implied lower "
                    "bound (when present) overrides EIA-trailing if tighter."
                ),
            }
            tmp = out_path.with_suffix(".json.tmp")
            try:
                with tmp.open("w") as f:
                    json.dump(payload, f, separators=(",", ":"))
                os.replace(tmp, out_path)
                n_written += 1
            except OSError as exc:
                print(f"WARN: failed to write {out_path}: {exc}", file=sys.stderr)
                try:
                    tmp.unlink()
                except OSError:
                    pass

    print(f"=== synthesize_aaa_national_regular ===")
    print(f"EIA weekly files read: {n_eia}")
    print(f"Implied-bound files read: {n_implied}")
    print(f"Daily files written: {n_written}")
    print(f"Files already present (skipped): {n_skipped_exists}")
    print(f"Out-of-range values rejected: {n_skipped_range}")
    print(f"Days where implied floor overrode EIA: {n_implied_overrides}")
    print(f"Output dir: {OUT_DIR}")
    return n_written


if __name__ == "__main__":
    synthesize()
