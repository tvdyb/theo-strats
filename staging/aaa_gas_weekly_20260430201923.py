"""AAA Gas Weekly threshold pipeline (KXAAAGASW family).

Family signature: aaa:gas:weekly_threshold

Despite the 'W' (weekly) suffix on the Kalshi series ticker, KXAAAGASW-* events
resolve on a SINGLE AAA print on a Monday (per spec rules_primary verbatim:
"according to AAA ... on May 4, 2026"). The pipeline is therefore structurally
identical to the daily KXAAAGASD pipeline with three deltas baked in by the
researcher: 2c strike spacing instead of 0.5c, horizon up to ~7 days instead
of 1, and a more lenient vol/spacing regime.

Inference is fully deterministic — no LLM, no randomness. Every numerical
constant has an inline comment with its source (spec section, reference theo
file, or theo_refresh.py line).
"""
from __future__ import annotations

import json
import math
import os
import re
import sys
from datetime import datetime, timezone, date
from pathlib import Path
from typing import Optional

# Allow running from staging/ without the package being installed.
_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from auto_theo.pipelines._base import (  # noqa: E402
    BlackoutError,
    DataPoint,
    InsufficientDataError,
    Pipeline,
    PipelineError,
    UnsupportedMarketError,
    make_theos_dict,
)


# --- Sanity constants (mirrored from theo_refresh.py lines 36-38) -----------
# These are NOT magic numbers chosen here; they are the canonical AAA scraper
# bounds in production. Carried over verbatim per spec.blackout_rules.
AAA_PRICE_MIN = 2.00          # theo_refresh.py:36 — US national avg has not been < $2 since 2016
AAA_PRICE_MAX = 7.00          # theo_refresh.py:37 — US national avg has never crossed ~$5.10
AAA_MAX_DAILY_MOVE = 0.30     # theo_refresh.py:38 — real AAA daily moves are typically < 8c
AAA_MIN_VALID_ROWS = 5        # theo_refresh.py:55-56 — fewer than 5 valid rows = bad parse, raise

# --- Model constants --------------------------------------------------------
SIGMA_1D = 0.013              # spec.vol_estimation_hints.method_suggestion + reference theo
                              # /Users/wilsonw/Downloads/theos/KXAAAGASD-26APR29.json sigma_used=0.013
                              # ("the production daily-gas implied σ at boundary").
SIGMA_HARD_CAP = 0.10         # spec.vol_estimation_hints.method_suggestion explicitly mandates
                              # "σ ≤ 0.10". The current rally regime realized 7-day move is 27c,
                              # i.e. ≈10x the stationary 1d σ. We let σ inflate via sqrt(h) but
                              # ceiling it at 10c rather than quote insanely wide. Refusal is
                              # better than fabrication once σ would exceed 10c.
DEFAULT_DRIFT_ANCHOR = 0.0    # spec.vol_estimation_hints recommends a small positive default
                              # for the spring-summer rally; we conservatively set 0.0 and let
                              # an analyst override via env var (read once at construction).
BAND_CENTS = 6.0              # matches reference theo KXAAAGASD-26APR29.json band_cents=6.0
STRIKE_SPACING = 0.02         # spec.tradability_reason: "26 strikes 2c apart from $4.00 to $4.50"
STRIKE_GRID_LO = 4.00         # spec.tradability_reason: lower bound of canonical grid
STRIKE_GRID_N = 26            # spec.tradability_reason: 26 strikes (4.00, 4.02, ..., 4.50)

# --- HTTP constants ---------------------------------------------------------
AAA_HOMEPAGE_URL = "https://gasprices.aaa.com/"          # spec.underlying.data_sources[0]
AAA_FAILOVER_URL = "https://gasprices.aaa.com/?state=US" # spec.underlying.data_sources[1]
HTTP_TIMEOUT_S = 10                                      # theo_refresh.py:47 (timeout=10)
USER_AGENT = (                                           # theo_refresh.py:13-14
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
)

# Tag for archive_root subdirectory used in historical mode.
# Mirrors spec.underlying.data_sources[0].historical_archive_url note that
# backfill is "auto_theo/archive/aaa/national_regular/<date>.json".
ARCHIVE_SOURCE_DIR = "aaa/national_regular"

# Confidence rule input — denominator for vol_to_spacing_ratio.
# vol_to_spacing_ratio = σ_h / STRIKE_SPACING per the standard helper.
SPACING_FOR_CONFIDENCE = STRIKE_SPACING


def _build_strike_grid() -> list[float]:
    """Canonical AAA gas weekly strike grid: 26 strikes 2c apart from 4.00 to 4.50.

    Cited in spec.tradability_reason. Rounded to 3 decimals to match Kalshi
    ticker formatting (e.g. KXAAAGASW-26MAY04-4.300).
    """
    return [round(STRIKE_GRID_LO + STRIKE_SPACING * i, 3) for i in range(STRIKE_GRID_N)]


def _parse_strike_from_ticker(ticker: str) -> Optional[float]:
    """Extract the strike from a Kalshi market ticker.

    Mirror of theo_refresh._parse_strike (theo_refresh.py:22-29). Last hyphen
    segment, optionally prefixed with 'T'/'P', parsed as float. Returns None
    on failure (callers should treat as UnsupportedMarketError).
    """
    tail = str(ticker).rsplit("-", 1)[-1]
    if tail and tail[0].upper() in ("T", "P"):
        tail = tail[1:]
    try:
        return float(tail)
    except (TypeError, ValueError):
        return None


class AAAGasWeeklyPipeline(Pipeline):
    """Pipeline for KXAAAGASW-* (weekly-cadence Kalshi series, single AAA print resolution)."""

    family_signature = "aaa:gas:weekly_threshold"
    # AAA publishes daily; refreshing every 15 minutes gives us several scrapes per
    # publish window without hammering the page. 900s is a safe over-frequency for
    # a daily-publish source (see spec researcher_notes — Kalshi 429s came from
    # API calls, not AAA). Comment per project convention.
    refresh_cadence_seconds = 900  # 15 minutes — AAA publishes daily; this is over-frequent on purpose.

    def __init__(self, spec: dict, mode: str = "live", archive_root: Path | None = None):
        super().__init__(spec=spec, mode=mode, archive_root=archive_root)
        # Read analyst override once at construction. Not re-read per call so
        # that build_theos remains a deterministic function of (spec, now, archive).
        try:
            self._drift_anchor = float(
                os.environ.get("AAA_GAS_WEEKLY_DRIFT_ANCHOR", DEFAULT_DRIFT_ANCHOR)
            )
        except (TypeError, ValueError):
            # On parse failure, fall back to default rather than crash. An invalid
            # env var should not prevent quoting; refusal is reserved for data issues.
            self._drift_anchor = DEFAULT_DRIFT_ANCHOR

    # ------------------------------------------------------------------ data
    def data_as_of(self, source: dict, now: datetime) -> DataPoint:
        """Fetch AAA today as of `now`. Live or historical depending on mode."""
        if self.mode == "live":
            return self._fetch_live(source, now)
        if self.mode == "historical":
            return self._fetch_historical(source, now)
        # Should be unreachable — base class validates mode in __init__.
        raise InsufficientDataError(f"unknown mode: {self.mode}")

    def _fetch_live(self, source: dict, now: datetime) -> DataPoint:
        """Scrape AAA homepage (with failover) and apply all three sanity guards.

        On any failure raises InsufficientDataError with a precise reason.
        """
        try:
            import requests  # local import so historical-mode tests don't need it
        except ImportError as exc:
            raise InsufficientDataError(f"requests not available for live mode: {exc}")

        url = source.get("url", AAA_HOMEPAGE_URL)
        last_error: Optional[str] = None
        for attempt_url in (url, AAA_FAILOVER_URL):
            try:
                r = requests.get(attempt_url, headers={"User-Agent": USER_AGENT}, timeout=HTTP_TIMEOUT_S)
                r.raise_for_status()
            except Exception as exc:
                last_error = f"{attempt_url}: {type(exc).__name__}: {exc}"
                continue

            rows = self._parse_aaa_rows(r.text)
            if len(rows) < AAA_MIN_VALID_ROWS:
                # spec.blackout_rules: "fewer than 5 valid rows from the regex, raise rather than guess"
                last_error = (
                    f"{attempt_url}: only {len(rows)} valid rows parsed "
                    f"(need >= {AAA_MIN_VALID_ROWS})"
                )
                continue

            today, yest = rows[0], rows[1]
            # Range guard (theo_refresh.py:58-62)
            if not (AAA_PRICE_MIN <= today <= AAA_PRICE_MAX):
                raise InsufficientDataError(
                    f"AAA today=${today:.3f} outside plausible range "
                    f"[${AAA_PRICE_MIN:.2f}, ${AAA_PRICE_MAX:.2f}] (source={attempt_url})"
                )
            if not (AAA_PRICE_MIN <= yest <= AAA_PRICE_MAX):
                raise InsufficientDataError(
                    f"AAA yesterday=${yest:.3f} outside plausible range "
                    f"[${AAA_PRICE_MIN:.2f}, ${AAA_PRICE_MAX:.2f}] (source={attempt_url})"
                )
            # Daily-move guard (theo_refresh.py:63-65)
            if abs(today - yest) > AAA_MAX_DAILY_MOVE:
                raise InsufficientDataError(
                    f"AAA daily move |today-yest|=${abs(today - yest):.3f} > "
                    f"${AAA_MAX_DAILY_MOVE:.2f} max (source={attempt_url}); "
                    f"likely stale page or parse error"
                )

            return DataPoint(
                value=today,
                publication_timestamp=now,  # AAA does not publish a precise timestamp; scrape time is best estimate
                effective_date=datetime(now.year, now.month, now.day, tzinfo=timezone.utc),
                source_url=attempt_url,
                raw={
                    "today": today,
                    "yesterday": yest,
                    "week_ago": rows[2] if len(rows) > 2 else None,
                    "month_ago": rows[3] if len(rows) > 3 else None,
                    "year_ago": rows[4] if len(rows) > 4 else None,
                },
            )

        # Both URLs exhausted.
        raise InsufficientDataError(
            f"AAA scrape failed on both homepage and failover: {last_error}"
        )

    @staticmethod
    def _parse_aaa_rows(html: str) -> list[float]:
        """Mirror of theo_refresh.fetch_aaa() row extraction (lines 49-54).

        Iterate <tr>...</tr>; first column of each row whose regex matches
        4-6 prices is appended. Stop after 5 rows (today, yest, week, month, year).
        """
        rows: list[float] = []
        for tr in re.finditer(r"<tr[^>]*>(.*?)</tr>", html, re.S):
            prices = re.findall(r"\$\d\.\d{3}", tr.group(1))
            if 4 <= len(prices) <= 6:
                rows.append(float(prices[0].lstrip("$")))
            if len(rows) == 5:
                break
        return rows

    def _fetch_historical(self, source: dict, now: datetime) -> DataPoint:
        """Read most recent archive file <= now.date() with publication_timestamp <= now."""
        archive_dir = self.archive_root / ARCHIVE_SOURCE_DIR
        if not archive_dir.exists():
            raise InsufficientDataError(f"archive dir missing: {archive_dir}")

        target_date = now.date()
        candidates = []
        for p in archive_dir.glob("*.json"):
            stem = p.stem  # YYYY-MM-DD
            try:
                d = datetime.strptime(stem, "%Y-%m-%d").date()
            except ValueError:
                continue
            if d <= target_date:
                candidates.append((d, p))
        if not candidates:
            raise InsufficientDataError(
                f"no AAA archive entries on or before {target_date.isoformat()}"
            )
        candidates.sort(reverse=True)
        for d, path in candidates:
            try:
                payload = json.loads(path.read_text())
            except Exception as exc:
                raise InsufficientDataError(f"archive parse error {path}: {exc}")
            # Schema: at minimum {value, publication_timestamp}. Tolerate scalar shorthand.
            if isinstance(payload, (int, float)):
                value = float(payload)
                pub_ts = datetime.combine(d, datetime.min.time(), tzinfo=timezone.utc)
            elif isinstance(payload, dict):
                if "value" not in payload:
                    continue
                value = float(payload["value"])
                pub_raw = payload.get("publication_timestamp")
                if pub_raw is None:
                    pub_ts = datetime.combine(d, datetime.min.time(), tzinfo=timezone.utc)
                else:
                    pub_ts = datetime.fromisoformat(str(pub_raw).replace("Z", "+00:00"))
            else:
                continue
            if pub_ts > now:
                continue  # leakage guard
            # Range guard still applies in historical mode.
            if not (AAA_PRICE_MIN <= value <= AAA_PRICE_MAX):
                raise InsufficientDataError(
                    f"archive {path} value=${value:.3f} outside plausible range"
                )
            return DataPoint(
                value=value,
                publication_timestamp=pub_ts,
                effective_date=datetime.combine(d, datetime.min.time(), tzinfo=timezone.utc),
                source_url=str(path),
                raw=payload if isinstance(payload, dict) else {"value": value},
            )
        raise InsufficientDataError(
            f"no AAA archive entry with publication_timestamp <= {now.isoformat()}"
        )

    # --------------------------------------------------------------- helpers
    def _resolution_datetime(self) -> datetime:
        """Parse spec.resolution.resolution_timestamp."""
        ts = self.spec["resolution"]["resolution_timestamp"]
        return datetime.fromisoformat(str(ts).replace("Z", "+00:00"))

    def _consecutive_stable(self, now: datetime) -> bool:
        """Resolution-day stability check.

        TODO: wire this to a small persistent log of recent scrapes (>= 2 entries
        >= 60s apart returning the same value). Per spec.blackout_rules entry on
        resolution-day pinning. For staging we conservatively return False so we
        never pin spuriously — pinning a strike to {0.005, 0.995} on stale data
        would be much worse than continuing to quote the model.
        """
        return False

    # ----------------------------------------------------------------- build
    def build_theos(self, event_ticker: str, now: datetime) -> dict:
        # 1. Blackout check (spec.blackout_calendar handled by base helper)
        self._check_blackout(now)

        # 2. Fetch AAA today (propagates InsufficientDataError on guard fail)
        primary_source = self.spec["underlying"]["data_sources"][0]
        dp = self.data_as_of(primary_source, now)
        aaa_today = dp.value

        # 3. Horizon check
        resolution_dt = self._resolution_datetime()
        horizon_seconds = (resolution_dt - now).total_seconds()
        horizon_days = horizon_seconds / 86400.0
        if horizon_days <= 0:
            raise InsufficientDataError(
                f"horizon_days={horizon_days:.4f} <= 0 (now={now.isoformat()} "
                f"after resolution_time={resolution_dt.isoformat()})"
            )

        # 4. σ_h = σ_1 * sqrt(h_days), capped at 0.10 per spec mandate.
        sigma_h_uncapped = SIGMA_1D * math.sqrt(horizon_days)
        sigma_h = min(sigma_h_uncapped, SIGMA_HARD_CAP)

        # 5. μ = AAA_today + drift_anchor. drift_anchor read at construction.
        mu = aaa_today + self._drift_anchor

        # 6+8. Build strike map. Use canonical 26-strike 2c grid from spec.
        # We compute over the full grid rather than only the requested ticker so
        # the live theo file matches the production schema (a strikes dict per event).
        strike_grid = _build_strike_grid()
        event_prefix = event_ticker.rsplit("-", 1)[0]  # KXAAAGASW-26MAY04
        # Validate that the requested ticker's strike is in the grid; if not,
        # this is the wrong family.
        requested_strike = _parse_strike_from_ticker(event_ticker)
        if requested_strike is None:
            raise UnsupportedMarketError(
                f"could not parse strike from ticker {event_ticker!r}"
            )
        if not any(abs(requested_strike - s) < 1e-9 for s in strike_grid):
            raise UnsupportedMarketError(
                f"strike {requested_strike} not on canonical AAA gas weekly grid "
                f"[{STRIKE_GRID_LO:.2f}, {STRIKE_GRID_LO + STRIKE_SPACING * (STRIKE_GRID_N - 1):.2f}]"
            )

        # 7. Resolution-day pinning. Only pin on stable consecutive scrapes.
        is_resolution_day = now.date() == resolution_dt.date()
        pinning = is_resolution_day and self._consecutive_stable(now)

        strikes_out: dict[str, float] = {}
        for s in strike_grid:
            tk = f"{event_prefix}-{s:.3f}"  # e.g. KXAAAGASW-26MAY04-4.300
            if pinning:
                # Pin to {0.005, 0.995} per spec.blackout_rules resolution-day rule.
                # Pinned bound floor 0.005 / ceiling 0.995 matches reference theo
                # KXAAAGASD-26APR29.json (clipped to [0.005, 0.995]).
                p_yes = 0.995 if aaa_today > s else 0.005
            else:
                z = (s - mu) / sigma_h
                p_yes = 1.0 - self._norm_cdf(z)
            strikes_out[tk] = round(p_yes, 6)

        # 10. Confidence
        data_age_seconds = (now - dp.publication_timestamp).total_seconds()
        # Negative age can occur in malformed archives; clamp to 0 for safety.
        if data_age_seconds < 0:
            data_age_seconds = 0.0
        confidence = self._confidence_from(
            data_age_seconds=data_age_seconds,
            vol_to_spacing_ratio=sigma_h / SPACING_FOR_CONFIDENCE,
            seconds_to_resolution=max(horizon_seconds, 0.0),
        )

        # 11. Assemble theos dict via shared helper.
        method_str = (
            "NormalCDF mu=AAA_today+drift, sigma=0.013*sqrt(h_days) capped at 0.10"
        )
        if pinning:
            method_str += " [PINNED: resolution-day stable scrape]"

        return make_theos_dict(
            event=self.spec["event_ticker"],
            underlying=self.spec["underlying"]["name"],
            as_of=now,
            resolution_time=resolution_dt,
            current_value=aaa_today,
            sigma_used=round(sigma_h, 6),
            method=method_str,
            confidence=confidence,
            band_cents=BAND_CENTS,
            blackouts=list(self.spec.get("blackout_calendar", [])),
            strikes=strikes_out,
        )


# ---------------------------------------------------------- sanity __main__
if __name__ == "__main__":
    spec_path = Path("/Users/wilsonw/mm-setup/auto_theo/specs/KXAAAGASW-26MAY04.json")
    spec = json.loads(spec_path.read_text())
    pipeline = AAAGasWeeklyPipeline(spec=spec, mode="live")
    try:
        out = pipeline.build_theos(
            "KXAAAGASW-26MAY04-4.300",
            datetime.now(timezone.utc),
        )
        print(json.dumps(out, indent=2, default=str))
    except PipelineError as exc:
        print(f"[refusal] {type(exc).__name__}: {exc.reason}")
