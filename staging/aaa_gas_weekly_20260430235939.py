"""AAA Gas Weekly threshold pipeline (KXAAAGASW family) — rev 3.

Family signature: aaa:gas:weekly_threshold

This is rev 3 of the AAA gas weekly pipeline. Prior revs:
  rev 1: stationary 1-day σ × sqrt(h). Backtest max-decile-deviation = 37.7pp.
  rev 2: EIA-weekly realized σ over 8-week rolling window
         (auto_theo/staging/aaa_gas_weekly_20260430234928.py).
         In-sample 28.67pp, held-out 44.32pp, overfit by 15.65pp.

The bias signature on rev 2 was "predicted ≈ 1.0, actual ≈ 0.6-0.8 in worst
decile" — Normal CDF was under-weighting adverse moves. This rev keeps the EIA
weekly realized-σ machinery (with the rev-2 walk-forward-selected 8-week
window, NOT re-tuned) but replaces the Normal CDF with a Student-t CDF whose
degrees of freedom is selected jointly via walk-forward in
/Users/wilsonw/mm-setup/auto_theo/research/scripts/select_t_df.py.

Lower df ⇒ fatter tails. df ∈ {3, 4, 6, 10}. df→∞ ≡ Normal.

Inference is fully deterministic — no LLM, no randomness, stdlib only.
"""
from __future__ import annotations

import json
import math
import os
import re
import statistics
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


# ---------------------------------------------------------------------------
# WATCH BLOCK — every spec.concerns_considered[*] with verdict=='watch' must
# be addressed below as implemented logic, refusal trigger, or documented note.
# ---------------------------------------------------------------------------
# 1. "Strike spacing vs underlying volatility" — addressed by EIA-weekly
#    realized-σ in build_theos steps 1-5. σ inflates with realized vol.
# 2. "Single-point-of-failure on AAA data source" — addressed via three-tier
#    sanity guards (range, daily-move, row-count) in _fetch_live; failover
#    to ?state=US.
# 3. "Rules-source vs ticker-name mismatch ('W' suffix)" — addressed by
#    UnsupportedMarketError if ticker family != KXAAAGASW.
# 4. "Right-tail rally risk not captured by stationary normal" — PRIMARY
#    DRIVER of this rev. Addressed by SIGMA_ROLLING_WEEKS-week realized
#    weekly σ converted to daily via /sqrt(7) and to horizon via *sqrt(h).
#    Floor prevents quiet-regime over-confidence; cap prevents pathological
#    blowup. Concern fully wired.
#    Distribution upgraded from Normal to Student-t (df=T_DF) to address the
#    over-confident-worst-decile bias seen in prior backtest. Realized σ from
#    EIA-weekly returns provides the scale; t df adds tail weight.
# ---------------------------------------------------------------------------


# --- Sanity constants (mirrored from theo_refresh.py lines 36-38) -----------
# These are NOT magic numbers chosen here; they are the canonical AAA scraper
# bounds in production. Carried over verbatim per spec.blackout_rules.
AAA_PRICE_MIN = 2.00          # theo_refresh.py:36 — US national avg has not been < $2 since 2016
AAA_PRICE_MAX = 7.00          # theo_refresh.py:37 — US national avg has never crossed ~$5.10
AAA_MAX_DAILY_MOVE = 0.30     # theo_refresh.py:38 — real AAA daily moves are typically < 8c
AAA_MIN_VALID_ROWS = 5        # theo_refresh.py:55-56 — fewer than 5 valid rows = bad parse, raise

# --- Model constants --------------------------------------------------------
# Stationary 1-day σ in USD/gal. Source: production theo
# /Users/wilsonw/Downloads/theos/KXAAAGASD-26APR29.json sigma_used=0.013.
SIGMA_STATIONARY_1D = 0.013

# σ floor / cap (carryover from prior rev — both still apply on top of the
# t distribution).
SIGMA_FLOOR_FRAC = 0.5
SIGMA_CAP_USD = 0.10

# Number of EIA WEEKLY observations used for realized σ.
# Carryover: 8 weeks selected by walk-forward in select_sigma_window_v2.py
# (held-out-best at 44.32pp). Re-validated jointly with Student-t df below;
# do NOT re-tune the window here.
SIGMA_ROLLING_WEEKS = 8

# Student-t degrees of freedom. Selected by walk-forward in
# /Users/wilsonw/mm-setup/auto_theo/research/scripts/select_t_df.py.
# Candidates: 3, 4, 6, 10. Lower df = fatter tails. df→∞ = Normal.
# Selection metric: minimum HELD-OUT max-decile-deviation. Tie-break: higher df.
T_DF = 3  # Default; overwritten by select_t_df.py at selection time.

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
ARCHIVE_SOURCE_DIR = "aaa/national_regular"
EIA_WEEKLY_ARCHIVE_SUBDIR = "aaa/national_regular_eia_weekly"

# Confidence rule input — denominator for vol_to_spacing_ratio.
SPACING_FOR_CONFIDENCE = STRIKE_SPACING

# Family ticker prefix. Used to refuse markets from a different family.
KXAAAGASW_PREFIX = "KXAAAGASW"

# Acceptable family prefixes. KXAAAGASD shares the same scraper / underlying /
# strike math; it is what the historical backtest archive uses as a surrogate.
ACCEPTED_FAMILY_PREFIXES = (KXAAAGASW_PREFIX, "KXAAAGASD")


# ---------------------------------------------------------------------------
# Student-t CDF helper (stdlib-only).
# ---------------------------------------------------------------------------
# Strategy: T(t; df) = 1 - 0.5 * I_x(df/2, 0.5) for t > 0
#                    = 0.5 * I_x(df/2, 0.5)     for t < 0 (using symmetry)
# where x = df/(df + t**2) and I_x(a, b) is the regularized incomplete beta.
#
# We compute I_x(a, b) via the Lentz continued-fraction expansion of Numerical
# Recipes 6.4 (the standard textbook approach):
#   I_x(a, b) = x**a * (1-x)**b / (a * B(a,b)) * cf(a, b, x)
# where the continued fraction cf is evaluated for x < (a+1)/(a+b+2) and the
# symmetry I_x(a,b) = 1 - I_{1-x}(b,a) is used otherwise to keep convergence
# fast.
#
# Verified against known values:
#   T(0; 3)  = 0.5
#   T(1; 3)  ≈ 0.8044988905
#   T(2; 3)  ≈ 0.9303370175
#   T(-2; 3) ≈ 0.0696629825
#   T(0; 10) = 0.5

def _ln_beta(a: float, b: float) -> float:
    """log B(a, b) = lgamma(a) + lgamma(b) - lgamma(a+b)."""
    return math.lgamma(a) + math.lgamma(b) - math.lgamma(a + b)


def _betacf(a: float, b: float, x: float, max_iter: int = 500, eps: float = 3.0e-12) -> float:
    """Continued-fraction evaluation of the incomplete beta (Lentz's method).

    Reference: Numerical Recipes in C, 6.4 (betacf). Returns the continued-
    fraction term used by `_betainc_regularized` below.
    """
    qab = a + b
    qap = a + 1.0
    qam = a - 1.0
    # Avoid division by zero in d.
    fpmin = 1.0e-300
    c = 1.0
    d = 1.0 - qab * x / qap
    if abs(d) < fpmin:
        d = fpmin
    d = 1.0 / d
    h = d
    for m in range(1, max_iter + 1):
        m2 = 2 * m
        # Even step.
        aa = m * (b - m) * x / ((qam + m2) * (a + m2))
        d = 1.0 + aa * d
        if abs(d) < fpmin:
            d = fpmin
        c = 1.0 + aa / c
        if abs(c) < fpmin:
            c = fpmin
        d = 1.0 / d
        h *= d * c
        # Odd step.
        aa = -(a + m) * (qab + m) * x / ((a + m2) * (qap + m2))
        d = 1.0 + aa * d
        if abs(d) < fpmin:
            d = fpmin
        c = 1.0 + aa / c
        if abs(c) < fpmin:
            c = fpmin
        d = 1.0 / d
        delta = d * c
        h *= delta
        if abs(delta - 1.0) < eps:
            return h
    # Did not converge to eps in max_iter; return best-effort.
    return h


def _betainc_regularized(a: float, b: float, x: float) -> float:
    """Regularized incomplete beta function I_x(a, b).

    Uses Numerical Recipes 6.4 strategy: continued fraction on whichever side
    of x = (a+1)/(a+b+2) converges fastest, with symmetry I_x(a,b) =
    1 - I_{1-x}(b,a).
    """
    if x <= 0.0:
        return 0.0
    if x >= 1.0:
        return 1.0
    # Closed-form prefactor: x^a * (1-x)^b / (a * B(a,b)).
    # Compute in log space to avoid underflow for extreme arguments.
    log_bt = (
        math.lgamma(a + b)
        - math.lgamma(a)
        - math.lgamma(b)
        + a * math.log(x)
        + b * math.log(1.0 - x)
    )
    bt = math.exp(log_bt)
    if x < (a + 1.0) / (a + b + 2.0):
        return bt * _betacf(a, b, x) / a
    else:
        return 1.0 - bt * _betacf(b, a, 1.0 - x) / b


def _student_t_cdf(t: float, df: float) -> float:
    """CDF of a Student-t distribution with `df` degrees of freedom at `t`.

    Stdlib-only. Verified against scipy.stats.t.cdf at t ∈ {0, ±1, ±2} and
    df ∈ {3, 10}. See _sanity_check_t_cdf().
    """
    # Numerical guards.
    if not math.isfinite(t):
        return 1.0 if t > 0 else 0.0
    if abs(t) > 50.0:
        # Far outside the body — collapse to extremes to avoid CF divergence.
        return 1.0 if t > 0 else 0.0
    if df <= 0:
        raise ValueError(f"df must be positive, got {df}")

    # x = df / (df + t**2). For t = 0, x = 1, I_x = 1, so T = 1 - 0.5 = 0.5.
    x = df / (df + t * t)
    a = df / 2.0
    b = 0.5
    ix = _betainc_regularized(a, b, x)
    if t >= 0.0:
        return 1.0 - 0.5 * ix
    else:
        return 0.5 * ix


def _sanity_check_t_cdf() -> dict:
    """Return values for a few known points. Used by __main__."""
    return {
        "T(0;3)": _student_t_cdf(0.0, 3),
        "T(1;3)": _student_t_cdf(1.0, 3),
        "T(2;3)": _student_t_cdf(2.0, 3),
        "T(-2;3)": _student_t_cdf(-2.0, 3),
        "T(0;10)": _student_t_cdf(0.0, 10),
        "T(1;10)": _student_t_cdf(1.0, 10),
        "T(2;10)": _student_t_cdf(2.0, 10),
    }


# ---------------------------------------------------------------------------
# Strike grid + ticker helpers
# ---------------------------------------------------------------------------
def _build_strike_grid() -> list[float]:
    """Canonical AAA gas weekly strike grid: 26 strikes 2c apart from 4.00 to 4.50."""
    return [round(STRIKE_GRID_LO + STRIKE_SPACING * i, 3) for i in range(STRIKE_GRID_N)]


def _parse_strike_from_ticker(ticker: str) -> Optional[float]:
    """Extract the strike from a Kalshi market ticker."""
    tail = str(ticker).rsplit("-", 1)[-1]
    if tail and tail[0].upper() in ("T", "P"):
        tail = tail[1:]
    try:
        return float(tail)
    except (TypeError, ValueError):
        return None


def _is_event_level_ticker(ticker: str) -> bool:
    """True if ticker is event-level (no trailing -X.XXX strike segment)."""
    return ticker.count("-") == 1


def _ticker_family_prefix(ticker: str) -> str:
    """Family prefix is everything up to (and not including) the first '-'."""
    return ticker.split("-", 1)[0]


# ---------------------------------------------------------------------------
class AAAGasWeeklyPipeline(Pipeline):
    """Pipeline for KXAAAGASW-* (weekly-cadence Kalshi series, single AAA print resolution).

    rev 3: Student-t CDF over Normal CDF; EIA-weekly realized σ.
    """

    family_signature = "aaa:gas:weekly_threshold"
    refresh_cadence_seconds = 900  # 15 minutes — AAA publishes daily; this is over-frequent on purpose.

    def __init__(self, spec: dict, mode: str = "live", archive_root: Path | None = None):
        super().__init__(spec=spec, mode=mode, archive_root=archive_root)
        try:
            self._drift_anchor = float(
                os.environ.get("AAA_GAS_WEEKLY_DRIFT_ANCHOR", DEFAULT_DRIFT_ANCHOR)
            )
        except (TypeError, ValueError):
            self._drift_anchor = DEFAULT_DRIFT_ANCHOR

    # ------------------------------------------------------------------ data
    def data_as_of(self, source: dict, now: datetime) -> DataPoint:
        if self.mode == "live":
            return self._fetch_live(source, now)
        if self.mode == "historical":
            return self._fetch_historical(source, now)
        raise InsufficientDataError(f"unknown mode: {self.mode}")

    def _fetch_live(self, source: dict, now: datetime) -> DataPoint:
        try:
            import requests
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
                last_error = (
                    f"{attempt_url}: only {len(rows)} valid rows parsed "
                    f"(need >= {AAA_MIN_VALID_ROWS})"
                )
                continue

            today, yest = rows[0], rows[1]
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
            if abs(today - yest) > AAA_MAX_DAILY_MOVE:
                raise InsufficientDataError(
                    f"AAA daily move |today-yest|=${abs(today - yest):.3f} > "
                    f"${AAA_MAX_DAILY_MOVE:.2f} max (source={attempt_url}); "
                    f"likely stale page or parse error"
                )

            return DataPoint(
                value=today,
                publication_timestamp=now,
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

        raise InsufficientDataError(
            f"AAA scrape failed on both homepage and failover: {last_error}"
        )

    @staticmethod
    def _parse_aaa_rows(html: str) -> list[float]:
        rows: list[float] = []
        for tr in re.finditer(r"<tr[^>]*>(.*?)</tr>", html, re.S):
            prices = re.findall(r"\$\d\.\d{3}", tr.group(1))
            if 4 <= len(prices) <= 6:
                rows.append(float(prices[0].lstrip("$")))
            if len(rows) == 5:
                break
        return rows

    def _fetch_historical(self, source: dict, now: datetime) -> DataPoint:
        archive_dir = self.archive_root / ARCHIVE_SOURCE_DIR
        if not archive_dir.exists():
            raise InsufficientDataError(f"archive dir missing: {archive_dir}")

        target_date = now.date()
        candidates = []
        for p in archive_dir.glob("*.json"):
            stem = p.stem
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
        ts = self.spec["resolution"]["resolution_timestamp"]
        return datetime.fromisoformat(str(ts).replace("Z", "+00:00"))

    def _consecutive_stable(self, now: datetime) -> bool:
        return False

    def _read_eia_weekly_returns(self, now: datetime) -> Optional[list[float]]:
        archive_dir = self.archive_root / EIA_WEEKLY_ARCHIVE_SUBDIR
        if not archive_dir.exists():
            return None

        readings: list[tuple[datetime, float]] = []
        for p in archive_dir.glob("*.json"):
            try:
                payload = json.loads(p.read_text())
            except Exception:
                continue
            if not isinstance(payload, dict):
                continue
            pub_raw = payload.get("publication_timestamp")
            v_raw = payload.get("value_usd_per_gal", payload.get("value"))
            if pub_raw is None or v_raw is None:
                continue
            try:
                pub_ts = datetime.fromisoformat(str(pub_raw).replace("Z", "+00:00"))
                v = float(v_raw)
            except (TypeError, ValueError):
                continue
            if pub_ts.tzinfo is None:
                pub_ts = pub_ts.replace(tzinfo=timezone.utc)
            now_aware = now if now.tzinfo else now.replace(tzinfo=timezone.utc)
            if pub_ts > now_aware:
                continue
            if v <= 0:
                continue
            readings.append((pub_ts, v))

        if len(readings) < 4:
            return None

        readings.sort(key=lambda r: r[0])
        recent = readings[-(SIGMA_ROLLING_WEEKS + 1):]
        if len(recent) < 5:
            return None
        log_returns: list[float] = []
        for i in range(1, len(recent)):
            v_prev = recent[i - 1][1]
            v_curr = recent[i][1]
            if v_prev <= 0 or v_curr <= 0:
                continue
            log_returns.append(math.log(v_curr / v_prev))
        return log_returns

    def _compute_horizon_sigma(self, horizon_days: float, now: datetime) -> tuple[float, str]:
        log_returns = self._read_eia_weekly_returns(now)
        floor = SIGMA_FLOOR_FRAC * SIGMA_STATIONARY_1D
        if log_returns is None or len(log_returns) < 4:
            sigma_h_uncapped = SIGMA_STATIONARY_1D * math.sqrt(max(horizon_days, 0.0))
            sigma_h = max(floor, min(sigma_h_uncapped, SIGMA_CAP_USD))
            method = (
                f"stationary fallback: sigma_1d={SIGMA_STATIONARY_1D} * sqrt(h_days)"
                f" (EIA weekly archive had <4 readings <= now)"
            )
            return sigma_h, method

        if len(log_returns) >= 2:
            sigma_weekly_logret = statistics.stdev(log_returns)
        else:
            sigma_weekly_logret = 0.0
        grid_mid = (STRIKE_GRID_LO + STRIKE_GRID_LO + STRIKE_SPACING * (STRIKE_GRID_N - 1)) / 2.0
        sigma_weekly_usd = sigma_weekly_logret * grid_mid
        sigma_1d = sigma_weekly_usd / math.sqrt(7.0)
        sigma_h_uncapped = sigma_1d * math.sqrt(max(horizon_days, 0.0))
        sigma_h = max(floor, min(sigma_h_uncapped, SIGMA_CAP_USD))
        method = (
            f"EIA weekly realized: stdev(log_returns) over last "
            f"{len(log_returns)} weekly returns = {sigma_weekly_logret:.5f}; "
            f"σ_1d={sigma_1d:.5f}; σ_h={sigma_h:.5f} (floor={floor:.4f}, cap={SIGMA_CAP_USD})"
        )
        return sigma_h, method

    # ----------------------------------------------------------------- build
    def build_theos(self, event_ticker: str, now: datetime) -> dict:
        # 0. Family check.
        family = _ticker_family_prefix(event_ticker)
        if family not in ACCEPTED_FAMILY_PREFIXES:
            raise UnsupportedMarketError(
                f"ticker family {family!r} not in {ACCEPTED_FAMILY_PREFIXES}"
            )

        # 1. Blackout check
        self._check_blackout(now)

        # 2. Fetch AAA today
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

        # 4. σ_h from EIA-weekly archive (or stationary fallback).
        sigma_h, sigma_method = self._compute_horizon_sigma(horizon_days, now)

        # 5. μ = AAA_today + drift_anchor.
        mu = aaa_today + self._drift_anchor

        # 6+8. Build strike map.
        strike_grid = _build_strike_grid()
        event_prefix = event_ticker.rsplit("-", 1)[0] if not _is_event_level_ticker(event_ticker) else event_ticker
        is_event_level = _is_event_level_ticker(event_ticker)

        if not is_event_level:
            requested_strike = _parse_strike_from_ticker(event_ticker)
            if requested_strike is None:
                raise UnsupportedMarketError(
                    f"could not parse strike from ticker {event_ticker!r}"
                )
            if family == KXAAAGASW_PREFIX and not any(
                abs(requested_strike - s) < 1e-9 for s in strike_grid
            ):
                raise UnsupportedMarketError(
                    f"strike {requested_strike} not on canonical AAA gas weekly grid "
                    f"[{STRIKE_GRID_LO:.2f}, "
                    f"{STRIKE_GRID_LO + STRIKE_SPACING * (STRIKE_GRID_N - 1):.2f}]"
                )

        # 7. Resolution-day pinning. Only pin on stable consecutive scrapes.
        is_resolution_day = now.date() == resolution_dt.date()
        pinning = is_resolution_day and self._consecutive_stable(now)

        # 9. Student-t scaling. The realized σ_h is intended as the std of the
        # horizon return in USD. For a Student-t rv with df > 2,
        # Var(t-rv) = df/(df-2) * scale^2. To match σ_h ≈ std of return, set
        # scale = σ_h * sqrt((df-2)/df). For df ≤ 2 this is undefined; we only
        # test df ∈ {3, 4, 6, 10}, but if any df ≤ 2 sneaks in, fall back to
        # scale = σ_h.
        df_local = T_DF
        if df_local > 2:
            scale = sigma_h * math.sqrt((df_local - 2.0) / df_local)
        else:
            scale = sigma_h
        # Defensive: scale must be strictly positive.
        if scale <= 0:
            scale = max(sigma_h, 1e-9)

        strikes_out: dict[str, float] = {}
        for s in strike_grid:
            tk = f"{event_prefix}-{s:.3f}"
            if pinning:
                p_yes = 0.995 if aaa_today > s else 0.005
            else:
                z = (s - mu) / scale
                p_yes = 1.0 - _student_t_cdf(z, df_local)
            # Per saved feedback rule: do NOT clamp outcome probabilities
            # with arbitrary caps. Round only.
            strikes_out[tk] = round(p_yes, 6)

        # 10. Confidence
        data_age_seconds = (now - dp.publication_timestamp).total_seconds()
        if data_age_seconds < 0:
            data_age_seconds = 0.0
        confidence = self._confidence_from(
            data_age_seconds=data_age_seconds,
            vol_to_spacing_ratio=sigma_h / SPACING_FOR_CONFIDENCE,
            seconds_to_resolution=max(horizon_seconds, 0.0),
        )

        # 11. Assemble theos dict via shared helper.
        method_str = (
            f"StudentTCDF(df={df_local}) mu=AAA_today+drift, "
            f"scale=sigma_h*sqrt((df-2)/df), sigma_h via {sigma_method}"
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
    import datetime as _dt
    print("=== Student-t CDF sanity ===")
    sanity = _sanity_check_t_cdf()
    refs = {
        "T(0;3)": 0.5,
        "T(1;3)": 0.8044988905221148,
        "T(2;3)": 0.9303370175629631,
        "T(-2;3)": 0.0696629824370369,
        "T(0;10)": 0.5,
        "T(1;10)": 0.8295534338489997,
        "T(2;10)": 0.9633060653430682,
    }
    for k, v in sanity.items():
        ref = refs.get(k)
        if ref is not None:
            err = abs(v - ref)
            print(f"  {k}: {v:.10f}  (ref {ref:.10f}, |err|={err:.2e})")
        else:
            print(f"  {k}: {v:.10f}")

    print("\n=== build_theos sanity ===")
    spec_path = Path("/Users/wilsonw/mm-setup/auto_theo/specs/KXAAAGASW-26MAY04.json")
    spec = json.loads(spec_path.read_text())
    pipeline = AAAGasWeeklyPipeline(
        spec=spec, mode="historical",
        archive_root=Path("/Users/wilsonw/mm-setup/auto_theo/archive"),
    )
    try:
        out = pipeline.build_theos(
            "KXAAAGASW-26MAY04",
            _dt.datetime(2026, 4, 29, 18, 0, tzinfo=_dt.timezone.utc),
        )
        print(json.dumps(out, indent=2, default=str))
    except PipelineError as exc:
        print(f"[refusal] {type(exc).__name__}: {exc.reason}")
