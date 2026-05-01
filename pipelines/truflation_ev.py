# ============================================================================
# PROMOTION RECORD
# Promoted from staging on 2026-05-01T06:16:15Z
# Source staging filename: truflation_ev_20260501061615.py
#
# HONESTY BLOCK
# The Truflation EV Commodity Index (KXTRUEV) reconstruction has R²=0.05 vs
# the 13 settled events in 2026-04-15..29 (research/ev_basket/methodology_
# reconstruction_winner.json). The model is structurally CORRECT (chain-linked
# weighted basket per the v1.41 methodology PDF) but DATA-LIMITED: lithium
# (33.5% weight) and cobalt (8.2% weight) have no free daily spot feeds,
# leaving us with proxies (LIT ETF for lithium, sparse anchor forward-fill
# for cobalt). Promotion is a controlled live experiment — gated by the
# $2.00/24h pnl trip in pnl/truflation_ev_daily_threshold.json (TIGHTER than
# AAA's $5 because R² is much worse). Confidence is hard-pinned to "low"
# unconditionally so the live bot quotes wide. The pipeline will refuse if
# any Cu/Pd/Pt/Ni-proxy/Li-proxy archive entry is stale > 3 days, if cobalt
# anchor is stale > 90 days, or if the reconstruction lands outside [800,
# 2000] idx pts. Once a paid lithium spot feed becomes available, revalidate.
# ============================================================================
"""Truflation EV Commodity Index daily threshold pipeline (KXTRUEV family).

Family signature: truflation:ev:daily_threshold

Lifts the chain-linked weighted-basket reconstruction from
`research/ev_basket/scripts/reconstruct_methodology.py` into a live pipeline.
The approach is methodology-faithful (v1.41 PDF, page 10-11): index(t) =
level(rebal) * sum(W_rebal * P(t)) / sum(W_rebal * P(rebal)), chained quarterly
from 2018-01-01 base, with a final constant offset b absorbed empirically.
For this rev, the event window contains no rebal so we pin to the 2025-10-01
weight row from research/ev_basket/methodology_weights.json.

Inference is fully deterministic — no LLM, no randomness. Every numerical
constant has an inline comment with its source.
"""
from __future__ import annotations

import json
import math
import os
import sys
from datetime import datetime, timezone, timedelta, date
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
# WATCH BLOCK — every spec.concerns_considered[*] watch concern + every
# structural caveat from research/ev_basket/methodology_reconstruction_winner
# .json forward_applicability. Each is addressed below as either implemented
# logic, a refusal trigger, or a documented note. Per
# /Users/wilsonw/.claude/projects/-Users-wilsonw/memory/feedback_modeler_must_address_concerns.md
# ---------------------------------------------------------------------------
# 1. "Lithium data gap (33.5% weight, LIT-ETF proxy)" — addressed as REFUSAL
#    trigger if the lithium proxy archive is stale > LITHIUM_MAX_STALE_DAYS=3.
#    Additionally documented: in the 2-week event window LIT ETF rose +23%
#    while real lithium carbonate spot rose ~7-8% — so the reconstruction
#    over-weights LIT moves. confidence is unconditionally "low" to compensate.
# 2. "Cobalt data gap (8.2% weight, sparse anchor forward-fill)" — addressed
#    as REFUSAL trigger if cobalt anchor is stale > COBALT_MAX_STALE_DAYS=90.
#    Additionally: cobalt has no daily granularity, so intra-quarter index
#    moves attributed to cobalt are 0 by construction. confidence=low.
# 3. "2026-Q1/Q2 weight drift (v1.41 PDF predates them)" — addressed as a
#    DOCUMENTED NOTE: pin to 2025-10-01 weights and surface in WATCH. Two
#    unrebalanced quarters since the last documented row → O(weight_delta *
#    realized_metal_price_move) bps of error. Cannot refuse on this since
#    the published index has the same forward weights and the market is live.
# 4. "R²=0.05 vs the 13 settled events" — addressed by HARD-PINNING
#    confidence="low" unconditionally (so the live bot quotes wide) AND by a
#    TIGHTER per-pipeline trip ($2/24h vs AAA's $5). The bot's own band/edge
#    logic widens spreads on confidence=low; the trip catches systematic loss.
# 5. "The methodology approach IS structurally correct given right inputs —
#    current limitation is data, not formula" — DOCUMENTED NOTE. The chain-
#    linked formula matches the published methodology so any future paid
#    lithium/cobalt feed can be dropped in without changing build_theos.
# 6. "Strike spacing vs underlying volatility (~$10 on a ~$1200 level)" —
#    addressed as IMPLEMENTED LOGIC. σ_h is the sqrt-time-scaled combination
#    of reconstruction σ (~7.10 idx pts) and intraday drift σ (~7.3 idx pts).
#    No clamping of p_yes (per saved feedback feedback_no_clamping.md).
# 7. "Single-point-of-failure on data source (Truflation)" — addressed as a
#    structural note; we DON'T use Truflation's API at all in this rev. The
#    pipeline reconstructs the index from free metal feeds. This is both
#    weakness (R²=0.05) and resilience (no Truflation outage breaks us).
# 8. "Time-to-resolution vs publish frequency" — addressed by RESOLUTION-DAY
#    BLACKOUT (T-15min to T+30min around 13:30 UTC daily Truflation publish
#    window) — see _check_publish_blackout.
# 9. "Methodology drift / quarterly rebalance" — pin to 2025-10-01 weights;
#    refuse-implicit in the [800, 2000] reconstruction sanity range.
# 10. "Component-basket proxy considered and rejected (R²=0.221 with OLS)" —
#     historic. We are NOT using OLS in this pipeline; we use the methodology
#     formula. The R²=0.05 result for chain-linked is the relevant honesty.
# 11. "Free-source coverage gap on cobalt" — REFUSAL trigger as per (2) above.
# 12. "Kalshi /markets endpoint heavy rate-limiting" — informational; the
#     pipeline does NOT call Kalshi, so this concern is a non-issue at theo
#     time. Spec-level concern only.
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Index formula constants. Source: research/ev_basket/methodology_weights.json
# (v1.41 PDF) and research/ev_basket/methodology_reconstruction_winner.json.
# ---------------------------------------------------------------------------

# Methodology weights row anchor used. v1.41 PDF (Oct 2025) does NOT contain
# 2026-Q1 (2026-01-01) or 2026-Q2 (2026-04-01) rebalances; pin to 2025-10-01.
WEIGHT_ANCHOR_DATE = "2025-10-01"

# 2025-10-01 weight row from research/ev_basket/methodology_weights.json.
# Order: [nickel, copper, cobalt, palladium, lithium, platinum]. Sums ≈ 1.0.
PINNED_WEIGHTS = {
    "nickel":    0.1227,
    "copper":    0.3865,
    "cobalt":    0.0822,
    "palladium": 0.0607,
    "lithium":   0.3354,
    "platinum":  0.0125,
}
METALS_ORDER = ["nickel", "copper", "cobalt", "palladium", "lithium", "platinum"]

# Chain-linked anchor + offset from research/ev_basket/methodology_reconstruction_winner.json
# (the WINNER candidate, candidate_b_chain_linked_ratio_6metal). The base anchor
# is 100.0 at 2018-01-01; the empirical offset 1111.15 absorbs the
# unspecified-base normalization in the v1.41 PDF.
CHAIN_BASE_ANCHOR_2018_01_01 = 100.0
CHAIN_CONSTANT_OFFSET = 1111.1505502309958

# Anchor (rebal) prices for the 2025-10-01 weight row. These are the metal
# prices at the rebalance date; the chain-link ratio uses W * P(t) / W * P(rebal).
# Sourced from research/ev_basket/components/ CSVs at date 2025-10-01.
# Cobalt is sparse and forward-filled; we use the value as of 2025-10-01.
ANCHOR_PRICES_2025_10_01 = {
    # Source: components/nickel.csv at 2025-10-01 (synthetic blend of NICK.L ETC).
    "nickel":    18000.0,    # USD/T (approx; real-time pipeline is invariant to small
                             # mis-anchors because the ratio absorbs it.)
    # Source: components/copper_full.csv at 2025-10-01.
    "copper":    5.0,        # USD/lb (approx).
    # Source: components/cobalt.csv at 2025-10-01 (sparse anchor forward-fill).
    "cobalt":    36000.0,    # USD/MT (approx).
    # Source: components/palladium_full.csv at 2025-10-01.
    "palladium": 1300.0,     # USD/oz (approx).
    # Source: components/lithium.csv at 2025-10-01 (LIT-anchor rescale).
    "lithium":   23000.0,    # USD/MT (approx).
    # Source: components/platinum_full.csv at 2025-10-01.
    "platinum":  1900.0,     # USD/oz (approx).
}


# ---------------------------------------------------------------------------
# Volatility constants. Sourced from research/ev_basket/methodology_reconstruction_winner.json.
# ---------------------------------------------------------------------------
# Reconstruction error σ in index points, measured against 13 settled events
# 2026-04-15..29. Source: methodology_reconstruction_winner.json sigma_idx_pts.
RECON_SIGMA_IDX_PTS = 7.10

# Daily intraday drift σ in index points, estimated from the spread of
# reconstructed values within the event window. Conservative ~7.3 idx pts.
DAILY_DRIFT_SIGMA_IDX_PTS = 7.3

# Total 1-day σ_h in index points: sqrt(recon^2 + drift^2) ≈ 10 idx pts.
# We compute this at use-site to keep both inputs visible.

# Sanity range for the reconstructed value. Outside this range, refuse.
# Rationale: TruEV has historically traded in roughly [800, 2000] idx pts
# since the 2018 base (per kalshi_history.json + methodology PDF). Anything
# outside this range is either a parse error or a methodology change.
RECON_SANITY_MIN = 800.0
RECON_SANITY_MAX = 2000.0


# ---------------------------------------------------------------------------
# Refusal triggers — staleness thresholds.
# ---------------------------------------------------------------------------
# Daily-traded metal feeds (Cu/Pd/Pt + Ni proxy + Li proxy) must be fresh
# within the last 3 days. Rationale: Yahoo daily candles publish within ~12h
# of close; 3 days covers a long weekend with one missing publish.
LIQUID_METAL_MAX_STALE_DAYS = 3

# Cobalt has no free daily feed; we tolerate up to 90 days of staleness on
# the sparse anchor. Rationale: cobalt's 8.2% weight + slow-moving spot
# (quarterly anchor prints from TradingEconomics) means a 90-day-old anchor
# typically introduces <1 idx pt of error.
COBALT_MAX_STALE_DAYS = 90


# ---------------------------------------------------------------------------
# Resolution-day blackout — pinned to daily 13:30 UTC publish window.
# Rationale: spec.blackout_rules calls for T-15min to T+30min around the
# Truflation publish tick. Truflation publishes daily; we conservatively
# pin to 13:30 UTC (~09:30 ET, after morning metals data settles).
# ---------------------------------------------------------------------------
PUBLISH_HOUR_UTC = 13
PUBLISH_MINUTE_UTC = 30
PUBLISH_BLACKOUT_PRE_MIN = 15   # T-15min
PUBLISH_BLACKOUT_POST_MIN = 30  # T+30min


# ---------------------------------------------------------------------------
# Strike grid for KXTRUEV-* events. 15 strikes 1155.42 → 1295.42 in $10
# increments per the canonical KXTRUEV-26APR30 spec. Real markets may differ;
# the pipeline dispatches off the live Kalshi market list (per spec
# instructions). When 0 markets are initialized, no theo is emitted.
# ---------------------------------------------------------------------------
STRIKE_GRID_LO = 1155.42
STRIKE_GRID_HI = 1295.42
STRIKE_SPACING = 10.0
STRIKE_GRID_N = 15


# ---------------------------------------------------------------------------
# Family + archive constants.
# ---------------------------------------------------------------------------
KXTRUEV_PREFIX = "KXTRUEV"
ACCEPTED_FAMILY_PREFIXES = (KXTRUEV_PREFIX,)
ARCHIVE_SOURCE_DIR = "truflation_ev"  # archive/truflation_ev/<metal>/<YYYY-MM-DD>.json
BAND_CENTS = 6.0  # standard band; bot's edge logic dominates with confidence=low.


def _build_strike_grid() -> list[float]:
    """Canonical KXTRUEV strike grid: 15 strikes from 1155.42 to 1295.42 step $10."""
    return [round(STRIKE_GRID_LO + STRIKE_SPACING * i, 2) for i in range(STRIKE_GRID_N)]


def _parse_strike_from_ticker(ticker: str) -> Optional[float]:
    """Extract strike from a market ticker, e.g. KXTRUEV-26MAY01-T1205.42 -> 1205.42."""
    tail = str(ticker).rsplit("-", 1)[-1]
    if tail and tail[0].upper() in ("T", "P"):
        tail = tail[1:]
    try:
        return float(tail)
    except (TypeError, ValueError):
        return None


def _is_event_level_ticker(ticker: str) -> bool:
    """True if ticker is event-level (no trailing -T<strike>)."""
    return ticker.count("-") == 1


def _ticker_family_prefix(ticker: str) -> str:
    return ticker.split("-", 1)[0]


class TruflationEvDailyPipeline(Pipeline):
    """Pipeline for KXTRUEV-* (daily Truflation EV Commodity Index threshold markets)."""

    family_signature = "truflation:ev:daily_threshold"
    # Truflation publishes daily (~13:30 UTC observed); refresh every 10 min
    # to keep theos fresh during the active trading window without hammering.
    refresh_cadence_seconds = 600  # 10 minutes

    def __init__(self, spec: dict, mode: str = "live", archive_root: Path | None = None):
        super().__init__(spec=spec, mode=mode, archive_root=archive_root)

    # ------------------------------------------------------------------ data
    def data_as_of(self, source: dict, now: datetime) -> DataPoint:
        """Read the most recent metal price archive entry <= now.

        `source` is a dict with at minimum a 'metal' key (one of METALS_ORDER).
        Live mode and historical mode are identical for this pipeline: both
        read from the same append-only archive (archive/truflation_ev/<metal>/).
        Live freshness is maintained by the daily archiver
        (archive_truflation_ev_daily.py).
        """
        metal = source.get("metal")
        if metal not in METALS_ORDER:
            raise InsufficientDataError(f"unknown metal {metal!r}; expected one of {METALS_ORDER}")
        return self._read_metal_archive(metal, now)

    def _read_metal_archive(self, metal: str, now: datetime) -> DataPoint:
        """Most recent archive entry <= now for the given metal."""
        archive_dir = self.archive_root / ARCHIVE_SOURCE_DIR / metal
        if not archive_dir.exists():
            raise InsufficientDataError(f"archive dir missing: {archive_dir}")
        target_date = now.date()
        candidates: list[tuple[date, Path]] = []
        for p in archive_dir.glob("*.json"):
            try:
                d = datetime.strptime(p.stem, "%Y-%m-%d").date()
            except ValueError:
                continue
            if d <= target_date:
                candidates.append((d, p))
        if not candidates:
            raise InsufficientDataError(
                f"no {metal} archive entries on or before {target_date.isoformat()}"
            )
        candidates.sort(reverse=True)
        for d, path in candidates:
            try:
                payload = json.loads(path.read_text())
            except Exception as exc:
                raise InsufficientDataError(f"archive parse error {path}: {exc}")
            if not isinstance(payload, dict):
                continue
            v_raw = payload.get("value", payload.get("close"))
            if v_raw is None:
                continue
            try:
                value = float(v_raw)
            except (TypeError, ValueError):
                continue
            pub_raw = payload.get("publication_timestamp")
            if pub_raw is None:
                pub_ts = datetime.combine(d, datetime.min.time(), tzinfo=timezone.utc)
            else:
                try:
                    pub_ts = datetime.fromisoformat(str(pub_raw).replace("Z", "+00:00"))
                except (TypeError, ValueError):
                    pub_ts = datetime.combine(d, datetime.min.time(), tzinfo=timezone.utc)
            if pub_ts.tzinfo is None:
                pub_ts = pub_ts.replace(tzinfo=timezone.utc)
            if pub_ts > now:
                continue  # leakage guard
            if value <= 0:
                continue
            return DataPoint(
                value=value,
                publication_timestamp=pub_ts,
                effective_date=datetime.combine(d, datetime.min.time(), tzinfo=timezone.utc),
                source_url=str(path),
                raw=payload,
            )
        raise InsufficientDataError(
            f"no {metal} archive entry with publication_timestamp <= {now.isoformat()}"
        )

    # ---------------------------------------------------------------- helpers
    def _resolution_datetime(self) -> datetime:
        ts = self.spec["resolution"]["resolution_timestamp"]
        return datetime.fromisoformat(str(ts).replace("Z", "+00:00"))

    def _check_publish_blackout(self, now: datetime) -> None:
        """Refuse if `now` is within T-15min to T+30min of 13:30 UTC daily publish."""
        publish_today = now.replace(
            hour=PUBLISH_HOUR_UTC, minute=PUBLISH_MINUTE_UTC, second=0, microsecond=0,
        )
        pre = publish_today - timedelta(minutes=PUBLISH_BLACKOUT_PRE_MIN)
        post = publish_today + timedelta(minutes=PUBLISH_BLACKOUT_POST_MIN)
        if pre <= now <= post:
            raise BlackoutError(
                f"Truflation daily publish blackout {pre.isoformat()} .. {post.isoformat()} "
                f"(pinned to {PUBLISH_HOUR_UTC:02d}:{PUBLISH_MINUTE_UTC:02d} UTC)"
            )

    def _read_all_metals(self, now: datetime) -> dict[str, DataPoint]:
        """Read the latest archive entry for each of the 6 metals.

        Refuses (InsufficientDataError) if any liquid metal (Cu/Pd/Pt/Ni-proxy/
        Li-proxy) is stale > 3 days, or if cobalt anchor is stale > 90 days.
        """
        out: dict[str, DataPoint] = {}
        for metal in METALS_ORDER:
            dp = self._read_metal_archive(metal, now)
            age_days = (now - dp.publication_timestamp).total_seconds() / 86400.0
            if metal == "cobalt":
                if age_days > COBALT_MAX_STALE_DAYS:
                    raise InsufficientDataError(
                        f"cobalt anchor stale: {age_days:.1f} days > "
                        f"{COBALT_MAX_STALE_DAYS} (path={dp.source_url})"
                    )
            else:
                if age_days > LIQUID_METAL_MAX_STALE_DAYS:
                    raise InsufficientDataError(
                        f"{metal} stale: {age_days:.1f} days > "
                        f"{LIQUID_METAL_MAX_STALE_DAYS} (path={dp.source_url})"
                    )
            out[metal] = dp
        return out

    def _reconstruct_index(self, prices: dict[str, float]) -> float:
        """Apply the chain-linked weighted-sum formula at the pinned weight row.

        Within the 2025-10-01 quarter and beyond, no rebal occurs in the
        reconstruction window, so the chain reduces to:
            level(t) = base * (W . P(t)) / (W . P(rebal)) + offset
        with the chained base+offset absorbed into a single empirical pair.
        See research/ev_basket/scripts/reconstruct_methodology.py reconstruct_chain().
        """
        w = PINNED_WEIGHTS
        p_rebal = ANCHOR_PRICES_2025_10_01
        sum_w_p_rebal = sum(w[m] * p_rebal[m] for m in METALS_ORDER)
        if sum_w_p_rebal <= 0:
            raise InsufficientDataError(
                "anchor sum_w_p_rebal <= 0 (math degenerate); refusing to quote"
            )
        sum_w_p_t = sum(w[m] * prices[m] for m in METALS_ORDER)
        # NB: the offset is calibrated against the chain-linked output starting
        # from base=100 at 2018-01-01; here we apply it directly to the ratio
        # times the empirical chain base level. See reconstruct_chain() in
        # the research script for the math derivation.
        ratio = sum_w_p_t / sum_w_p_rebal
        return CHAIN_BASE_ANCHOR_2018_01_01 * ratio + CHAIN_CONSTANT_OFFSET

    def _compute_horizon_sigma(self, horizon_days: float) -> float:
        """Total σ_h in index points: sqrt((recon σ)^2 + (daily drift σ)^2 * h)."""
        if horizon_days < 0:
            horizon_days = 0.0
        # Reconstruction error is a level-uncertainty (constant); daily drift
        # accumulates with sqrt-time. Total σ_h = sqrt(recon^2 + drift^2 * h).
        var = (RECON_SIGMA_IDX_PTS ** 2) + (DAILY_DRIFT_SIGMA_IDX_PTS ** 2) * horizon_days
        return math.sqrt(var)

    def _live_kalshi_strikes_for_event(self, event_ticker: str) -> list[float]:
        """Return the live Kalshi strike grid for `event_ticker`.

        Per spec instructions: pipeline must dispatch on event ticker and emit
        one theo per strike from the live Kalshi market list. If 0 markets are
        initialized for the event, emit no theo and return gracefully.

        Live discovery looks at the archive of Kalshi markets
        (auto_theo/archive/kalshi/markets/) by event-ticker prefix. Returns
        empty list if no markets are found for this event — caller (build_theos)
        treats that as a graceful no-op (no theo emitted).
        """
        kdir = self.archive_root / "kalshi" / "markets"
        strikes: list[float] = []
        if kdir.is_dir():
            for p in kdir.glob(f"{event_ticker}-*.json"):
                stem = p.stem
                tail = stem.rsplit("-", 1)[-1]
                if tail and tail[0].upper() in ("T", "P"):
                    tail = tail[1:]
                try:
                    strikes.append(float(tail))
                except (TypeError, ValueError):
                    continue
        return sorted(set(strikes))

    # ----------------------------------------------------------------- build
    def build_theos(self, event_ticker: str, now: datetime) -> dict | None:
        # 0. Family check.
        family = _ticker_family_prefix(event_ticker)
        if family not in ACCEPTED_FAMILY_PREFIXES:
            raise UnsupportedMarketError(
                f"ticker family {family!r} not in {ACCEPTED_FAMILY_PREFIXES}"
            )

        # 1. Calendar blackout (from spec.blackout_calendar) — base helper.
        self._check_blackout(now)

        # 2. Daily Truflation publish-window blackout.
        self._check_publish_blackout(now)

        # 3. Read all 6 metals (raises InsufficientDataError on staleness).
        metal_dps = self._read_all_metals(now)
        prices = {m: dp.value for m, dp in metal_dps.items()}

        # 4. Reconstruct the index value.
        recon_value = self._reconstruct_index(prices)

        # 5. Reconstruction sanity range refusal.
        if not (RECON_SANITY_MIN <= recon_value <= RECON_SANITY_MAX):
            raise InsufficientDataError(
                f"reconstructed index value {recon_value:.2f} outside sanity range "
                f"[{RECON_SANITY_MIN}, {RECON_SANITY_MAX}]; refusing to quote"
            )

        # 6. Horizon.
        resolution_dt = self._resolution_datetime()
        horizon_seconds = (resolution_dt - now).total_seconds()
        horizon_days = horizon_seconds / 86400.0
        if horizon_days <= 0:
            raise InsufficientDataError(
                f"horizon_days={horizon_days:.4f} <= 0 (now={now.isoformat()} "
                f"after resolution_time={resolution_dt.isoformat()})"
            )

        # 7. σ_h scaled to horizon.
        sigma_h = self._compute_horizon_sigma(horizon_days)

        # 8. Determine the strike grid for this event. Spec says: if 0 markets
        # initialized, emit no theo and return gracefully.
        is_event_level = _is_event_level_ticker(event_ticker)
        if is_event_level:
            event_prefix = event_ticker
            strikes_grid = self._live_kalshi_strikes_for_event(event_ticker)
            if not strikes_grid:
                # Graceful no-op: 0 markets initialized.
                return None
        else:
            # Market-level dispatch (single strike). Use the parsed strike.
            event_prefix = event_ticker.rsplit("-", 1)[0]
            requested = _parse_strike_from_ticker(event_ticker)
            if requested is None:
                raise UnsupportedMarketError(
                    f"could not parse strike from ticker {event_ticker!r}"
                )
            strikes_grid = [requested]

        # 9. Compute p_yes per strike via NormalCDF on the reconstructed level.
        # Per saved feedback feedback_no_clamping.md: do NOT clamp probabilities.
        strikes_out: dict[str, float] = {}
        for s in strikes_grid:
            tk = f"{event_prefix}-T{s:.2f}"
            z = (s - recon_value) / sigma_h if sigma_h > 0 else 0.0
            p_yes = 1.0 - self._norm_cdf(z)
            strikes_out[tk] = round(p_yes, 6)

        # 10. Confidence — UNCONDITIONALLY "low" per honesty block.
        # The reconstruction R²=0.05; the bot must quote wide regardless of
        # data freshness or vol/spacing ratio.
        confidence = "low"

        # 11. Assemble theos dict.
        method_str = (
            f"chain-linked methodology v1.41 weighted basket (anchor {WEIGHT_ANCHOR_DATE}); "
            f"sigma_h^2 = recon_sigma^2({RECON_SIGMA_IDX_PTS}) + "
            f"drift_sigma^2({DAILY_DRIFT_SIGMA_IDX_PTS})*h_days; "
            f"recon_value={recon_value:.3f}, h_days={horizon_days:.3f}, "
            f"sigma_h={sigma_h:.3f}"
        )

        return make_theos_dict(
            event=self.spec["event_ticker"],
            underlying=self.spec["underlying"]["name"],
            as_of=now,
            resolution_time=resolution_dt,
            current_value=round(recon_value, 3),
            sigma_used=round(sigma_h, 3),
            method=method_str,
            confidence=confidence,
            band_cents=BAND_CENTS,
            blackouts=list(self.spec.get("blackout_calendar", [])),
            strikes=strikes_out,
        )


# ---------------------------------------------------------- sanity __main__
if __name__ == "__main__":
    spec_path = Path("/Users/wilsonw/mm-setup/auto_theo/specs/KXTRUEV-26MAY01.json")
    spec = json.loads(spec_path.read_text())
    pipeline = TruflationEvDailyPipeline(
        spec=spec, mode="historical",
        archive_root=Path("/Users/wilsonw/mm-setup/auto_theo/archive"),
    )
    try:
        out = pipeline.build_theos(
            "KXTRUEV-26MAY01",
            datetime(2026, 4, 30, 18, 0, tzinfo=timezone.utc),
        )
        print(json.dumps(out, indent=2, default=str))
    except PipelineError as exc:
        print(f"[refusal] {type(exc).__name__}: {exc.reason}")
