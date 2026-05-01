"""Base class and refusal exceptions for auto-theo pipelines.

Every pipeline inherits from `Pipeline` and implements `build_theos`. The
contract is strict: pipelines compute deterministic functions of (spec, now,
archive) and either return a theos dict or raise a refusal exception.

Refusal is a first-class outcome. The live bot respects "no theo" by not
quoting; an honest refusal is always better than a fabricated theo.
"""
from __future__ import annotations

import json
import math
import os
import tempfile
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


class PipelineError(Exception):
    """Base for all pipeline refusals. Always carry a reason string."""
    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


class BlackoutError(PipelineError):
    """The current `now` is inside a blackout window. The bot should pull quotes."""


class InsufficientDataError(PipelineError):
    """Required data is missing, stale beyond tolerance, or malformed."""


class UnsupportedMarketError(PipelineError):
    """The market doesn't fit this pipeline's family. Check family_signature."""


@dataclass(frozen=True)
class DataPoint:
    """A single observation from a data source.

    publication_timestamp is sacred: it is the wall-clock time the value
    became available to the public. Everything in the pipeline is gated by
    this — no read can return a DataPoint with publication_timestamp > now.
    """
    value: float
    publication_timestamp: datetime
    effective_date: datetime  # what the value is "for" (may differ from when it published)
    source_url: str
    raw: dict


class Pipeline(ABC):
    family_signature: str = "abstract"
    refresh_cadence_seconds: int = 3600  # how often the orchestrator runs build_theos

    def __init__(self, spec: dict, mode: str = "live", archive_root: Path | None = None):
        if mode not in ("live", "historical"):
            raise ValueError(f"unknown mode: {mode}")
        self.spec = spec
        self.mode = mode
        self.archive_root = archive_root or Path("auto_theo/archive")

    @abstractmethod
    def build_theos(self, event_ticker: str, now: datetime) -> dict:
        """Produce a theos JSON dict for `event_ticker` as of `now`.

        Must satisfy:
        - Same (spec, now, archive) → same output. No randomness, no clock reads.
        - Every data fetch is parameterized by `now`. No leakage of >`now` data.
        - Returns the schema documented in the live bot README.
        - Raises BlackoutError, InsufficientDataError, or UnsupportedMarketError
          rather than returning a bogus theo.
        """

    @abstractmethod
    def data_as_of(self, source: dict, now: datetime) -> DataPoint | list[DataPoint]:
        """Fetch data from a spec source as of `now`.

        Live mode: hits the live API but filters by publication_timestamp <= now.
        Historical mode: reads from `archive_root/<source_id>/` and filters.
        """

    # -- Helpers shared across pipelines ----------------------------------

    def _check_blackout(self, now: datetime) -> None:
        """Raise BlackoutError if `now` is inside any blackout window in the spec."""
        for window in self.spec.get("blackout_calendar", []):
            start = datetime.fromisoformat(window["start"])
            end = datetime.fromisoformat(window["end"])
            if start <= now <= end:
                raise BlackoutError(f"{window['reason']} (until {end.isoformat()})")

    def _confidence_from(self, *, data_age_seconds: float, vol_to_spacing_ratio: float,
                         seconds_to_resolution: float) -> str:
        """Standard confidence rule. Pipelines may override but most shouldn't.

        - "high" iff data is fresh (<24h), vol fits comfortably (<1.0x spacing),
          and resolution isn't imminent.
        - "low" iff any of: data is stale (>72h), vol blows out spacing (>2x),
          or resolution is in <1h.
        - "medium" otherwise.
        """
        if (data_age_seconds > 72 * 3600 or
                vol_to_spacing_ratio > 2.0 or
                seconds_to_resolution < 3600):
            return "low"
        if (data_age_seconds < 24 * 3600 and
                vol_to_spacing_ratio < 1.0 and
                seconds_to_resolution > 6 * 3600):
            return "high"
        return "medium"

    def _atomic_write_json(self, path: Path, data: dict) -> None:
        """Write JSON atomically. The live bot reads on mtime change; a partial
        write would parse as corrupt and the bot would silently use stale theos
        or worse. tempfile + os.replace is atomic on POSIX."""
        path.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(mode="w", dir=path.parent, delete=False) as f:
            json.dump(data, f, indent=2, default=str)
            tmp = f.name
        os.replace(tmp, path)

    @staticmethod
    def _norm_cdf(z: float) -> float:
        return 0.5 * (1.0 + math.erf(z / math.sqrt(2.0)))


def make_theos_dict(*, event: str, underlying: str, as_of: datetime, resolution_time: datetime,
                    current_value: float, sigma_used: float, method: str, confidence: str,
                    band_cents: float, blackouts: list, strikes: dict[str, float]) -> dict:
    """Construct a theos dict matching the live bot's expected schema.

    Centralized so every pipeline produces the same shape. If the bot's schema
    changes, change it here.
    """
    return {
        "event": event,
        "underlying": underlying,
        "as_of": as_of.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
        "resolution_time": resolution_time.isoformat(),
        "current_value": current_value,
        "sigma_used": sigma_used,
        "method": method,
        "confidence": confidence,
        "band_cents": band_cents,
        "blackouts": blackouts,
        "strikes": strikes,
    }
