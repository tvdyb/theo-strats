"""Auto-theo orchestrator.

Loads every production pipeline from ``auto_theo/pipelines/``, matches each
spec to the right pipeline class via ``family_signature``, and on a periodic
schedule calls ``build_theos`` for each event and writes the result to
``/Users/wilsonw/Downloads/theos/<event_ticker>.json``.

Hard rules (per CLAUDE.md):
* The LLM is never in the inference path. Theos come from deterministic
  Python pipelines on a schedule.
* Refusals (``BlackoutError``, ``InsufficientDataError``,
  ``UnsupportedMarketError``) are first-class outcomes; the bot respects
  "no theo" by not quoting, so we never write a fabricated theo on failure.
* Per-pipeline PnL trip is honored: any pipeline whose pnl JSON has
  ``tripped: true`` is skipped this cycle. Untripping is manual.
* Globally tripped events listed in
  ``/Users/wilsonw/Downloads/kalshi_tripped_events.json`` are skipped.
* Atomic writes (tempfile + os.replace) for every theos file -- a partial
  write would parse as corrupt and the live bot would silently use stale
  theos.

CLI
---
python3 -m auto_theo.orchestrator [--dry-run] [--once]
                                  [--theos-dir PATH] [--archive-root PATH]
                                  [--specs-dir PATH] [--pnl-dir PATH]
                                  [--tripped-events-file PATH]
                                  [--log-level INFO]

Stdlib only.
"""
from __future__ import annotations

import argparse
import importlib
import inspect
import json
import logging
import logging.handlers
import os
import pkgutil
import signal
import sys
import tempfile
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from auto_theo.pipelines._base import (
    BlackoutError,
    InsufficientDataError,
    Pipeline,
    UnsupportedMarketError,
)

# ---------------------------------------------------------------------------
# Defaults — keep in sync with pnl_monitor.py and the live bot's read paths.
# ---------------------------------------------------------------------------
DEFAULT_THEOS_DIR = Path("/Users/wilsonw/Downloads/theos")
DEFAULT_ARCHIVE_ROOT = Path("/Users/wilsonw/mm-setup/auto_theo/archive")
DEFAULT_SPECS_DIR = Path("/Users/wilsonw/mm-setup/auto_theo/specs")
DEFAULT_PNL_DIR = Path("/Users/wilsonw/mm-setup/auto_theo/pnl")
DEFAULT_TRIPPED_FILE = Path("/Users/wilsonw/Downloads/kalshi_tripped_events.json")
DEFAULT_LOG_FILE = Path("/Users/wilsonw/mm-setup/auto_theo/orchestrator.log")
DEFAULT_REFRESH_FALLBACK_SECONDS = 300  # used iff registry is empty -- keeps loop alive

# Consecutive-failure threshold before WARN-logging per event. Per spec.
FAILURE_WARN_THRESHOLD = 5

logger = logging.getLogger("auto_theo.orchestrator")


# ---------------------------------------------------------------------------
# Helpers shared with pnl_monitor: pnl-state filename derivation.
# ---------------------------------------------------------------------------

def _safe_pnl_filename(family_signature: str) -> str:
    """Map a family_signature to the pnl JSON filename used by pnl_monitor.

    Discovered by inspecting the bootstrap file
    ``auto_theo/pnl/aaa_gas_weekly_threshold.json`` whose ``pipeline_name``
    field is ``aaa:gas:weekly_threshold``: the mapping is "colons replaced
    by underscores". We additionally normalize any path-unsafe characters
    so future signatures don't escape the pnl directory.
    """
    safe = family_signature.replace(":", "_")
    # Defensive: strip path separators if a future signature ever contains them.
    safe = safe.replace("/", "_").replace("\\", "_")
    return safe + ".json"


def _atomic_write_json(path: Path, data: Any) -> None:
    """Atomically write JSON. Mirrors the helper in pnl_monitor.py."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=".theo_", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(data, f, indent=2, default=str)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text())


# ---------------------------------------------------------------------------
# Pipeline discovery.
# ---------------------------------------------------------------------------

def discover_pipeline_classes() -> dict[str, type[Pipeline]]:
    """Walk ``auto_theo.pipelines`` and return a registry
    ``family_signature -> Pipeline subclass``.

    Skips:
    * the ABC ``Pipeline`` itself
    * private modules whose name starts with ``_`` (e.g. ``_base``)
    * classes whose ``family_signature`` is the abstract default
      ``"abstract"``

    Conflicting family_signature => ERROR-exit.
    """
    registry: dict[str, type[Pipeline]] = {}
    pkg = importlib.import_module("auto_theo.pipelines")
    pkg_path = list(pkg.__path__)
    for modinfo in pkgutil.iter_modules(pkg_path):
        name = modinfo.name
        if name.startswith("_"):
            continue
        full_name = f"auto_theo.pipelines.{name}"
        try:
            mod = importlib.import_module(full_name)
        except Exception as exc:
            logger.error("Failed to import %s: %s", full_name, exc)
            logger.debug("import traceback:\n%s", traceback.format_exc())
            continue
        for _, obj in inspect.getmembers(mod, inspect.isclass):
            if obj is Pipeline:
                continue
            if not issubclass(obj, Pipeline):
                continue
            # Only register classes actually defined in this module (avoid
            # picking up Pipeline subclasses re-imported from elsewhere).
            if obj.__module__ != full_name:
                continue
            sig = getattr(obj, "family_signature", "abstract")
            if not sig or sig == "abstract":
                logger.warning(
                    "Pipeline class %s.%s has no family_signature; skipping",
                    full_name, obj.__name__,
                )
                continue
            if sig in registry:
                msg = (f"Duplicate family_signature '{sig}': "
                       f"{registry[sig].__module__}.{registry[sig].__name__} "
                       f"and {full_name}.{obj.__name__}")
                logger.error(msg)
                raise SystemExit(2)
            registry[sig] = obj
            logger.info("Registered pipeline %s.%s -> family_signature=%s",
                        full_name, obj.__name__, sig)
    return registry


# ---------------------------------------------------------------------------
# Event registry: event_ticker -> (pipeline_class, spec).
# ---------------------------------------------------------------------------

def build_event_registry(specs_dir: Path,
                         class_registry: dict[str, type[Pipeline]],
                         ) -> dict[str, tuple[type[Pipeline], dict]]:
    """Match each spec to a pipeline class.

    WARN-and-skip if no class matches. ERROR-exit if two specs claim the
    same event_ticker.
    """
    out: dict[str, tuple[type[Pipeline], dict]] = {}
    if not specs_dir.exists():
        logger.warning("Specs dir does not exist: %s", specs_dir)
        return out
    for path in sorted(specs_dir.glob("*.json")):
        try:
            spec = _read_json(path)
        except Exception as exc:
            logger.warning("Spec %s unreadable: %s", path, exc)
            continue
        ev = (spec.get("event_ticker") or "").strip()
        fam = (spec.get("family_signature") or "").strip()
        if not ev:
            logger.warning("Spec %s has no event_ticker; skipping", path)
            continue
        if not fam:
            logger.warning("Spec %s has no family_signature; skipping", path)
            continue
        cls = class_registry.get(fam)
        if cls is None:
            logger.warning(
                "No pipeline class registered for family_signature '%s' "
                "(spec=%s, event=%s); skipping",
                fam, path.name, ev,
            )
            continue
        if ev in out:
            other_path = "<previous spec>"
            msg = (f"Duplicate event_ticker '{ev}' across specs: "
                   f"{other_path} and {path.name}")
            logger.error(msg)
            raise SystemExit(3)
        out[ev] = (cls, spec)
        logger.info("Registered event %s -> %s (%s)",
                    ev, cls.__name__, path.name)
    return out


# ---------------------------------------------------------------------------
# Tripped-events cache. Read once per loop iteration.
# ---------------------------------------------------------------------------

def _load_tripped_events(path: Path) -> set[str]:
    """Load the set of globally tripped event_tickers.

    Cached for the duration of one loop pass by the caller (passed in as a
    fresh dict each iteration). Format mirrors kalshi_rewards_app.py:
    ``{event_ticker: {"ts":..., "reason":..., ...}}``.
    """
    try:
        d = json.loads(path.read_text())
    except FileNotFoundError:
        return set()
    except Exception as exc:
        logger.warning("Tripped-events file %s unreadable, treating as empty: %s",
                       path, exc)
        return set()
    if not isinstance(d, dict):
        return set()
    return {str(k).strip().upper() for k in d.keys() if k}


# ---------------------------------------------------------------------------
# Pnl-state read for the per-pipeline trip check.
# ---------------------------------------------------------------------------

def _is_pipeline_tripped(pnl_dir: Path, family_signature: str) -> bool:
    """Return True iff the pnl-state JSON for this pipeline says tripped.

    Missing / malformed file => not tripped (log DEBUG). The pnl_monitor
    bootstraps these files; absence just means the monitor hasn't run yet.
    """
    pnl_path = pnl_dir / _safe_pnl_filename(family_signature)
    if not pnl_path.exists():
        logger.debug("No pnl state for %s at %s; assuming not tripped",
                     family_signature, pnl_path)
        return False
    try:
        state = _read_json(pnl_path)
    except Exception as exc:
        logger.warning("Pnl state %s unreadable, assuming not tripped: %s",
                       pnl_path, exc)
        return False
    return bool(state.get("tripped"))


# ---------------------------------------------------------------------------
# Per-event refresh.
# ---------------------------------------------------------------------------

def refresh_event(event_ticker: str,
                  cls: type[Pipeline],
                  spec: dict,
                  *,
                  archive_root: Path,
                  theos_dir: Path,
                  pnl_dir: Path,
                  tripped_events: set[str],
                  failure_counts: dict[str, int],
                  dry_run: bool,
                  now: datetime) -> str:
    """Refresh a single event. Returns a short status string for logging.

    Status values: ``"skip:tripped_pipeline"``, ``"skip:tripped_event"``,
    ``"ok"``, ``"blackout"``, ``"insufficient"``, ``"unsupported"``,
    ``"error"``, ``"dry_run"``.
    """
    fam = getattr(cls, "family_signature", "abstract")

    if _is_pipeline_tripped(pnl_dir, fam):
        logger.info("Skipping %s: pipeline %s is tripped on PnL", event_ticker, fam)
        return "skip:tripped_pipeline"

    if event_ticker.strip().upper() in tripped_events:
        logger.info("Skipping %s: event listed in tripped_events file", event_ticker)
        return "skip:tripped_event"

    try:
        pipeline = cls(spec, mode="live", archive_root=archive_root)
    except Exception as exc:
        logger.error("Failed to instantiate %s for %s: %s",
                     cls.__name__, event_ticker, exc)
        logger.debug("instantiate traceback:\n%s", traceback.format_exc())
        return "error"

    try:
        theo = pipeline.build_theos(event_ticker, now)
    except BlackoutError as exc:
        # Blackouts are expected; do not write, but reset failure counter so
        # an event coming out of blackout starts fresh.
        logger.info("Blackout for %s: %s", event_ticker, exc.reason)
        failure_counts[event_ticker] = 0
        return "blackout"
    except InsufficientDataError as exc:
        n = failure_counts.get(event_ticker, 0) + 1
        failure_counts[event_ticker] = n
        msg = (f"InsufficientData for {event_ticker}: {exc.reason} "
               f"(consecutive failures: {n})")
        if n > FAILURE_WARN_THRESHOLD:
            logger.warning(msg)
        else:
            logger.info(msg)
        return "insufficient"
    except UnsupportedMarketError as exc:
        n = failure_counts.get(event_ticker, 0) + 1
        failure_counts[event_ticker] = n
        msg = (f"UnsupportedMarket for {event_ticker}: {exc.reason} "
               f"(consecutive failures: {n})")
        if n > FAILURE_WARN_THRESHOLD:
            logger.warning(msg)
        else:
            logger.info(msg)
        return "unsupported"
    except Exception as exc:
        # Catch-all: do not crash. Full traceback at ERROR.
        logger.error("Pipeline %s build_theos(%s) crashed: %s",
                     cls.__name__, event_ticker, exc)
        logger.error("traceback:\n%s", traceback.format_exc())
        return "error"

    # Success.
    failure_counts[event_ticker] = 0

    # Graceful no-op: a pipeline may return None to signal "no theo to emit
    # right now" (e.g. KXTRUEV markets not yet initialized by Kalshi). Skip
    # the write entirely and treat as a benign cycle.
    if theo is None:
        logger.info("Pipeline %s returned None for %s; no theo emitted "
                    "(graceful no-op)", cls.__name__, event_ticker)
        return "no_theo"

    out_path = theos_dir / f"{event_ticker}.json"
    if dry_run:
        # Per spec: skip writes, log payload at DEBUG.
        try:
            payload_preview = json.dumps(theo, indent=2, default=str)
        except Exception:
            payload_preview = repr(theo)
        logger.debug("[dry-run] would write %s:\n%s", out_path, payload_preview)
        logger.info("[dry-run] built theo for %s (skipped write)", event_ticker)
        return "dry_run"
    try:
        _atomic_write_json(out_path, theo)
    except Exception as exc:
        logger.error("Failed to write theos for %s -> %s: %s",
                     event_ticker, out_path, exc)
        logger.error("traceback:\n%s", traceback.format_exc())
        return "error"
    logger.info("Wrote theo for %s -> %s", event_ticker, out_path)
    return "ok"


# ---------------------------------------------------------------------------
# Scheduling.
# ---------------------------------------------------------------------------

def _loop_sleep_seconds(class_registry: dict[str, type[Pipeline]]) -> int:
    """Return the loop tick interval.

    ``min(class.refresh_cadence_seconds for class in registered) or 300``
    Per spec: if the registry is empty we still want a sane default so the
    loop doesn't busy-wait or sleep forever.
    """
    cadences = [int(getattr(c, "refresh_cadence_seconds", 0) or 0)
                for c in class_registry.values()]
    cadences = [c for c in cadences if c > 0]
    if not cadences:
        return DEFAULT_REFRESH_FALLBACK_SECONDS
    return min(cadences)


class _ShutdownFlag:
    def __init__(self):
        self.stop = False

    def set(self, *_):
        self.stop = True


def run_pass(*,
             event_registry: dict[str, tuple[type[Pipeline], dict]],
             archive_root: Path,
             theos_dir: Path,
             pnl_dir: Path,
             tripped_events_file: Path,
             last_refresh_at: dict[str, float],
             failure_counts: dict[str, int],
             force_all: bool,
             dry_run: bool) -> dict[str, int]:
    """One scheduling pass: iterate the event registry, refresh those whose
    cadence has elapsed (or all events if force_all)."""
    # Load tripped-events file ONCE per pass (cached for the loop duration).
    tripped_events = _load_tripped_events(tripped_events_file)

    counters: dict[str, int] = {}
    now_unix = time.time()
    now_dt = datetime.now(timezone.utc)
    for event_ticker, (cls, spec) in event_registry.items():
        cadence = int(getattr(cls, "refresh_cadence_seconds", 0) or 0)
        if cadence <= 0:
            cadence = DEFAULT_REFRESH_FALLBACK_SECONDS
        last = last_refresh_at.get(event_ticker, 0.0)
        due = force_all or (now_unix - last) >= cadence
        if not due:
            continue
        status = refresh_event(
            event_ticker, cls, spec,
            archive_root=archive_root,
            theos_dir=theos_dir,
            pnl_dir=pnl_dir,
            tripped_events=tripped_events,
            failure_counts=failure_counts,
            dry_run=dry_run,
            now=now_dt,
        )
        last_refresh_at[event_ticker] = now_unix
        counters[status] = counters.get(status, 0) + 1
    return counters


def run_forever(*,
                event_registry: dict[str, tuple[type[Pipeline], dict]],
                class_registry: dict[str, type[Pipeline]],
                archive_root: Path,
                theos_dir: Path,
                pnl_dir: Path,
                tripped_events_file: Path,
                dry_run: bool) -> None:
    flag = _ShutdownFlag()
    signal.signal(signal.SIGINT, flag.set)
    signal.signal(signal.SIGTERM, flag.set)

    last_refresh_at: dict[str, float] = {}
    failure_counts: dict[str, int] = {}
    tick = _loop_sleep_seconds(class_registry)
    logger.info("orchestrator starting: events=%d classes=%d tick=%ds dry_run=%s",
                len(event_registry), len(class_registry), tick, dry_run)

    while not flag.stop:
        cycle_start = time.monotonic()
        try:
            counters = run_pass(
                event_registry=event_registry,
                archive_root=archive_root,
                theos_dir=theos_dir,
                pnl_dir=pnl_dir,
                tripped_events_file=tripped_events_file,
                last_refresh_at=last_refresh_at,
                failure_counts=failure_counts,
                force_all=False,
                dry_run=dry_run,
            )
            if counters:
                logger.info("pass complete: %s",
                            ", ".join(f"{k}={v}" for k, v in sorted(counters.items())))
        except Exception as exc:
            # The pass itself should never crash the loop. refresh_event
            # already swallows per-event errors; this is belt-and-suspenders.
            logger.error("Top-level pass errored: %s", exc)
            logger.error("traceback:\n%s", traceback.format_exc())

        # Sleep in 1s slices so SIGINT/SIGTERM are responsive. We wait for
        # any in-flight refresh to finish (which it already has by here)
        # before exiting -- the ABC's contract is synchronous build_theos.
        elapsed = time.monotonic() - cycle_start
        remaining = max(0.0, tick - elapsed)
        end = time.monotonic() + remaining
        while not flag.stop and time.monotonic() < end:
            time.sleep(min(1.0, end - time.monotonic()))

    logger.info("orchestrator shutting down cleanly")
    logging.shutdown()


# ---------------------------------------------------------------------------
# Logging setup.
# ---------------------------------------------------------------------------

def _configure_logging(log_level: str, log_file: Path) -> None:
    """Configure the root logger with both a stderr handler and a rotating
    file handler at ``auto_theo/orchestrator.log`` (10MB x 3 backups)."""
    log_file.parent.mkdir(parents=True, exist_ok=True)
    level = getattr(logging, log_level.upper(), logging.INFO)
    fmt = logging.Formatter(
        "%(asctime)s %(levelname)s %(name)s %(message)s")

    root = logging.getLogger()
    root.setLevel(level)

    # Drop any pre-existing handlers from prior imports / repeated test runs.
    for h in list(root.handlers):
        root.removeHandler(h)

    sh = logging.StreamHandler(stream=sys.stderr)
    sh.setLevel(level)
    sh.setFormatter(fmt)
    root.addHandler(sh)

    fh = logging.handlers.RotatingFileHandler(
        log_file, maxBytes=10 * 1024 * 1024, backupCount=3, encoding="utf-8")
    fh.setLevel(level)
    fh.setFormatter(fmt)
    root.addHandler(fh)


# ---------------------------------------------------------------------------
# CLI entrypoint.
# ---------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--dry-run", action="store_true",
                   help="Skip writes to theos/; log would-write payload at DEBUG.")
    p.add_argument("--once", action="store_true",
                   help="Run a single pass over all events and exit 0.")
    p.add_argument("--theos-dir", type=Path, default=DEFAULT_THEOS_DIR)
    p.add_argument("--archive-root", type=Path, default=DEFAULT_ARCHIVE_ROOT)
    p.add_argument("--specs-dir", type=Path, default=DEFAULT_SPECS_DIR)
    p.add_argument("--pnl-dir", type=Path, default=DEFAULT_PNL_DIR)
    p.add_argument("--tripped-events-file", type=Path, default=DEFAULT_TRIPPED_FILE)
    p.add_argument("--log-level", default="INFO",
                   choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    p.add_argument("--log-file", type=Path, default=DEFAULT_LOG_FILE,
                   help=argparse.SUPPRESS)  # primarily for tests
    args = p.parse_args(argv)

    _configure_logging(args.log_level, args.log_file)

    logger.info(
        "orchestrator boot: theos_dir=%s archive_root=%s specs_dir=%s "
        "pnl_dir=%s tripped_events_file=%s once=%s dry_run=%s",
        args.theos_dir, args.archive_root, args.specs_dir, args.pnl_dir,
        args.tripped_events_file, args.once, args.dry_run,
    )

    class_registry = discover_pipeline_classes()
    logger.info("Discovered %d production pipeline class(es)", len(class_registry))

    event_registry = build_event_registry(args.specs_dir, class_registry)
    logger.info("Mapped %d event(s) to pipelines", len(event_registry))

    if args.once:
        last_refresh_at: dict[str, float] = {}
        failure_counts: dict[str, int] = {}
        counters = run_pass(
            event_registry=event_registry,
            archive_root=args.archive_root,
            theos_dir=args.theos_dir,
            pnl_dir=args.pnl_dir,
            tripped_events_file=args.tripped_events_file,
            last_refresh_at=last_refresh_at,
            failure_counts=failure_counts,
            force_all=True,
            dry_run=args.dry_run,
        )
        logger.info("--once complete: %s",
                    ", ".join(f"{k}={v}" for k, v in sorted(counters.items()))
                    or "(no events refreshed)")
        logging.shutdown()
        return 0

    run_forever(
        event_registry=event_registry,
        class_registry=class_registry,
        archive_root=args.archive_root,
        theos_dir=args.theos_dir,
        pnl_dir=args.pnl_dir,
        tripped_events_file=args.tripped_events_file,
        dry_run=args.dry_run,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
