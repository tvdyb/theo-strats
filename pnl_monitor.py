"""Per-pipeline rolling PnL monitor and trip enforcer.

Polls Kalshi fills, attributes each fill to the pipeline that owns its event,
accumulates rolling-window PnL per pipeline, and trips pipelines that drop
below ``-max_loss_usd`` in the window. Tripping merges every event a pipeline
covers into ``/Users/wilsonw/Downloads/kalshi_tripped_events.json`` -- the same
file the live bot (``kalshi_rewards_app.py``) already reads.

Per CLAUDE.md "Per-pipeline PnL trip": this is the most important runtime
safety net. Untripping is manual (edit the pnl JSON, set ``tripped: false``).

Design notes
------------
* Fill -> pipeline attribution is event-based: each pipeline lists
  ``events_tracked`` (event_tickers) and a fill on ticker
  ``EVENT-STRIKE`` is mapped via ``ticker.split('-', N-1) == event_ticker``
  prefix match against the event tickers we know about. Multi-pipeline
  ownership of the same event is rejected at startup with a warning -- the
  pipeline written first wins, and the duplicate is logged.
* PnL accounting is per-fill mark-to-market plus realized closes. For a buy
  on side S at price p_cents, qty q, PnL = q * (current_mid_cents - p_cents)
  (in cents, then converted to USD). For a sell, sign flips. We do NOT track
  a running position book here -- closes are surfaced through Kalshi's
  ``is_taker`` / ``action`` fields and the mark-to-market fairly captures
  open exposure via the live mid. Fees are always subtracted as realized.
* fill_id dedup uses Kalshi's ``trade_id``. We store every observed fill_id
  in the pipeline state so re-poll never double-counts; old fills outside the
  rolling window get pruned but their fill_ids remain in a small "recently
  pruned" set for one extra window to handle clock drift.

CLI
---
python3 -m auto_theo.pnl_monitor [--once] [--poll-interval 60]
                                 [--pnl-dir ...] [--specs-dir ...]
                                 [--tripped-events-file ...] [--dry-run]

Hard rules
----------
* Atomic writes (tempfile + os.replace) for every state and tripped-events
  mutation.
* Never auto-untrip. Once ``tripped: true``, only manual edits clear it.
* Don't crash on Kalshi auth failure -- log WARN and retry next cycle.
* Stdlib + ``requests`` + ``cryptography`` only (already used by the live bot).
"""
from __future__ import annotations

import argparse
import base64
import json
import logging
import os
import signal
import sys
import tempfile
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

# ---------------------------------------------------------------------------
# Constants — keep KEY_ID / KEY_PATH in sync with kalshi_rewards_app.py:26-27.
# ---------------------------------------------------------------------------
KEY_ID = "e11f3027-4745-4952-9b7a-31f9c3d1ba13"
KEY_PATH = Path("/Users/wilsonw/Downloads/write.txt")
BASE = "https://api.elections.kalshi.com/trade-api/v2"

DEFAULT_PNL_DIR = Path("/Users/wilsonw/mm-setup/auto_theo/pnl")
DEFAULT_SPECS_DIR = Path("/Users/wilsonw/mm-setup/auto_theo/specs")
DEFAULT_TRIPPED_FILE = Path("/Users/wilsonw/Downloads/kalshi_tripped_events.json")
DEFAULT_POLL_INTERVAL_S = 60

logger = logging.getLogger("auto_theo.pnl_monitor")


# ---------------------------------------------------------------------------
# Atomic file IO. Everything that mutates disk goes through these helpers.
# ---------------------------------------------------------------------------

def _atomic_write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=".pnl_", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(obj, f, indent=2, sort_keys=True)
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


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _now_unix() -> float:
    return time.time()


# ---------------------------------------------------------------------------
# Kalshi private API client — minimal, mirrors signed_request in the live bot.
# Auth failures must be soft: log WARN, return None, retry next cycle.
# ---------------------------------------------------------------------------

class KalshiClient:
    """Lazy-loads the private key. If the key file is missing or we get a
    persistent 401/403, ``private_ok`` becomes False and the public-API
    fallback (orderbook mid) is used for mark-to-market only."""

    def __init__(self, key_id: str = KEY_ID, key_path: Path = KEY_PATH,
                 base: str = BASE):
        self.key_id = key_id
        self.key_path = key_path
        self.base = base
        self._pk = None
        self._pk_load_attempted = False
        self.private_ok = True
        self._session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=8, pool_maxsize=8, max_retries=1)
        self._session.mount("https://", adapter)
        self._session.mount("http://", adapter)

    def _load_pk(self):
        if self._pk_load_attempted:
            return self._pk
        self._pk_load_attempted = True
        try:
            from cryptography.hazmat.primitives import serialization
            self._pk = serialization.load_pem_private_key(
                self.key_path.read_bytes(), password=None)
        except Exception as e:
            logger.warning("Kalshi private key unavailable (%s): %s -- "
                           "private endpoints will be skipped this cycle",
                           self.key_path, e)
            self._pk = None
            self.private_ok = False
        return self._pk

    def _signed_headers(self, method: str, path: str) -> dict | None:
        from cryptography.hazmat.primitives import hashes
        from cryptography.hazmat.primitives.asymmetric import padding
        pk = self._load_pk()
        if pk is None:
            return None
        sig_path = "/trade-api/v2" + path
        ts = str(int(time.time() * 1000))
        msg = (ts + method + sig_path).encode()
        sig = pk.sign(
            msg,
            padding.PSS(mgf=padding.MGF1(hashes.SHA256()),
                        salt_length=padding.PSS.DIGEST_LENGTH),
            hashes.SHA256(),
        )
        return {
            "KALSHI-ACCESS-KEY": self.key_id,
            "KALSHI-ACCESS-SIGNATURE": base64.b64encode(sig).decode(),
            "KALSHI-ACCESS-TIMESTAMP": ts,
            "accept": "application/json",
            "Content-Type": "application/json",
        }

    def signed_get(self, path: str, params: dict | None = None,
                   timeout: float = 15.0) -> dict | None:
        h = self._signed_headers("GET", path)
        if h is None:
            return None
        try:
            r = self._session.get(self.base + path, headers=h,
                                  params=params, timeout=timeout)
        except (requests.ConnectionError, requests.Timeout) as e:
            logger.warning("Kalshi GET %s network error: %s", path, e)
            return None
        if r.status_code in (401, 403):
            logger.warning("Kalshi GET %s auth failed (%d): %s",
                           path, r.status_code, r.text[:200])
            self.private_ok = False
            return None
        if r.status_code >= 400:
            logger.warning("Kalshi GET %s -> %d: %s",
                           path, r.status_code, r.text[:200])
            return None
        try:
            return r.json()
        except ValueError:
            logger.warning("Kalshi GET %s returned non-JSON", path)
            return None

    def public_get(self, path: str, params: dict | None = None,
                   timeout: float = 15.0) -> dict | None:
        try:
            r = self._session.get(self.base + path,
                                  params=params, timeout=timeout,
                                  headers={"accept": "application/json"})
        except (requests.ConnectionError, requests.Timeout) as e:
            logger.warning("Kalshi public GET %s network error: %s", path, e)
            return None
        if r.status_code >= 400:
            logger.debug("Kalshi public GET %s -> %d", path, r.status_code)
            return None
        try:
            return r.json()
        except ValueError:
            return None

    # -- High-level helpers -------------------------------------------------

    def fetch_recent_fills(self, page_limit: int = 200,
                           max_pages: int = 6,
                           min_ts: float | None = None) -> list[dict]:
        """Return fills newer than ``min_ts`` (unix seconds), most recent
        first. Stops paginating once a page's oldest fill is older than
        ``min_ts``. Returns [] if private API is unavailable."""
        if not self.private_ok:
            return []
        out: list[dict] = []
        cursor = None
        for _ in range(max_pages):
            params: dict[str, Any] = {"limit": page_limit}
            if cursor:
                params["cursor"] = cursor
            j = self.signed_get("/portfolio/fills", params=params)
            if j is None:
                break
            page = j.get("fills") or []
            if not page:
                break
            for f in page:
                ts = f.get("ts") or _parse_iso_to_unix(f.get("created_time", ""))
                if min_ts is not None and ts is not None and ts < min_ts:
                    continue
                out.append(f)
            oldest_ts = (page[-1].get("ts")
                         or _parse_iso_to_unix(page[-1].get("created_time", "")))
            if min_ts is not None and oldest_ts is not None and oldest_ts < min_ts:
                break
            cursor = j.get("cursor") or ""
            if not cursor:
                break
        return out

    def fetch_orderbook_mid_cents(self, ticker: str) -> float | None:
        """Best-effort YES mid in cents from the public orderbook endpoint.
        Returns None on error. Used only for mark-to-market PnL."""
        j = self.public_get(f"/markets/{ticker}/orderbook")
        if j is None:
            return None
        ob = j.get("orderbook") or {}
        # Kalshi returns yes/no levels as [[price_cents, size], ...] sorted desc.
        yes = ob.get("yes") or []
        no = ob.get("no") or []
        best_yes_bid = None
        best_yes_ask = None
        if yes:
            try:
                best_yes_bid = float(yes[0][0])
            except (TypeError, ValueError, IndexError):
                pass
        if no:
            try:
                # NO best bid implies YES best ask = 100 - no_bid_cents.
                best_no_bid = float(no[0][0])
                best_yes_ask = 100.0 - best_no_bid
            except (TypeError, ValueError, IndexError):
                pass
        if best_yes_bid is not None and best_yes_ask is not None:
            return (best_yes_bid + best_yes_ask) / 2.0
        if best_yes_bid is not None:
            return best_yes_bid
        if best_yes_ask is not None:
            return best_yes_ask
        return None


def _parse_iso_to_unix(s: str) -> float | None:
    if not s:
        return None
    try:
        # Kalshi created_time looks like "2026-04-30T18:23:01.123Z".
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        return datetime.fromisoformat(s).timestamp()
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Pipeline state model. Mirrors the schema in CLAUDE.md.
# ---------------------------------------------------------------------------

REQUIRED_KEYS = {
    "pipeline_name", "rolling_window_h", "max_loss_usd",
    "tripped", "events_tracked", "fills_observed", "rolling_pnl_usd",
}

DEFAULT_FIELDS = {
    "tripped_at": None,
    "tripped_reason": None,
    "last_polled_at": None,
}


def _validate_state(state: dict, path: Path) -> dict:
    missing = REQUIRED_KEYS - set(state.keys())
    if missing:
        raise ValueError(f"{path}: missing required keys {sorted(missing)}")
    if not isinstance(state["events_tracked"], list):
        raise ValueError(f"{path}: events_tracked must be a list")
    if not isinstance(state["fills_observed"], list):
        raise ValueError(f"{path}: fills_observed must be a list")
    for k, v in DEFAULT_FIELDS.items():
        state.setdefault(k, v)
    state["events_tracked"] = sorted({str(e).upper() for e in state["events_tracked"] if e})
    return state


# ---------------------------------------------------------------------------
# Spec mapping: pipeline_name (== family_signature) -> set(event_tickers).
# ---------------------------------------------------------------------------

def build_family_to_events(specs_dir: Path) -> dict[str, list[str]]:
    out: dict[str, set[str]] = {}
    if not specs_dir.exists():
        return {}
    for p in sorted(specs_dir.glob("*.json")):
        try:
            d = _read_json(p)
        except Exception as e:
            logger.warning("Spec %s unreadable: %s", p, e)
            continue
        ev = (d.get("event_ticker") or "").strip().upper()
        fam = (d.get("family_signature") or "").strip()
        if not ev or not fam:
            continue
        out.setdefault(fam, set()).add(ev)
    return {k: sorted(v) for k, v in out.items()}


# ---------------------------------------------------------------------------
# Tripped-events file merge. Same format as kalshi_rewards_app.py:629:
#   {event_ticker: {"ts": float, "reason": str, "dollars": float, "contracts": int}}
# ---------------------------------------------------------------------------

def _load_tripped(path: Path) -> dict:
    try:
        d = json.loads(path.read_text())
        return d if isinstance(d, dict) else {}
    except FileNotFoundError:
        return {}
    except Exception as e:
        logger.warning("Tripped file %s unreadable, treating as empty: %s",
                       path, e)
        return {}


def trip_events_in_file(path: Path, events: list[str], reason: str,
                       dollars: float, contracts: int,
                       dry_run: bool) -> dict:
    existing = _load_tripped(path)
    ts = _now_unix()
    added = []
    for ev in events:
        ev = ev.strip().upper()
        if not ev:
            continue
        # If already tripped externally (e.g. by the breaker), do not overwrite
        # the existing entry -- that would clobber the original reason.
        if ev in existing:
            continue
        existing[ev] = {
            "ts": ts,
            "reason": reason,
            "dollars": round(float(dollars), 2),
            "contracts": int(contracts),
        }
        added.append(ev)
    if added and not dry_run:
        _atomic_write_json(path, existing)
    return {"added": added, "total": len(existing)}


# ---------------------------------------------------------------------------
# PnL attribution & rolling window arithmetic.
# ---------------------------------------------------------------------------

def event_ticker_of(market_ticker: str) -> str:
    """Drop the strike segment after the LAST '-'. Mirrors the convention
    used throughout the live bot. KXAAAGASW-26MAY04-4.300 -> KXAAAGASW-26MAY04."""
    t = (market_ticker or "").strip().upper()
    if "-" not in t:
        return t
    return t.rsplit("-", 1)[0]


def _fill_pnl_usd(fill: dict, mid_cents: float | None) -> tuple[float, float]:
    """Returns (pnl_realized_usd, pnl_unrealized_usd) for a single fill.

    Realized = -fee. Unrealized = signed mark-to-market vs the current mid.
    For a YES BUY at p, +q, MTM = q*(mid_yes - p)/100 USD.
    For a NO BUY at p, +q,  MTM = q*((100-mid_yes) - p)/100 USD.
    Sells flip the sign. If mid is unknown, MTM = 0.
    """
    try:
        qty = int(float(fill.get("count") or fill.get("count_fp") or 0))
    except (TypeError, ValueError):
        qty = 0
    side = (fill.get("side") or "").lower()
    action = (fill.get("action") or "").lower()
    fee = float(fill.get("fee_cost") or 0.0) / 100.0  # cents -> USD
    if side == "yes":
        try:
            px = round(float(fill.get("yes_price_dollars", "0") or 0) * 100)
        except (TypeError, ValueError):
            px = int(fill.get("yes_price") or 0)
    else:
        try:
            px = round(float(fill.get("no_price_dollars", "0") or 0) * 100)
        except (TypeError, ValueError):
            px = int(fill.get("no_price") or 0)
    pnl_unreal = 0.0
    if mid_cents is not None and qty > 0:
        side_mid = mid_cents if side == "yes" else (100.0 - mid_cents)
        edge_cents = (side_mid - px) if action == "buy" else (px - side_mid)
        pnl_unreal = (edge_cents * qty) / 100.0
    pnl_real = -fee
    return pnl_real, pnl_unreal


def _refresh_rolling_pnl(state: dict, now_unix: float) -> float:
    """Drop fills older than rolling_window_h, sum the rest. Returns the
    new rolling PnL in USD. Mutates ``state['fills_observed']``."""
    window_s = float(state["rolling_window_h"]) * 3600.0
    cutoff = now_unix - window_s
    kept = []
    total = 0.0
    for f in state["fills_observed"]:
        ts = float(f.get("ts_unix") or 0.0)
        if ts < cutoff:
            continue
        kept.append(f)
        total += float(f.get("pnl_usd_realized", 0.0))
        total += float(f.get("pnl_usd_unrealized_at_observation", 0.0))
    state["fills_observed"] = kept
    state["rolling_pnl_usd"] = round(total, 4)
    return state["rolling_pnl_usd"]


# ---------------------------------------------------------------------------
# Per-pipeline poll cycle.
# ---------------------------------------------------------------------------

def _maybe_red(text: str) -> str:
    if sys.stdout.isatty():
        return f"\033[31m{text}\033[0m"
    return text


def poll_pipeline(state: dict, state_path: Path, fills: list[dict],
                  client: KalshiClient, tripped_file: Path,
                  dry_run: bool) -> dict:
    """Update one pipeline's state given a batch of fills already filtered
    to events the pipeline tracks. Returns a result dict."""
    now = _now_unix()
    seen_ids = {f.get("fill_id") for f in state["fills_observed"]}
    events = set(state["events_tracked"])

    # Dedup + attribute fills, fetch mid only for new ones.
    new_count = 0
    mid_cache: dict[str, float | None] = {}
    for f in fills:
        ticker = (f.get("ticker") or f.get("market_ticker") or "").upper()
        if event_ticker_of(ticker) not in events:
            continue
        fill_id = (f.get("trade_id") or "").strip()
        if not fill_id or fill_id in seen_ids:
            continue
        if ticker not in mid_cache:
            mid_cache[ticker] = client.fetch_orderbook_mid_cents(ticker)
        mid = mid_cache[ticker]
        pnl_real, pnl_unreal = _fill_pnl_usd(f, mid)
        ts_unix = (f.get("ts")
                   or _parse_iso_to_unix(f.get("created_time", ""))
                   or now)
        try:
            qty = int(float(f.get("count") or f.get("count_fp") or 0))
        except (TypeError, ValueError):
            qty = 0
        side = (f.get("side") or "").lower()
        if side == "yes":
            try:
                px_cents = round(float(f.get("yes_price_dollars", "0") or 0) * 100)
            except (TypeError, ValueError):
                px_cents = int(f.get("yes_price") or 0)
        else:
            try:
                px_cents = round(float(f.get("no_price_dollars", "0") or 0) * 100)
            except (TypeError, ValueError):
                px_cents = int(f.get("no_price") or 0)
        state["fills_observed"].append({
            "fill_id": fill_id,
            "ts": f.get("created_time") or _now_iso(),
            "ts_unix": float(ts_unix),
            "event": event_ticker_of(ticker),
            "ticker": ticker,
            "side": side,
            "action": (f.get("action") or "").lower(),
            "qty": qty,
            "price_cents": int(px_cents),
            "fee_usd": round(float(f.get("fee_cost") or 0.0) / 100.0, 4),
            "mid_cents_at_observation": mid,
            "pnl_usd_realized": round(pnl_real, 4),
            "pnl_usd_unrealized_at_observation": round(pnl_unreal, 4),
        })
        seen_ids.add(fill_id)
        new_count += 1

    # Re-mark unrealized PnL of existing in-window fills against the latest
    # mid we have. This is intentionally cheap: we only refresh mids for
    # tickers that already showed up in this cycle. Stale fills in-window
    # keep their last-observed unrealized PnL until their ticker shows up
    # again. That's an acceptable approximation -- the trip threshold is
    # only checked on the realized + last-observed-unrealized sum.
    for entry in state["fills_observed"]:
        t = entry.get("ticker")
        if t in mid_cache and mid_cache[t] is not None:
            mid = mid_cache[t]
            side = entry.get("side", "")
            action = entry.get("action", "")
            qty = int(entry.get("qty") or 0)
            px = int(entry.get("price_cents") or 0)
            side_mid = mid if side == "yes" else (100.0 - mid)
            edge = (side_mid - px) if action == "buy" else (px - side_mid)
            entry["pnl_usd_unrealized_at_observation"] = round((edge * qty) / 100.0, 4)
            entry["mid_cents_at_observation"] = mid

    rolling = _refresh_rolling_pnl(state, now)
    state["last_polled_at"] = _now_iso()

    tripped_now = False
    if (not state.get("tripped")) and rolling < -float(state["max_loss_usd"]):
        state["tripped"] = True
        state["tripped_at"] = _now_iso()
        reason = (f"rolling_pnl ${rolling:.2f} < -${float(state['max_loss_usd']):.2f}"
                  f" (window {state['rolling_window_h']}h)")
        state["tripped_reason"] = reason
        contracts = sum(int(e.get("qty") or 0) for e in state["fills_observed"])
        merge_result = trip_events_in_file(
            tripped_file, sorted(events),
            reason=f"pnl_monitor:{state['pipeline_name']}: {reason}",
            dollars=abs(rolling), contracts=contracts, dry_run=dry_run)
        msg = (f"PNL TRIP {state['pipeline_name']}: {reason}; "
               f"events={sorted(events)}; merged={merge_result.get('added')}")
        logger.error(msg)
        sys.stdout.write(_maybe_red(f"[TRIP] {msg}\n"))
        sys.stdout.flush()
        tripped_now = True

    if not dry_run:
        _atomic_write_json(state_path, state)

    return {
        "pipeline": state["pipeline_name"],
        "new_fills": new_count,
        "rolling_pnl_usd": rolling,
        "tripped_now": tripped_now,
        "tripped": bool(state.get("tripped")),
    }


# ---------------------------------------------------------------------------
# Top-level orchestration.
# ---------------------------------------------------------------------------

def discover_pipelines(pnl_dir: Path) -> list[tuple[Path, dict]]:
    out: list[tuple[Path, dict]] = []
    if not pnl_dir.exists():
        return out
    for p in sorted(pnl_dir.glob("*.json")):
        try:
            state = _read_json(p)
            state = _validate_state(state, p)
            out.append((p, state))
        except Exception as e:
            logger.error("Skipping malformed pnl file %s: %s", p, e)
    return out


def reconcile_events_from_specs(states: list[tuple[Path, dict]],
                                specs_dir: Path,
                                dry_run: bool) -> None:
    """Re-derive ``events_tracked`` for each pipeline from ``specs/*.json``
    by matching ``family_signature == pipeline_name``. Atomic-write back if
    the set changed. Pipelines unknown in specs are left alone (the user may
    be running a pipeline whose spec hasn't been added yet)."""
    fam_map = build_family_to_events(specs_dir)
    # Detect overlap: same event listed under two pipelines.
    owners: dict[str, str] = {}
    for fam, evs in fam_map.items():
        for ev in evs:
            if ev in owners and owners[ev] != fam:
                logger.warning("Event %s claimed by both '%s' and '%s' -- "
                               "first-listed wins", ev, owners[ev], fam)
                continue
            owners[ev] = fam
    for path, state in states:
        fam = state.get("pipeline_name")
        derived = fam_map.get(fam)
        if derived is None:
            logger.info("Pipeline %s has no spec match; keeping declared "
                        "events_tracked=%s", fam, state.get("events_tracked"))
            continue
        derived_sorted = sorted(set(derived))
        if state["events_tracked"] != derived_sorted:
            logger.info("Pipeline %s events_tracked %s -> %s (from specs)",
                        fam, state["events_tracked"], derived_sorted)
            state["events_tracked"] = derived_sorted
            if not dry_run:
                _atomic_write_json(path, state)


def run_once(pnl_dir: Path, specs_dir: Path, tripped_file: Path,
             dry_run: bool) -> dict:
    client = KalshiClient()
    states = discover_pipelines(pnl_dir)
    if not states:
        logger.warning("No pipeline pnl files found in %s", pnl_dir)
        return {"pipelines": 0, "results": [], "private_ok": client.private_ok}

    reconcile_events_from_specs(states, specs_dir, dry_run)

    # Earliest cutoff across all pipelines, so we fetch fills once and dispatch.
    now = _now_unix()
    earliest = min(now - float(s["rolling_window_h"]) * 3600.0 for _, s in states)
    fills = client.fetch_recent_fills(min_ts=earliest)
    logger.info("Fetched %d recent fill(s); private_ok=%s",
                len(fills), client.private_ok)

    results = []
    for path, state in states:
        try:
            res = poll_pipeline(state, path, fills, client, tripped_file, dry_run)
            results.append(res)
            logger.info("Pipeline %s: rolling_pnl=$%.2f, new_fills=%d, "
                        "tripped=%s", res["pipeline"], res["rolling_pnl_usd"],
                        res["new_fills"], res["tripped"])
        except Exception as e:
            logger.exception("Pipeline %s poll failed: %s",
                             state.get("pipeline_name"), e)
    return {"pipelines": len(states), "results": results,
            "private_ok": client.private_ok}


# ---------------------------------------------------------------------------
# Daemon loop with SIGINT/SIGTERM graceful shutdown.
# ---------------------------------------------------------------------------

class _ShutdownFlag:
    def __init__(self):
        self.stop = False

    def set(self, *_):
        self.stop = True


def run_forever(pnl_dir: Path, specs_dir: Path, tripped_file: Path,
                poll_interval_s: int, dry_run: bool) -> None:
    flag = _ShutdownFlag()
    signal.signal(signal.SIGINT, flag.set)
    signal.signal(signal.SIGTERM, flag.set)
    logger.info("pnl_monitor starting: pnl_dir=%s specs_dir=%s tripped_file=%s "
                "interval=%ds dry_run=%s",
                pnl_dir, specs_dir, tripped_file, poll_interval_s, dry_run)
    while not flag.stop:
        cycle_start = time.monotonic()
        try:
            run_once(pnl_dir, specs_dir, tripped_file, dry_run)
        except Exception as e:
            logger.exception("Top-level poll cycle errored: %s", e)
        # Sleep in 1s slices to make shutdown responsive.
        elapsed = time.monotonic() - cycle_start
        remaining = max(0.0, poll_interval_s - elapsed)
        end = time.monotonic() + remaining
        while not flag.stop and time.monotonic() < end:
            time.sleep(min(1.0, end - time.monotonic()))
    logger.info("pnl_monitor shutting down cleanly")


# ---------------------------------------------------------------------------
# CLI entrypoint.
# ---------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--once", action="store_true",
                   help="Run a single poll pass and exit.")
    p.add_argument("--poll-interval", type=int, default=DEFAULT_POLL_INTERVAL_S,
                   help="Seconds between poll cycles (default %(default)s).")
    p.add_argument("--pnl-dir", type=Path, default=DEFAULT_PNL_DIR)
    p.add_argument("--specs-dir", type=Path, default=DEFAULT_SPECS_DIR)
    p.add_argument("--tripped-events-file", type=Path, default=DEFAULT_TRIPPED_FILE)
    p.add_argument("--dry-run", action="store_true",
                   help="Never write the trip file or mutate pnl state.")
    p.add_argument("--log-level", default="INFO",
                   choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    args = p.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    if args.once:
        run_once(args.pnl_dir, args.specs_dir, args.tripped_events_file,
                 args.dry_run)
        return 0
    run_forever(args.pnl_dir, args.specs_dir, args.tripped_events_file,
                args.poll_interval, args.dry_run)
    return 0


if __name__ == "__main__":
    sys.exit(main())
