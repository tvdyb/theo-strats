"""Auto-theo batch runner — sequencing driver for multi-event onboarding.

This is intentionally a stub: it sequences what the four subagents
(researcher / modeler / backtester / integrator) should run for a list of
event_tickers, but does NOT call them. It has no LLM access. When a step
requires a subagent (e.g. researcher to produce a missing spec, modeler to
build a new pipeline class), the batch_runner prints a Claude Code command
the user can paste into their session. When a step is purely mechanical
(spec exists + pipeline class exists), the batch_runner just calls the
orchestrator to refresh theos for that event.

Stdlib only.

CLI
---
python3 -m auto_theo.batch_runner --events EV1,EV2,... [--max-parallel N] [--dry-run]

Outputs a final summary table:
  event | spec_status | pipeline_status | theo_status

The --max-parallel flag is accepted for forward-compat (per spec) but the
current implementation runs orchestration serially via a single
`orchestrator --once` call — which itself iterates events deterministically.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# Resolve project paths from this file's location so the script works no matter
# where it's invoked from.
_HERE = Path(__file__).resolve().parent
_REPO_ROOT = _HERE.parent  # /Users/wilsonw/mm-setup
SPECS_DIR = _HERE / "specs"
PIPELINES_DIR = _HERE / "pipelines"
THEOS_DIR = Path("/Users/wilsonw/Downloads/theos")


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text())


def _spec_status(event_ticker: str) -> tuple[str, dict | None]:
    """Return ('exists' | 'missing', spec_dict_or_None)."""
    p = SPECS_DIR / f"{event_ticker}.json"
    if not p.exists():
        return "missing", None
    try:
        return "exists", _read_json(p)
    except Exception:
        return "exists_unreadable", None


def _discover_pipeline_family_signatures() -> set[str]:
    """Mirror of orchestrator.discover_pipeline_classes() at the textual
    level. We don't import the orchestrator (it has heavy side effects
    around logging) — we just scan pipelines/*.py for `family_signature = "..."`
    declarations on Pipeline subclasses.

    To stay accurate without parsing AST, we delegate to the orchestrator's
    discovery function via its public API.
    """
    # Import lazily so a partial install doesn't break --help.
    try:
        # Ensure repo root is on sys.path for `auto_theo.*` imports.
        if str(_REPO_ROOT) not in sys.path:
            sys.path.insert(0, str(_REPO_ROOT))
        from auto_theo.orchestrator import discover_pipeline_classes  # type: ignore
        return set(discover_pipeline_classes().keys())
    except SystemExit:
        # discover_pipeline_classes raises SystemExit on duplicate signatures.
        # That's a user-visible error; surface it.
        raise
    except Exception as exc:
        print(f"[batch_runner] WARN: pipeline discovery failed: {exc}",
              file=sys.stderr)
        return set()


def _run_orchestrator_once(dry_run: bool) -> int:
    """Invoke `python3 -m auto_theo.orchestrator --once` from _REPO_ROOT.

    Returns the orchestrator's exit code. We use subprocess (not in-process
    import) so the orchestrator's logging / signal handling don't pollute the
    batch_runner's stdout, and so a crash in one event doesn't kill us.
    """
    cmd = [sys.executable, "-m", "auto_theo.orchestrator", "--once"]
    if dry_run:
        cmd.append("--dry-run")
    print(f"[batch_runner] running: {' '.join(cmd)} (cwd={_REPO_ROOT})")
    try:
        r = subprocess.run(cmd, cwd=str(_REPO_ROOT), check=False)
        return r.returncode
    except Exception as exc:
        print(f"[batch_runner] orchestrator invocation crashed: {exc}",
              file=sys.stderr)
        return 1


def _theo_status(event_ticker: str) -> str:
    """ok | missing | stale (>1h old)."""
    p = THEOS_DIR / f"{event_ticker}.json"
    if not p.exists():
        return "missing"
    try:
        mtime = p.stat().st_mtime
    except OSError:
        return "missing"
    age_s = time.time() - mtime
    if age_s > 3600:
        return f"stale_{int(age_s)}s"
    return "ok"


def _print_table(rows: list[dict]) -> None:
    """Pretty-print the summary table to stdout. ASCII only."""
    cols = ["event", "spec_status", "pipeline_status", "theo_status"]
    widths = {c: max(len(c), max((len(str(r.get(c, ""))) for r in rows), default=0))
              for c in cols}
    sep = "+" + "+".join("-" * (widths[c] + 2) for c in cols) + "+"
    header = "|" + "|".join(f" {c.ljust(widths[c])} " for c in cols) + "|"
    print(sep)
    print(header)
    print(sep)
    for r in rows:
        print("|" + "|".join(
            f" {str(r.get(c, '')).ljust(widths[c])} " for c in cols) + "|")
    print(sep)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--events", required=True,
                   help="Comma-separated list of event tickers, e.g. "
                        "KXAAAGASW-26MAY11,KXAAAGASW-26MAY18")
    p.add_argument("--max-parallel", type=int, default=5,
                   help="Forward-compat; current impl runs orchestrator serially.")
    p.add_argument("--dry-run", action="store_true",
                   help="Pass --dry-run to the orchestrator (skip theo writes).")
    args = p.parse_args(argv)

    events = [e.strip() for e in args.events.split(",") if e.strip()]
    if not events:
        print("[batch_runner] no events provided", file=sys.stderr)
        return 2

    print(f"[batch_runner] starting at "
          f"{datetime.now(timezone.utc).isoformat()} | "
          f"events={len(events)} max_parallel={args.max_parallel} "
          f"dry_run={args.dry_run}")

    # 1. Spec status per event.
    per_event: dict[str, dict] = {}
    missing_specs: list[str] = []
    for ev in events:
        st, spec = _spec_status(ev)
        per_event[ev] = {
            "event": ev,
            "spec_status": st,
            "spec": spec,
            "pipeline_status": "?",
            "theo_status": "?",
        }
        if st == "missing":
            missing_specs.append(ev)

    # 2. For events with no spec, print the researcher prompt the user must run.
    if missing_specs:
        print()
        print("[batch_runner] MISSING SPECS — paste each of these into your "
              "Claude Code session (batch_runner has no LLM access):")
        for ev in missing_specs:
            print(f"  > Run researcher subagent on {ev}")
        print()

    # 3. Group remaining (spec'd) events by family_signature.
    family_to_events: dict[str, list[str]] = {}
    for ev, info in per_event.items():
        spec = info["spec"]
        if not isinstance(spec, dict):
            continue
        fam = (spec.get("family_signature") or "").strip()
        if not fam:
            info["pipeline_status"] = "spec_missing_family_signature"
            continue
        family_to_events.setdefault(fam, []).append(ev)

    # 4. Discover pipeline classes — find which families lack a pipeline.
    known_families = _discover_pipeline_family_signatures()
    families_without_pipeline: list[str] = []
    for fam, evs in family_to_events.items():
        if fam in known_families:
            for ev in evs:
                per_event[ev]["pipeline_status"] = "exists"
        else:
            families_without_pipeline.append(fam)
            for ev in evs:
                per_event[ev]["pipeline_status"] = "missing"

    if families_without_pipeline:
        print()
        print("[batch_runner] FAMILIES WITHOUT PIPELINE — paste each of these "
              "into your Claude Code session:")
        for fam in families_without_pipeline:
            example_event = family_to_events[fam][0]
            if args.dry_run:
                print(f"  > [dry-run] would prompt: Run modeler subagent on "
                      f"family {fam} from spec {example_event}")
            else:
                print(f"  > Run modeler subagent on family {fam} "
                      f"from spec {example_event}")
        print()

    # 5. For families WITH a pipeline class, run orchestrator --once to refresh.
    runnable_events = [ev for ev, info in per_event.items()
                       if info["pipeline_status"] == "exists"]
    if runnable_events:
        print(f"[batch_runner] {len(runnable_events)} event(s) have an "
              f"existing pipeline; calling orchestrator --once to refresh "
              f"theos.")
        rc = _run_orchestrator_once(dry_run=args.dry_run)
        print(f"[batch_runner] orchestrator exit code: {rc}")
    else:
        print("[batch_runner] no events have an existing pipeline; skipping "
              "orchestrator call.")

    # 6. Re-check theo files. In --dry-run mode the orchestrator does not write,
    #    so theo_status will report based on whatever was already on disk.
    for ev, info in per_event.items():
        info["theo_status"] = _theo_status(ev)

    # 7. Print the final summary table.
    print()
    print("[batch_runner] FINAL SUMMARY:")
    rows = [{k: info[k] for k in ("event", "spec_status",
                                  "pipeline_status", "theo_status")}
            for info in per_event.values()]
    _print_table(rows)

    # Exit code: 0 iff every event ended in (spec=exists, pipeline=exists,
    # theo=ok). Anything else => non-zero so a CI loop notices.
    all_ok = all(
        info["spec_status"] == "exists"
        and info["pipeline_status"] == "exists"
        and info["theo_status"] == "ok"
        for info in per_event.values()
    )
    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
