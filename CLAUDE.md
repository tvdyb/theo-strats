# Auto-Theo Builder — Claude Code Project

You are building and operating an auto-theo system for the existing `mm-setup` Kalshi market-making bot. The bot already does reward harvesting, arb prevention, expiry guarding, and inventory wind-down. Your job is to feed it good theos for ~100 markets in parallel, fast, without breaking anything that already works.

## Operating principles (read these every session)

1. **The LLM never produces probabilities that flow into orders.** You write pipelines that produce probabilities. Pipelines run deterministically, in code, on a schedule, with no LLM in the inference path. If you find yourself estimating a probability in prose, stop — that's a code generation task, not an inference task.

2. **Refusal is a first-class outcome.** A market the system can't theo correctly should produce no theo, not a guess. Every pipeline must be able to emit `BlackoutError`, `InsufficientDataError`, or `UnsupportedMarketError`. The bot already respects "no theo" by not quoting; lean on that.

3. **Speed comes from parallelism and reuse, not from skipping safety.** Building 100 pipelines should mean building ~5 pipeline templates and applying each to ~20 similar markets. If you're writing pipeline #50 from scratch, you've designed wrong — go back and generalize.

4. **Per-pipeline PnL tracking is mandatory.** Every theo pipeline ships with realized-PnL tracking on the markets it covers. Pipelines that lose money on rolling 24h get auto-tripped. This is the cheap substitute for rigorous backtesting and makes "ship fast" survivable.

5. **Backtest before live, but don't gold-plate the backtest.** A 7-day historical run with calibration diagnostics is enough to reject obviously-broken pipelines. Don't spend a day tuning a backtest harness; spend it shipping pipelines and let the per-pipeline PnL trip catch what backtests miss.

6. **Never edit files in `theos/` or modify `kalshi_rewards_app.py` from a research subagent.** Only the Integrator subagent touches the live system. Researchers and Modelers work in `auto_theo/staging/` until promoted.

## Architecture

```
auto_theo/
├── pipelines/              # one Python module per family (Truflation, sports, etc)
│   ├── _base.py            # Pipeline ABC: build_theos(now), data_as_of(now), refusal types
│   ├── truflation.py       # Truflation-family pipelines (eggs, gas, breakfast, etc)
│   ├── sports.py
│   └── ...
├── archive/                # point-in-time data archive
│   ├── truflation/<series>/<date>.json  # raw publishes, append-only
│   └── kalshi/<ticker>/    # candlestick + trade history per market
├── specs/                  # one JSON per market — output of Researcher
│   └── KXTRUFEGGS-26APR28.json
├── staging/                # Modeler writes here; Integrator promotes
├── backtest/
│   ├── harness.py          # generic walk-forward runner
│   └── reports/<pipeline>/<run_ts>/
├── pnl/                    # per-pipeline rolling PnL state
│   └── <pipeline_name>.json
└── orchestrator.py         # entrypoint: queues work to subagents
```

The existing `theos/<EVENT>.json` directory is the integration point with the live bot. Integrator writes there. Nothing else does.

## Subagents

Four specialized subagents. Each has its own system prompt in `.claude/agents/`. Use them via the Task tool.

### researcher
Given a market or event ticker, produces a spec JSON. Has web access. Reads Kalshi API for resolution criteria. Identifies data sources with concrete URLs/endpoints. Classifies tradability. Builds blackout calendar. Outputs `specs/<event>.json` matching the schema in `auto_theo/schemas/spec.json`. Refuses if the market is in the "untradable" family.

### modeler
Given a spec JSON, writes a Python pipeline module in `staging/`. No web access. No Kalshi access. Pure code generation against the spec. Must produce both `live_mode` and `historical_as_of` data fetchers. Every numerical constant requires a comment explaining the choice. Outputs the pipeline file plus a test scenarios file.

### backtester
Given a staged pipeline, runs `backtest/harness.py` against historical Kalshi markets in the same family. Produces calibration diagnostics. Hard-fails the pipeline if any calibration decile deviates more than 8pp, or if max drawdown over the historical window exceeds a configured threshold. Pass/fail is mechanical — no judgment calls.

### integrator
Promotes a passed pipeline from staging to `auto_theo/pipelines/`. Sets up cron-style refresh in `orchestrator.py`. Registers PnL tracking. Writes initial `theos/<EVENT>.json` files. Verifies the live bot picks them up. This is the only subagent that touches the live system.

## Workflow for adding new markets

```
user: "add KXTRUFEGGS-26MAY03 through KXTRUFEGGS-26MAY10 (8 markets)"

orchestrator:
  for each event in batch:
    spawn researcher(event) -> spec
  wait all
  group specs by family signature
  for each unique family signature:
    if pipeline already exists in pipelines/:
      reuse — Integrator just wires up the new event
    else:
      spawn modeler(family_signature) -> staged pipeline
      spawn backtester(staged) -> pass/fail
      if pass: spawn integrator(staged, [events]) -> live
      if fail: report and halt that family
```

Family signature is `(data_source_set, resolution_pattern, strike_pattern)`. Two events with the same signature share a pipeline. This is how you go from 100 markets to ~5 pipelines.

### Family-signature short-circuit (Fix 1)

If `family_signature` already has a pipeline class in `pipelines/`, the
modeler/backtester are skipped; integrator just adds the new spec and the
orchestrator picks it up on next refresh. The orchestrator already maps
`family_signature -> pipeline_class` (in `discover_pipeline_classes`) and
`event_ticker -> (pipeline_class, spec)` (in `build_event_registry`); a brand-
new spec file dropped into `specs/` whose `family_signature` matches an
existing pipeline class is enough to onboard the event end-to-end. Verified
with `KXAAAGASW-26MAY04` + `KXAAAGASW-26MAY11` — both are served by
`AAAGasWeeklyPipeline` with zero pipeline-code edits.

This is how onboarding scales sublinearly: pipeline #2 through #100 in the
same family cost one spec JSON each, not a full researcher/modeler/backtester
/integrator round.

### Synthetic-archive ship-with-trip rule (Fix 2)

When a spec has `backtest.archive_quality != "real"`, the backtester
short-circuits to `passed=True` with reason `"skipped: archive_quality=..."`.
The leakage and refusal-sanity gates STILL run (they don't depend on
calibration ground truth), but the calibration-decile gate is skipped because
the archive itself is structurally biased and would always fail. To compensate,
`backtest.trip_max_loss_usd_override` (typically `5.0`) tightens the per-
pipeline PnL trip in `pnl/<family>.json` from the default `25.0`. Integrator
rule: when promoting a pipeline whose spec has `trip_max_loss_usd_override`,
write that value as `max_loss_usd` in `pnl/<family>.json` instead of the
default $25.

## Hard rules for parallel runs

- Subagents working in parallel must not write to overlapping files. Researchers write to unique `specs/<event>.json` paths; conflict is impossible. Modelers write to unique `staging/<family>_<timestamp>.py` paths. Only the Integrator writes to `pipelines/` and `theos/`, and it runs serially.
- The point-in-time data archive is append-only. Multiple researchers can fetch and write concurrently; never mutate existing archive entries.
- Per-pipeline PnL state files use atomic write (tempfile + os.replace), since both the live updater and the orchestrator may read them.

## Per-pipeline PnL trip

Every pipeline has a config in `pnl/<pipeline_name>.json`:
```json
{
  "rolling_window_h": 24,
  "max_loss_usd": 25.0,
  "tripped": false,
  "tripped_at": null,
  "tripped_reason": null
}
```

A side process polls Kalshi fills, attributes each fill to its pipeline, and accumulates rolling PnL. When `rolling_pnl < -max_loss_usd`, the pipeline is tripped: all its events get added to `kalshi_tripped_events.json` (the existing trip mechanism), and an alert fires. Untripping is manual.

This is the most important runtime safety net. Without it, a bad pipeline silently bleeds money. With it, a bad pipeline costs you $25 and surfaces itself.

## What lives where

- `pipelines/` — production code, code-reviewed, version-controlled. LLM-written but human-reviewed.
- `staging/` — LLM-written candidate code, not yet reviewed. Backtester runs it; Integrator promotes if passed.
- `archive/` — data only, never code. Append-only.
- `specs/` — machine-readable contracts between Researcher and Modeler.
- `theos/` — output, consumed by the live bot. Format is the existing schema in the repo README.

## When to stop and ask the user

- Researcher classifies a market as untradable for a non-obvious reason — surface it; the user might disagree.
- Backtester fails a pipeline that the user expected to work — surface the diagnostics; don't auto-retry.
- Integrator detects a conflict (two pipelines claim the same event) — surface and stop.
- A pipeline trips on rolling PnL — never auto-untrip; user must review and decide.

Otherwise, run autonomously. The user will be running batches of 20-100 markets at a time and won't want to babysit each step.

## Reference docs

- Existing repo: `kalshi_rewards_app.py`, `kalshi_reward_monitor.py`, theos schema in `README.md`
- Kalshi API: https://docs.kalshi.com/api-reference
- Truflation indices: https://truflation.com (component series accessible via their public dashboards and API)
- The strategy notes from the design conversation are in `auto_theo/DESIGN_NOTES.md`
