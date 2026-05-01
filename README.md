# auto_theo — Claude Code project

Automated theo generation for the `mm-setup` Kalshi market-making bot.

## Quickstart

```
cd mm-setup
claude
```

Then in Claude Code:

```
> Run the researcher subagent on KXTRUFEGGS-26MAY03 through KXTRUFEGGS-26MAY10
  (8 events). After each spec is written, group by family_signature, run the
  modeler on any new families, then backtester, then integrator.
```

Or use the orchestrator directly:

```
> Onboard 100 markets: pull all currently-active events from the Kalshi
  incentive_programs endpoint, run the full pipeline. Run researchers in
  parallel up to 10 at a time. Modelers and backtesters can run in parallel
  per family. Integrators run serially.
```

## How the subagents work

Four subagents in `.claude/agents/`:

1. **researcher** — webfetches Kalshi + data sources, writes `specs/<event>.json`
2. **modeler** — reads spec, writes Python pipeline to `staging/`
3. **backtester** — runs pipeline on historical Kalshi markets, mechanical pass/fail
4. **integrator** — promotes passed pipelines, writes `theos/`, registers PnL tracking

The main `CLAUDE.md` at the repo root orchestrates them. Read it for the workflow rules.

## Why the split

Single-agent code generation tends to drift between research, math, and integration concerns — silently changing assumptions across phases. Splitting them forces explicit hand-offs via JSON specs, which means:

- Researcher's data-source claims are auditable (the spec is the contract).
- Modeler's math is reviewable (the pipeline is plain Python with named constants).
- Backtester's verdict is mechanical (pass/fail thresholds are hardcoded).
- Integrator's writes to live are isolated (only one subagent touches `theos/`).

Subagents also parallelize cleanly. A single Claude Code session can spin up 10 researchers on 10 events at once and coalesce results, then build out 5 pipelines in parallel for the 5 distinct families discovered.

## Directory layout

```
auto_theo/
├── DESIGN_NOTES.md         # carry-forward context from the design conversation
├── README.md               # this file
├── schemas/
│   └── spec.json           # JSON schema for researcher output
├── pipelines/              # production pipelines (only Integrator writes here)
│   └── _base.py            # Pipeline ABC, refusal exceptions, helpers
├── staging/                # Modeler writes here, Backtester runs here
├── archive/                # point-in-time data archive (append-only)
│   └── README.md           # archive layout and as-of query semantics
├── specs/                  # Researcher output
├── backtest/
│   ├── harness.py          # walk-forward runner (call from Backtester)
│   └── reports/
├── pnl/                    # rolling PnL state per family
├── orchestrator.py         # registry + scheduler for live pipeline refresh
└── pnl_monitor.py          # polls fills, attributes to families, trips on loss
```

## What's intentionally not here

- A general "ask Claude to estimate this probability" interface. The whole point is to remove the LLM from the inference path. If you want a quick gut check on a strike, use the existing manual theos process.
- A Web UI. The live bot has its own UI. The auto-theo system runs as background processes and writes its outputs to files the bot already consumes.
- A "smart" backtester that reasons about borderline pipelines. Pass/fail is mechanical for a reason — to make speed/safety tradeoffs explicit.

## Operating tips

- **Run the orchestrator overnight only after a clean week of daytime ops.** New pipelines should be reviewed during the day; let the PnL trip catch issues at night, not new bugs.
- **Trip thresholds are conservative on purpose.** $25/24h per family is roughly "the cost of one bad fill." If you raise it, raise it deliberately and write down why in `INCIDENTS.md`.
- **The held-out window in backtests is non-negotiable.** Resist the urge to pass pipelines that look fine in-sample but degrade on hold-out — they will degrade further in live trading.
- **Specs are immutable contracts.** If a Researcher's spec turns out to be wrong, fix it in a new spec with a new timestamp. Don't mutate. The corresponding pipeline gets re-built and re-backtested from the new spec.

## Adding new market families

When the Researcher encounters a market that doesn't fit any existing `model_family_suggestion`:

1. Researcher classifies it explicitly (e.g., `"custom"`) with notes.
2. Stop. Surface to the user. Don't auto-extend the system to a family it wasn't designed for.
3. User decides whether to add a new pipeline pattern, classify the market untradable, or hand-build a one-off theo.

Most "new" families will turn out to be variants of existing ones with different data sources. Reuse aggressively.
