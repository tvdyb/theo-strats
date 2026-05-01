# Auto-Theo Design Notes

Context carried forward from the design conversation that produced this system. Read once on first session; don't need to re-read every time.

## Strategy basics (the live bot already does these)

- Reward harvesting on Kalshi liquidity incentive markets. Top-300 cumulative size on each side qualifies. Reward weight per contract is *probably* linear in price (cents) but this is unverified — see TODO in the README.
- Adverse fills are the main risk. The bot avoids quoting inside its theo's no-quote band (`band_cents`), and the trip mechanism cancels everything on an event when losses accumulate.
- Takes (crossing the spread) are disabled overnight — they're the highest-EV-loss feature when a theo is wrong.
- Inventory wind-down posts passive sells when long, with a price floor to avoid burning margin lockup on penny exits.

## Why auto-theos

100+ active reward-paying markets at any time. Hand-built theos don't scale. The auto-theo system replaces the manual `theos/<EVENT>.json` files with pipelines that compute them.

## The split between LLM and code

The LLM:
- Reads market descriptions, identifies data sources, classifies tradability (Researcher).
- Writes deterministic Python pipelines from specs (Modeler).
- Runs backtests and emits mechanical pass/fail (Backtester).
- Wires passed pipelines into the live system (Integrator).

The LLM never:
- Outputs a probability that flows into orders.
- Estimates a sigma, a fair value, or a no-quote band.
- Makes pass/fail backtest decisions based on judgment.

If you find yourself wanting an LLM to "look at this and tell me if it's right," you're using the LLM wrong. Either the question can be reduced to a numerical threshold (in which case code it), or it needs human judgment (in which case ask the user).

## Speed/safety tradeoffs

The user explicitly wants to move fast on ~100 markets. The cheap defenses that make speed safe:

1. **Refusal as default for ambiguous markets.** Untradable verdicts are a feature, not a failure.
2. **Per-pipeline rolling PnL trip.** $25/24h kills bad pipelines automatically. Cheaper than rigorous backtesting.
3. **Pipeline reuse via family signatures.** 100 markets ≈ 5 pipelines. Each pipeline gets backtested once, applied to ~20 events.
4. **Atomic writes everywhere.** No partial-state corruption of the live bot's theos directory.

What's deliberately skipped to ship fast:
- Full L2 orderbook reconstruction in backtests. Use BBO + trade flow, accept the approximation.
- Forward-test shadow periods. The 24h PnL trip is the substitute.
- Per-market hand-tuning. If a pipeline doesn't work for a market in its family, the pipeline is wrong, not the market.

## Failure modes to watch

- **Silent miscalibration.** A pipeline that's 5pp off everywhere bleeds money slowly; PnL trip catches it eventually but reward losses pre-trip can be substantial. Calibration backtest should catch most of these.
- **Data source death.** A Truflation endpoint goes 500 for an hour; pipelines stop refreshing; bot trades on stale theos. The existing staleness gate (`max_theo_age_s = 14400`) handles this — pipelines just need to write an honest `as_of` timestamp.
- **Spec drift.** Researcher writes a spec, Modeler generates code, then the underlying source changes its API. No automated catch — the pipeline starts raising InsufficientDataError, the live bot trades less, the user notices via the dashboard. Acceptable.
- **Backtest leakage.** The most common bug. Mitigated by the leakage check in the harness; if it ever passes a pipeline that's actually leaking, the held-out window catches it.

## Things to revisit later

- Empirical validation of the reward weight function (linear vs 1/2^n). Affects optimal quoting depth.
- Fill-rate feedback loop on the no-quote band. Mentioned in the design conversation; not in the v1 of this system. PnL trip is the cruder substitute.
- Whether to extend the system to political/event markets (currently classified untradable_no_signal). Probably not worth it — the math doesn't work and Polymarket arb opportunities are better attacked directly.

## Live bot integration points (don't touch the bot itself)

The auto-theo system writes to:
- `theos/<EVENT>.json` — picked up by `kalshi_rewards_app.py` on mtime change
- `kalshi_blocked_markets.json` — for permanent blocks
- `kalshi_tripped_events.json` — for PnL-driven trips

The auto-theo system reads from:
- Kalshi `/portfolio/fills` for PnL attribution
- Kalshi `/markets`, `/events`, `/markets/{}/candlesticks` for research and backtesting
- Configured external data sources per spec

Never edit `kalshi_rewards_app.py` from this project. The bot is stable and tested. The auto-theo system extends it via the existing JSON file extension points only.
