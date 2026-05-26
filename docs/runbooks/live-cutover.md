# Runbook — Live Trader Cutover, Revert, and Phase-1 Exit Gate

Operator procedure for standing up the **live (real-money) trader** on a Linux
host alongside the existing paper trader, backing it out, and the criteria that
gate promotion out of Phase 1.

This runbook ties together the two artifacts shipped by sub-project E
(v1.6.0-PR6): the unit `deploy/live-trader.service` and the config template
`config/live-trader.example.yaml`.

---

## ⚠️ Status / Scope — READ FIRST (what this delivers vs what it does NOT)

**v1.6.0-PR6 / sub-project E delivers cutover-INFRASTRUCTURE-readiness — NOT
order-placing live trading.**

What is functional at the end of E:

- The live daemon boots, connects to the Kalshi WebSocket feed, and runs the
  full money-safe infrastructure: the fail-closed boot **coherence gate**, the
  **money-safe SIGTERM drain** (await in-flight place→persist → cancel order
  loops → close store once → final alert), the coherence/risk **kill-switch**
  wiring, the **live notification** channels, the **live reporting** read path,
  and the **CR-5 replay-vs-live executor-parity machinery**.
- The live trader runs as a separate process, against a separate DB
  (`data/live_trades.db`), with separate config and logs, **alongside the
  untouched paper trader** on the same WS feed.

What is **NOT** functional at the end of E — **the daemon does NOT place sized
real orders:**

- Dispatch-side order **sizing is deferred past E** (the spec's own SC-F2 +
  SC-I2). `_handle_enter` builds an `OrderRequest` with `size_contracts=0` as a
  documented placeholder, and `build_entry_order` (the gate-budget sized-request
  builder) is **not yet wired into the dispatch path**.
- Consequence: a live entry currently produces a `size_contracts=0` request,
  which `LiveExecutor` **rejects** by design (its defense-in-depth
  `size_contracts <= 0 → invalid_intended_size` guard). The live trader stands
  up the infrastructure and observes the feed; it does **not** transmit a sized
  real-money order.

**Placing the first real sized order is BLOCKED on two post-E prerequisites,
both of which must land AFTER v1.6.0-PR6:**

1. The **post-E dispatch sizing wire** — the deferred `gate_entry(ctx)` call +
   `build_entry_order` sizing wired into `_handle_enter` (a tracked next
   task/PR, per SC-F2 / SC-I2).
2. The **≥5-real-bundle CR-5 parity gate** passing — the authoritative
   live-vs-paper executor-parity verdict, run against ≥5 real captured bundles
   (see [The ≥5-real-bundle CR-5 gate](#the-5-real-bundle-cr-5-gate-sc-i2)).

So: `systemctl start edge-catcher-live-trader` **stands up and verifies the
infrastructure**. It does **not** immediately trade real money. The Phase-1
cutover steps below are framed accordingly — "stand up + verify the
infrastructure." The order-placing milestone is called out explicitly where it
is blocked.

---

## Pre-cutover checklist

Perform on the live-trading host (`/opt/edge-catcher`, the `edge-catcher`
service user — matching the deploy units).

1. **Code present and built.** Same venv layout as the paper trader
   (`deploy/README.md`):

   ```bash
   cd /opt/edge-catcher
   git pull
   .venv/bin/pip install -e ".[ai,ui]"
   ```

2. **Copy the live config template and fill it in.** The template is tracked;
   your filled-in copy lives in the **gitignored** `config.local/`:

   ```bash
   cp config/live-trader.example.yaml config.local/live-trader.yaml
   # then edit config.local/live-trader.yaml
   ```

   In `config.local/live-trader.yaml` confirm/set:
   - `executor: live` (arms the real-money executor; the boot coherence gate
     cross-checks every other field against this).
   - `db_path: data/live_trades.db` (must contain `live_trades` — the coherence
     gate rejects a live run pointed at a `paper_trades` DB, and vice versa).
   - Under `strategies:`, enable **only** Phase-1-approved strategies and their
     2-series allowlists.
   - The Phase-1 risk caps (see step 5).

3. **Set the trade-scope credentials via the EnvironmentFile — never in the
   repo.** The unit reads `EnvironmentFile=-/opt/edge-catcher/.env`. Write a
   bare `KEY=VALUE` file (not `~/.bashrc`, not the repo) containing the
   live-account signing creds the coherence gate resolves:

   ```bash
   # /opt/edge-catcher/.env  (mode 0600, owned by the edge-catcher user)
   KALSHI_LIVE_KEY_ID=...
   KALSHI_LIVE_PRIVATE_KEY=...        # RSA PEM, no passphrase
   ```

   These are the **trade-scope** keys (distinct from the paper/data
   `KALSHI_API_KEY`). The coherence gate signs a local test string with them at
   boot (no network); a missing or invalid key aborts boot before any order.

   > The example config deliberately does **not** pin `live_key_id_env` /
   > `live_private_key_env` — the defaults `KALSHI_LIVE_KEY_ID` /
   > `KALSHI_LIVE_PRIVATE_KEY` are correct. Override only if you store the keys
   > under non-default env-var names.

4. **Provision the live notification channels.** The live config names two
   channels that must exist in your unified notifications config
   (`notifications.config_path`, default `config.local/notifications.yaml`):
   - `notifications.live_channel` — general live alerts (fills, daily P&L).
     Example value `pnl_discord`.
   - `notifications.live_risk_channel` — the **dedicated** kill-switch /
     auto-panic channel. Example value `ops_slack`. This MUST be **distinct**
     from `live_channel` so a halt signal is never lost in the fill noise.

   Both channel **names** must appear in the `channels:` map of your
   `config.local/notifications.yaml`. The boot coherence gate fails closed if
   either channel is unconfigured or unresolvable (a tripped kill-switch alert
   must never go nowhere).

5. **Confirm the Phase-1 caps.** These six numbers in the `risk:` block define
   the current phase (validated at boot by the coherence gate). Phase-1
   reference values:

   | Key | Phase-1 value | Meaning |
   |---|---|---|
   | `sizing_pct` | `0.005` | 0.5% of equity per trade (fixed-fraction arm) |
   | `daily_loss_pct` | `0.02` | 2% of equity — daily soft-kill threshold |
   | `drawdown_pct` | `0.05` | 5% from closed-equity peak — lifetime guard |
   | `max_open` | `5` | max concurrent open positions |
   | `min_fill_contracts` | `3` | reject signals that size below this |
   | `absolute_panic_floor_cents` | `3000` | $30 absolute equity floor (hard stop) |
   | `absolute_max_cents` | `5000` | $50 per-order dollar cap (bug guard) |

   The static floors (`absolute_panic_floor_cents`, `absolute_max_cents`) do
   **not** scale with bankroll — they are bug protection. Confirm all six are
   present and correct for your bankroll before enabling live execution.

---

## Install — alongside the UNTOUCHED paper trader (CR-2)

The live trader is a **new** systemd unit installed next to the existing paper
trader. They are fully isolated: separate process, separate DB
(`data/live_trades.db` vs the paper DB), separate config, separate log — but the
**same WS feed**. The paper trader unit (`deploy/paper-trader.service`) is
**unchanged** by this PR and keeps running exactly as before.

The repo ships no dedicated live install script; mirror the paper-trader install
house style (`deploy/install-paper-trader.sh`). As root, once:

```bash
# 1. Log directory (shared with the paper trader — already exists if the
#    paper trader is installed; the chown is idempotent).
mkdir -p /var/log/edge-catcher
chown edge-catcher:edge-catcher /var/log/edge-catcher

# 2. Install the live unit alongside the paper unit. Mirror the paper
#    installer's "edge-catcher-<name>" unit naming.
cp /opt/edge-catcher/deploy/live-trader.service \
   /etc/systemd/system/edge-catcher-live-trader.service
systemctl daemon-reload
systemctl enable edge-catcher-live-trader.service
```

The unit's `ExecStart` is:

```
/opt/edge-catcher/.venv/bin/python -m edge_catcher live-trade \
    --config config.local/live-trader.yaml
```

`Restart=always`, `RestartSec=5`, `KillSignal=SIGTERM`, `TimeoutStopSec=30`.
A C-side auto-kill (panic / drawdown / daily-loss trip) does **not** exit the
process — the gate stays in KILL state (rejects new entries, still allows
exits). Only a crash or `systemctl stop` stops it, so `Restart=always` can never
clear operator intent.

Confirm the paper trader is unaffected:

```bash
systemctl status edge-catcher-paper-trader   # still active (running)
```

---

## First-run checklist (stand up + verify the infrastructure)

> Recall the Status/Scope banner: this verifies the **infrastructure boots and
> is healthy**. It does **not** verify real-money order placement — that is
> blocked on the post-E sizing wire.

Start the unit and walk the checks in order:

```bash
sudo systemctl start edge-catcher-live-trader
```

1. **Coherence gate passes (no boot abort).** The fail-closed boot gate
   (`_assert_mode_coherence`) validates `executor` ⟺ db path ⟺ trade-scope creds
   ⟺ both notification channels ⟺ Phase-1 caps. If any disagree, the engine
   raises a `RuntimeError` whose message contains `coherence` plus the failed
   check, and the unit will be in a restart loop. Inspect:

   ```bash
   sudo journalctl -u edge-catcher-live-trader -n 50 --no-pager
   ```

   A line mentioning `coherence` means the config is incoherent — read the
   message (it names the failed dimension: `executor` / `db` / `creds` /
   `channel` / `risk_channel` / `caps`), fix `config.local/live-trader.yaml`
   (or the `.env` creds, or `config.local/notifications.yaml`) against
   `config/live-trader.example.yaml`, and restart. **No coherence line in the
   boot logs = the gate passed.**

2. **Cutover-verification beacon is present.** The `engine/` package emits a
   unique boot beacon. Grep the journal for the **exact** substring:

   ```bash
   sudo journalctl -u edge-catcher-live-trader | grep \
     "engine\[G\]: live executor wired, package=edge_catcher.engine"
   ```

   Its presence proves the new `edge_catcher.engine` package is loaded — **not**
   the OLD `monitors/` engine (whose generic "Engine starting" line this beacon
   deliberately replaces) — **and** that the daemon is actually in live mode: the
   executor word is mode-driven, so a `paper` word on this (live) service means a
   paper config is deployed to it — stop and fix. If the beacon is absent
   entirely, the wrong code is running; do not proceed.

3. **`NRestarts=0` watch post-deploy.** A healthy daemon does not restart. Watch
   it for a few minutes:

   ```bash
   systemctl show -p NRestarts edge-catcher-live-trader
   ```

   A climbing `NRestarts` means it is crash-looping (most often a coherence
   abort — see step 1, or a missing dependency). Investigate before continuing.

4. **First notification lands on the LIVE channel.** Confirm the boot/heartbeat
   alert arrives on the channel named by `notifications.live_channel` (not the
   paper channel). This proves the live notification path is wired end-to-end.

5. **Zero inherited positions (CR-3 — live starts FLAT).** The live trader
   begins with an **empty** `data/live_trades.db`; **no paper positions
   migrate**. Confirm the live DB starts flat:

   ```bash
   sqlite3 data/live_trades.db "SELECT COUNT(*) FROM trades;"   # expect 0 at first boot
   ```

   Positions only ever rehydrate from `data/live_trades.db` itself (via the
   reconciler), never from the paper DB. A restart is a flat start.

> **Order placement is NOT verified here.** With the dispatch sizing wire
> deferred, the daemon will not transmit a sized real order even after all five
> checks pass. The "first real sized order" milestone is gated on the post-E
> sizing wire **and** the ≥5-real-bundle CR-5 gate below.

---

## Revert (NORMATIVE)

To back the live trader out completely. The paper trader is **unaffected**:

```bash
sudo systemctl stop edge-catcher-live-trader
sudo systemctl disable edge-catcher-live-trader
```

`stop` is **safe even mid-trade.** `systemctl stop` sends `SIGTERM`, which the
entrypoint turns into the money-safe drain (F1/F2, `TimeoutStopSec=30` budget):

1. set the operator-kill flag (stop arming new entries),
2. stop intake (the WS loop exits — no new tick can start a new order),
3. await any in-flight place→persist write to finish (shielded — never
   interrupted mid-write),
4. cancel the order-state loops,
5. close the trade store **once**,
6. send a final alert.

If the drain somehow exceeds 30s, systemd escalates to `SIGKILL`. Because a
shielded place→persist is awaited first, `stop` will not strand a half-written
real-money trade.

The paper trader keeps running throughout — its unit is untouched:

```bash
systemctl status edge-catcher-paper-trader   # still active (running)
```

To fully remove the unit (optional, after disabling):

```bash
sudo rm /etc/systemd/system/edge-catcher-live-trader.service
sudo systemctl daemon-reload
```

---

## Phase-1 exit gate (NORMATIVE — promotion criteria)

Do **not** promote out of Phase 1 (raise the caps) until **all** of the
following hold:

- **100 closed live trades**, AND
- live **`fill_pct` mean ≥ 0.85**, AND
- live **$/trade within 40% of paper** (compare the live trader's per-trade
  dollar outcome against the paper trader running the same strategies on the
  same feed), AND
- **≥5 CR-5 bundles passing** (see the next section).

Phase progression is a **YAML-only change** — there is no code change. Bump the
six caps in `config.local/live-trader.yaml` (`sizing_pct`, `daily_loss_pct`,
`drawdown_pct`, `max_open`, `min_fill_contracts`, and — if your bankroll
warrants — the static floors), then restart:

```bash
sudo systemctl restart edge-catcher-live-trader
```

The coherence gate re-validates the new caps at boot; an out-of-range value
aborts boot before trading resumes.

---

## The ≥5-real-bundle CR-5 gate (SC-I2)

CR-5 is the **live-vs-paper executor-parity** check: it proves the `LiveExecutor`
and `PaperExecutor` produce the same trade rows from the same captured-bundle
market state, so live execution will not silently diverge from the paper results
the strategy was validated on.

- **CI runs harness-correctness on a tracked synthetic fixture bundle** via
  `tests/test_executor_replay_parity.py`. That confirms the comparison machinery
  is correct and ready (entry-parity on the fixture; the tolerance is
  exit_price / blended_entry / fill_size **exact**, slippage_cents **±1¢**).
- **The authoritative parity verdict is a Pi/local runbook step**, NOT the CI
  test: run the same harness against **≥5 REAL captured bundles** on the
  live-trading host (or locally). Real bundles are **gitignored** (private-data
  scope) — they are produced by enabling the `capture:` block in
  `config.local/live-trader.yaml` and let the daily rotation assemble bundles.

This gate has two roles:

1. It is one of the four Phase-1 exit-gate criteria above.
2. Together with the **post-E dispatch sizing wire**, it is a **prerequisite to
   placing real sized orders at all** (per the Status/Scope banner). The in-E
   CR-5 harness deliberately injects a book-derived sizing equivalent so it can
   test executor-translation parity *now*, before the deferred dispatch sizing
   lands; the ≥5-real-bundle run is the real-money-grade verdict that must pass
   **after** that sizing wire ships.

---

## Quick reference

| Action | Command |
|---|---|
| Start | `sudo systemctl start edge-catcher-live-trader` |
| Stop (money-safe drain) | `sudo systemctl stop edge-catcher-live-trader` |
| Revert | `sudo systemctl stop edge-catcher-live-trader && sudo systemctl disable edge-catcher-live-trader` |
| Status | `sudo systemctl status edge-catcher-live-trader` |
| Restart counter | `systemctl show -p NRestarts edge-catcher-live-trader` |
| Logs | `tail -f /var/log/edge-catcher/live-trader.log` or `sudo journalctl -u edge-catcher-live-trader -f` |
| Boot beacon | `sudo journalctl -u edge-catcher-live-trader \| grep "engine\[G\]: live executor wired, package=edge_catcher.engine"` |
| Live DB starts flat | `sqlite3 data/live_trades.db "SELECT COUNT(*) FROM trades;"` |
