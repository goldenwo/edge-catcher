"""argparse CLI for live order placement.

Verbs: place / cancel / status / balance / positions.
Confirmation: interactive by default; `--yes` skips.
Dollar cap: enforced before any HTTP call. The library has its own ABSOLUTE_MAX
backstop; this CLI cap (from live-trader.yaml) is the user-facing dev-mode floor.
"""

from __future__ import annotations
import argparse
import asyncio
import re
import sys
from datetime import datetime, timezone

from edge_catcher.live.audit import AuditLogger
from edge_catcher.live.client import (
	KalshiOrderClient,
	OrderRequest,
)
from edge_catcher.live.config import load_config, ABSOLUTE_MAX_ORDER_DOLLARS, LiveConfig
from edge_catcher.live.errors import (
	CapExceededError,
	KalshiAPIError,
	LiveError,
)


def main(argv: list[str] | None = None) -> int:
	"""Sync CLI entry point — wraps the async client at this boundary."""
	return asyncio.run(_main_async(argv))


async def _main_async(argv: list[str] | None) -> int:
	parser = _build_parser()
	args = parser.parse_args(argv)
	if not args.verb:
		parser.print_help()
		return 2
	cfg = load_config()
	audit = AuditLogger(cfg.audit_log_path)
	async with KalshiOrderClient(cfg, audit) as client:
		try:
			return await _dispatch(args, client, cfg)
		except CapExceededError as e:
			print(f"REJECTED: {e}", file=sys.stderr)
			return 3
		except KalshiAPIError as e:
			print(f"KALSHI ERROR: {e}", file=sys.stderr)
			return 4
		except LiveError as e:
			print(f"LIVE ERROR: {e}", file=sys.stderr)
			return 5
		except KeyError as e:
			# Missing required env var (KALSHI_KEY_ID / KALSHI_PRIVATE_KEY).
			# auth.py raises bare KeyError; surface it via the LiveError exit
			# path per spec §Error handling.
			print(f"LIVE ERROR: Missing required env var: {e}", file=sys.stderr)
			return 5


def _build_parser() -> argparse.ArgumentParser:
	parser = argparse.ArgumentParser(
		prog="edge_catcher.live",
		description="Live order placement CLI for Kalshi (Sub-project A)",
	)
	sub = parser.add_subparsers(dest="verb")

	p_place = sub.add_parser("place", help="Place a Kalshi limit order")
	p_place.add_argument("--ticker", required=True)
	p_place.add_argument("--side", choices=["yes", "no"], required=True)
	p_place.add_argument("--action", choices=["buy", "sell"], default="buy")
	p_place.add_argument("--count", type=int, required=True)
	p_place.add_argument("--price", type=int, required=True, help="Limit price in cents (1-99)")
	p_place.add_argument("--tif", choices=["gtc", "ioc", "fok"], default="gtc")
	p_place.add_argument("--yes", action="store_true", help="Skip confirmation prompt")

	p_cancel = sub.add_parser("cancel", help="Cancel an order by ID")
	p_cancel.add_argument("order_id")
	p_cancel.add_argument("--yes", action="store_true")

	p_status = sub.add_parser("status", help="Look up an order by ID")
	p_status.add_argument("order_id")

	sub.add_parser("balance", help="Show account balance")
	sub.add_parser("positions", help="Show open positions")

	p_kill_clear = sub.add_parser(
		"kill-clear",
		help="Clear an auto-tripped kill switch row (operator ack)",
	)
	p_kill_clear.add_argument(
		"kill_id",
		type=int,
		help="ID of the kill_switch row to clear (see live_trades.db)",
	)
	p_kill_clear.add_argument(
		"--note",
		default="",
		help="Optional operator note (appended to cleared_by field)",
	)

	return parser


async def _dispatch(args: argparse.Namespace, client: KalshiOrderClient, cfg) -> int:
	if args.verb == "place":
		return await _do_place(args, client, cfg)
	if args.verb == "cancel":
		return await _do_cancel(args, client)
	if args.verb == "status":
		return await _do_status(args, client)
	if args.verb == "balance":
		return await _do_balance(client)
	if args.verb == "positions":
		return await _do_positions(client)
	if args.verb == "kill-clear":
		return _do_kill_clear(args, cfg)
	return 2


async def _do_place(args: argparse.Namespace, client: KalshiOrderClient, cfg) -> int:
	req = OrderRequest(
		ticker=args.ticker,
		action=args.action,
		side=args.side,
		count=args.count,
		limit_price_cents=args.price,
		time_in_force=args.tif,
	)
	exposure = req.exposure_dollars
	if exposure > cfg.cli_max_order_dollars:
		raise CapExceededError(exposure, cfg.cli_max_order_dollars, "CLI cap")

	if not args.yes:
		_print_place_confirmation(req, cfg)
		ans = input("Confirm? [y/N]: ").strip().lower()
		if ans not in ("y", "yes"):
			print("Cancelled.")
			return 1

	order = await client.place(req)
	print(
		f"Placed order_id={order.order_id} status={order.status}"
		f" count={order.count} price={order.limit_price_cents}c"
	)
	return 0


def _print_place_confirmation(req: OrderRequest, cfg) -> None:
	print("About to place order:")
	print(f"  Market:    {req.ticker}")
	print(f"  Side:      {req.side} ({req.action})")
	print(f"  Count:     {req.count} contracts")
	print(f"  Price:     {req.limit_price_cents}c (limit)")
	print(f"  TIF:       {req.time_in_force}")
	print(f"  Exposure:  ${req.exposure_dollars:.2f}  "
		  f"(CLI cap: ${cfg.cli_max_order_dollars:.2f}, "
		  f"ABS_MAX: ${ABSOLUTE_MAX_ORDER_DOLLARS:.2f})")


async def _do_cancel(args: argparse.Namespace, client: KalshiOrderClient) -> int:
	if not args.yes:
		ans = input(f"Cancel order {args.order_id}? [y/N]: ").strip().lower()
		if ans not in ("y", "yes"):
			print("Aborted.")
			return 1
	result = await client.cancel(args.order_id)
	print(f"Cancelled order_id={result.order_id} status={result.status}")
	return 0


async def _do_status(args: argparse.Namespace, client: KalshiOrderClient) -> int:
	order = await client.status(args.order_id)
	print(f"order_id={order.order_id}")
	print(f"  ticker:    {order.ticker}")
	print(f"  side:      {order.side} ({order.action})")
	print(f"  count:     {order.count} ({order.filled_count} filled)")
	print(f"  price:     {order.limit_price_cents}c")
	print(f"  tif:       {order.time_in_force}")
	print(f"  status:    {order.status}")
	return 0


async def _do_balance(client: KalshiOrderClient) -> int:
	bal = await client.balance()
	print(f"balance: ${bal.balance_cents / 100:.2f}")
	return 0


async def _do_positions(client: KalshiOrderClient) -> int:
	positions = await client.positions()
	if not positions:
		print("(no open positions)")
		return 0
	for p in positions:
		print(f"  {p.ticker:30s} {p.side} count={p.count} avg=${p.average_price_cents / 100:.4f}")
	return 0


# Charset + length limit for operator notes that land in the ``cleared_by``
# audit column. Stripping non-printable / non-ASCII characters keeps log
# rendering and UI display predictable; the 200-char ceiling matches the
# practical width of audit table columns without over-truncating real notes.
_AUDIT_NOTE_DISALLOWED = re.compile(r"[^\x20-\x7E]")
_AUDIT_NOTE_MAX_LEN = 200


def _sanitize_audit_note(note: str) -> str:
	"""Strip non-printable / non-ASCII chars from an operator note and
	truncate to 200 chars before it lands in the ``cleared_by`` column.

	Failure mode prevented: a stray newline, ANSI escape, NUL byte, or
	emoji in ``--note`` would corrupt downstream log rendering and any
	UI / Discord webhook that renders the audit row. Sanitization runs
	silently (vs. loud rejection) because operator-CLI ergonomics favour
	"strip the tab, keep the note" over "re-run because you had a stray
	character".
	"""
	cleaned = _AUDIT_NOTE_DISALLOWED.sub("", note).strip()
	return cleaned[:_AUDIT_NOTE_MAX_LEN]


def _do_kill_clear(args: argparse.Namespace, cfg: LiveConfig) -> int:
	"""Clear an auto-tripped kill switch row (Sub-project C operator command).

	Calls KillSwitch.clear(kill_id, cleared_by, now) so the gate stops
	rejecting signals on the next tick after operator review.

	KillSwitch lives in engine/risk.py (Agent A's scope); this CLI command
	imports it at call time so the live/cli module doesn't fail to import
	when risk.py is absent during paper-only deployments.

	Args:
		args.kill_id: Integer ID from the kill_switch table.
		args.note:    Optional operator note appended to the cleared_by field.
	"""
	import sqlite3  # noqa: PLC0415
	from pathlib import Path  # noqa: PLC0415

	db_path = Path(getattr(cfg, "db_path", "data/live_trades.db"))
	if not db_path.exists():
		print(f"ERROR: live_trades.db not found at {db_path}", file=sys.stderr)
		print("Ensure the live trader has run at least once to create the DB.", file=sys.stderr)
		return 1

	try:
		from edge_catcher.engine.risk import KillSwitch  # noqa: PLC0415
	except ImportError:
		print(
			"ERROR: engine/risk.py not available — kill-clear requires Sub-project C (PR 3).",
			file=sys.stderr,
		)
		return 1

	now = datetime.now(timezone.utc)
	cleared_by = "operator-cli"
	if args.note:
		cleared_by = f"operator-cli: {_sanitize_audit_note(args.note)}"

	conn = sqlite3.connect(str(db_path))
	conn.row_factory = sqlite3.Row
	try:
		ks = KillSwitch(conn)
		ks.clear(args.kill_id, cleared_by=cleared_by, now=now)
		conn.commit()
		print(f"Cleared kill_switch row id={args.kill_id} (cleared_by={cleared_by!r})")
		return 0
	except Exception as exc:
		print(f"ERROR: {exc}", file=sys.stderr)
		return 1
	finally:
		conn.close()


if __name__ == "__main__":
	raise SystemExit(main())
