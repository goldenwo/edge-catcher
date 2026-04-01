"""Event-driven backtester for Kalshi prediction market strategies."""

import json
import math
import os
import sqlite3
import statistics
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from edge_catcher.storage.db import get_connection
from edge_catcher.storage.models import Market, Trade
from edge_catcher.runner.strategies import Signal, Strategy


# ---------------------------------------------------------------------------
# Position / CompletedTrade
# ---------------------------------------------------------------------------

@dataclass
class Position:
	ticker: str
	side: str           # 'yes' or 'no'
	entry_price: int    # cents (actual price paid, after slippage)
	entry_time: datetime
	size: int
	strategy: str


@dataclass
class CompletedTrade:
	ticker: str
	side: str
	strategy: str
	entry_price: int
	entry_time: datetime
	exit_price: int
	exit_time: datetime
	pnl_cents: int
	exit_reason: str    # 'settlement', 'take_profit', 'stop_loss'


# ---------------------------------------------------------------------------
# Portfolio
# ---------------------------------------------------------------------------

class Portfolio:
	def __init__(self, initial_cash: float, fee_pct: float = 0.07) -> None:
		self.cash: float = initial_cash
		self.initial_cash: float = initial_cash
		self.fee_pct: float = fee_pct
		self.total_fees_paid: int = 0
		self.positions: dict[tuple[str, str], Position] = {}
		self.equity_snapshots: list[tuple[datetime, float]] = []
		# Running counters (O(1) memory regardless of trade count)
		self.total_trades: int = 0
		self.wins: int = 0
		self.losses: int = 0
		self.net_pnl_cents: int = 0
		self._sum_win_pnl: float = 0.0
		self._sum_loss_pnl: float = 0.0
		self._pnl_values: list[int] = []           # all pnl ints — tiny vs full objects
		self._trade_sample: list[CompletedTrade] = []  # ring buffer, last 100
		self._per_strategy: dict[str, dict] = {}   # running per-strategy counters

	def _record_trade(self, ct: CompletedTrade) -> None:
		"""Accumulate a completed trade into running counters. O(1) per trade."""
		self.total_trades += 1
		self.net_pnl_cents += ct.pnl_cents
		self._pnl_values.append(ct.pnl_cents)
		if ct.pnl_cents > 0:
			self.wins += 1
			self._sum_win_pnl += ct.pnl_cents
		else:
			self.losses += 1
			self._sum_loss_pnl += ct.pnl_cents
		# Ring-buffer sample: keep only the last 100 full objects
		self._trade_sample.append(ct)
		if len(self._trade_sample) > 100:
			self._trade_sample.pop(0)
		# Per-strategy running counters
		s = self._per_strategy.setdefault(ct.strategy, {
			'total_trades': 0, 'wins': 0, 'losses': 0,
			'net_pnl_cents': 0, '_sum_win_pnl': 0.0, '_sum_loss_pnl': 0.0,
		})
		s['total_trades'] += 1
		s['net_pnl_cents'] += ct.pnl_cents
		if ct.pnl_cents > 0:
			s['wins'] += 1
			s['_sum_win_pnl'] += ct.pnl_cents
		else:
			s['losses'] += 1
			s['_sum_loss_pnl'] += ct.pnl_cents

	def has_position(self, ticker: str, strategy: str) -> bool:
		return (ticker, strategy) in self.positions

	def open_position(
		self,
		signal: Signal,
		strategy_name: str,
		time: datetime,
		slippage: int,
	) -> bool:
		"""Deduct cost from cash and record position. Returns False if insufficient cash."""
		actual_entry = signal.price + slippage
		cost = actual_entry * signal.size
		if cost > self.cash:
			return False
		self.cash -= cost
		self.positions[(signal.ticker, strategy_name)] = Position(
			ticker=signal.ticker,
			side=signal.side,
			entry_price=actual_entry,
			entry_time=time,
			size=signal.size,
			strategy=strategy_name,
		)
		return True

	def close_position(
		self,
		ticker: str,
		strategy: str,
		exit_price: int,
		time: datetime,
		reason: str,
		slippage: int,
	) -> Optional[CompletedTrade]:
		"""Close an open position at exit_price (slippage subtracted). Returns CompletedTrade."""
		pos = self.positions.pop((ticker, strategy), None)
		if pos is None:
			return None
		actual_exit = max(0, exit_price - slippage)
		self.cash += actual_exit * pos.size
		pnl = (actual_exit - pos.entry_price) * pos.size
		if pnl > 0 and self.fee_pct > 0:
			fee = int(pnl * self.fee_pct)
			pnl -= fee
			self.cash -= fee
			self.total_fees_paid += fee
		ct = CompletedTrade(
			ticker=ticker,
			side=pos.side,
			strategy=strategy,
			entry_price=pos.entry_price,
			entry_time=pos.entry_time,
			exit_price=actual_exit,
			exit_time=time,
			pnl_cents=pnl,
			exit_reason=reason,
		)
		self._record_trade(ct)
		return ct

	def settle_position(
		self,
		ticker: str,
		strategy: str,
		result: str,
		time: datetime,
	) -> Optional[CompletedTrade]:
		"""Settle a position at binary outcome (100 if win, 0 if loss). No slippage at settlement."""
		pos = self.positions.pop((ticker, strategy), None)
		if pos is None:
			return None
		if pos.side == 'yes':
			settlement_price = 100 if result == 'yes' else 0
		else:  # 'no'
			settlement_price = 100 if result == 'no' else 0
		self.cash += settlement_price * pos.size
		pnl = (settlement_price - pos.entry_price) * pos.size
		if pnl > 0 and self.fee_pct > 0:
			fee = int(pnl * self.fee_pct)
			pnl -= fee
			self.cash -= fee
			self.total_fees_paid += fee
		ct = CompletedTrade(
			ticker=ticker,
			side=pos.side,
			strategy=strategy,
			entry_price=pos.entry_price,
			entry_time=pos.entry_time,
			exit_price=settlement_price,
			exit_time=time,
			pnl_cents=pnl,
			exit_reason='settlement',
		)
		self._record_trade(ct)
		return ct

	def get_equity(self) -> float:
		"""Cash + positions marked at entry price (conservative: no unrealized gain/loss)."""
		position_value = sum(pos.entry_price * pos.size for pos in self.positions.values())
		return self.cash + position_value

	def snapshot(self, time: datetime) -> None:
		self.equity_snapshots.append((time, self.get_equity()))


# ---------------------------------------------------------------------------
# BacktestResult
# ---------------------------------------------------------------------------

@dataclass
class BacktestResult:
	total_trades: int
	wins: int
	losses: int
	net_pnl_cents: int
	total_fees_paid: int
	sharpe: float
	max_drawdown_pct: float
	win_rate: float
	avg_win_cents: float
	avg_loss_cents: float
	equity_curve: list[tuple[datetime, float]]
	per_strategy: dict[str, dict]
	trade_sample: list[CompletedTrade]  # last 100 completed trades (ring buffer)

	def summary(self) -> str:
		lines = [
			'=== Backtest Results ===',
			f'Total trades:    {self.total_trades}',
			f'Wins / Losses:   {self.wins} / {self.losses}',
			f'Win rate:        {self.win_rate:.1%}',
			f'Net P&L:         {self.net_pnl_cents:+d}¢  ({self.net_pnl_cents / 100:+.2f}$)',
			f'Fees paid:       {self.total_fees_paid}¢  ({self.total_fees_paid / 100:.2f}$)',
			f'Avg win:         {self.avg_win_cents:+.1f}¢',
			f'Avg loss:        {self.avg_loss_cents:+.1f}¢',
			f'Sharpe:          {self.sharpe:.3f}',
			f'Max drawdown:    {self.max_drawdown_pct:.2f}%',
			'',
			'--- Per-Strategy ---',
		]
		for strat, s in sorted(self.per_strategy.items()):
			lines.append(
				f"  [{strat}] trades={s['total_trades']} "
				f"wins={s['wins']} losses={s['losses']} "
				f"win_rate={s['win_rate']:.1%} "
				f"net_pnl={s['net_pnl_cents']:+d}¢"
			)
		return '\n'.join(lines)

	def to_dict(self) -> dict:
		return {
			'total_trades': self.total_trades,
			'wins': self.wins,
			'losses': self.losses,
			'net_pnl_cents': self.net_pnl_cents,
			'total_fees_paid': self.total_fees_paid,
			'sharpe': self.sharpe,
			'max_drawdown_pct': self.max_drawdown_pct,
			'win_rate': self.win_rate,
			'avg_win_cents': self.avg_win_cents,
			'avg_loss_cents': self.avg_loss_cents,
			'equity_curve': [(t.isoformat(), e) for t, e in self.equity_curve[-1000:]],  # keep last 1000 points
			'per_strategy': self.per_strategy,
			'trade_log': [
				{
					'ticker': ct.ticker,
					'side': ct.side,
					'strategy': ct.strategy,
					'entry_price': ct.entry_price,
					'entry_time': ct.entry_time.isoformat(),
					'exit_price': ct.exit_price,
					'exit_time': ct.exit_time.isoformat(),
					'pnl_cents': ct.pnl_cents,
					'exit_reason': ct.exit_reason,
				}
				for ct in self.trade_sample  # at most 100 entries (ring buffer)
			],
		}


# ---------------------------------------------------------------------------
# DB helpers (local, avoids re-importing private symbols from db.py)
# ---------------------------------------------------------------------------

def _parse_dt(s: Optional[str]) -> Optional[datetime]:
	if not s:
		return None
	dt = datetime.fromisoformat(s)
	if dt.tzinfo is None:
		dt = dt.replace(tzinfo=timezone.utc)
	return dt


def _row_to_market(row: sqlite3.Row) -> Market:
	return Market(
		ticker=row['ticker'],
		event_ticker=row['event_ticker'],
		series_ticker=row['series_ticker'],
		title=row['title'],
		status=row['status'],
		result=row['result'],
		yes_bid=row['yes_bid'],
		yes_ask=row['yes_ask'],
		last_price=row['last_price'],
		open_interest=row['open_interest'],
		volume=row['volume'],
		expiration_time=_parse_dt(row['expiration_time']),
		close_time=_parse_dt(row['close_time']),
		created_time=_parse_dt(row['created_time']),
		settled_time=_parse_dt(row['settled_time']),
		open_time=_parse_dt(row['open_time']),
		notional_value=row['notional_value'],
		floor_strike=row['floor_strike'],
		cap_strike=row['cap_strike'],
		raw_data=row['raw_data'],
	)


def _row_to_trade(row: sqlite3.Row) -> Trade:
	return Trade(
		trade_id=row['trade_id'],
		ticker=row['ticker'],
		yes_price=row['yes_price'],
		no_price=row['no_price'],
		count=row['count'],
		taker_side=row['taker_side'],
		created_time=_parse_dt(row['created_time']) or datetime.now(timezone.utc),
		raw_data=row['raw_data'],
	)


# ---------------------------------------------------------------------------
# Metrics computation
# ---------------------------------------------------------------------------

def _compute_metrics(
	portfolio: 'Portfolio',
	equity_snapshots: list[tuple[datetime, float]],
) -> tuple[float, float, float, float, float, int, int, dict[str, dict]]:
	"""Returns (sharpe, max_drawdown_pct, win_rate, avg_win, avg_loss, wins, losses, per_strategy).
	Reads pre-accumulated counters from portfolio — O(1) space, O(trades) time for Sharpe only."""
	total = portfolio.total_trades
	wins = portfolio.wins
	losses = portfolio.losses
	win_rate = wins / total if total > 0 else 0.0
	avg_win = portfolio._sum_win_pnl / wins if wins > 0 else 0.0
	avg_loss = portfolio._sum_loss_pnl / losses if losses > 0 else 0.0

	# Sharpe from _pnl_values (list of ints, negligible memory vs full objects)
	sharpe = 0.0
	if total >= 2:
		std = statistics.stdev(portfolio._pnl_values)
		if std > 0:
			sharpe = statistics.mean(portfolio._pnl_values) / std * math.sqrt(total)

	# Max drawdown from equity snapshots
	max_dd = 0.0
	if len(equity_snapshots) >= 2:
		peak = equity_snapshots[0][1]
		for _, eq in equity_snapshots:
			if eq > peak:
				peak = eq
			if peak > 0:
				dd = (peak - eq) / peak * 100.0
				if dd > max_dd:
					max_dd = dd

	# Finalize per-strategy dicts (convert running sums to rates/averages)
	per_strategy: dict[str, dict] = {}
	for strat, s in portfolio._per_strategy.items():
		t = s['total_trades']
		per_strategy[strat] = {
			'total_trades': t,
			'wins': s['wins'],
			'losses': s['losses'],
			'net_pnl_cents': s['net_pnl_cents'],
			'win_rate': s['wins'] / t if t > 0 else 0.0,
			'avg_win_cents': s['_sum_win_pnl'] / s['wins'] if s['wins'] > 0 else 0.0,
			'avg_loss_cents': s['_sum_loss_pnl'] / s['losses'] if s['losses'] > 0 else 0.0,
		}

	return sharpe, max_dd, win_rate, avg_win, avg_loss, wins, losses, per_strategy


# ---------------------------------------------------------------------------
# EventBacktester
# ---------------------------------------------------------------------------

class EventBacktester:
	"""Stream trades from DB in time order, feed to strategies, settle on close_time."""

	def run(
		self,
		series: str,
		strategies: list[Strategy],
		start: Optional[date] = None,
		end: Optional[date] = None,
		initial_cash: float = 10000.0,
		slippage_cents: int = 1,
		db_path: Path = Path('data/kalshi.db'),
		fee_pct: float = 0.07,
	) -> BacktestResult:
		# Ensure SQLite temp files go to the DB directory, not /tmp (which may be a small tmpfs)
		db_dir = str(Path(db_path).parent.resolve())
		os.environ.setdefault('SQLITE_TMPDIR', db_dir)
		conn = get_connection(db_path)
		try:
			return self._run(conn, series, strategies, start, end, initial_cash, slippage_cents, fee_pct)
		finally:
			conn.close()

	def _run(
		self,
		conn: sqlite3.Connection,
		series: str,
		strategies: list[Strategy],
		start: Optional[date],
		end: Optional[date],
		initial_cash: float,
		slippage_cents: int,
		fee_pct: float = 0.07,
	) -> BacktestResult:
		# --- 1. Load markets for series (with optional date bounds) ---
		market_query = 'SELECT * FROM markets WHERE series_ticker = ?'
		market_params: list = [series]
		if start:
			market_query += ' AND close_time >= ?'
			market_params.append(start.isoformat())
		if end:
			end_dt = end + timedelta(days=1)
			market_query += ' AND open_time <= ?'
			market_params.append(end_dt.isoformat())

		market_rows = conn.execute(market_query, market_params).fetchall()
		market_map: dict[str, Market] = {}
		for row in market_rows:
			m = _row_to_market(row)
			market_map[m.ticker] = m

		if not market_map:
			# Return empty result if no matching markets
			return BacktestResult(
				total_trades=0, wins=0, losses=0, net_pnl_cents=0,
				total_fees_paid=0,
				sharpe=0.0, max_drawdown_pct=0.0, win_rate=0.0,
				avg_win_cents=0.0, avg_loss_cents=0.0,
				equity_curve=[], per_strategy={}, trade_sample=[],
			)

		tickers = list(market_map.keys())
		portfolio = Portfolio(initial_cash, fee_pct=fee_pct)

		# --- 2. Build temp table for efficient ticker join ---
		conn.execute('CREATE TEMP TABLE IF NOT EXISTS _bt_tickers (ticker TEXT PRIMARY KEY)')
		conn.execute('DELETE FROM _bt_tickers')
		conn.executemany('INSERT INTO _bt_tickers VALUES (?)', [(t,) for t in tickers])

		# --- 3. Stream trades in time order ---
		trade_query = '''
			SELECT t.trade_id, t.ticker, t.yes_price, t.no_price, t.count,
			       t.taker_side, t.created_time, t.raw_data
			FROM trades t
			INNER JOIN _bt_tickers bt ON t.ticker = bt.ticker
		'''
		trade_params: list = []
		if start:
			trade_query += ' WHERE t.created_time >= ?'
			trade_params.append(start.isoformat())
		if end:
			sep = ' AND ' if start else ' WHERE '
			end_dt = end + timedelta(days=1)
			trade_query += f'{sep}t.created_time < ?'
			trade_params.append(end_dt.isoformat())
		trade_query += ' ORDER BY t.created_time ASC'

		cursor = conn.execute(trade_query, trade_params)
		trade_count = 0

		for row in cursor:
			trade = _row_to_trade(row)
			market = market_map.get(trade.ticker)
			if market is None:
				continue

			# --- 4a. Lazy settlement sweep ---
			# Iterate over a snapshot of keys; settle_position pops from the dict
			for (t_ticker, t_strategy) in list(portfolio.positions.keys()):
				if (t_ticker, t_strategy) not in portfolio.positions:
					continue
				pos_market = market_map.get(t_ticker)
				if (
					pos_market is not None
					and pos_market.close_time is not None
					and trade.created_time > pos_market.close_time
					and pos_market.result is not None
				):
					portfolio.settle_position(
						t_ticker, t_strategy, pos_market.result,
						pos_market.close_time,
					)

			trade_count += 1
			if trade_count % 10000 == 0:  # was 1000 — reduce snapshot frequency to save RAM
				portfolio.snapshot(trade.created_time)

			# --- 4b. Skip strategy dispatch for trades from closed markets ---
			if market.close_time is not None and trade.created_time > market.close_time:
				continue

			for strategy in strategies:
				signals = strategy.on_trade(trade, market, portfolio)
				for signal in signals:
					if signal.action == 'buy':
						portfolio.open_position(signal, strategy.name, trade.created_time, slippage_cents)
					elif signal.action == 'sell':
						reason = (
							'take_profit' if 'take_profit' in signal.reason
							else 'stop_loss' if 'stop_loss' in signal.reason
							else 'manual'
						)
						portfolio.close_position(
							signal.ticker, strategy.name, signal.price,
							trade.created_time, reason, slippage_cents,
						)

	
		# --- 5. Final settlement sweep ---
		for (t_ticker, t_strategy) in list(portfolio.positions.keys()):
			pos_market = market_map.get(t_ticker)
			if pos_market is not None and pos_market.result is not None:
				settle_time = pos_market.close_time or datetime.now(timezone.utc)
				portfolio.settle_position(t_ticker, t_strategy, pos_market.result, settle_time)

		# Final equity snapshot
		if portfolio.equity_snapshots:
			last_time = portfolio.equity_snapshots[-1][0]
		else:
			last_time = datetime.now(timezone.utc)
		portfolio.snapshot(last_time)

		conn.execute('DROP TABLE IF EXISTS _bt_tickers')

		# --- 6. Compute metrics from running accumulators ---
		sharpe, max_dd, win_rate, avg_win, avg_loss, wins, losses, per_strategy = _compute_metrics(
			portfolio, portfolio.equity_snapshots,
		)

		return BacktestResult(
			total_trades=portfolio.total_trades,
			wins=wins,
			losses=losses,
			net_pnl_cents=portfolio.net_pnl_cents,
			total_fees_paid=portfolio.total_fees_paid,
			sharpe=sharpe,
			max_drawdown_pct=max_dd,
			win_rate=win_rate,
			avg_win_cents=avg_win,
			avg_loss_cents=avg_loss,
			equity_curve=portfolio.equity_snapshots,
			per_strategy=per_strategy,
			trade_sample=portfolio._trade_sample,
		)
