"""Event-driven backtester for prediction market strategies."""

import heapq
import math
import os
import sqlite3
import statistics
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Callable, Literal, Optional

from edge_catcher.fees import ZERO_FEE
from edge_catcher.storage.db import get_connection
from edge_catcher.storage.models import Market, Trade
from edge_catcher.runner.strategies import Signal, Strategy

if TYPE_CHECKING:
	from edge_catcher.research.data_source_resolver import ResolvedSource


# ---------------------------------------------------------------------------
# Position / CompletedTrade
# ---------------------------------------------------------------------------

@dataclass
class Position:
	ticker: str
	side: Literal['yes', 'no']
	entry_price: int    # cents (actual price paid, after slippage)
	entry_time: datetime
	size: int
	strategy: str
	entry_fee: float = 0.0


@dataclass
class CompletedTrade:
	ticker: str
	side: Literal['yes', 'no']
	strategy: str
	entry_price: int
	entry_time: datetime
	exit_price: int
	exit_time: datetime
	pnl_cents: float
	exit_reason: str    # 'settlement', 'take_profit', 'stop_loss'
	fee_cents: float = 0.0


# ---------------------------------------------------------------------------
# Portfolio
# ---------------------------------------------------------------------------

class Portfolio:
	def __init__(self, initial_cash: float, fee_fn: Optional[Callable[[int, int], float]] = None) -> None:
		self.cash: float = initial_cash
		self.initial_cash: float = initial_cash
		self.fee_fn: Callable[[int, int], float] = fee_fn or ZERO_FEE.calculate
		self.total_fees_paid: float = 0.0
		self.positions: dict[tuple[str, str], Position] = {}
		self.equity_snapshots: list[tuple[datetime, float]] = []
		# Running counters (O(1) memory regardless of trade count)
		self.total_trades: int = 0
		self.wins: int = 0
		self.losses: int = 0
		self.net_pnl_cents: int = 0
		self._sum_win_pnl: float = 0.0
		self._sum_loss_pnl: float = 0.0
		self._pnl_values: list[float] = []         # all pnl values — float to match CompletedTrade.pnl_cents
		self._trade_sample: list[CompletedTrade] = []  # ring buffer, last 100
		self._per_strategy: dict[str, dict] = {}   # running per-strategy counters
		self._per_strategy_curves: dict[str, list[tuple[datetime, float]]] = {}  # cumulative P&L curves

	def _record_trade(self, ct: CompletedTrade) -> None:
		"""Accumulate a completed trade into running counters. O(1) per trade."""
		self.total_trades += 1
		# Round to int when accumulating into the integer-cents counter; the
		# precise float value is preserved in _pnl_values for Sharpe.
		self.net_pnl_cents += int(round(ct.pnl_cents))
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
			'_pnl_values': [],
		})
		s['total_trades'] += 1
		s['net_pnl_cents'] += ct.pnl_cents
		s['_pnl_values'].append(ct.pnl_cents)
		if ct.pnl_cents > 0:
			s['wins'] += 1
			s['_sum_win_pnl'] += ct.pnl_cents
		else:
			s['losses'] += 1
			s['_sum_loss_pnl'] += ct.pnl_cents
		# Per-strategy equity curve (initial cash + cumulative P&L)
		curve = self._per_strategy_curves.setdefault(ct.strategy, [])
		curve.append((ct.exit_time, self.initial_cash + s['net_pnl_cents']))

	def has_position(self, ticker: str, strategy: str) -> bool:
		return (ticker, strategy) in self.positions

	def open_position(
		self,
		signal: Signal,
		strategy_name: str,
		time: datetime,
		slippage: int,
		fee_fn: Optional[Callable[[int, int], float]] = None,
	) -> bool:
		"""Deduct cost and entry fee from cash and record position. Returns False if insufficient cash."""
		actual_entry = signal.price + slippage
		cost = actual_entry * signal.size
		fee = (fee_fn or self.fee_fn)(actual_entry, signal.size)
		if cost + fee > self.cash:
			return False
		self.cash -= cost + fee
		self.total_fees_paid += fee
		self.positions[(signal.ticker, strategy_name)] = Position(
			ticker=signal.ticker,
			side=signal.side,
			entry_price=actual_entry,
			entry_time=time,
			size=signal.size,
			strategy=strategy_name,
			entry_fee=fee,
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
		fee_fn: Optional[Callable[[int, int], float]] = None,
	) -> Optional[CompletedTrade]:
		"""Close an open position at exit_price (slippage subtracted). Returns CompletedTrade."""
		pos = self.positions.pop((ticker, strategy), None)
		if pos is None:
			return None
		actual_exit = max(0, exit_price - slippage)
		exit_fee = (fee_fn or self.fee_fn)(actual_exit, pos.size)
		self.cash += actual_exit * pos.size - exit_fee
		self.total_fees_paid += exit_fee
		pnl = (actual_exit - pos.entry_price) * pos.size - pos.entry_fee - exit_fee
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
			fee_cents=pos.entry_fee + exit_fee,
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
		pnl = (settlement_price - pos.entry_price) * pos.size - pos.entry_fee
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
			fee_cents=pos.entry_fee,
		)
		self._record_trade(ct)
		return ct

	def get_equity(self) -> float:
		"""Cash + positions marked at entry price (conservative: no unrealized gain/loss)."""
		position_value = sum(pos.entry_price * pos.size for pos in self.positions.values())
		return self.cash + position_value

	def snapshot(self, time: datetime) -> None:
		self.equity_snapshots.append((time, self.get_equity()))


def _downsample(pts: list[tuple[datetime, float]], max_pts: int) -> list[tuple[str, float]]:
	"""Evenly downsample a time series to max_pts, always keeping first and last."""
	n = len(pts)
	if n <= max_pts:
		return [(t.isoformat(), v) for t, v in pts]
	indices = {0, n - 1}
	for i in range(1, max_pts - 1):
		indices.add(round(i * (n - 1) / (max_pts - 1)))
	return [(pts[i][0].isoformat(), pts[i][1]) for i in sorted(indices)]


# ---------------------------------------------------------------------------
# BacktestResult
# ---------------------------------------------------------------------------

@dataclass
class BacktestResult:
	total_trades: int
	wins: int
	losses: int
	net_pnl_cents: int
	total_fees_paid: float
	sharpe: float
	max_drawdown_pct: float
	win_rate: float
	avg_win_cents: float
	avg_loss_cents: float
	equity_curve: list[tuple[datetime, float]]
	per_strategy: dict[str, dict]
	per_strategy_curves: dict[str, list[tuple[datetime, float]]]
	trade_sample: list[CompletedTrade]  # last 100 completed trades (ring buffer)
	# All per-trade P&L in cents; float matches CompletedTrade.pnl_cents.
	pnl_values: list[float] = field(default_factory=list)

	def summary(self) -> str:
		gross_pnl = self.net_pnl_cents + self.total_fees_paid
		lines = [
			'=== Backtest Results ===',
			f'Total trades:    {self.total_trades}',
			f'Wins / Losses:   {self.wins} / {self.losses}',
			f'Win rate:        {self.win_rate:.1%}',
			f'Gross P&L:       {gross_pnl:+.2f}¢  ({gross_pnl / 100:+.2f}$)',
			f'Fees paid:       {self.total_fees_paid:.2f}¢  ({self.total_fees_paid / 100:.2f}$)',
			f'Net P&L:         {self.net_pnl_cents:+.2f}¢  ({self.net_pnl_cents / 100:+.2f}$)',
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
				f"net_pnl={s['net_pnl_cents']:+.0f}¢"
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
			'equity_curve': _downsample(self.equity_curve, 1000),
			'per_strategy': self.per_strategy,
			'per_strategy_curves': {
				name: _downsample(pts, 1000)
				for name, pts in self.per_strategy_curves.items()
			},
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
					'fee_cents': ct.fee_cents,
					'exit_reason': ct.exit_reason,
				}
				for ct in self.trade_sample  # at most 100 entries (ring buffer)
			],
			'pnl_values': self.pnl_values,
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
# TradeStream / merge_streams — multi-DB support
# ---------------------------------------------------------------------------

class TradeStream:
	"""Yields (trade, market, db_path) tuples from a single DB in time order."""

	def __init__(self, db_path: str, series: str, start: date | None = None, end: date | None = None):
		self.db_path = db_path
		self.series = series
		self.start = start
		self.end = end

	def iter_trades(self):
		"""Yield (trade, market, db_path) tuples sorted by created_time."""
		conn = get_connection(Path(self.db_path))
		try:
			# Load markets
			market_query = 'SELECT * FROM markets WHERE series_ticker = ?'
			params: list = [self.series]
			if self.start:
				market_query += ' AND close_time >= ?'
				params.append(self.start.isoformat())
			if self.end:
				end_dt = self.end + timedelta(days=1)
				market_query += ' AND open_time <= ?'
				params.append(end_dt.isoformat())

			market_map = {}
			for row in conn.execute(market_query, params).fetchall():
				m = _row_to_market(row)
				market_map[m.ticker] = m

			if not market_map:
				return

			tickers = list(market_map.keys())
			conn.execute('CREATE TEMP TABLE IF NOT EXISTS _bt_tickers (ticker TEXT PRIMARY KEY)')
			conn.execute('DELETE FROM _bt_tickers')
			conn.executemany('INSERT INTO _bt_tickers VALUES (?)', [(t,) for t in tickers])

			where_clauses = []
			trade_params: list = []
			if self.start:
				where_clauses.append('t.created_time >= ?')
				trade_params.append(self.start.isoformat())
			if self.end:
				end_dt = self.end + timedelta(days=1)
				where_clauses.append('t.created_time < ?')
				trade_params.append(end_dt.isoformat())
			where_sql = (' WHERE ' + ' AND '.join(where_clauses)) if where_clauses else ''

			trade_query = f'''
				SELECT t.trade_id, t.ticker, t.yes_price, t.no_price, t.count,
				       t.taker_side, t.created_time, t.raw_data
				FROM trades t
				INNER JOIN _bt_tickers bt ON t.ticker = bt.ticker
				{where_sql}
				ORDER BY t.created_time ASC
			'''

			for row in conn.execute(trade_query, trade_params):
				trade = _row_to_trade(row)
				market = market_map.get(trade.ticker)
				if market is not None:
					yield (trade, market, self.db_path)
		finally:
			conn.close()


def merge_streams(streams: list[TradeStream]):
	"""Merge multiple TradeStreams in time order. Deterministic tiebreaker on db_path."""
	if len(streams) == 1:
		yield from streams[0].iter_trades()
		return

	def keyed(stream):
		for trade, market, db_path in stream.iter_trades():
			yield (trade.created_time.isoformat(), db_path), (trade, market, db_path)

	for _key, item in heapq.merge(*[keyed(s) for s in streams]):
		yield item


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
		strat_sharpe = 0.0
		pnls = s['_pnl_values']
		if t >= 2:
			std = statistics.stdev(pnls)
			if std > 0:
				strat_sharpe = statistics.mean(pnls) / std * math.sqrt(t)
		per_strategy[strat] = {
			'total_trades': t,
			'wins': s['wins'],
			'losses': s['losses'],
			'net_pnl_cents': s['net_pnl_cents'],
			'win_rate': s['wins'] / t if t > 0 else 0.0,
			'avg_win_cents': s['_sum_win_pnl'] / s['wins'] if s['wins'] > 0 else 0.0,
			'avg_loss_cents': s['_sum_loss_pnl'] / s['losses'] if s['losses'] > 0 else 0.0,
			'sharpe': strat_sharpe,
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
		*,
		db_path: Path,
		start: Optional[date] = None,
		end: Optional[date] = None,
		initial_cash: float = 10000.0,
		slippage_cents: int = 1,
		fee_fn: Optional[Callable[[int, int], float]] = None,
		on_progress: Optional[Callable[[dict], None]] = None,
		is_cancelled: Optional[Callable[[], bool]] = None,
	) -> BacktestResult:
		# Ensure SQLite temp files go to the DB directory, not /tmp (which may be a small tmpfs)
		db_dir = str(Path(db_path).parent.resolve())
		os.environ.setdefault('SQLITE_TMPDIR', db_dir)
		conn = get_connection(db_path)
		try:
			return self._run(
				conn, series, strategies, start, end, initial_cash, slippage_cents,
				fee_fn, on_progress, is_cancelled,
			)
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
		fee_fn: Optional[Callable[[int, int], float]] = None,
		on_progress: Optional[Callable[[dict], None]] = None,
		is_cancelled: Optional[Callable[[], bool]] = None,
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

		_cancelled = is_cancelled or (lambda: False)
		_empty = BacktestResult(
			total_trades=0, wins=0, losses=0, net_pnl_cents=0,
			total_fees_paid=0,
			sharpe=0.0, max_drawdown_pct=0.0, win_rate=0.0,
			avg_win_cents=0.0, avg_loss_cents=0.0,
			equity_curve=[], per_strategy={}, per_strategy_curves={}, trade_sample=[],
			pnl_values=[],
		)

		if not market_map or _cancelled():
			return _empty

		tickers = list(market_map.keys())
		portfolio = Portfolio(initial_cash, fee_fn=fee_fn)

		# --- 2. Build temp table for efficient ticker join ---
		conn.execute('CREATE TEMP TABLE IF NOT EXISTS _bt_tickers (ticker TEXT PRIMARY KEY)')
		conn.execute('DELETE FROM _bt_tickers')
		conn.executemany('INSERT INTO _bt_tickers VALUES (?)', [(t,) for t in tickers])

		if _cancelled():
			conn.execute('DROP TABLE IF EXISTS _bt_tickers')
			return _empty

		# --- 3. Build shared WHERE clause for trade queries ---
		where_clauses = []
		trade_params: list = []
		if start:
			where_clauses.append('t.created_time >= ?')
			trade_params.append(start.isoformat())
		if end:
			end_dt = end + timedelta(days=1)
			where_clauses.append('t.created_time < ?')
			trade_params.append(end_dt.isoformat())
		where_sql = (' WHERE ' + ' AND '.join(where_clauses)) if where_clauses else ''

		# Estimated total for progress reporting
		estimated_total = conn.execute(
			f'SELECT COUNT(*) FROM trades t INNER JOIN _bt_tickers bt ON t.ticker = bt.ticker{where_sql}',
			trade_params,
		).fetchone()[0]

		if _cancelled():
			conn.execute('DROP TABLE IF EXISTS _bt_tickers')
			return _empty

		# Fire initial progress callback
		if on_progress is not None:
			on_progress({
				"trades_processed": 0,
				"trades_estimated": estimated_total,
				"total_trades": 0,
				"wins": 0,
				"losses": 0,
				"net_pnl_cents": 0,
			})

		trade_query = f'''
			SELECT t.trade_id, t.ticker, t.yes_price, t.no_price, t.count,
			       t.taker_side, t.created_time, t.raw_data
			FROM trades t
			INNER JOIN _bt_tickers bt ON t.ticker = bt.ticker
			{where_sql}
			ORDER BY t.created_time ASC
		'''

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
			if trade_count % 1000 == 0:
				if _cancelled():
					break
				if trade_count % 10000 == 0:
					portfolio.snapshot(trade.created_time)
				if on_progress is not None:
					on_progress({
						"trades_processed": trade_count,
						"trades_estimated": estimated_total,
						"total_trades": portfolio.total_trades,
						"wins": portfolio.wins,
						"losses": portfolio.losses,
						"net_pnl_cents": portfolio.net_pnl_cents,
					})

			# --- 4b. Skip strategy dispatch for trades from closed markets ---
			if market.close_time is not None and trade.created_time > market.close_time:
				continue

			for strategy in strategies:
				signals = strategy.on_trade(trade, market, portfolio)
				for signal in signals:
					# Binary contract prices are 1-99¢ — skip impossible prices
					if not (1 <= signal.price <= 99):
						continue
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


		cursor.close()

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

		# Prepend initial data point to each per-strategy curve
		if portfolio.equity_snapshots:
			start_time = portfolio.equity_snapshots[0][0]
			for curve in portfolio._per_strategy_curves.values():
				curve.insert(0, (start_time, portfolio.initial_cash))

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
			per_strategy_curves=portfolio._per_strategy_curves,
			trade_sample=portfolio._trade_sample,
			pnl_values=list(portfolio._pnl_values),
		)

	def run_multi(
		self,
		resolved: 'ResolvedSource',
		strategies: list[Strategy],
		start: date | None = None,
		end: date | None = None,
		initial_cash: float = 10000.0,
		slippage_cents: int = 1,
		on_progress=None,
		is_cancelled=None,
	) -> BacktestResult:
		"""Run backtest across one or more primary data sources."""

		# Single-primary fast path
		if len(resolved.primaries) == 1:
			src = resolved.primaries[0]
			return self.run(
				series=src.series,
				strategies=strategies,
				start=start,
				end=end,
				initial_cash=initial_cash,
				slippage_cents=slippage_cents,
				db_path=Path(src.db_path),
				fee_fn=src.fee_model.calculate,
				on_progress=on_progress,
				is_cancelled=is_cancelled,
			)

		# Multi-primary: build streams and merge
		streams = [
			TradeStream(db_path=src.db_path, series=src.series, start=start, end=end)
			for src in resolved.primaries
		]

		# Build ticker -> fee_model mapping
		ticker_to_fee = {}
		for src in resolved.primaries:
			conn = get_connection(Path(src.db_path))
			try:
				for row in conn.execute('SELECT ticker FROM markets WHERE series_ticker = ?', [src.series]):
					ticker_to_fee[row[0]] = src.fee_model
			finally:
				conn.close()

		# Collect all markets for settlement
		all_markets: dict[str, Market] = {}
		for src in resolved.primaries:
			conn = get_connection(Path(src.db_path))
			try:
				market_query = 'SELECT * FROM markets WHERE series_ticker = ?'
				params: list = [src.series]
				if start:
					market_query += ' AND close_time >= ?'
					params.append(start.isoformat())
				if end:
					end_dt = end + timedelta(days=1)
					market_query += ' AND open_time <= ?'
					params.append(end_dt.isoformat())
				for row in conn.execute(market_query, params).fetchall():
					m = _row_to_market(row)
					all_markets[m.ticker] = m
			finally:
				conn.close()

		_cancelled = is_cancelled or (lambda: False)

		portfolio = Portfolio(initial_cash, fee_fn=lambda p, s: 0.0)

		trade_count = 0
		for trade, market, db_path in merge_streams(streams):
			if _cancelled():
				break

			# Settlement sweep
			for (t_ticker, t_strategy) in list(portfolio.positions.keys()):
				if (t_ticker, t_strategy) not in portfolio.positions:
					continue
				pos_market = all_markets.get(t_ticker)
				if (pos_market and pos_market.close_time and
					trade.created_time > pos_market.close_time and pos_market.result):
					portfolio.settle_position(t_ticker, t_strategy, pos_market.result, pos_market.close_time)

			trade_count += 1
			if trade_count % 10000 == 0:
				portfolio.snapshot(trade.created_time)

			if market.close_time and trade.created_time > market.close_time:
				continue

			for strategy in strategies:
				signals = strategy.on_trade(trade, market, portfolio)
				for signal in signals:
					if not (1 <= signal.price <= 99):
						continue
					# Apply per-ticker fee
					fee_calc = ticker_to_fee.get(signal.ticker, ZERO_FEE).calculate
					if signal.action == 'buy':
						portfolio.open_position(signal, strategy.name, trade.created_time, slippage_cents,
							fee_fn=fee_calc)
					elif signal.action == 'sell':
						reason = (
							'take_profit' if 'take_profit' in signal.reason
							else 'stop_loss' if 'stop_loss' in signal.reason
							else 'close'
						)
						portfolio.close_position(
							signal.ticker, strategy.name, signal.price,
							trade.created_time, reason, slippage_cents,
							fee_fn=fee_calc,
						)

		# Final settlement
		for (t_ticker, t_strategy) in list(portfolio.positions.keys()):
			pos_market = all_markets.get(t_ticker)
			if pos_market and pos_market.result:
				portfolio.settle_position(t_ticker, t_strategy, pos_market.result,
					pos_market.close_time or datetime.now(timezone.utc))

		portfolio.snapshot(datetime.now(timezone.utc))

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
			per_strategy_curves=portfolio._per_strategy_curves,
			trade_sample=portfolio._trade_sample,
			pnl_values=list(portfolio._pnl_values),
		)
