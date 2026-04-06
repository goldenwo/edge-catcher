"""Parameter Sensitivity gate — perturb numeric params ±15%, check for graceful degradation."""

from __future__ import annotations

import ast
import importlib
import logging
import math
import random
import re
import threading
import time
from pathlib import Path

from edge_catcher.research.hypothesis import Hypothesis, HypothesisResult

from .gate import Gate, GateContext, GateResult

logger = logging.getLogger(__name__)

# Module-level lock shared across all ParameterSensitivityGate instances,
# protecting concurrent writes to strategies_local.py.
_FILE_LOCK = threading.Lock()


class ParameterSensitivityGate(Gate):
	"""Fail strategies whose performance is fragile to small parameter changes."""

	name = "param_sensitivity"

	def __init__(
		self,
		perturbation_pct: float = 0.15,
		min_neighbors_passing: float = 0.5,
		max_sharpe_degradation: float = 0.5,
		max_params: int = 5,
		timeout_seconds: float = 1800,  # 30 minutes
	) -> None:
		self.perturbation_pct = perturbation_pct
		self.min_neighbors_passing = min_neighbors_passing
		self.max_sharpe_degradation = max_sharpe_degradation
		self.max_params = max_params
		self.timeout_seconds = timeout_seconds
		self._file_lock = _FILE_LOCK

	def check(self, result: HypothesisResult, context: GateContext) -> GateResult:
		if context.agent is None:
			return GateResult(
				passed=False, gate_name=self.name,
				reason="no agent available for sensitivity backtests",
				details={},
			)

		strategy_name = context.hypothesis.strategy
		code = context.agent.read_strategy_code(strategy_name)
		if code is None:
			return GateResult(
				passed=False, gate_name=self.name,
				reason=f"cannot read source for strategy '{strategy_name}'",
				details={},
			)

		# Find numeric parameters
		params = _extract_numeric_params(code)
		if not params:
			return GateResult(
				passed=True, gate_name=self.name,
				reason="no numeric parameters to perturb",
				details={"params_found": 0},
			)

		# Cap at max_params, randomly selected
		if len(params) > self.max_params:
			rng = random.Random(hash(context.hypothesis.dedup_key()))
			params = rng.sample(params, self.max_params)

		# Normalize original Sharpe to per-trade scale so comparison with
		# neighbors isn't biased by differing trade counts.
		orig_trades = result.total_trades
		original_sharpe = (
			result.sharpe / math.sqrt(orig_trades) if orig_trades >= 1 else 0.0
		)
		min_acceptable = original_sharpe * self.max_sharpe_degradation
		deadline = time.monotonic() + self.timeout_seconds

		params_tested: dict[str, dict] = {}
		neighbors_passing = 0
		neighbors_total = 0

		for param_name, original_value in params:
			if time.monotonic() > deadline:
				break

			low = _perturb(original_value, -self.perturbation_pct)
			high = _perturb(original_value, self.perturbation_pct)
			neighbor_sharpes: list[float | None] = []

			for perturbed_value in [low, high]:
				if time.monotonic() > deadline:
					neighbor_sharpes.append(None)
					neighbors_total += 1
					continue

				# Generate modified code
				temp_name = f"{strategy_name}__sens_{param_name}_{_sanitize(perturbed_value)}"
				modified_code = _replace_param(code, strategy_name, param_name, perturbed_value, temp_name)

				# Run backtest with modified strategy
				sharpe = self._run_neighbor(
					context, modified_code, temp_name,
				)
				neighbor_sharpes.append(sharpe)
				neighbors_total += 1

				if sharpe is not None and sharpe >= min_acceptable:
					neighbors_passing += 1

			params_tested[param_name] = {
				"original": original_value,
				"neighbors": [low, high],
				"sharpes": neighbor_sharpes,
			}

		pass_rate = neighbors_passing / neighbors_total if neighbors_total > 0 else 0.0

		details = {
			"params_tested": params_tested,
			"neighbors_passing": neighbors_passing,
			"neighbors_total": neighbors_total,
			"pass_rate": round(pass_rate, 3),
			"min_acceptable_sharpe": round(min_acceptable, 3),
		}

		passed = pass_rate >= self.min_neighbors_passing

		gte_sym = "\u2265"
		cmp_sym = gte_sym if passed else "<"
		reason = (
			f"{neighbors_passing}/{neighbors_total} neighbors have Sharpe {gte_sym} {min_acceptable:.2f} "
			f"(pass rate {pass_rate:.0%} {cmp_sym} {self.min_neighbors_passing:.0%})"
		)

		return GateResult(passed=passed, gate_name=self.name, reason=reason, details=details)

	def _run_neighbor(
		self,
		context: GateContext,
		modified_code: str,
		temp_name: str,
	) -> float | None:
		"""Save temp strategy, run backtest, clean up. Returns Sharpe or None."""
		from edge_catcher.runner.strategy_parser import (
			validate_strategy_code, save_strategy, STRATEGIES_LOCAL_PATH,
			STRATEGIES_LOCAL_MODULE,
		)

		ok, error = validate_strategy_code(modified_code)
		if not ok:
			logger.warning("Sensitivity neighbor '%s' failed validation: %s", temp_name, error)
			return None

		with self._file_lock:
			result = save_strategy(modified_code, temp_name, STRATEGIES_LOCAL_PATH)
			if not result.get("ok"):
				logger.warning("Failed to save sensitivity neighbor '%s': %s", temp_name, result.get("error"))
				return None

			try:
				mod = importlib.import_module(STRATEGIES_LOCAL_MODULE)
				importlib.reload(mod)
			except Exception as exc:
				logger.warning("Failed to reload after saving '%s': %s", temp_name, exc)
				self._cleanup(temp_name)
				return None

			h = Hypothesis(
				strategy=temp_name,
				series=context.hypothesis.series,
				db_path=context.hypothesis.db_path,
				start_date=context.hypothesis.start_date,
				end_date=context.hypothesis.end_date,
				fee_pct=context.hypothesis.fee_pct,
			)

			data = context.agent.run_backtest_only(h)
			self._cleanup(temp_name)

		if data is None:
			return None
		bt_sharpe = data.get("sharpe", 0.0)
		trades = data.get("total_trades", 0)
		# Normalize to per-trade Sharpe to match the original's scale.
		return bt_sharpe / math.sqrt(trades) if trades >= 1 else 0.0

	def _cleanup(self, temp_name: str) -> None:
		"""Remove temporary strategy from strategies_local.py."""
		from edge_catcher.runner.strategy_parser import (
			STRATEGIES_LOCAL_PATH, STRATEGIES_LOCAL_MODULE,
		)

		if not STRATEGIES_LOCAL_PATH.exists():
			return

		source = STRATEGIES_LOCAL_PATH.read_text()
		try:
			tree = ast.parse(source)
		except SyntaxError:
			return

		lines = source.splitlines()
		# Find and remove the temp class
		for node in tree.body:
			if not isinstance(node, ast.ClassDef):
				continue
			# Check if this class has name = temp_name
			for item in node.body:
				if (
					isinstance(item, ast.Assign)
					and any(isinstance(t, ast.Name) and t.id == "name" for t in item.targets)
					and isinstance(item.value, ast.Constant)
					and item.value.value == temp_name
				):
					start_line = node.lineno - 1
					end_line = node.end_lineno or node.lineno
					lines[start_line:end_line] = []
					STRATEGIES_LOCAL_PATH.write_text("\n".join(lines) + "\n")
					try:
						mod = importlib.import_module(STRATEGIES_LOCAL_MODULE)
						importlib.reload(mod)
					except Exception:
						pass
					return


def _extract_numeric_params(code: str) -> list[tuple[str, int | float]]:
	"""Extract numeric class attribute assignments from strategy source."""
	try:
		tree = ast.parse(code)
	except SyntaxError:
		return []

	params: list[tuple[str, int | float]] = []
	for node in ast.walk(tree):
		if not isinstance(node, ast.ClassDef):
			continue
		for item in node.body:
			if not isinstance(item, ast.Assign):
				continue
			for target in item.targets:
				if not isinstance(target, ast.Name):
					continue
				if target.id == "name":
					continue  # skip the strategy name attribute
				if isinstance(item.value, ast.Constant) and isinstance(item.value.value, (int, float)):
					val = item.value.value
					if val in (0, 1, True, False):
						continue  # skip booleans/flags
					params.append((target.id, val))
	return params


def _perturb(value: int | float, pct: float) -> int | float:
	"""Perturb a value by pct, preserving type."""
	result = value * (1 + pct)
	if isinstance(value, int):
		result = int(round(result))
		if result == value:  # ensure it actually changed
			result += 1 if pct > 0 else -1
	return result


def _replace_param(
	code: str, original_name: str, param_name: str,
	new_value: int | float, new_strategy_name: str,
) -> str:
	"""Generate modified strategy code with one param changed and a new name."""
	lines = code.splitlines()
	result_lines: list[str] = []

	for line in lines:
		# Replace strategy name
		if re.match(r'\s*name\s*=\s*["\']', line):
			line = re.sub(
				r'(name\s*=\s*["\'])([^"\']+)(["\'])',
				rf'\g<1>{new_strategy_name}\g<3>',
				line,
			)
		# Replace class name
		elif re.match(r'\s*class\s+\w+', line):
			line = re.sub(
				r'(class\s+)\w+',
				rf'\g<1>{new_strategy_name.replace("-", "_")}',
				line,
			)
		# Replace the target param
		elif re.match(rf'\s*{re.escape(param_name)}\s*=\s*', line):
			if isinstance(new_value, int):
				line = re.sub(
					rf'({re.escape(param_name)}\s*=\s*)[\d.]+',
					rf'\g<1>{new_value}',
					line,
				)
			else:
				line = re.sub(
					rf'({re.escape(param_name)}\s*=\s*)[\d.]+',
					rf'\g<1>{new_value:.6g}',
					line,
				)
		result_lines.append(line)

	return "\n".join(result_lines)


def _sanitize(value: int | float) -> str:
	"""Make a value safe for use in a strategy name."""
	return str(value).replace(".", "_").replace("-", "neg")
