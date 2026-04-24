"""Parameter Sensitivity gate — perturb numeric params ±15%, check for graceful degradation."""

from __future__ import annotations

import ast
import importlib
import logging
import math
import random
import shutil
import threading
import time

from edge_catcher.research.hypothesis import Hypothesis, HypothesisResult

from .gate import Gate, GateContext, GateResult

logger = logging.getLogger(__name__)

# Module-level lock shared across all ParameterSensitivityGate instances,
# protecting concurrent writes to strategies_local.py.
_FILE_LOCK = threading.Lock()

# Parameters that are not tunable "strategy edge" knobs — perturbation
# is meaningless or destructive. Matches the CLI's skip set at
# cli/backtest.py:53.
SKIP_PARAMS: set[str] = {"size", "btc_closes", "ohlc_provider"}


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
				data_sources=context.hypothesis.data_sources,
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
		"""Remove a sensitivity-generated temporary strategy from strategies_local.py.

		Hardened after the 2026-04-13 Task 5 v2 sweep wiped the file: this
		function now refuses to write unless every safety invariant holds,
		and always backs up to ``strategies_local.py.bak`` before touching
		the real file. Invariants:

		1. ``temp_name`` must contain ``__sens_`` (matches the naming scheme
		   from ``_run_neighbor``). Refuses otherwise — we will never
		   silently delete a real strategy by mistake.
		2. strategies_local.py must parse cleanly. A previous version
		   fell back to line-level text filtering on SyntaxError, which
		   could remove unrelated lines; that fallback is gone.
		3. The AST splice must remove **exactly one** top-level class.
		   Any other delta (0 or ≥2) aborts without writing.
		4. The post-splice file must parse as valid Python.
		5. The post-splice file must still contain at least one non-temp
		   class — refuses to reduce the file to an empty or sens-only
		   state.

		Any failed invariant logs an error and leaves the file untouched.
		"""
		from edge_catcher.runner.strategy_parser import (
			STRATEGIES_LOCAL_PATH, STRATEGIES_LOCAL_MODULE,
		)

		if not STRATEGIES_LOCAL_PATH.exists():
			return

		# Invariant 1: only clean up sensitivity-temp names.
		if "__sens_" not in temp_name:
			logger.error(
				"_cleanup refused: %r is not a sensitivity-temp name",
				temp_name,
			)
			return

		# encoding="utf-8" is load-bearing on Windows: the default locale
		# codec (cp1252) raises UnicodeDecodeError on any non-ASCII byte
		# in strategies_local.py, and agent.run_hypothesis catches the
		# exception as a "validation pipeline error", silently demoting
		# real candidates to "explore". Discovered during Task 5 sweep.
		source = STRATEGIES_LOCAL_PATH.read_text(encoding="utf-8")

		# Invariant 2: current file must parse cleanly.
		try:
			tree = ast.parse(source)
		except SyntaxError as exc:
			logger.error(
				"_cleanup refused: strategies_local.py has syntax error (%s); "
				"manual repair required before cleanup of %r can proceed",
				exc, temp_name,
			)
			return

		classes_before = [n for n in tree.body if isinstance(n, ast.ClassDef)]
		non_temp_before = sum(
			1 for n in classes_before if not _class_has_temp_name(n)
		)

		# Locate the target class by its ``name = temp_name`` attribute.
		target_class: ast.ClassDef | None = None
		for node in classes_before:
			for item in node.body:
				if (
					isinstance(item, ast.Assign)
					and any(isinstance(t, ast.Name) and t.id == "name" for t in item.targets)
					and isinstance(item.value, ast.Constant)
					and item.value.value == temp_name
				):
					target_class = node
					break
			if target_class is not None:
				break

		if target_class is None:
			# Temp class not present — nothing to remove, nothing to write.
			return

		lines = source.splitlines()
		start_line = target_class.lineno - 1
		end_line = target_class.end_lineno or target_class.lineno
		new_lines = lines[:start_line] + lines[end_line:]
		new_source = "\n".join(new_lines) + "\n"

		# Invariant 4: post-splice file must parse.
		try:
			new_tree = ast.parse(new_source)
		except SyntaxError as exc:
			logger.error(
				"_cleanup refused: splice would leave strategies_local.py "
				"with a syntax error (%s). temp_name=%r",
				exc, temp_name,
			)
			return

		new_classes = [n for n in new_tree.body if isinstance(n, ast.ClassDef)]

		# Invariant 3: exactly one class removed.
		if len(new_classes) != len(classes_before) - 1:
			logger.error(
				"_cleanup refused: splice would change class count by %d "
				"(expected -1). temp_name=%r",
				len(new_classes) - len(classes_before), temp_name,
			)
			return

		# Invariant 5: non-temp class count must not drop.
		non_temp_after = sum(1 for n in new_classes if not _class_has_temp_name(n))
		if non_temp_after < non_temp_before:
			logger.error(
				"_cleanup refused: splice would remove a non-temp class "
				"(%d -> %d). temp_name=%r",
				non_temp_before, non_temp_after, temp_name,
			)
			return
		if non_temp_after == 0:
			logger.error(
				"_cleanup refused: splice would leave strategies_local.py "
				"with zero non-temp classes. temp_name=%r",
				temp_name,
			)
			return

		# All invariants hold — back up before writing.
		backup_path = STRATEGIES_LOCAL_PATH.with_suffix(".py.bak")
		try:
			shutil.copy2(STRATEGIES_LOCAL_PATH, backup_path)
		except Exception as exc:
			logger.error(
				"_cleanup refused: backup copy to %s failed: %s",
				backup_path, exc,
			)
			return

		STRATEGIES_LOCAL_PATH.write_text(new_source, encoding="utf-8")

		try:
			mod = importlib.import_module(STRATEGIES_LOCAL_MODULE)
			importlib.reload(mod)
		except Exception:
			pass


def _class_has_temp_name(class_node: ast.ClassDef) -> bool:
	"""True if the class defines ``name = <str containing '__sens_'>``.

	Used by ParameterSensitivityGate._cleanup to tell sensitivity-generated
	classes apart from real strategies without relying on the caller's
	temp_name matching.
	"""
	for item in class_node.body:
		if (
			isinstance(item, ast.Assign)
			and any(isinstance(t, ast.Name) and t.id == "name" for t in item.targets)
			and isinstance(item.value, ast.Constant)
			and isinstance(item.value.value, str)
			and "__sens_" in item.value.value
		):
			return True
	return False


def _extract_numeric_params(code: str) -> list[tuple[str, int | float]]:
	"""Extract numeric parameters from a strategy's ``__init__`` defaults.

	Strategies in this codebase declare tunable parameters as ``__init__``
	arguments with numeric defaults (e.g. ``min_price: int = 5``). This
	function walks the AST of a single class definition and returns
	``(param_name, default_value)`` for every positional-or-keyword arg
	whose default is a real numeric constant.

	Booleans are excluded (via both type annotation and ``type(v) is bool``),
	but legitimate int defaults of ``0`` or ``1`` are kept.
	"""
	try:
		tree = ast.parse(code)
	except SyntaxError:
		return []

	params: list[tuple[str, int | float]] = []
	for class_node in ast.walk(tree):
		if not isinstance(class_node, ast.ClassDef):
			continue
		for item in class_node.body:
			if not isinstance(item, ast.FunctionDef) or item.name != "__init__":
				continue
			args = item.args
			pos_args = args.args[1:]  # drop ``self``
			pos_defaults = args.defaults
			offset = len(pos_args) - len(pos_defaults)
			for idx, arg in enumerate(pos_args):
				if idx < offset:
					continue
				_maybe_add_numeric_param(params, arg, pos_defaults[idx - offset])
			for arg, default in zip(args.kwonlyargs, args.kw_defaults):
				if default is None:
					continue
				_maybe_add_numeric_param(params, arg, default)
	return params


def _maybe_add_numeric_param(
	out: list[tuple[str, int | float]],
	arg: ast.arg,
	default_node: ast.AST,
) -> None:
	"""Append ``(name, value)`` to ``out`` if ``arg`` is a numeric parameter."""
	if arg.arg in SKIP_PARAMS:
		return
	if arg.annotation is not None:
		ann = arg.annotation
		if isinstance(ann, ast.Name) and ann.id == "bool":
			return
	if not isinstance(default_node, ast.Constant):
		return
	val = default_node.value
	if type(val) is bool:
		return
	if isinstance(val, (int, float)):
		out.append((arg.arg, val))


def _perturb(value: int | float, pct: float) -> int | float:
	"""Perturb a value by pct, preserving type."""
	result = value * (1 + pct)
	if isinstance(value, int):
		result = int(round(result))
		if result == value:  # ensure it actually changed
			result += 1 if pct > 0 else -1
	return result


def _tabify(code: str) -> str:
	"""Convert ast.unparse 4-space indentation to tabs to match strategies_local.py."""
	return code.replace("    ", "\t")


def _replace_param(
	code: str, original_name: str, param_name: str,
	new_value: int | float, new_strategy_name: str,
) -> str:
	"""Generate modified strategy code with one ``__init__`` default changed.

	Also renames the class itself (hyphens replaced with underscores to keep
	it a valid Python identifier) and rewrites the class-level ``name``
	attribute so the strategy registers under ``new_strategy_name``.
	"""
	tree = ast.parse(code)
	new_class_name = new_strategy_name.replace("-", "_")

	for class_node in ast.walk(tree):
		if not isinstance(class_node, ast.ClassDef):
			continue
		class_node.name = new_class_name
		for item in class_node.body:
			if (
				isinstance(item, ast.Assign)
				and any(isinstance(t, ast.Name) and t.id == "name" for t in item.targets)
			):
				item.value = ast.Constant(value=new_strategy_name)
				continue
			if isinstance(item, ast.FunctionDef) and item.name == "__init__":
				_replace_init_default(item, param_name, new_value)

	ast.fix_missing_locations(tree)
	return _tabify(ast.unparse(tree))


def _replace_init_default(
	init_node: ast.FunctionDef,
	param_name: str,
	new_value: int | float,
) -> None:
	"""Mutate ``init_node`` to set ``param_name``'s default to ``new_value``.

	Logs a warning if ``param_name`` is not present in the signature or
	has no default — that shouldn't happen if the caller is feeding names
	from ``_extract_numeric_params``, but drift between the two could
	silently produce unchanged neighbor code which would then look
	identical to the original in backtest results.
	"""
	args = init_node.args
	pos_args = args.args[1:]  # drop ``self``
	pos_defaults = args.defaults
	offset = len(pos_args) - len(pos_defaults)
	for idx, arg in enumerate(pos_args):
		if arg.arg != param_name:
			continue
		if idx < offset:
			logger.warning(
				"_replace_init_default: '%s' has no default — skipping",
				param_name,
			)
			return
		pos_defaults[idx - offset] = ast.Constant(value=new_value)
		return
	for idx, arg in enumerate(args.kwonlyargs):
		if arg.arg == param_name and args.kw_defaults[idx] is not None:
			args.kw_defaults[idx] = ast.Constant(value=new_value)
			return
	logger.warning(
		"_replace_init_default: param '%s' not found in __init__ signature",
		param_name,
	)


def _sanitize(value: int | float) -> str:
	"""Make a value safe for use in a strategy name."""
	return str(value).replace(".", "_").replace("-", "neg")
