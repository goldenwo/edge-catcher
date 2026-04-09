"""AST-based strategy file parsing: list strategies, validate code, save to file."""
from __future__ import annotations

import ast
import hashlib
import logging
import re
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# ── Centralized strategy paths ───────────────────────────────────────────────
# Change these four constants to reorganize strategy files into subfolders.
STRATEGIES_PUBLIC_PATH = Path("edge_catcher/runner/strategies.py")
STRATEGIES_LOCAL_PATH = Path("edge_catcher/runner/strategies_local.py")
STRATEGIES_PUBLIC_MODULE = "edge_catcher.runner.strategies"
STRATEGIES_LOCAL_MODULE = "edge_catcher.runner.strategies_local"

# Default preamble for new strategies_local.py files
_PREAMBLE = '''\
"""Local strategies — gitignored. Your edge stays private."""
from edge_catcher.runner.strategies import Strategy, Signal, VolumeMixin, MomentumMixin
from edge_catcher.storage.models import Market, Trade

'''


def _camel_to_snake(name: str) -> str:
	"""Convert CamelCase to snake_case."""
	s = re.sub(r'([A-Z]+)([A-Z][a-z])', r'\1_\2', name)
	s = re.sub(r'([a-z\d])([A-Z])', r'\1_\2', s)
	return s.lower()


def _extract_name_attr(node: ast.ClassDef) -> Optional[str]:
	"""Extract the `name` class attribute if it's a string literal."""
	for item in node.body:
		if isinstance(item, ast.Assign):
			for target in item.targets:
				if isinstance(target, ast.Name) and target.id == 'name':
					if isinstance(item.value, ast.Constant) and isinstance(item.value.value, str):
						return item.value.value
	return None


def _base_names(node: ast.ClassDef) -> set[str]:
	"""Return the set of simple base class names for a ClassDef."""
	names: set[str] = set()
	for base in node.bases:
		if isinstance(base, ast.Name):
			names.add(base.id)
		elif isinstance(base, ast.Attribute):
			names.add(base.attr)
	return names


def list_strategies(
	file_path: Optional[Path] = None,
	source: Optional[str] = None,
) -> list[dict]:
	"""Parse strategy classes from a file or source string.

	Uses a two-pass scan: first finds classes that directly subclass Strategy
	(or any name containing 'Strategy'), then finds classes whose bases include
	any already-identified strategy class. This handles filtered variants
	where the base is a local strategy class rather than Strategy itself.

	Returns list of {"name": str, "class_name": str}.
	Returns empty list on syntax errors or missing files.
	"""
	if source is None:
		if file_path is None or not file_path.exists():
			return []
		try:
			source = file_path.read_text(encoding="utf-8")
		except OSError:
			return []

	try:
		tree = ast.parse(source)
	except SyntaxError:
		logger.warning("Syntax error parsing strategies, returning empty list")
		return []

	classes = [node for node in tree.body if isinstance(node, ast.ClassDef)]

	# Pass 1: direct Strategy subclasses
	strategy_class_names: set[str] = set()
	for node in classes:
		bases = _base_names(node)
		if any('Strategy' in b for b in bases):
			strategy_class_names.add(node.name)

	# Pass 2: classes that inherit from any known strategy class (transitive)
	changed = True
	while changed:
		changed = False
		for node in classes:
			if node.name in strategy_class_names:
				continue
			if _base_names(node) & strategy_class_names:
				strategy_class_names.add(node.name)
				changed = True

	results = []
	for node in classes:
		if node.name in strategy_class_names:
			name = _extract_name_attr(node) or _camel_to_snake(node.name)
			results.append({"name": name, "class_name": node.name})
	return results


def validate_strategy_code(code: str) -> tuple[bool, Optional[str]]:
	"""Validate that code is safe to write to strategies_local.py.

	Checks:
	1. Syntactically valid Python
	2. Contains at least one class definition
	3. No module-level statements beyond imports, class defs, assignments, and string expressions

	Returns (ok, error_message).
	"""
	try:
		tree = ast.parse(code)
	except SyntaxError as e:
		return False, f"Syntax error: {e}"

	has_class = any(isinstance(node, ast.ClassDef) for node in tree.body)

	for node in tree.body:
		if isinstance(node, ast.ClassDef):
			pass  # class definitions are fine
		elif isinstance(node, (ast.Import, ast.ImportFrom)):
			pass  # imports are fine
		elif isinstance(node, ast.Assign):
			pass  # module-level constants are fine
		elif isinstance(node, ast.Expr) and isinstance(node.value, ast.Constant):
			pass  # docstrings / string literals are fine
		else:
			if not has_class:
				return False, f"No class definition found; disallowed statement: {type(node).__name__} at line {node.lineno}"
			return False, f"Disallowed module-level statement: {type(node).__name__} at line {node.lineno}"

	if not has_class:
		return False, "No class definition found in code"

	return True, None


def compute_code_hash(code: str) -> str:
	"""SHA256 of code with class names normalized out. Catches exact renamed duplicates."""
	try:
		tree = ast.parse(code)
	except SyntaxError:
		return hashlib.sha256(code.encode()).hexdigest()

	lines = code.splitlines(keepends=True)

	# Normalize class names using line numbers (bottom-up to preserve positions)
	class_nodes = [n for n in tree.body if isinstance(n, ast.ClassDef)]
	for node in sorted(class_nodes, key=lambda n: n.lineno, reverse=True):
		line_idx = node.lineno - 1
		lines[line_idx] = lines[line_idx].replace(f"class {node.name}", "class _STRATEGY_", 1)

	# Normalize the name attribute using line numbers
	name_assigns = [
		n for n in ast.walk(tree)
		if (isinstance(n, ast.Assign)
			and len(n.targets) == 1
			and isinstance(n.targets[0], ast.Name)
			and n.targets[0].id == "name"
			and isinstance(n.value, ast.Constant)
			and isinstance(n.value.value, str))
	]
	for node in sorted(name_assigns, key=lambda n: n.lineno, reverse=True):
		line_idx = node.lineno - 1
		old_val = node.value.value
		lines[line_idx] = lines[line_idx].replace(f'"{old_val}"', '"_STRATEGY_NAME_"', 1)
		lines[line_idx] = lines[line_idx].replace(f"'{old_val}'", '"_STRATEGY_NAME_"', 1)

	normalized = "".join(lines)
	return hashlib.sha256(normalized.strip().encode()).hexdigest()


def compute_ast_fingerprint(code: str) -> Optional[str]:
	"""Structural AST fingerprint. Catches same logic with different names/comments.

	Returns SHA256 hex string, or None if code is unparseable.
	"""
	try:
		tree = ast.parse(code)
	except SyntaxError:
		return None

	signature: list[str] = []

	for node in ast.walk(tree):
		if isinstance(node, ast.FunctionDef):
			signature.append(f"method:{node.name}:args={len(node.args.args)}")
		elif isinstance(node, ast.If):
			signature.append("ctrl:if")
		elif isinstance(node, ast.For):
			signature.append("ctrl:for")
		elif isinstance(node, ast.While):
			signature.append("ctrl:while")
		elif isinstance(node, ast.Constant) and isinstance(node.value, (int, float)):
			signature.append(f"num:{node.value}")
		elif isinstance(node, ast.Attribute):
			signature.append(f"attr:{node.attr}")

	fingerprint_str = "|".join(signature)
	return hashlib.sha256(fingerprint_str.encode()).hexdigest()


def cleanup_dead_strategies(file_path: Path, dead_names: list[str]) -> list[str]:
	"""Remove class definitions for dead strategies from a file.

	Returns list of strategy class names that were actually removed.
	"""
	if not dead_names or not file_path.exists():
		return []

	content = file_path.read_text(encoding="utf-8")
	try:
		tree = ast.parse(content)
	except SyntaxError:
		return []

	dead_set = set(dead_names)
	# Find class nodes to remove, sorted by line number descending (remove from bottom up)
	to_remove = []
	for node in tree.body:
		if isinstance(node, ast.ClassDef) and node.name in dead_set:
			to_remove.append(node)

	if not to_remove:
		return []

	lines = content.splitlines(keepends=True)
	removed_names = []

	# Remove from bottom to top so line numbers stay valid
	for node in sorted(to_remove, key=lambda n: n.lineno, reverse=True):
		start = node.lineno - 1  # 0-indexed
		end = node.end_lineno     # exclusive
		# Also remove blank lines after the class (up to 2)
		while end < len(lines) and lines[end].strip() == "":
			end += 1
			if end - node.end_lineno >= 2:
				break
		del lines[start:end]
		removed_names.append(node.name)

	file_path.write_text("".join(lines), encoding="utf-8")
	return removed_names


def save_strategy(
	code: str,
	strategy_name: str,
	file_path: Path,
) -> dict:
	"""Save a strategy class to the target file.

	Algorithm:
	1. Validate the submitted code via AST
	2. Create file with preamble if it doesn't exist
	3. Try to parse existing file; on failure, append with separator
	4. If class with matching name found, replace it; otherwise append

	Returns {"ok": True, "path": str} or {"ok": False, "error": str}.
	"""
	ok, error = validate_strategy_code(code)
	if not ok:
		return {"ok": False, "error": error}

	# Extract the class name from the new code for matching
	try:
		new_tree = ast.parse(code)
	except SyntaxError:
		return {"ok": False, "error": "Failed to parse new code"}

	new_class_names = set()
	for node in new_tree.body:
		if isinstance(node, ast.ClassDef):
			new_class_names.add(node.name)

	if not file_path.exists():
		file_path.parent.mkdir(parents=True, exist_ok=True)
		file_path.write_text(_PREAMBLE + code.rstrip() + "\n", encoding="utf-8")
		return {"ok": True, "path": str(file_path)}

	existing = file_path.read_text(encoding="utf-8")

	# Try to parse existing file
	try:
		existing_tree = ast.parse(existing)
	except SyntaxError:
		# Existing file has errors — append with separator
		file_path.write_text(
			existing.rstrip() + "\n\n\n# --- Added strategy ---\n\n" + code.rstrip() + "\n",
			encoding="utf-8",
		)
		return {"ok": True, "path": str(file_path)}

	# Find existing class to replace
	existing_lines = existing.splitlines(keepends=True)
	replaced = False

	for node in existing_tree.body:
		if isinstance(node, ast.ClassDef) and node.name in new_class_names:
			# Find the end of this class (next top-level node or EOF)
			start_line = node.lineno - 1  # 0-indexed
			end_line = node.end_lineno  # exclusive (ast uses 1-indexed end_lineno)
			new_lines = (
				existing_lines[:start_line]
				+ [code.rstrip() + "\n"]
				+ existing_lines[end_line:]
			)
			file_path.write_text("".join(new_lines), encoding="utf-8")
			replaced = True
			break

	if not replaced:
		file_path.write_text(
			existing.rstrip() + "\n\n\n" + code.rstrip() + "\n",
			encoding="utf-8",
		)

	return {"ok": True, "path": str(file_path)}
