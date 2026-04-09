"""Tests for AST-based strategy file parsing and code validation."""

import textwrap
from pathlib import Path

import pytest

from edge_catcher.runner.strategy_parser import (
    list_strategies,
    validate_strategy_code,
    save_strategy,
)


# ── list_strategies ──────────────────────────────────────────────────────────

def test_list_strategies_from_string():
    """Extracts strategy class names and name attributes from source code."""
    source = textwrap.dedent('''\
        from edge_catcher.runner.strategies import Strategy, Signal

        class MyStrategy(Strategy):
            name = 'my-strat'
            def on_trade(self, trade, market, portfolio):
                return []

        class AnotherStrategy(Strategy):
            name = 'another'
            def on_trade(self, trade, market, portfolio):
                return []
    ''')
    result = list_strategies(source=source)
    assert len(result) == 2
    assert result[0] == {"name": "my-strat", "class_name": "MyStrategy"}
    assert result[1] == {"name": "another", "class_name": "AnotherStrategy"}


def test_list_strategies_name_fallback():
    """Falls back to snake_case class name when name attribute is missing."""
    source = textwrap.dedent('''\
        from edge_catcher.runner.strategies import Strategy

        class MyCustomStrat(Strategy):
            def on_trade(self, trade, market, portfolio):
                return []
    ''')
    result = list_strategies(source=source)
    assert result[0] == {"name": "my_custom_strat", "class_name": "MyCustomStrat"}


def test_list_strategies_syntax_error():
    """Returns empty list on syntax errors (graceful degradation)."""
    result = list_strategies(source="class Broken(Strategy\n    name = 'x'")
    assert result == []


def test_list_strategies_from_file(tmp_path):
    """Reads strategies from a file path."""
    f = tmp_path / "strats.py"
    f.write_text(textwrap.dedent('''\
        from edge_catcher.runner.strategies import Strategy
        class Foo(Strategy):
            name = 'foo'
            def on_trade(self, trade, market, portfolio):
                return []
    '''))
    result = list_strategies(file_path=f)
    assert len(result) == 1
    assert result[0]["name"] == "foo"


def test_list_strategies_missing_file():
    """Returns empty list for non-existent file."""
    result = list_strategies(file_path=Path("/nonexistent/file.py"))
    assert result == []


def test_list_strategies_transitive_inheritance():
    """Discovers strategies that inherit from another strategy, not Strategy directly."""
    source = textwrap.dedent('''\
        from edge_catcher.runner.strategies import Strategy

        class BaseStrat(Strategy):
            name = 'base'
            def on_trade(self, trade, market, portfolio):
                return []

        class FilteredStrat(BaseStrat):
            name = 'filtered'
            def on_trade(self, trade, market, portfolio):
                return []
    ''')
    result = list_strategies(source=source)
    assert len(result) == 2
    assert result[0] == {"name": "base", "class_name": "BaseStrat"}
    assert result[1] == {"name": "filtered", "class_name": "FilteredStrat"}


def test_list_strategies_deep_transitive():
    """Discovers strategies through multiple levels of inheritance."""
    source = textwrap.dedent('''\
        from edge_catcher.runner.strategies import Strategy

        class Level1(Strategy):
            name = 'level1'
            def on_trade(self, trade, market, portfolio):
                return []

        class Level2(Level1):
            name = 'level2'

        class Level3(Level2):
            name = 'level3'
    ''')
    result = list_strategies(source=source)
    assert len(result) == 3
    names = [r["name"] for r in result]
    assert names == ["level1", "level2", "level3"]


def test_list_strategies_mixin_not_picked_up():
    """Non-strategy mixins are not discovered, but mixin+strategy combos are."""
    source = textwrap.dedent('''\
        from edge_catcher.runner.strategies import Strategy

        class SomeMixin:
            pass

        class BaseStrat(Strategy):
            name = 'base'
            def on_trade(self, trade, market, portfolio):
                return []

        class MixedStrat(SomeMixin, BaseStrat):
            name = 'mixed'
            def on_trade(self, trade, market, portfolio):
                return []
    ''')
    result = list_strategies(source=source)
    assert len(result) == 2
    names = [r["name"] for r in result]
    assert "base" in names
    assert "mixed" in names
    # SomeMixin should NOT appear
    class_names = [r["class_name"] for r in result]
    assert "SomeMixin" not in class_names


# ── validate_strategy_code ───────────────────────────────────────────────────

def test_validate_valid_code():
    """Valid strategy code passes validation."""
    code = textwrap.dedent('''\
        from edge_catcher.runner.strategies import Strategy, Signal
        class TestStrat(Strategy):
            name = 'test'
            def on_trade(self, trade, market, portfolio):
                return []
    ''')
    ok, error = validate_strategy_code(code)
    assert ok is True
    assert error is None


def test_validate_syntax_error():
    """Syntax errors are caught."""
    ok, error = validate_strategy_code("class Broken(")
    assert ok is False
    assert "syntax" in error.lower() or "invalid" in error.lower()


def test_validate_no_class():
    """Code without a class definition fails."""
    ok, error = validate_strategy_code("x = 1\nprint('hello')")
    assert ok is False
    assert "class" in error.lower()


def test_validate_rejects_dangerous_module_level():
    """Code with module-level statements beyond imports and classes fails."""
    code = textwrap.dedent('''\
        import os
        os.system('rm -rf /')
        class Bad(Strategy):
            name = 'bad'
    ''')
    ok, error = validate_strategy_code(code)
    assert ok is False


# ── save_strategy ────────────────────────────────────────────────────────────

def test_save_strategy_creates_file(tmp_path):
    """Creates strategies_local.py with preamble + strategy when file doesn't exist."""
    target = tmp_path / "strategies_local.py"
    code = textwrap.dedent('''\
        class NewStrat(Strategy):
            name = 'new-strat'
            def on_trade(self, trade, market, portfolio):
                return []
    ''')
    result = save_strategy(code, "new-strat", target)
    assert result["ok"] is True
    assert target.exists()
    content = target.read_text()
    assert "class NewStrat" in content
    assert "from edge_catcher.runner.strategies import" in content


def test_save_strategy_appends(tmp_path):
    """Appends a new strategy to an existing file."""
    target = tmp_path / "strategies_local.py"
    target.write_text(textwrap.dedent('''\
        from edge_catcher.runner.strategies import Strategy, Signal

        class Existing(Strategy):
            name = 'existing'
            def on_trade(self, trade, market, portfolio):
                return []
    '''))
    code = textwrap.dedent('''\
        class NewStrat(Strategy):
            name = 'new-strat'
            def on_trade(self, trade, market, portfolio):
                return []
    ''')
    result = save_strategy(code, "new-strat", target)
    assert result["ok"] is True
    content = target.read_text()
    assert "class Existing" in content
    assert "class NewStrat" in content


def test_save_strategy_replaces(tmp_path):
    """Replaces an existing strategy class by name."""
    target = tmp_path / "strategies_local.py"
    target.write_text(textwrap.dedent('''\
        from edge_catcher.runner.strategies import Strategy, Signal

        class MyStrat(Strategy):
            name = 'my-strat'
            def on_trade(self, trade, market, portfolio):
                return []
    '''))
    new_code = textwrap.dedent('''\
        class MyStrat(Strategy):
            name = 'my-strat'
            def on_trade(self, trade, market, portfolio):
                return [Signal(action='buy', ticker=trade.ticker, side='yes',
                               price=trade.yes_price, size=1, reason='updated')]
    ''')
    result = save_strategy(new_code, "my-strat", target)
    assert result["ok"] is True
    content = target.read_text()
    assert content.count("class MyStrat") == 1
    assert "updated" in content


def test_save_strategy_rejects_invalid(tmp_path):
    """Rejects syntactically invalid code."""
    target = tmp_path / "strategies_local.py"
    result = save_strategy("class Broken(", "broken", target)
    assert result["ok"] is False
    assert "error" in result
