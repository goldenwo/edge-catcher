"""Tests for bundle config-snapshot fidelity (the resolved --config is bundled)."""

from pathlib import Path

from edge_catcher.engine.capture.bundle import _write_config_snapshot


def _legacy(repo_root: Path) -> Path:
	p = repo_root / "config.local" / "paper-trader.yaml"
	p.parent.mkdir(parents=True, exist_ok=True)
	p.write_text("strategies:\n  debut-fade:\n    enabled: true\n", encoding="utf-8")
	return p


def test_snapshots_resolved_config_when_provided(tmp_path):
	repo_root = tmp_path / "repo"
	_legacy(repo_root)  # legacy exists but must NOT be chosen
	live_cfg = tmp_path / "config.local" / "paper-trader-spotfair.yaml"
	live_cfg.parent.mkdir(parents=True, exist_ok=True)
	live_cfg.write_text("strategies:\n  spot-fair-ot-t12:\n    enabled: true\n", encoding="utf-8")
	bundle = tmp_path / "bundle"
	bundle.mkdir()

	_write_config_snapshot(bundle, repo_root, live_cfg)

	out = (bundle / "paper-trader.yaml").read_text(encoding="utf-8")
	assert "spot-fair-ot-t12" in out          # the live config was snapshotted
	assert "debut-fade" not in out            # NOT the hard-coded legacy


def test_falls_back_to_legacy_when_config_path_none(tmp_path):
	repo_root = tmp_path / "repo"
	_legacy(repo_root)
	bundle = tmp_path / "bundle"
	bundle.mkdir()

	_write_config_snapshot(bundle, repo_root, None)

	assert "debut-fade" in (bundle / "paper-trader.yaml").read_text(encoding="utf-8")


def test_falls_back_to_legacy_when_config_path_missing(tmp_path):
	repo_root = tmp_path / "repo"
	_legacy(repo_root)
	bundle = tmp_path / "bundle"
	bundle.mkdir()

	_write_config_snapshot(bundle, repo_root, tmp_path / "does-not-exist.yaml")

	assert "debut-fade" in (bundle / "paper-trader.yaml").read_text(encoding="utf-8")


def test_omits_when_no_config_anywhere(tmp_path):
	repo_root = tmp_path / "repo"   # no config.local/paper-trader.yaml created
	repo_root.mkdir()
	bundle = tmp_path / "bundle"
	bundle.mkdir()

	_write_config_snapshot(bundle, repo_root, None)

	assert not (bundle / "paper-trader.yaml").exists()
