"""Regression test for the additive MC seam on `_bucket_bonferroni_verdict`.

Framework-only: synthetic bucket dicts and synthetic day/price rows, mirroring
the shape of the existing MC-null-gate unit tests in `tests/test_test_runner.py`
(`TestMcNullGate`). Pins three properties of the `mc_pvalue_fn` / `mc_seed`
seam without revealing anything about what consumes it:

  (a) the default path (both kwargs omitted, or both explicitly None) is
      byte-identical to the pre-seam behavior;
  (b) an injected `mc_pvalue_fn` replaces the MC null call entirely and is
      invoked with the documented `(rows, z_obs, n_sims, seed)` contract;
  (c) `mc_seed` given alone forwards into `mc_null_pvalue`'s existing `seed`
      parameter and changes nothing else.
"""

from edge_catcher.research import test_runner
from edge_catcher.research.test_runner import (
	EDGE_EXISTS,
	NO_EDGE,
	_bucket_bonferroni_verdict,
)


def _bucket() -> dict:
	"""A single bucket that clears every gate ahead of the MC null (same
	shape as TestMcNullGate's `bucket()` fixture): strongly significant,
	fee-positive, no taker/degenerate/per-market fields supplied (those
	gates no-op when their fields are absent)."""
	return {
		"bucket_lo": 0.20, "bucket_hi": 0.30,
		"z": 8.93, "n_clusters": 28, "fee_adj": 0.03, "edge": 0.04,
	}


def _healthy_rows() -> list[tuple[str, int, float]]:
	"""28 well-populated days at 50c — an honest MC null that clears easily
	(same shape as TestMcNullGate's `healthy` fixture)."""
	return [
		(f"2026-03-{d:02d}", 1, 0.50)
		for d in range(1, 29)
		for _ in range(20)
	]


class TestMcSeam:
	def test_default_path_identical_with_kwargs_omitted_or_explicit_none(self):
		b_omitted = _bucket()
		res_omitted = _bucket_bonferroni_verdict(
			[b_omitted], 2.0, 0.0, True, mc_rows_fn=lambda b: _healthy_rows(),
		)
		b_explicit = _bucket()
		res_explicit = _bucket_bonferroni_verdict(
			[b_explicit], 2.0, 0.0, True, mc_rows_fn=lambda b: _healthy_rows(),
			mc_pvalue_fn=None, mc_seed=None,
		)
		assert res_omitted == res_explicit
		assert b_omitted["mc_p"] == b_explicit["mc_p"]
		assert b_omitted["mc_gate_ok"] is b_explicit["mc_gate_ok"] is True
		assert res_omitted[0] == EDGE_EXISTS

	def test_mc_pvalue_fn_forces_no_edge_and_receives_call_contract(self):
		calls: list[tuple] = []

		def stub(rows, z_obs, n_sims, seed):
			calls.append((rows, z_obs, n_sims, seed))
			return 1.0  # never clears any alpha -> mc gate fails

		rows = _healthy_rows()
		b = _bucket()
		res = _bucket_bonferroni_verdict(
			[b], 2.0, 0.0, True, mc_rows_fn=lambda bucket: rows, mc_pvalue_fn=stub,
		)

		assert res[0] == NO_EDGE
		assert b["mc_p"] == 1.0
		assert b["mc_gate_ok"] is False

		assert len(calls) == 1
		called_rows, called_z, called_n, called_seed = calls[0]
		assert called_rows == rows
		assert called_z == b["z"]
		assert isinstance(called_n, int) and called_n > 0
		assert called_seed is None  # mc_seed not supplied in this call

	def test_mc_seed_forwards_to_mc_null_pvalue_and_changes_nothing_else(self, monkeypatch):
		captured: list = []

		def fake_mc_null_pvalue(rows, z_obs, n_sims, seed=None):
			captured.append(seed)
			return 0.0001  # small + deterministic: clears every alpha here

		monkeypatch.setattr(test_runner, "mc_null_pvalue", fake_mc_null_pvalue)

		b_default = _bucket()
		res_default = _bucket_bonferroni_verdict(
			[b_default], 2.0, 0.0, True, mc_rows_fn=lambda b: _healthy_rows(),
		)
		b_seeded = _bucket()
		res_seeded = _bucket_bonferroni_verdict(
			[b_seeded], 2.0, 0.0, True, mc_rows_fn=lambda b: _healthy_rows(),
			mc_seed=123,
		)

		assert captured == [None, 123]
		assert res_default[0] == res_seeded[0] == EDGE_EXISTS
		assert res_default[2:] == res_seeded[2:]  # z_stat, fee_adjusted_edge unchanged
		assert b_default["mc_p"] == b_seeded["mc_p"] == 0.0001
