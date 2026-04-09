"""Tests for the LLM-first loop phase ordering."""

from unittest.mock import MagicMock, patch

import pytest


class TestLoopPhaseOrder:
    def test_default_mode_runs_hypothesis_phase_first(self):
        """In default mode (not grid_only, not llm_only), Phase 1 should be hypothesis-driven."""
        from edge_catcher.research.loop import LoopOrchestrator

        orch = LoopOrchestrator.__new__(LoopOrchestrator)
        call_order = []

        orch.grid_only = False
        orch.llm_only = False
        orch.refine_only = False
        orch.max_runs = 10
        orch.max_llm_calls = 5
        orch.start_date = "2025-01-01"
        orch.end_date = "2025-12-31"
        orch.fee_pct = 1.0
        orch.force = False
        orch.parallel = 1
        orch.max_refinements = 3
        orch.research_db = "data/research.db"
        orch.max_time_seconds = None
        orch._cached_results = None
        orch.max_stuck_runs = 3
        orch.output_path = None
        orch.run_id = "test-run-id"
        orch.cancel_event = None
        orch.on_progress = None

        with patch.object(orch, '_run_hypothesis_phase', return_value=([], 0)) as mock_hyp, \
             patch.object(orch, '_run_expand_phase', return_value=[]) as mock_expand, \
             patch('edge_catcher.research.refinement.RefinementExecutor.run', return_value=[]) as mock_refine, \
             patch.object(orch, '_discover_strategies', return_value=[]), \
             patch.object(orch, '_discover_series', return_value={}), \
             patch.object(orch, '_list_results', return_value=[]), \
             patch('edge_catcher.research.observer.ResearchObserver.write_phase_outcomes'), \
             patch('edge_catcher.research.observer.ResearchObserver.write_journal_summary', return_value="stuck"), \
             patch('edge_catcher.research.observer.ResearchObserver.update_kill_registry'), \
             patch('edge_catcher.research.observer.ResearchObserver.cleanup_dead_strategies'), \
             patch('edge_catcher.research.loop.ResearchAgent'), \
             patch('edge_catcher.research.loop.RunQueue'), \
             patch('edge_catcher.research.loop.ResearchJournal' if hasattr(__import__('edge_catcher.research.loop', fromlist=['ResearchJournal']), 'ResearchJournal') else 'edge_catcher.research.journal.ResearchJournal') as MockJournal:

            MockJournal.return_value.get_latest_trajectory.return_value = None

            orch.tracker = MagicMock()
            orch.audit = MagicMock()
            orch.audit.compute_result_hash = MagicMock(return_value="hash")
            orch.audit.record_integrity = MagicMock()

            def track_hypothesis(*args, **kwargs):
                call_order.append("hypothesis")
                return [], 0
            def track_expand(*args, **kwargs):
                call_order.append("expand")
                return []
            def track_refine(*args, **kwargs):
                call_order.append("refine")
                return []

            mock_hyp.side_effect = track_hypothesis
            mock_expand.side_effect = track_expand
            mock_refine.side_effect = track_refine

            orch.run()

            assert call_order[0] == "hypothesis", f"Expected hypothesis first, got {call_order}"

    def test_llm_only_mode_runs_ideate_phase(self):
        """In llm_only mode, Phase 1 should use _run_ideate_phase (legacy path)."""
        from edge_catcher.research.loop import LoopOrchestrator

        orch = LoopOrchestrator.__new__(LoopOrchestrator)

        orch.grid_only = False
        orch.llm_only = True
        orch.refine_only = False
        orch.max_runs = 10
        orch.max_llm_calls = 5
        orch.start_date = "2025-01-01"
        orch.end_date = "2025-12-31"
        orch.fee_pct = 1.0
        orch.force = False
        orch.parallel = 1
        orch.max_refinements = 3
        orch.research_db = "data/research.db"
        orch.max_time_seconds = None
        orch._cached_results = None
        orch.max_stuck_runs = 3
        orch.output_path = None
        orch.run_id = "test-run-id"
        orch.cancel_event = None
        orch.on_progress = None

        with patch.object(orch, '_run_ideate_phase', return_value=([], 0)) as mock_ideate, \
             patch.object(orch, '_run_hypothesis_phase', return_value=([], 0)) as mock_hyp, \
             patch.object(orch, '_run_expand_phase', return_value=[]), \
             patch('edge_catcher.research.refinement.RefinementExecutor.run', return_value=[]), \
             patch.object(orch, '_discover_strategies', return_value=[]), \
             patch.object(orch, '_discover_series', return_value={}), \
             patch.object(orch, '_list_results', return_value=[]), \
             patch('edge_catcher.research.observer.ResearchObserver.write_phase_outcomes'), \
             patch('edge_catcher.research.observer.ResearchObserver.write_journal_summary', return_value="stuck"), \
             patch('edge_catcher.research.observer.ResearchObserver.update_kill_registry'), \
             patch('edge_catcher.research.observer.ResearchObserver.cleanup_dead_strategies'), \
             patch('edge_catcher.research.loop.ResearchAgent'), \
             patch('edge_catcher.research.loop.RunQueue'), \
             patch('edge_catcher.research.loop.ResearchJournal' if hasattr(__import__('edge_catcher.research.loop', fromlist=['ResearchJournal']), 'ResearchJournal') else 'edge_catcher.research.journal.ResearchJournal') as MockJournal:

            MockJournal.return_value.get_latest_trajectory.return_value = None

            orch.tracker = MagicMock()
            orch.audit = MagicMock()
            orch.audit.compute_result_hash = MagicMock(return_value="hash")
            orch.audit.record_integrity = MagicMock()

            orch.run()

            mock_ideate.assert_called_once()
            mock_hyp.assert_not_called()


    def test_grid_only_skips_ideate(self):
        """In grid_only mode, should NOT call _run_ideate_phase."""
        from edge_catcher.research.loop import LoopOrchestrator

        orch = LoopOrchestrator.__new__(LoopOrchestrator)

        orch.grid_only = True
        orch.llm_only = False
        orch.refine_only = False
        orch.max_runs = 10
        orch.max_llm_calls = 5
        orch.start_date = "2025-01-01"
        orch.end_date = "2025-12-31"
        orch.fee_pct = 1.0
        orch.force = False
        orch.parallel = 1
        orch.max_refinements = 3
        orch.research_db = "data/research.db"
        orch.max_time_seconds = None
        orch._cached_results = None
        orch.max_stuck_runs = 3
        orch.output_path = None
        orch.run_id = "test-run-id"
        orch.cancel_event = None
        orch.on_progress = None

        with patch.object(orch, '_discover_strategies', return_value=["example"]), \
             patch.object(orch, '_discover_series', return_value={"data/test.db": ["TEST_SERIES"]}), \
             patch.object(orch, '_list_results', return_value=[]), \
             patch('edge_catcher.research.observer.ResearchObserver.write_phase_outcomes'), \
             patch('edge_catcher.research.observer.ResearchObserver.write_journal_summary', return_value="stuck"), \
             patch('edge_catcher.research.observer.ResearchObserver.update_kill_registry'), \
             patch('edge_catcher.research.observer.ResearchObserver.cleanup_dead_strategies'), \
             patch('edge_catcher.research.loop.ResearchAgent'), \
             patch('edge_catcher.research.loop.RunQueue') as MockQueue, \
             patch('edge_catcher.research.loop.GridPlanner') as MockPlanner, \
             patch('edge_catcher.research.journal.ResearchJournal') as MockJournal:

            MockJournal.return_value.get_latest_trajectory.return_value = None

            orch.tracker = MagicMock()
            orch.audit = MagicMock()
            orch.audit.compute_result_hash = MagicMock(return_value="hash")
            orch.audit.record_integrity = MagicMock()

            mock_planner = MockPlanner.return_value
            mock_planner.generate.return_value = []

            mock_queue = MockQueue.return_value
            mock_queue.submit.return_value = []

            orch.run()

            # GridPlanner should have been called
            mock_planner.generate.assert_called()
            # _run_ideate_phase should NOT exist as a call
            assert not hasattr(orch, '_run_ideate_phase_called')
