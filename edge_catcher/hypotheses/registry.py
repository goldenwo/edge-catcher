import importlib
import logging
from pathlib import Path
from typing import Dict, Any, Optional

import yaml

logger = logging.getLogger(__name__)


def _load_hypothesis_configs(config_path: Path) -> Dict[str, dict]:
    """Load hypothesis entries from config_path and its .local sibling, merged.

    Entries are keyed by their YAML key. Additionally, if a hypothesis has a
    'module' field, the last segment of the module path is added as an alias
    so modules can look themselves up by their internal HYPOTHESIS_ID.
    """
    merged: Dict[str, dict] = {}
    local_dir = config_path.parent / (config_path.name + ".local")
    for cfg_dir in [config_path, local_dir]:
        hyp_yaml = cfg_dir / "hypotheses.yaml"
        if hyp_yaml.exists():
            with open(hyp_yaml) as f:
                data = yaml.safe_load(f) or {}
            merged.update(data.get("hypotheses", {}))
    # Add module-name aliases so modules can self-lookup by HYPOTHESIS_ID
    aliases: Dict[str, dict] = {}
    for hyp_id, cfg in merged.items():
        module_path = cfg.get("module", "")
        module_name = module_path.rsplit(".", 1)[-1] if module_path else ""
        if module_name and module_name != hyp_id and module_name not in merged:
            aliases[module_name] = cfg
    merged.update(aliases)
    return merged


def discover(config_path: Path = Path("config")) -> Dict[str, Any]:
    """Import all hypothesis modules listed in hypotheses.yaml.

    Returns {hypothesis_id: module} for each entry that has a 'module' key
    and can be successfully imported.
    """
    configs = _load_hypothesis_configs(config_path)
    modules: Dict[str, Any] = {}
    for hyp_id, hyp_config in configs.items():
        module_path = hyp_config.get("module")
        if not module_path:
            logger.warning("Hypothesis %s has no 'module' key — skipping", hyp_id)
            continue
        try:
            mod = importlib.import_module(module_path)
            modules[hyp_id] = mod
            logger.info("Discovered hypothesis: %s (%s)", hyp_id, module_path)
        except ImportError as e:
            logger.error(
                "Failed to import hypothesis %s (%s): %s", hyp_id, module_path, e
            )
    return modules


def run_hypothesis(
    hyp_id: str,
    db_conn,
    config_path: Path = Path("config"),
) -> Any:
    """Run a single hypothesis module's run() function.

    Raises:
        ValueError: hypothesis not found in config or has no 'module' key
        AttributeError: module has no 'run' function
    """
    configs = _load_hypothesis_configs(config_path)
    if hyp_id not in configs:
        raise ValueError(f"Hypothesis '{hyp_id}' not found in hypotheses.yaml")

    module_path = configs[hyp_id].get("module")
    if not module_path:
        raise ValueError(f"Hypothesis '{hyp_id}' has no 'module' configured")

    mod = importlib.import_module(module_path)
    if not hasattr(mod, "run"):
        raise AttributeError(
            f"Hypothesis module {module_path} has no 'run' function"
        )

    logger.info("Running hypothesis: %s", hyp_id)
    # Pass hyp_config if run() accepts it, so modules don't need to re-lookup
    import inspect
    sig = inspect.signature(mod.run)
    if "hyp_config" in sig.parameters:
        return mod.run(db_conn, config_path, hyp_config=configs[hyp_id])
    return mod.run(db_conn, config_path)


def run_all(
    db_conn,
    config_path: Path = Path("config"),
) -> Dict[str, Any]:
    """Discover and run all hypotheses. Returns {hyp_id: result_or_error}."""
    modules = discover(config_path)
    results: Dict[str, Any] = {}
    for hyp_id, mod in modules.items():
        if hasattr(mod, "run"):
            try:
                results[hyp_id] = mod.run(db_conn, config_path)
                logger.info("Hypothesis %s completed: %s", hyp_id, results[hyp_id].verdict)
            except Exception as e:
                logger.error("Hypothesis %s failed: %s", hyp_id, e)
                results[hyp_id] = {"error": str(e)}
        else:
            logger.warning("Hypothesis module for %s has no 'run' function", hyp_id)
    return results
