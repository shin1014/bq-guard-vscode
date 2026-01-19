from __future__ import annotations

import copy
import json
from typing import Any, Dict

import yaml
from platformdirs import user_cache_dir, user_config_dir

DEFAULT_CONFIG: Dict[str, Any] = {
    "app": {
        "default_project": None,
        "default_location": None,
        "preview_rows": 50,
        "page_size": 1000,
        "limits": {
            "warn_bytes": 107374182400,
            "block_bytes": 536870912000,
        },
        "policy": {
            "enforce_partition_filter": True,
            "block_multi_statement": True,
            "warn_select_star": True,
            "warn_cross_join": True,
            "warn_suspect_join": True,
            "warn_ddl_dml": True,
            "allow_execute_with_warnings": True,
        },
        "exceptions": {
            "partition_exempt_tables": [],
        },
        "cache": {
            "schema_version": 1,
        },
        "bq": {
            "use_query_cache": False,
            "labels": {
                "app": "bq-guard",
                "env": "gce",
            },
        },
        "ui": {
            "auto_estimate_debounce_ms": 900,
        },
    }
}


class ConfigLoader:
    def __init__(self) -> None:
        self.config_dir = user_config_dir("bq_guard")
        self.config_path = f"{self.config_dir}/config.yaml"
        self._config = None

    def load(self) -> Dict[str, Any]:
        if self._config is not None:
            return self._config
        data = copy.deepcopy(DEFAULT_CONFIG)
        try:
            with open(self.config_path, "r", encoding="utf-8") as handle:
                loaded = yaml.safe_load(handle) or {}
            data = self._merge(data, loaded)
        except FileNotFoundError:
            self._ensure_default_written(data)
        except Exception:
            self._ensure_default_written(data)
        self._config = self._validate(data)
        return self._config

    def _ensure_default_written(self, data: Dict[str, Any]) -> None:
        import os

        os.makedirs(self.config_dir, exist_ok=True)
        with open(self.config_path, "w", encoding="utf-8") as handle:
            yaml.safe_dump(data, handle, sort_keys=False)

    def _merge(self, base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
        for key, value in override.items():
            if isinstance(value, dict) and isinstance(base.get(key), dict):
                base[key] = self._merge(base[key], value)
            else:
                base[key] = value
        return base

    def _validate(self, data: Dict[str, Any]) -> Dict[str, Any]:
        def safe_int(path: str, default: int) -> int:
            try:
                parts = path.split(".")
                current = data
                for part in parts:
                    current = current[part]
                if isinstance(current, int) and current >= 0:
                    return current
            except Exception:
                return default
            return default

        data["app"]["preview_rows"] = safe_int("app.preview_rows", 50)
        data["app"]["page_size"] = safe_int("app.page_size", 1000)
        data["app"]["limits"]["warn_bytes"] = safe_int("app.limits.warn_bytes", 107374182400)
        data["app"]["limits"]["block_bytes"] = safe_int("app.limits.block_bytes", 536870912000)
        data["app"]["cache"]["schema_version"] = safe_int("app.cache.schema_version", 1)
        data["app"]["ui"]["auto_estimate_debounce_ms"] = safe_int(
            "app.ui.auto_estimate_debounce_ms", 900
        )
        return data

    def as_json(self) -> str:
        return json.dumps(self.load())


def get_cache_path() -> str:
    cache_dir = user_cache_dir("bq_guard")
    return f"{cache_dir}/table_meta_cache.json"


def get_history_path() -> str:
    from platformdirs import user_state_dir

    history_dir = user_state_dir("bq_guard")
    return f"{history_dir}/history.jsonl"
