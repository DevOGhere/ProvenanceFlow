"""Tests for the centralised Settings configuration module."""
import os
from pathlib import Path
from unittest.mock import patch

import pytest

from src.provenanceflow.config import Settings, get_settings


def test_settings_defaults_are_paths():
    s = Settings()
    assert isinstance(s.raw_data_path, Path)
    assert isinstance(s.processed_data_path, Path)
    assert isinstance(s.prov_db_path, Path)


def test_settings_default_source():
    s = Settings()
    assert s.pipeline_source == "nasa_gistemp"


def test_settings_default_log_level():
    s = Settings()
    assert s.log_level == "INFO"


def test_settings_gistemp_url_is_nasa():
    s = Settings()
    assert "giss.nasa.gov" in s.gistemp_url


def test_settings_env_var_override():
    with patch.dict(os.environ, {"PROV_DB_PATH": "/tmp/override.db"}):
        s = Settings()
        assert s.prov_db_path == Path("/tmp/override.db")


def test_settings_pipeline_source_override():
    with patch.dict(os.environ, {"PIPELINE_SOURCE": "local_csv"}):
        s = Settings()
        assert s.pipeline_source == "local_csv"


def test_get_settings_returns_settings_instance():
    # Clear lru_cache to ensure fresh call
    get_settings.cache_clear()
    s = get_settings()
    assert isinstance(s, Settings)


def test_get_settings_is_cached():
    get_settings.cache_clear()
    s1 = get_settings()
    s2 = get_settings()
    assert s1 is s2
