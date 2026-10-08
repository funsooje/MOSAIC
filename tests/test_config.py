import json
import pytest
from pathlib import Path

from geometrics.config import GeoMetricsConfig, load_config, save_config


def test_default_config():
    cfg = GeoMetricsConfig()
    assert cfg.db_url == "sqlite:///geometrics.db"
    assert cfg.gdrive_base == ""
    assert cfg.backend == "hiergp"


def test_save_and_load_roundtrip(tmp_path):
    path = tmp_path / "config.json"
    cfg = GeoMetricsConfig(
        db_url="postgresql://user:pass@localhost/geo",
        gdrive_base="/Users/funsooje/Google Drive/My Drive",
        backend="h3",
    )
    save_config(cfg, path=path)
    assert path.exists()

    loaded = load_config(path=path)
    assert loaded.db_url == cfg.db_url
    assert loaded.gdrive_base == cfg.gdrive_base
    assert loaded.backend == cfg.backend


def test_load_returns_defaults_when_file_missing(tmp_path):
    path = tmp_path / "nonexistent.json"
    cfg = load_config(path=path)
    assert cfg == GeoMetricsConfig()


def test_save_creates_parent_dirs(tmp_path):
    path = tmp_path / "nested" / "dir" / "config.json"
    save_config(GeoMetricsConfig(), path=path)
    assert path.exists()


def test_saved_file_is_valid_json(tmp_path):
    path = tmp_path / "config.json"
    save_config(GeoMetricsConfig(db_url="sqlite:///test.db"), path=path)
    with open(path) as f:
        data = json.load(f)
    assert data["db_url"] == "sqlite:///test.db"
