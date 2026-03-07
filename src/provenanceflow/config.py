"""
Centralised configuration for ProvenanceFlow.

All hardcoded paths and URLs move here. Env vars override defaults.
Load via get_settings() — the result is cached for the process lifetime.

Example override:
    PROV_DB_PATH=/data/prov.db python -m provenanceflow
"""
from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Data source
    gistemp_url: str = Field(
        default="https://data.giss.nasa.gov/gistemp/tabledata_v4/GLB.Ts+dSST.csv"
    )
    pipeline_source: str = Field(default="nasa_gistemp")

    # Filesystem paths
    raw_data_path: Path = Field(default=Path("data/raw"))
    processed_data_path: Path = Field(default=Path("data/processed"))
    prov_db_path: Path = Field(default=Path("provenance_store/lineage.db"))

    # Runtime
    log_level: str = Field(default="INFO")
    api_host: str = Field(default="0.0.0.0")
    api_port: int = Field(default=8000)


@lru_cache
def get_settings() -> Settings:
    """Return a cached Settings singleton. Override env vars before first call."""
    return Settings()


def configure_logging(level: str | None = None) -> None:
    """Configure stdlib logging with a structured format.

    Uses get_settings().log_level unless an explicit level is passed.
    Call once at application startup (CLI entry point, Airflow tasks).
    """
    import logging
    logging.basicConfig(
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
        level=getattr(logging, (level or get_settings().log_level).upper(), logging.INFO),
    )
