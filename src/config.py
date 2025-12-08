"""
Configuration management for the Data Ingestion Subsystem.
Loads settings from YAML configuration files and environment variables.
"""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv


@dataclass
class SourceConfig:
    """Configuration for a single data source."""
    name: str
    type: str
    path: str
    target_table: str
    pk: list[str]
    schema: dict[str, str]
    rules: list[dict[str, str]] = field(default_factory=list)
    
    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "SourceConfig":
        return cls(
            name=data["name"],
            type=data["type"],
            path=data.get("path", ""),
            target_table=data["target_table"],
            pk=data["pk"],
            schema=data["schema"],
            rules=data.get("rules", []),
        )


@dataclass
class AppConfig:
    """Main application configuration."""
    db_url: str
    batch_size: int
    on_conflict: str
    sources: list[SourceConfig]
    
    @classmethod
    def from_yaml(cls, config_path: str | Path) -> "AppConfig":
        """Load configuration from a YAML file."""
        load_dotenv()
        
        with open(config_path, "r") as f:
            data = yaml.safe_load(f)
        
        defaults = data.get("defaults", {})
        
        # Allow environment variable override for db_url
        db_url = os.getenv("DATABASE_URL", defaults.get("db_url", ""))
        
        sources = [
            SourceConfig.from_dict(src) 
            for src in data.get("sources", [])
        ]
        
        return cls(
            db_url=db_url,
            batch_size=defaults.get("batch_size", 5000),
            on_conflict=defaults.get("on_conflict", "upsert"),
            sources=sources,
        )
    
    def get_source(self, name: str) -> SourceConfig | None:
        """Get a source configuration by name."""
        for source in self.sources:
            if source.name == name:
                return source
        return None
