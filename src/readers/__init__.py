"""
Data readers package.
"""

from readers.api_reader import APIReader


def get_reader(source_config):
    """Get the API reader for a source."""
    if source_config.type.lower() != "api":
        raise ValueError(f"Unsupported source type: {source_config.type}. Only 'api' is supported.")
    return APIReader(source_config)


__all__ = ["APIReader", "get_reader"]
