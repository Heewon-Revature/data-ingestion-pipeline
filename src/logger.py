"""
Structured logging module for the ingestion pipeline.
"""

import logging
import sys
from datetime import datetime


class StructuredLogger:
    """Provides structured logging for ingestion runs."""
    
    def __init__(self, name: str = "ingest"):
        self.logger = logging.getLogger(name)
        self._setup_logger()
        self._run_stats: dict[str, dict] = {}
    
    def _setup_logger(self):
        """Configure the logger with structured format."""
        if not self.logger.handlers:
            handler = logging.StreamHandler(sys.stdout)
            formatter = logging.Formatter(
                "%(levelname)s %(name)s.%(message)s"
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)
    
    def start_run(self, source_name: str, rows: int, path: str = "") -> None:
        """Log the start of an ingestion run."""
        self._run_stats[source_name] = {
            "start_time": datetime.now(),
            "input_rows": rows,
        }
        self.logger.info(f"start source={source_name} rows={rows} path={path}")
    
    def log_cleaned(self, source_name: str, valid: int, rejected: int) -> None:
        """Log cleaning results."""
        self._run_stats[source_name]["valid"] = valid
        self._run_stats[source_name]["rejected"] = rejected
        self.logger.info(f"cleaned source={source_name} valid={valid} rejected={rejected}")
    
    def log_load(
        self, 
        source_name: str, 
        inserted: int, 
        updated: int, 
        duration: float
    ) -> None:
        """Log load results."""
        self._run_stats[source_name]["inserted"] = inserted
        self._run_stats[source_name]["updated"] = updated
        self._run_stats[source_name]["load_duration"] = duration
        self.logger.info(
            f"load source={source_name} inserted={inserted} updated={updated} duration={duration:.2f}s"
        )
    
    def end_run(self, source_name: str, status: str = "success") -> None:
        """Log the end of an ingestion run."""
        stats = self._run_stats.get(source_name, {})
        start_time = stats.get("start_time")
        
        if start_time:
            total_duration = (datetime.now() - start_time).total_seconds()
            stats["total_duration"] = total_duration
        
        self.logger.info(f"end source={source_name} status={status}")
    
    def log_error(self, source_name: str, error: str) -> None:
        """Log an error."""
        self.logger.error(f"error source={source_name} error={error}")
    
    def get_summary(self, source_name: str) -> str:
        """Get a summary string for a run."""
        stats = self._run_stats.get(source_name, {})
        return (
            f"Source: {source_name} | "
            f"Input: {stats.get('input_rows', 0)} | "
            f"Valid: {stats.get('valid', 0)} | "
            f"Rejected: {stats.get('rejected', 0)} | "
            f"Inserted: {stats.get('inserted', 0)} | "
            f"Duration: {stats.get('total_duration', 0):.2f}s"
        )


logger = StructuredLogger()
