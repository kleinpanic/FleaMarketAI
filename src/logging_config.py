"""Structured JSON logging for FleaMarketAI.

Usage:
    from .logging_config import setup_logging
    setup_logging("validator", level=logging.INFO)
    
    # Then use normal logging - it outputs JSON
    log.info("Key validated", extra={"provider": "openai", "is_valid": True})
"""

import json
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict


class JSONFormatter(logging.Formatter):
    """Format log records as JSON."""
    
    def format(self, record: logging.LogRecord) -> str:
        log_obj: Dict[str, Any] = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        
        # Add extra fields
        for key in ["provider", "is_valid", "source_url", "job_id", "queue_depth", 
                    "validation_count", "rate_limit_hit", "circuit_breaker"]:
            if hasattr(record, key):
                log_obj[key] = getattr(record, key)
        
        # Add exception info if present
        if record.exc_info:
            log_obj["exception"] = self.formatException(record.exc_info)
        
        return json.dumps(log_obj)


def setup_logging(component: str, level: int = logging.INFO, json_output: bool = True):
    """Set up structured logging for a component.
    
    Args:
        component: Name of the component (validator, discoverer, revalidator)
        level: Logging level
        json_output: If True, output JSON; if False, plain text
    """
    log_dir = Path(__file__).resolve().parents[1] / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    
    logger = logging.getLogger()
    logger.setLevel(level)
    
    # Clear existing handlers
    logger.handlers = []
    
    # File handler
    file_handler = logging.handlers.RotatingFileHandler(
        log_dir / f"{component}.log",
        maxBytes=5 * 1024 * 1024,
        backupCount=5,
    )
    
    if json_output:
        file_handler.setFormatter(JSONFormatter())
    else:
        file_handler.setFormatter(logging.Formatter(
            fmt="%(asctime)s  %(levelname)-8s  %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        ))
    
    logger.addHandler(file_handler)
    
    # Console handler (always plain text for readability)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(logging.Formatter(
        fmt="%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    ))
    logger.addHandler(console_handler)
    
    return logger
