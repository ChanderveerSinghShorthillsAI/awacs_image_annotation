"""
Centralized logging configuration for the AWACS project.

Usage:
    from ai_tool.awacs_logger import setup_logger
    logger = setup_logger("awacs.backend")
    logger.info("Server started")

All loggers write to stdout (matching previous print() behavior).
Named loggers allow filtering by component.
"""

import logging
import sys


def setup_logger(name: str, level=logging.INFO) -> logging.Logger:
    """Create and return a named logger with console handler.

    Args:
        name:  Dotted logger name, e.g. "awacs.backend", "awacs.cdc.consumer".
        level: Logging level (default: INFO).

    Returns:
        Configured Logger instance.
    """
    logger = logging.getLogger(name)

    # Avoid duplicate handlers when the module is re-imported or the
    # function is called more than once for the same logger name.
    if not logger.handlers:
        logger.setLevel(level)

        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(level)

        formatter = logging.Formatter(
            "%(asctime)s | %(name)-25s | %(levelname)-7s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

        # Prevent log messages from propagating to the root logger,
        # which would cause duplicate output when other libraries
        # (uvicorn, kafka, etc.) configure the root logger.
        logger.propagate = False

    return logger
