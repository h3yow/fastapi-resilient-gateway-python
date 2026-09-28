"""Logging helpers: health changes and circuit breaker transitions."""

import logging

logger = logging.getLogger("gateway")


def configure_logging(level: int = logging.INFO) -> None:
    """Configure one-line INFO logging for health and breaker transitions.

    Idempotent: attaching handlers more than once is avoided.
    """
    if logger.handlers:
        logger.setLevel(level)
        return
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s")
    )
    logger.addHandler(handler)
    logger.setLevel(level)
    logger.propagate = False
