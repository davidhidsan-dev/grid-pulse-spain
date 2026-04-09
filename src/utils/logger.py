import logging


def get_logger(name: str) -> logging.Logger:
    """
    Create and return a configured logger.

    The logger writes messages to the console using a consistent format:
    timestamp | level | module name | message
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setLevel(logging.INFO)

        formatter = logging.Formatter(
            fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)

        logger.addHandler(handler)

    logger.propagate = False
    return logger