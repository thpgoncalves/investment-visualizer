import logging


LOG_SECTION_SEPARATOR = "-" * 72


def log_section_separator(logger: logging.Logger) -> None:
    logger.info(LOG_SECTION_SEPARATOR)
