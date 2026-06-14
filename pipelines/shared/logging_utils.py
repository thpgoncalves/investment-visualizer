import logging


LOG_SECTION_SEPARATOR = "-" * 72


class PipelineWarningCollector(logging.Handler):
    def __init__(self) -> None:
        super().__init__(level=logging.WARNING)
        self.messages: list[str] = []
        self.collecting = True

    def emit(self, record: logging.LogRecord) -> None:
        if (
            self.collecting
            and record.levelno == logging.WARNING
            and record.name.startswith("pipelines")
        ):
            self.messages.append(record.getMessage())

    @property
    def count(self) -> int:
        return len(self.messages)


def log_section_separator(logger: logging.Logger) -> None:
    logger.info(LOG_SECTION_SEPARATOR)


def log_warning_summary(
    logger: logging.Logger,
    collector: PipelineWarningCollector,
) -> None:
    if not collector.messages:
        return

    collector.collecting = False

    try:
        logger.warning("⚠️ PIPELINE WARNINGS | total=%s", collector.count)

        for index, message in enumerate(collector.messages, start=1):
            message = message.removeprefix("⚠️ ")
            logger.warning(
                "⚠️ WARNING [%s/%s] | %s",
                index,
                collector.count,
                message,
            )

        logger.info(LOG_SECTION_SEPARATOR)
    finally:
        collector.collecting = True
