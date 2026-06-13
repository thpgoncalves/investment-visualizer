from __future__ import annotations

import logging
import sys
from time import perf_counter
from pathlib import Path

from infra.spark_utils import build_spark
from pipelines.gold.gold_metrics import run_gold_pipeline
from pipelines.shared.logging_utils import log_section_separator
from pipelines.silver.transformations import run_silver_pipeline


logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INPUT_PATH = PROJECT_ROOT / "data" / "bronze" / "economias.csv"
LOG_SEPARATOR = "=" * 72


def configure_pipeline_logging() -> None:
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%H:%M:%S",
        force=True,
    )
    logging.getLogger("py4j").setLevel(logging.WARNING)
    logging.getLogger("pyspark").setLevel(logging.WARNING)


def run_pipeline(input_path: str | Path = DEFAULT_INPUT_PATH) -> None:
    configure_pipeline_logging()
    input_path = Path(input_path)

    if not input_path.is_absolute():
        input_path = PROJECT_ROOT / input_path

    started_at = perf_counter()
    current_stage = "startup"

    logger.info(LOG_SEPARATOR)
    logger.info("🚀 PIPELINE | Investment Visualizer")
    logger.info("🚀 PIPELINE | Input=%s", input_path)
    logger.info(LOG_SEPARATOR)

    spark = None

    try:
        spark = build_spark(app_name="investment_visualizer_pipeline")

        current_stage = "silver"
        log_section_separator(logger)
        logger.info("🥈 SILVER | Starting")
        silver_snapshot_path = run_silver_pipeline(spark, input_path=str(input_path))
        logger.info("🥈 SILVER | Completed | output=%s", silver_snapshot_path)

        current_stage = "gold"
        log_section_separator(logger)
        logger.info("🥇 GOLD | Starting")
        gold_snapshot_paths = run_gold_pipeline(spark, input_path=silver_snapshot_path)
        logger.info("🥇 GOLD | Completed | files=%s", len(gold_snapshot_paths))
        logger.info("🥇 GOLD | Outputs=%s", gold_snapshot_paths)

        elapsed_seconds = perf_counter() - started_at
        logger.info(LOG_SEPARATOR)
        logger.info("🚀 PIPELINE | Completed successfully | duration=%.2fs", elapsed_seconds)
        logger.info(LOG_SEPARATOR)

    except Exception:
        elapsed_seconds = perf_counter() - started_at
        log_section_separator(logger)
        logger.exception(
            "🚀 PIPELINE | Failed | stage=%s | duration=%.2fs | input=%s",
            current_stage,
            elapsed_seconds,
            input_path,
        )
        raise

    finally:
        if spark is not None:
            spark.stop()
            log_section_separator(logger)
            logger.info("⚙️ SPARK | Session stopped")


if __name__ == "__main__":
    run_pipeline()
