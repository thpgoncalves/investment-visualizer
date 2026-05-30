from __future__ import annotations

import logging
from pathlib import Path

from infra.spark_utils import build_spark
from pipelines.gold.gold_metrics import run_gold_pipeline
from pipelines.silver.transformations import run_silver_pipeline


logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INPUT_PATH = PROJECT_ROOT / "data" / "bronze" / "economias.csv"


def run_pipeline(input_path: str | Path = DEFAULT_INPUT_PATH) -> None:
    input_path = Path(input_path)

    if not input_path.is_absolute():
        input_path = PROJECT_ROOT / input_path

    spark = build_spark(app_name="investment_visualizer_pipeline")

    try:
        logger.info("Running silver pipeline")
        silver_snapshot_path = run_silver_pipeline(spark, input_path=str(input_path))

        logger.info("Running gold pipeline")
        run_gold_pipeline(spark, input_path=silver_snapshot_path)

        logger.info("Pipeline finished successfully")

    finally:
        spark.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s - %(message)s")
    run_pipeline()
