from __future__ import annotations

import logging
import sys
from pathlib import Path
from time import perf_counter

from infra.spark_utils import build_spark
from pipelines.bronze.ingestion import archive_bronze_input, load_bronze_input
from pipelines.gold.gold_metrics import save_gold_snapshots, transform_gold
from pipelines.shared.logging_utils import (
    PipelineWarningCollector,
    log_section_separator,
    log_warning_summary,
)
from pipelines.silver.transformations import save_silver_snapshot, transform_silver

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INPUT_PATH = PROJECT_ROOT / "data" / "bronze" / "economias.csv"
LOG_SEPARATOR = "=" * 72


def configure_pipeline_logging() -> PipelineWarningCollector:
    """
    Configura a formatação e encoding do logging da pipeline,
    anexando o coletor de warnings da execução.

    Retorna:
        Instância de PipelineWarningCollector configurada.
    """
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

    warning_collector = PipelineWarningCollector()
    logging.getLogger().addHandler(warning_collector)
    return warning_collector


def run_pipeline(input_path: str | Path = DEFAULT_INPUT_PATH) -> None:
    """
    Orquestrador mestre da pipeline ETL do Investment Visualizer.
    Executa o ciclo de vida completo dos dados no estilo funcional:
    Bronze (ingestão) -> Silver (transformação e snapshot) ->
    Gold (modelagem analítica e 10 snapshots) -> Bronze (arquivamento).

    Parâmetros:
        input_path: Caminho para o arquivo CSV de entrada da camada Bronze.
    """
    warning_collector = configure_pipeline_logging()
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

        # 🥉 BRONZE | Ingestão e Preparação
        current_stage = "bronze_load"
        log_section_separator(logger)
        logger.info("🥉 BRONZE | Loading input")
        df_bronze = load_bronze_input(spark, input_path)

        # 🥈 SILVER | Transformação Pura e Persistência
        current_stage = "silver"
        log_section_separator(logger)
        logger.info("🥈 SILVER | Starting transformation")
        df_silver = transform_silver(df_bronze)
        silver_snapshot_path = save_silver_snapshot(df_silver)
        logger.info("🥈 SILVER | Completed | output=%s", silver_snapshot_path)

        # 🥇 GOLD | Modelagem Analítica Pura e Persistência dos 10 Datasets
        current_stage = "gold"
        log_section_separator(logger)
        logger.info("🥇 GOLD | Starting transformation")
        gold_datasets = transform_gold(df_silver)
        gold_snapshot_paths = save_gold_snapshots(gold_datasets)
        logger.info("🥇 GOLD | Completed | files=%s", len(gold_snapshot_paths))
        logger.info("🥇 GOLD | Outputs=%s", gold_snapshot_paths)

        # 🥉 BRONZE | Arquivamento do arquivo de entrada processado
        current_stage = "bronze_archive"
        log_section_separator(logger)
        logger.info("🥉 BRONZE | Archiving processed input")
        bronze_history_path = archive_bronze_input(input_path)
        logger.info("🥉 BRONZE | Archive completed | output=%s", bronze_history_path)

        elapsed_seconds = perf_counter() - started_at
        logger.info(LOG_SEPARATOR)
        log_warning_summary(logger, warning_collector)
        logger.info(
            "🚀 PIPELINE | Completed successfully | warnings=%s | duration=%.2fs",
            warning_collector.count,
            elapsed_seconds,
        )
        logger.info(LOG_SEPARATOR)

    except Exception:
        elapsed_seconds = perf_counter() - started_at
        log_section_separator(logger)
        log_warning_summary(logger, warning_collector)
        logger.exception(
            "🚀 PIPELINE | Failed | stage=%s | warnings=%s | duration=%.2fs | input=%s",
            current_stage,
            warning_collector.count,
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
