from __future__ import annotations

import logging

from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame

from pipelines.gold.metrics_builders import (
    build_contribution_metrics,
    build_contributions_context,
    build_distribution_metrics,
    build_home_metrics,
    build_institution_metrics,
    get_latest_portfolio_snapshot,
    get_variation,
    get_variation_with_flow,
)
from pipelines.shared.logging_utils import log_section_separator
from pipelines.shared.partition_handler import handler_partitions

logger = logging.getLogger(__name__)

__all__ = [
    "get_variation",
    "get_variation_with_flow",
    "get_latest_portfolio_snapshot",
    "build_contributions_context",
    "build_home_metrics",
    "build_contribution_metrics",
    "build_distribution_metrics",
    "build_institution_metrics",
    "transform_gold",
    "save_gold_snapshots",
]


def _cast_silver_input(df_silver: DataFrame) -> DataFrame:
    """
    Garante o cast canônico das colunas do DataFrame Silver de entrada
    caso ele tenha sido lido diretamente de arquivo CSV.

    Parâmetros:
        df_silver: DataFrame PySpark de entrada da camada Silver.

    Retorna:
        DataFrame PySpark com tipos devidamente projetados.
    """
    df_casted = df_silver.select(
        F.to_timestamp(F.col("timestamp")).alias("timestamp"),
        F.to_date(F.col("data_apuracao")).alias("data_apuracao"),
        F.col("bronze_row_id").cast("int").alias("bronze_row_id"),
        F.col("ano").cast("int").alias("ano"),
        F.col("mes_num").cast("int").alias("mes_num"),
        F.col("mes").cast("string").alias("mes"),
        F.col("instituicao_fin").cast("string").alias("instituicao_fin"),
        F.col("resumo").cast("string").alias("resumo"),
        F.col("tipo").cast("string").alias("tipo"),
        F.col("nome").cast("string").alias("nome"),
        F.col("qtd").cast("double").alias("qtd"),
        F.col("preco_medio").cast("double").alias("preco_medio"),
        F.col("preco_atual").cast("double").alias("preco_atual"),
        F.col("valor_total").cast("double").alias("valor_total"),
        F.col("aporte").cast("double").alias("aporte"),
        F.col("exposicao").cast("string").alias("exposicao"),
    )
    return df_casted


def transform_gold(df_silver: DataFrame) -> dict[str, DataFrame]:
    """
    Orquestra a transformação pura da camada Gold, delegando aos builders
    a geração dos 10 datasets analíticos consumidos pelo dashboard Streamlit.

    Parâmetros:
        df_silver: DataFrame PySpark processado da camada Silver.

    Retorna:
        Dicionário mapeando o identificador de cada dataset ao seu DataFrame PySpark.
    """
    log_section_separator(logger)
    logger.info("🥇 GOLD | Transformations started")

    df_prepared = _cast_silver_input(df_silver)

    input_stats = (
        df_prepared
        .agg(
            F.count(F.lit(1)).alias("rows"),
            F.countDistinct("instituicao_fin").alias("institutions"),
            F.min("data_apuracao").alias("min_date"),
            F.max("data_apuracao").alias("max_date"),
        )
        .first()
    )
    logger.info(
        "🥇 GOLD | Silver input summary | rows=%s | institutions=%s | period=%s to %s",
        input_stats["rows"],
        input_stats["institutions"],
        input_stats["min_date"],
        input_stats["max_date"],
    )

    contributions_ctx = build_contributions_context(df_prepared)
    df_dados_atuais = get_latest_portfolio_snapshot(df_prepared)

    home_metrics = build_home_metrics(df_prepared, contributions_ctx)
    contribution_metrics = build_contribution_metrics(contributions_ctx)
    distribution_metrics = build_distribution_metrics(df_dados_atuais)
    institution_metrics = build_institution_metrics(df_prepared, df_dados_atuais, contributions_ctx)

    gold_datasets = {
        **home_metrics,
        **contribution_metrics,
        **distribution_metrics,
        **institution_metrics,
    }
    logger.info("🥇 GOLD | Transformations completed | generated_datasets=%s", len(gold_datasets))
    return gold_datasets


def save_gold_snapshots(gold_datasets: dict[str, DataFrame]) -> list[str]:
    """
    Persiste os datasets analíticos da camada Gold na partição temporal correspondente
    em data/gold/<YYYYMM>/ utilizando o partition handler.

    Parâmetros:
        gold_datasets: Dicionário contendo os 10 DataFrames analíticos modelados.

    Retorna:
        Lista com os caminhos dos arquivos CSV salvos.
    """
    log_section_separator(logger)
    logger.info("🥇 GOLD | Writing snapshots | count=%s", len(gold_datasets))

    saved_paths = []
    for file_name, df_metric in gold_datasets.items():
        saved_path = handler_partitions(df_metric, "gold", file_name)
        saved_paths.append(saved_path)

    logger.info("🥇 GOLD | All snapshots saved | total_files=%s", len(saved_paths))
    return saved_paths
