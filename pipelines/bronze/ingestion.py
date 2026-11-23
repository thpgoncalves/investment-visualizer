from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

from pipelines.shared.partition_handler import handler_partitions

logger = logging.getLogger(__name__)

EXCEL_COLUMN_MAPPING = {
    "Carimbo de data/hora": "timestamp",
    "Data Apuração:": "data_apuracao",
    "Instituição Financeira:": "instituicao_fin",
    "Resumo Investimentos:": "resumo",
    "Aporte:": "aporte",
}


def load_bronze_input(spark: SparkSession, input_path: str | Path) -> DataFrame:
    """
    Carrega o arquivo CSV bruto da camada Bronze no PySpark.
    Normaliza cabeçalhos legados de formulário in-place quando necessário,
    valida a volumetria inicial e adiciona um identificador incremental de linha.

    Parâmetros:
        spark: Sessão ativa do Apache Spark.
        input_path: Caminho para o arquivo CSV de entrada.

    Retorna:
        DataFrame PySpark com o esquema Bronze e a coluna 'bronze_row_id'.
    """
    resolved_path = Path(input_path)

    try:
        df_check = pd.read_csv(resolved_path, nrows=0)
        if "instituicao_fin" not in df_check.columns:
            logger.warning("⚠️ BRONZE | Cabeçalhos de formulário detectados. Normalizando in-place...")
            df_orig = pd.read_csv(resolved_path)
            df_orig = df_orig.rename(columns=EXCEL_COLUMN_MAPPING)
            df_orig.to_csv(resolved_path, index=False, encoding="utf-8")
            logger.info("✅ BRONZE | Arquivo original normalizado | path=%s", resolved_path)
    except Exception as exc:
        logger.error("❌ BRONZE | Falha ao inspecionar cabeçalhos: %s", exc)

    logger.info("🥉 BRONZE | Reading CSV | path=%s", resolved_path)
    df_raw = spark.read.csv(
        path=str(resolved_path),
        sep=",",
        header=True,
        multiLine=True,
    )

    stats = (
        df_raw.agg(
            F.count(F.lit(1)).alias("rows"),
            F.countDistinct("instituicao_fin").alias("institutions"),
        )
        .first()
    )
    logger.info(
        "🥉 BRONZE | Input summary | rows=%s | institutions=%s | columns=%s",
        stats["rows"],
        stats["institutions"],
        df_raw.columns,
    )

    df_bronze = df_raw.withColumn("bronze_row_id", F.monotonically_increasing_id())
    return df_bronze


def archive_bronze_input(input_path: str | Path) -> str:
    """
    Arquiva o arquivo de entrada da camada Bronze movendo-o para data/bronze/history/.
    O nome do arquivo de destino recebe o prefixo da partição temporal (YYYYMM).

    Parâmetros:
        input_path: Caminho do arquivo CSV processado.

    Retorna:
        Caminho final do arquivo arquivado.
    """
    archive_path = handler_partitions(input_path, "bronze")
    return archive_path
