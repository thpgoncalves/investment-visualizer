from __future__ import annotations

import logging

from pyspark.sql import functions as F

from pipelines.shared.partition_handler import handler_partitions
from infra.spark_utils import normalize_ptbr_number
from pipelines.silver.tickers import get_tickers_price, handler_tickers_cache


logger = logging.getLogger(__name__)

INTERNATIONAL_TICKERS = [
    "BERK34",
    "IVVB11",
    "AAPL34",
    "MSFT34",
    "NVDC34",
    "GOGL34",
    "AMZO34",
    "TSLA34",
    "META34",
    "MELI34",
]


def run_silver_pipeline(
    spark,
    *,
    input_path: str,
) -> None:
    logger.info("Starting silver pipeline")

    logger.info("Reading raw CSV file")
    df = spark.read.csv(
        path=input_path,
        sep=",",
        header=True,
        multiLine=True,
    )

    logger.info("Normalizing and exploding summary lines")
    df = (
        df.withColumn("resumo", F.regexp_replace(F.col("resumo"), r"\r\n|\r", "\n"))
        .withColumn("resumo", F.split(F.col("resumo"), "\n"))
        .withColumn("resumo", F.explode(F.col("resumo")))
    )

    logger.info("Splitting summary fields into columns")
    parts = F.split(F.col("resumo"), r"\|")

    df = (
        df.withColumn("tipo", F.trim(parts.getItem(0)))
        .withColumn("nome", F.trim(parts.getItem(1)))
        .withColumn("qtd", F.trim(parts.getItem(2)))
        .withColumn("preco_medio", F.trim(parts.getItem(3)))
        .withColumn("preco_atual", F.trim(parts.getItem(4)))
    )

    logger.info("Normalizing pt-BR numeric/text fields")
    df = (
        df.withColumn("tipo", normalize_ptbr_number(F.col("tipo")))
        .withColumn("nome", normalize_ptbr_number(F.col("nome")))
        .withColumn("qtd", normalize_ptbr_number(F.col("qtd")))
        .withColumn("preco_medio", normalize_ptbr_number(F.col("preco_medio")))
        .withColumn("preco_atual", normalize_ptbr_number(F.col("preco_atual")))
    )

    logger.info("Creating date reference columns")
    month_mapping = F.create_map(
        F.lit(1), F.lit("Jan"),
        F.lit(2), F.lit("Fev"),
        F.lit(3), F.lit("Mar"),
        F.lit(4), F.lit("Abr"),
        F.lit(5), F.lit("Mai"),
        F.lit(6), F.lit("Jun"),
        F.lit(7), F.lit("Jul"),
        F.lit(8), F.lit("Ago"),
        F.lit(9), F.lit("Set"),
        F.lit(10), F.lit("Out"),
        F.lit(11), F.lit("Nov"),
        F.lit(12), F.lit("Dec"),
    )

    df = (
        df.withColumn("data_apuracao", F.to_date(F.col("data_apuracao"), "dd/MM/yyyy"))
        .withColumn("ano", F.year(F.col("data_apuracao")))
        .withColumn("mes_num", F.month(F.col("data_apuracao")))
        .withColumn("mes", month_mapping[F.col("mes_num")])
    )

    logger.info("Applying business normalization rules")
    df = (
        df.withColumn(
            "exposicao",
            F.when(F.col("nome").isin(INTERNATIONAL_TICKERS), "internacional").otherwise("nacional"),
        )
        .withColumn("tipo", F.lower(F.col("tipo")))
        .withColumn("instituicao_fin", F.upper(F.col("instituicao_fin")))
        .withColumn(
            "nome",
            F.when(F.col("tipo") == "stock", F.upper(F.col("nome"))).otherwise(F.lower(F.col("nome"))),
        )
    )

    logger.info("Casting final silver schema")
    df = df.select(
        F.to_timestamp(F.col("timestamp"), "dd/MM/yyyy HH:mm:ss").alias("timestamp"),
        F.col("data_apuracao").cast("date").alias("data_apuracao"),
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
        F.col("aporte").cast("double").alias("aporte"),
        F.col("exposicao").cast("string").alias("exposicao"),
    )

    logger.info("Enriching data with ticker prices")
    df_price = get_tickers_price(df)
    df_cache = handler_tickers_cache(df_price)

    df = (
        df.alias("base")
        .join(
            df_cache.alias("cache"),
            on=(
                (F.col("base.data_apuracao") == F.col("cache.data_apuracao"))
                & (F.col("base.nome") == F.col("cache.ticker"))
            ),
            how="left",
        )
        .withColumn(
            "preco_atual",
            F.when(F.col("base.preco_atual").isNull(), F.col("cache.close")).otherwise(F.col("base.preco_atual")),
        )
        .withColumn("valor_total", F.round(F.col("preco_atual") * F.col("base.qtd"), 2))
        .select(
            F.col("base.timestamp").alias("timestamp"),
            F.col("base.data_apuracao").alias("data_apuracao"),
            F.col("base.ano").alias("ano"),
            F.col("base.mes_num").alias("mes_num"),
            F.col("base.mes").alias("mes"),
            F.col("base.instituicao_fin").alias("instituicao_fin"),
            F.col("base.resumo").alias("resumo"),
            F.col("base.tipo").alias("tipo"),
            F.col("base.nome").alias("nome"),
            F.col("base.qtd").alias("qtd"),
            F.col("base.preco_medio").alias("preco_medio"),
            F.col("preco_atual").cast("double").alias("preco_atual"),
            F.col("valor_total").cast("double").alias("valor_total"),
            F.col("base.aporte").alias("aporte"),
            F.col("base.exposicao").alias("exposicao"),
        )
    )

    logger.info("Writing silver dataset")
    logger.info(handler_partitions(df, "silver"))

    logger.info("Silver pipeline finished successfully")
