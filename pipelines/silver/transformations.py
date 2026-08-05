from __future__ import annotations


import logging

from pyspark.sql import functions as F
from pyspark.sql.window import Window
import pandas as pd 

from infra.spark_utils import normalize_ptbr_number
from pipelines.shared.logging_utils import log_section_separator
from pipelines.shared.partition_handler import handler_partitions
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


def _normalize_and_validate_summary_lines(df):
    df_exploded = (
        df.withColumn("resumo", F.regexp_replace(F.col("resumo"), r"\r\n|\r", "\n"))
        .withColumn("_resumo_lines", F.split(F.col("resumo"), "\n"))
        .select(
            "*",
            F.posexplode(F.col("_resumo_lines")).alias("_resumo_line_index", "_resumo_line"),
        )
        .drop("resumo", "_resumo_lines")
        .withColumnRenamed("_resumo_line", "resumo")
        .withColumn("_resumo_line_number", F.col("_resumo_line_index") + F.lit(1))
        .drop("_resumo_line_index")
    )

    exploded_count = df_exploded.count()
    empty_line_condition = F.length(F.trim(F.col("resumo"))) == 0
    empty_line_count = df_exploded.filter(empty_line_condition).count()

    logger.info(
        "🥉 BRONZE | Summary lines | exploded=%s | empty_discarded=%s",
        exploded_count,
        empty_line_count,
    )

    df_non_empty = df_exploded.filter(~empty_line_condition)
    parts = F.split(F.col("resumo"), r"\|", -1)

    df_validated = (
        df_non_empty
        .withColumn("_summary_field_count", F.size(parts))
        .withColumn(
            "_missing_required_fields",
            F.array_compact(
                F.array(
                    F.when(F.length(F.trim(parts.getItem(0))) == 0, F.lit("tipo")),
                    F.when(F.length(F.trim(parts.getItem(1))) == 0, F.lit("nome")),
                    F.when(F.length(F.trim(parts.getItem(2))) == 0, F.lit("qtd")),
                    F.when(F.length(F.trim(parts.getItem(3))) == 0, F.lit("preco_medio")),
                )
            ),
        )
    )

    malformed_condition = (
        (F.col("_summary_field_count") != 5)
        | (F.size(F.col("_missing_required_fields")) > 0)
    )
    malformed_df = df_validated.filter(malformed_condition)
    malformed_count = malformed_df.count()

    if malformed_count:
        malformed_samples = [
            row.asDict(recursive=True)
            for row in (
                malformed_df
                .select(
                    "bronze_row_id",
                    "timestamp",
                    "data_apuracao",
                    "instituicao_fin",
                    "_resumo_line_number",
                    "resumo",
                    "_summary_field_count",
                    "_missing_required_fields",
                )
                .limit(10)
                .collect()
            )
        ]
        logger.error(
            "🥉 BRONZE | Malformed summary lines | count=%s | samples=%s",
            malformed_count,
            malformed_samples,
        )
        raise ValueError(
            "Malformed summary lines detected. "
            f"Expected exactly 5 fields and non-empty tipo, nome, qtd and preco_medio; "
            f"found {malformed_count} invalid line(s)."
        )

    valid_count = df_validated.count()
    logger.info("🥉 BRONZE | Summary validation passed | valid_lines=%s", valid_count)

    return df_validated.drop(
        "_resumo_line_number",
        "_summary_field_count",
        "_missing_required_fields",
    )


def _enrich_with_ticker_prices(df, df_cache):
    df_enriched = (
        df.alias("a")
        .join(
            df_cache.alias("b"),
            on=(
                (F.col("a.nome") == F.col("b.ticker"))
                & (F.col("a.data_apuracao") == F.col("b.data_apuracao"))
            ),
            how="left",
        )
        .select(
            "a.*",
            F.col("b.close").alias("_cache_close"),
        )
        .withColumn("preco_atual", F.coalesce("preco_atual", "_cache_close"))
        .drop("_cache_close")
    )

    missing_price_df = df_enriched.filter(
        (F.col("tipo") == "stock") & F.col("preco_atual").isNull()
    )

    if missing_price_df.limit(1).count():
        latest_cache_date = df_cache.agg(F.max("data_apuracao").alias("date")).first()["date"]
        fallback_samples = [
            row.asDict(recursive=True)
            for row in (
                missing_price_df
                .select("instituicao_fin", "nome", "data_apuracao")
                .limit(20)
                .collect()
            )
        ]
        logger.warning(
            "⚠️ 📈 PRICE | Same-day price not found | fallback_date=%s | positions=%s",
            latest_cache_date,
            fallback_samples,
        )

        fallback_window = Window.partitionBy("ticker").orderBy(F.col("data_apuracao").desc())

        fallback_cache = (
            df_cache
            .filter(F.col("close").isNotNull() & ~F.isnan("close") & (F.col("close") > 0))
            .withColumn("_rn", F.row_number().over(fallback_window))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
            .select(
                "ticker",
                F.col("close").alias("_fallback_close"),
            )
        )

        df_enriched = (
            df_enriched.alias("a")
            .join(
                fallback_cache.alias("b"),
                on=F.col("a.nome") == F.col("b.ticker"),
                how="left",
            )
            .select(
                "a.*",
                F.col("b._fallback_close"),
            )
            .withColumn("preco_atual", F.coalesce("preco_atual", "_fallback_close"))
            .drop("_fallback_close")
        )

    return (
        df_enriched
        .withColumn("valor_total", F.round(F.col("preco_atual") * F.col("qtd"), 2))
        .select(
            F.col("timestamp"),
            F.col("data_apuracao"),
            F.col("bronze_row_id"),
            F.col("ano"),
            F.col("mes_num"),
            F.col("mes"),
            F.col("instituicao_fin"),
            F.col("resumo"),
            F.col("tipo"),
            F.col("nome"),
            F.col("qtd"),
            F.col("preco_medio"),
            F.col("preco_atual").cast("double").alias("preco_atual"),
            F.col("valor_total").cast("double").alias("valor_total"),
            F.col("aporte"),
            F.col("exposicao"),
        )
    )


def _validate_silver_output(df) -> None:
    stats = (
        df.agg(
            F.count(F.lit(1)).alias("rows"),
            F.countDistinct("instituicao_fin").alias("institutions"),
            F.min("data_apuracao").alias("min_date"),
            F.max("data_apuracao").alias("max_date"),
            F.sum(F.when(F.col("preco_atual").isNull(), 1).otherwise(0)).alias("missing_price"),
            F.sum(F.when(F.col("valor_total").isNull(), 1).otherwise(0)).alias("missing_total"),
        )
        .first()
    )

    logger.info(
        "🥈 SILVER | Dataset summary | rows=%s | institutions=%s | period=%s to %s | "
        "missing_price=%s | missing_total=%s",
        stats["rows"],
        stats["institutions"],
        stats["min_date"],
        stats["max_date"],
        stats["missing_price"],
        stats["missing_total"],
    )

    if stats["rows"] == 0:
        logger.error("🥈 SILVER | Validation failed | reason=empty_dataset")
        raise ValueError("Silver validation failed: the final dataset is empty.")

    if stats["missing_price"] or stats["missing_total"]:
        invalid_samples = [
            row.asDict(recursive=True)
            for row in (
                df
                .filter(F.col("preco_atual").isNull() | F.col("valor_total").isNull())
                .select(
                    "bronze_row_id",
                    "data_apuracao",
                    "instituicao_fin",
                    "tipo",
                    "nome",
                    "qtd",
                    "preco_atual",
                    "valor_total",
                )
                .limit(20)
                .collect()
            )
        ]
        logger.error(
            "🥈 SILVER | Validation failed | missing_price=%s | missing_total=%s | samples=%s",
            stats["missing_price"],
            stats["missing_total"],
            invalid_samples,
        )
        raise ValueError(
            "Silver validation failed: positions with missing preco_atual or valor_total were found. "
            f"Invalid positions: {invalid_samples}"
        )

    logger.info("🥈 SILVER | Validation passed")


def run_silver_pipeline(
    spark,
    *,
    input_path: str,
) -> str:
    logger.info("🥈 SILVER | Pipeline started")

    try:
        df_check = pd.read_csv(input_path, nrows=0)
        if "instituicao_fin" not in df_check.columns:
            logger.warning("⚠️ Colunas do Excel detectadas no arquivo original. Corrigindo cabeçalhos in-place...")
            
            column_mapping = {
                "Carimbo de data/hora": "timestamp",
                "Data Apuração:": "data_apuracao",
                "Instituição Financeira:": "instituicao_fin",
                "Resumo Investimentos:": "resumo",
                "Aporte:": "aporte"
            }
            
            # Carrega o CSV inteiro pelo Pandas, renomeia e salva por cima dele mesmo
            df_orig = pd.read_csv(input_path)
            df_orig = df_orig.rename(columns=column_mapping)
            df_orig.to_csv(input_path, index=False, encoding="utf-8")
            logger.info("✅ Arquivo original '%s' atualizado com sucesso!", input_path)
            
    except Exception as e:
        logger.error("❌ Falha crítica ao tentar corrigir o arquivo original: %s", e)

    log_section_separator(logger)
    logger.info("🥉 BRONZE | Reading CSV | path=%s", input_path)
    df = spark.read.csv(
        path=input_path,
        sep=",",
        header=True,
        multiLine=True,
    )

    bronze_stats = (
        df.agg(
            F.count(F.lit(1)).alias("rows"),
            F.countDistinct("instituicao_fin").alias("institutions"),
        )
        .first()
    )
    logger.info(
        "🥉 BRONZE | Input summary | rows=%s | institutions=%s | columns=%s",
        bronze_stats["rows"],
        bronze_stats["institutions"],
        df.columns,
    )
    df = df.withColumn("bronze_row_id", F.monotonically_increasing_id())

    logger.info("🥉 BRONZE | Normalizing and validating summary lines")
    df = _normalize_and_validate_summary_lines(df)

    log_section_separator(logger)
    logger.info("🥈 SILVER | Transformations started")
    logger.info("🥈 SILVER | Splitting summary fields")
    parts = F.split(F.col("resumo"), r"\|")

    df = (
        df.withColumn("tipo", F.trim(parts.getItem(0)))
        .withColumn("nome", F.trim(parts.getItem(1)))
        .withColumn("qtd", F.trim(parts.getItem(2)))
        .withColumn("preco_medio", F.trim(parts.getItem(3)))
        .withColumn("preco_atual", F.trim(parts.getItem(4)))
    )

    logger.info("🥈 SILVER | Normalizing pt-BR values")
    df = (
        df.withColumn("tipo", normalize_ptbr_number(F.col("tipo")))
        .withColumn("nome", normalize_ptbr_number(F.col("nome")))
        .withColumn("qtd", normalize_ptbr_number(F.col("qtd")))
        .withColumn("preco_medio", normalize_ptbr_number(F.col("preco_medio")))
        .withColumn("preco_atual", normalize_ptbr_number(F.col("preco_atual")))
    )

    logger.info("🥈 SILVER | Creating date reference columns")
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

    logger.info("🥈 SILVER | Applying business rules")
    df = (
        df.withColumn("tipo", F.lower(F.col("tipo")))
        .withColumn(
            "tipo",
            F.when(
                F.col("tipo").isin("fundo imobiliario", "fundo imobiliário"),
                "stock",
            ).otherwise(F.col("tipo")),
        )
        .withColumn("instituicao_fin", F.upper(F.col("instituicao_fin")))
        .withColumn(
            "nome",
            F.when(F.col("tipo") == "stock", F.upper(F.col("nome"))).otherwise(F.lower(F.col("nome"))),
        )
        .withColumn(
            "exposicao",
            F.when(F.col("tipo") == "cripto", "internacional")
            .when(F.col("nome").isin(INTERNATIONAL_TICKERS), "internacional")
            .otherwise("nacional"),
        )
    )

    logger.info("🥈 SILVER | Casting final schema")
    df = df.select(
        F.to_timestamp(F.col("timestamp"), "dd/MM/yyyy HH:mm:ss").alias("timestamp"),
        F.col("data_apuracao").cast("date").alias("data_apuracao"),
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
        F.col("aporte").cast("double").alias("aporte"),
        F.col("exposicao").cast("string").alias("exposicao"),
    )

    log_section_separator(logger)
    logger.info("📈 YAHOO | Fetching ticker prices")
    df_price = get_tickers_price(df)

    log_section_separator(logger)
    logger.info("💾 CACHE | Updating ticker prices")
    df_cache = handler_tickers_cache(df_price)

    log_section_separator(logger)
    logger.info("🥈 SILVER | Merging ticker prices")
    df = _enrich_with_ticker_prices(df, df_cache)

    log_section_separator(logger)
    logger.info("🥈 SILVER | Validating final dataset")
    _validate_silver_output(df)

    log_section_separator(logger)
    logger.info("🥈 SILVER | Writing snapshot")
    silver_snapshot_path = handler_partitions(df, "silver")
    logger.info("🥈 SILVER | Snapshot ready | path=%s", silver_snapshot_path)

    log_section_separator(logger)
    logger.info("🥈 SILVER | Pipeline finished")
    return silver_snapshot_path
