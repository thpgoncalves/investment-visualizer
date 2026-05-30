from __future__ import annotations

import logging

from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.window import Window

from pipelines.shared.partition_handler import handler_partitions


logger = logging.getLogger(__name__)


GOLD_TABLE_FILE_NAMES = {
    "home_linha": "home_linha",
    "home_botoes": "home_botoes",
    "home_barras": "home_barras",
    "pizza_expo": "pizza_expo",
    "pizza_tipo": "pizza_tipo",
    "instituicao_linha": "instituicao_linha",
    "instituicao_label": "instituicao_label",
    "instituicao_barras": "instituicao_barras",
}


def get_variation(
    df: DataFrame,
    value_column: str,
    period_column: str,
    partition_columns: list[str],
) -> DataFrame:
    window = Window.partitionBy(*partition_columns).orderBy(F.col(period_column).asc())

    return (
        df
        .withColumn("previous_value", F.lag(F.col(value_column)).over(window))
        .withColumn(
            "variacao_percentual",
            F.when(
                F.col("previous_value").isNull() | (F.col("previous_value") == 0),
                F.lit(None),
            ).otherwise(
                F.round(
                    ((F.col(value_column) - F.col("previous_value"))
                     / F.col("previous_value"))
                    * 100.0,
                    2,
                )
            ),
        )
        .drop("previous_value")
    )


def prepare_silver_dataframe(df_silver: DataFrame) -> DataFrame:
    return df_silver.select(
        F.to_timestamp(F.col("timestamp")).alias("timestamp"),
        F.to_date(F.col("data_apuracao")).alias("data_apuracao"),
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


def get_latest_rows(
    df: DataFrame,
    partition_columns: list[str],
    order_column: str = "data_apuracao",
) -> DataFrame:
    window = Window.partitionBy(*partition_columns).orderBy(F.col(order_column).desc())

    return (
        df
        .withColumn("rn", F.row_number().over(window))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )


def build_home_line_table(df_silver: DataFrame) -> DataFrame:
    df_instituicao = (
        df_silver
        .groupBy("data_apuracao", "ano", "mes", "instituicao_fin")
        .agg(F.sum(F.col("valor_total")).alias("valor_total"))
        .select(
            F.col("data_apuracao"),
            F.col("ano"),
            F.col("mes"),
            F.lit("INSTITUICAO").alias("tipo_escopo"),
            F.col("instituicao_fin"),
            F.lit("valor_total").alias("nome_metrica"),
            F.round(F.col("valor_total"), 2).alias("valor_total"),
        )
    )

    df_total = (
        df_silver
        .groupBy("data_apuracao", "ano", "mes")
        .agg(F.sum(F.col("valor_total")).alias("valor_total"))
        .select(
            F.col("data_apuracao"),
            F.col("ano"),
            F.col("mes"),
            F.lit("HOME").alias("tipo_escopo"),
            F.lit("ALL").alias("instituicao_fin"),
            F.lit("valor_total").alias("nome_metrica"),
            F.round(F.col("valor_total"), 2).alias("valor_total"),
        )
    )

    return (
        get_variation(
            df_instituicao.unionByName(df_total),
            "valor_total",
            "data_apuracao",
            partition_columns=["instituicao_fin"],
        )
        .select(
            "data_apuracao",
            "ano",
            "mes",
            "tipo_escopo",
            "instituicao_fin",
            "nome_metrica",
            "valor_total",
            "variacao_percentual",
        )
    )


def build_home_buttons_table(df_home_linha: DataFrame) -> DataFrame:
    return get_latest_rows(
        df_home_linha,
        partition_columns=["instituicao_fin"],
    ).select(
        "data_apuracao",
        "ano",
        "mes",
        "tipo_escopo",
        "instituicao_fin",
        "nome_metrica",
        "valor_total",
        "variacao_percentual",
    )


def build_home_bar_table(df_home_linha: DataFrame) -> DataFrame:
    df_home_barras = (
        df_home_linha
        .groupBy("data_apuracao", "ano", "instituicao_fin")
        .agg(F.sum("valor_total").alias("valor_total"))
        .select(
            F.col("data_apuracao"),
            F.col("ano"),
            F.col("instituicao_fin"),
            F.round(F.col("valor_total"), 2).alias("valor_total"),
            F.lit("HOME").alias("tipo_escopo"),
            F.lit("valor_total_anual").alias("nome_metrica"),
        )
    )

    return (
        get_variation(
            df_home_barras,
            "valor_total",
            "data_apuracao",
            partition_columns=["instituicao_fin"],
        )
        .select(
            "data_apuracao",
            "ano",
            "instituicao_fin",
            "valor_total",
            "tipo_escopo",
            "nome_metrica",
            "variacao_percentual",
        )
    )


def build_latest_position_table(df_silver: DataFrame) -> DataFrame:
    return get_latest_rows(
        df_silver,
        partition_columns=["instituicao_fin", "tipo", "nome"],
    )


def build_pizza_expo_table(df_latest_positions: DataFrame) -> DataFrame:
    df_exposicao_all = (
        df_latest_positions
        .groupBy("data_apuracao", "exposicao")
        .agg(F.sum(F.col("valor_total")).alias("valor_total"))
        .select(
            F.col("data_apuracao"),
            F.col("exposicao"),
            F.round(F.col("valor_total"), 2).alias("valor_total"),
            F.lit("ALL").alias("instituicao_fin"),
        )
    )

    df_exposicao_instituicao = (
        df_latest_positions
        .groupBy("data_apuracao", "instituicao_fin", "exposicao")
        .agg(F.sum(F.col("valor_total")).alias("valor_total"))
        .select(
            F.col("data_apuracao"),
            F.col("exposicao"),
            F.round(F.col("valor_total"), 2).alias("valor_total"),
            F.col("instituicao_fin"),
        )
    )

    return df_exposicao_all.unionByName(df_exposicao_instituicao).select(
        "data_apuracao",
        "exposicao",
        "valor_total",
        "instituicao_fin",
    )


def build_pizza_tipo_table(df_latest_positions: DataFrame) -> DataFrame:
    df_tipo_all = (
        df_latest_positions
        .groupBy("data_apuracao", "tipo")
        .agg(F.sum(F.col("valor_total")).alias("valor_total"))
        .select(
            F.col("data_apuracao"),
            F.col("tipo"),
            F.round(F.col("valor_total"), 2).alias("valor_total"),
            F.lit("ALL").alias("instituicao_fin"),
        )
    )

    df_tipo_instituicao = (
        df_latest_positions
        .groupBy("data_apuracao", "instituicao_fin", "tipo")
        .agg(F.sum(F.col("valor_total")).alias("valor_total"))
        .select(
            F.col("data_apuracao"),
            F.col("tipo"),
            F.round(F.col("valor_total"), 2).alias("valor_total"),
            F.col("instituicao_fin"),
        )
    )

    return df_tipo_all.unionByName(df_tipo_instituicao).select(
        "data_apuracao",
        "tipo",
        "valor_total",
        "instituicao_fin",
    )


def build_instituicao_line_table(df_silver: DataFrame) -> DataFrame:
    df_instituicao = (
        df_silver
        .groupBy("data_apuracao", "ano", "mes", "instituicao_fin", "nome")
        .agg(F.sum(F.col("valor_total")).alias("valor_total"))
        .select(
            F.col("data_apuracao"),
            F.col("ano"),
            F.col("mes"),
            F.col("instituicao_fin"),
            F.col("nome"),
            F.round(F.col("valor_total"), 2).alias("valor_total"),
            F.lit("valor_total").alias("nome_metrica"),
            F.lit("INSTITUICAO").alias("tipo_escopo"),
        )
    )

    df_total = (
        df_silver
        .groupBy("data_apuracao", "ano", "mes", "instituicao_fin")
        .agg(F.sum(F.col("valor_total")).alias("valor_total"))
        .select(
            F.col("data_apuracao"),
            F.col("ano"),
            F.col("mes"),
            F.col("instituicao_fin"),
            F.lit("ALL").alias("nome"),
            F.round(F.col("valor_total"), 2).alias("valor_total"),
            F.lit("valor_total").alias("nome_metrica"),
            F.lit("INSTITUICAO").alias("tipo_escopo"),
        )
    )

    return (
        get_variation(
            df_instituicao.unionByName(df_total),
            "valor_total",
            "data_apuracao",
            partition_columns=["instituicao_fin", "nome"],
        )
        .select(
            "data_apuracao",
            "ano",
            "mes",
            "instituicao_fin",
            "nome",
            "valor_total",
            "nome_metrica",
            "tipo_escopo",
            "variacao_percentual",
        )
    )


def build_position_detail_table(df_latest_positions: DataFrame) -> DataFrame:
    df_with_entry = df_latest_positions.withColumn(
        "preco_entrada",
        F.col("preco_medio") * F.col("qtd"),
    )

    return (
        df_with_entry
        .groupBy("data_apuracao", "instituicao_fin", "nome")
        .agg(
            F.sum(F.col("qtd")).alias("qtd"),
            F.sum(F.col("preco_entrada")).alias("preco_entrada"),
            F.sum(F.col("valor_total")).alias("valor_total_detalhe"),
        )
        .withColumn(
            "preco_medio",
            F.when(F.col("qtd") == 0, F.lit(None)).otherwise(F.col("preco_entrada") / F.col("qtd")),
        )
        .withColumn(
            "preco_atual",
            F.when(F.col("qtd") == 0, F.lit(None)).otherwise(F.col("valor_total_detalhe") / F.col("qtd")),
        )
        .select(
            F.col("data_apuracao"),
            F.col("instituicao_fin"),
            F.col("nome"),
            F.round(F.col("qtd"), 8).alias("qtd"),
            F.round(F.col("preco_medio"), 2).alias("preco_medio"),
            F.round(F.col("preco_entrada"), 2).alias("preco_entrada"),
            F.round(F.col("preco_atual"), 2).alias("preco_atual"),
        )
    )


def build_instituicao_label_table(
    df_instituicao_linha: DataFrame,
    df_latest_positions: DataFrame,
) -> DataFrame:
    df_latest_values = get_latest_rows(
        df_instituicao_linha,
        partition_columns=["instituicao_fin", "nome"],
    )

    df_position_details = build_position_detail_table(df_latest_positions)

    return (
        df_latest_values.alias("metric")
        .join(
            df_position_details.alias("position"),
            on=["data_apuracao", "instituicao_fin", "nome"],
            how="left",
        )
        .select(
            F.col("metric.data_apuracao"),
            F.col("metric.ano"),
            F.col("metric.mes"),
            F.col("metric.instituicao_fin"),
            F.col("metric.nome"),
            F.col("position.qtd"),
            F.col("position.preco_medio"),
            F.col("position.preco_entrada"),
            F.col("position.preco_atual"),
            F.col("metric.valor_total"),
            F.when(
                F.col("position.preco_entrada").isNull() | (F.col("position.preco_entrada") == 0),
                F.lit(None),
            ).otherwise(
                F.round(
                    ((F.col("metric.valor_total") - F.col("position.preco_entrada"))
                     / F.col("position.preco_entrada"))
                    * 100.0,
                    2,
                )
            ).alias("variacao_percentual"),
            F.col("metric.nome_metrica"),
            F.col("metric.tipo_escopo"),
        )
        .select(
            "data_apuracao",
            "ano",
            "mes",
            "instituicao_fin",
            "nome",
            "qtd",
            "preco_medio",
            "preco_entrada",
            "preco_atual",
            "valor_total",
            "variacao_percentual",
            "nome_metrica",
            "tipo_escopo",
        )
    )


def build_instituicao_bar_table(df_instituicao_linha: DataFrame) -> DataFrame:
    df_instituicao_barras = (
        df_instituicao_linha
        .groupBy("data_apuracao", "ano", "instituicao_fin", "nome")
        .agg(F.sum("valor_total").alias("valor_total"))
        .select(
            F.col("data_apuracao"),
            F.col("ano"),
            F.col("instituicao_fin"),
            F.col("nome"),
            F.round(F.col("valor_total"), 2).alias("valor_total"),
            F.lit("valor_total_anual").alias("nome_metrica"),
            F.lit("INSTITUICAO").alias("tipo_escopo"),
        )
    )

    return (
        get_variation(
            df_instituicao_barras,
            "valor_total",
            "data_apuracao",
            partition_columns=["instituicao_fin", "nome"],
        )
        .select(
            "data_apuracao",
            "ano",
            "instituicao_fin",
            "nome",
            "valor_total",
            "nome_metrica",
            "tipo_escopo",
            "variacao_percentual",
        )
    )


def build_gold_tables(df_silver: DataFrame) -> dict[str, DataFrame]:
    df_silver = prepare_silver_dataframe(df_silver)
    df_latest_positions = build_latest_position_table(df_silver)

    df_home_linha = build_home_line_table(df_silver)
    df_instituicao_linha = build_instituicao_line_table(df_silver)

    return {
        "home_linha": df_home_linha,
        "home_botoes": build_home_buttons_table(df_home_linha),
        "home_barras": build_home_bar_table(df_home_linha),
        "pizza_expo": build_pizza_expo_table(df_latest_positions),
        "pizza_tipo": build_pizza_tipo_table(df_latest_positions),
        "instituicao_linha": df_instituicao_linha,
        "instituicao_label": build_instituicao_label_table(
            df_instituicao_linha,
            df_latest_positions,
        ),
        "instituicao_barras": build_instituicao_bar_table(df_instituicao_linha),
    }


def write_gold_tables(gold_tables: dict[str, DataFrame]) -> list[str]:
    saved_paths = []

    for table_name, file_name in GOLD_TABLE_FILE_NAMES.items():
        logger.info("Writing gold table: %s", table_name)
        saved_paths.append(handler_partitions(gold_tables[table_name], "gold", file_name))

    return saved_paths


def run_gold_pipeline(
    spark,
    *,
    input_path: str,
) -> list[str]:
    logger.info("Starting gold pipeline")

    df_silver = spark.read.csv(
        path=input_path,
        sep=",",
        header=True,
    )

    gold_tables = build_gold_tables(df_silver)
    saved_paths = write_gold_tables(gold_tables)

    logger.info("Gold pipeline finished successfully")
    return saved_paths
