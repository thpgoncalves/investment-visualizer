from __future__ import annotations

import logging

from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.window import Window

from pipelines.shared.partition_handler import handler_partitions


logger = logging.getLogger(__name__)


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
                    ((F.col(value_column) - F.col("previous_value")) / F.col("previous_value")) * 100.0,
                    2,
                )
            ),
        )
        .drop("previous_value")
    )


def run_gold_pipeline(
    spark,
    *,
    input_path: str,
) -> list[str]:
    logger.info("Starting gold pipeline")

    saved_paths = []

    df_silver = spark.read.csv(
        path=input_path,
        sep=",",
        header=True,
    )

    df_silver = df_silver.select(
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

    logger.info("Building home line table")
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

    df_home_linha = (
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
    saved_paths.append(handler_partitions(df_home_linha, "gold", "home_linha"))

    logger.info("Building home buttons table")
    latest_home_window = Window.partitionBy("instituicao_fin").orderBy(F.col("data_apuracao").desc())
    df_home_botoes = (
        df_home_linha
        .withColumn("rn", F.row_number().over(latest_home_window))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )
    saved_paths.append(handler_partitions(df_home_botoes, "gold", "home_botoes"))

    logger.info("Building home bar table")
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
    df_home_barras = (
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
    saved_paths.append(handler_partitions(df_home_barras, "gold", "home_barras"))

    latest_position_window = Window.partitionBy("instituicao_fin", "tipo", "nome").orderBy(
        F.col("data_apuracao").desc()
    )
    df_dados_atuais = (
        df_silver
        .withColumn("rn", F.row_number().over(latest_position_window))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

    logger.info("Building exposure pie table")
    df_exposicao_all = (
        df_dados_atuais
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
        df_dados_atuais
        .groupBy("data_apuracao", "instituicao_fin", "exposicao")
        .agg(F.sum(F.col("valor_total")).alias("valor_total"))
        .select(
            F.col("data_apuracao"),
            F.col("exposicao"),
            F.round(F.col("valor_total"), 2).alias("valor_total"),
            F.col("instituicao_fin"),
        )
    )

    df_exposicao = df_exposicao_all.unionByName(df_exposicao_instituicao).select(
        "data_apuracao",
        "exposicao",
        "valor_total",
        "instituicao_fin",
    )
    saved_paths.append(handler_partitions(df_exposicao, "gold", "pizza_expo"))

    logger.info("Building type pie table")
    df_tipo_all = (
        df_dados_atuais
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
        df_dados_atuais
        .groupBy("data_apuracao", "instituicao_fin", "tipo")
        .agg(F.sum(F.col("valor_total")).alias("valor_total"))
        .select(
            F.col("data_apuracao"),
            F.col("tipo"),
            F.round(F.col("valor_total"), 2).alias("valor_total"),
            F.col("instituicao_fin"),
        )
    )

    df_tipo = df_tipo_all.unionByName(df_tipo_instituicao).select(
        "data_apuracao",
        "tipo",
        "valor_total",
        "instituicao_fin",
    )
    saved_paths.append(handler_partitions(df_tipo, "gold", "pizza_tipo"))

    logger.info("Building institution line table")
    df_instituicao_page = (
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

    df_total_page = (
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

    df_instituicao_linha = (
        get_variation(
            df_instituicao_page.unionByName(df_total_page),
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
    saved_paths.append(handler_partitions(df_instituicao_linha, "gold", "instituicao_linha"))

    logger.info("Building institution label table")
    latest_institution_window = Window.partitionBy("instituicao_fin", "nome").orderBy(F.col("data_apuracao").desc())
    df_val_atual_page = (
        df_instituicao_linha
        .withColumn("rn", F.row_number().over(latest_institution_window))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

    df_position_details = (
        df_dados_atuais
        .withColumn("preco_entrada", F.col("preco_medio") * F.col("qtd"))
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

    df_instituicao_label = (
        df_val_atual_page.alias("metric")
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
    saved_paths.append(handler_partitions(df_instituicao_label, "gold", "instituicao_label"))

    logger.info("Building institution bar table")
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
    df_instituicao_barras = (
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
    saved_paths.append(handler_partitions(df_instituicao_barras, "gold", "instituicao_barras"))

    logger.info("Gold pipeline finished successfully")
    return saved_paths
