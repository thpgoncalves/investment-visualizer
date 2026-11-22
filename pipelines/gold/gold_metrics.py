from __future__ import annotations

import logging

from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.window import Window

from pipelines.shared.logging_utils import log_section_separator
from pipelines.shared.partition_handler import handler_partitions


logger = logging.getLogger(__name__)


def get_variation(
    df: DataFrame,
    value_column: str,
    period_column: str,
    partition_columns: list[str],
) -> DataFrame:
    """
    Calcula a variação percentual simples de uma métrica entre períodos consecutivos.

    Fórmula:
        ((valor_atual - valor_anterior) / valor_anterior) * 100

    Caso o valor anterior seja nulo ou zero, o resultado é definido como None.

    Parâmetros:
        df: DataFrame Spark de entrada.
        value_column: Nome da coluna numérica a comparar.
        period_column: Coluna temporal usada para ordenar a janela (ex: 'data_apuracao').
        partition_columns: Colunas para particionar a janela de cálculo (ex: ['instituicao_fin', 'nome']).

    Retorna:
        DataFrame Spark com a coluna 'variacao_percentual' adicionada.
    """
    logger.info(
        "🥇 GOLD | Calculating variation | value=%s | period=%s | partition=%s",
        value_column,
        period_column,
        partition_columns,
    )
    window = Window.partitionBy(*partition_columns).orderBy(F.col(period_column).asc())

    df_result = (
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

    return df_result


def get_variation_with_flow(
    df: DataFrame,
    value_column: str,
    flow_column: str,
    period_column: str,
    partition_columns: list[str],
) -> DataFrame:
    """
    Calcula a rentabilidade percentual de uma métrica descontando o efeito de aportes,
    seguindo a Opção A (Aporte no Final do Mês).

    Fórmula da Opção A:
        ((valor_atual - valor_anterior - aporte) / valor_anterior) * 100

    Premissa:
        Considera que novos aportes ocorreram ao final do período e não tiveram
        tempo hábil para render, impedindo que o aporte seja confundido com valorização patrimonial.
        Caso o valor anterior seja nulo ou zero, retorna None.
        Aportes nulos são tratados como zero via coalesce.

    Parâmetros:
        df: DataFrame Spark de entrada.
        value_column: Nome da coluna de valor patrimonial (ex: 'valor_total').
        flow_column: Nome da coluna contendo o aporte no período (ex: 'aporte').
        period_column: Coluna temporal usada para ordenação (ex: 'data_apuracao').
        partition_columns: Colunas para particionar a janela de cálculo (ex: ['instituicao_fin']).

    Retorna:
        DataFrame Spark com a coluna 'variacao_percentual' ajustada pelo fluxo financeiro.
    """
    logger.info(
        "🥇 GOLD | Calculating flow-adjusted variation (Option A) | value=%s | flow=%s | period=%s | partition=%s",
        value_column,
        flow_column,
        period_column,
        partition_columns,
    )
    window = Window.partitionBy(*partition_columns).orderBy(F.col(period_column).asc())

    df_result = (
        df
        .withColumn("previous_value", F.lag(F.col(value_column)).over(window))
        .withColumn(
            "variacao_percentual",
            F.when(
                F.col("previous_value").isNull() | (F.col("previous_value") == 0),
                F.lit(None),
            ).otherwise(
                F.round(
                    (
                        (F.col(value_column) - F.col("previous_value") - F.coalesce(F.col(flow_column), F.lit(0.0)))
                        / F.col("previous_value")
                    ) * 100.0,
                    2,
                )
            ),
        )
        .drop("previous_value")
    )

    return df_result


def run_gold_pipeline(
    spark,
    *,
    input_path: str,
) -> list[str]:
    """
    Executa a pipeline da camada Gold, transformando o snapshot da camada Silver
    em 10 datasets analíticos agregados para exibição no dashboard Streamlit.

    Datasets gerados:
        1. home_linha: Evolução mensal do patrimônio (ALL e por instituição) com rentabilidade Opção A.
        2. home_botoes: Posição mais recente de cada instituição e do total para os botões da Home.
        3. home_barras: Evolução anual do patrimônio com rentabilidade ajustada por fluxo anual.
        4. aportes_linha: Evolução mensal dos valores aportados por instituição e consolidado.
        5. aportes_barras: Total aportado anualmente por instituição e consolidado.
        6. pizza_expo: Distribuição percentual por exposição geográfica (nacional vs internacional) da última foto.
        7. pizza_tipo: Distribuição percentual por tipo de ativo (stock, cripto, renda fixa) da última foto.
        8. instituicao_linha: Evolução mensal por ativo (variação da cota) e consolidado da instituição (Opção A).
        9. instituicao_label: Tabela detalhada da carteira atual (retorno acumulado vs preço médio).
        10. instituicao_barras: Evolução anual por ativo e consolidado da instituição.

    Parâmetros:
        spark: Sessão ativa do Apache Spark.
        input_path: Caminho do arquivo CSV de snapshot da camada Silver.

    Retorna:
        Lista de strings com os caminhos dos arquivos CSV gravados em data/gold/<partition_ref>/.
    """
    logger.info("🥇 GOLD | Pipeline started | input=%s", input_path)

    saved_paths = []

    df_silver = spark.read.csv(
        path=input_path,
        sep=",",
        header=True,
    )

    df_silver = df_silver.select(
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

    input_stats = (
        df_silver
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

    # -------------------------------------------------------------------------
    # Base de aportes desduplicada para cálculo de rentabilidade real
    # -------------------------------------------------------------------------
    logger.info("🥇 GOLD | Preparing contribution lookup tables")
    df_aportes_base = (
        df_silver
        .filter(F.col("aporte").isNotNull())
        .select(
            "bronze_row_id",
            "timestamp",
            "data_apuracao",
            "ano",
            "mes_num",
            "mes",
            "instituicao_fin",
            "aporte",
        )
        .dropDuplicates(["bronze_row_id"])
    )

    df_aportes_inst_sum = (
        df_aportes_base
        .groupBy("data_apuracao", "instituicao_fin")
        .agg(F.sum("aporte").alias("aporte"))
    )

    df_aportes_total_sum = (
        df_aportes_base
        .groupBy("data_apuracao")
        .agg(F.sum("aporte").alias("aporte"))
        .withColumn("instituicao_fin", F.lit("ALL"))
    )

    df_aportes_lookup = df_aportes_inst_sum.unionByName(df_aportes_total_sum)

    df_aportes_ano_inst_sum = (
        df_aportes_base
        .groupBy("ano", "instituicao_fin")
        .agg(F.sum("aporte").alias("aporte_anual"))
    )

    df_aportes_ano_total_sum = (
        df_aportes_base
        .groupBy("ano")
        .agg(F.sum("aporte").alias("aporte_anual"))
        .withColumn("instituicao_fin", F.lit("ALL"))
    )

    df_aportes_ano_lookup = df_aportes_ano_inst_sum.unionByName(df_aportes_ano_total_sum)

    # -------------------------------------------------------------------------
    # 1/10: Home Linha (Evolução Mensal Consolidada e por Instituição)
    # -------------------------------------------------------------------------
    log_section_separator(logger)
    logger.info("🥇 GOLD [1/10] | Building home line")
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

    df_home_linha_base = (
        df_instituicao.unionByName(df_total)
        .join(df_aportes_lookup, on=["data_apuracao", "instituicao_fin"], how="left")
        .fillna({"aporte": 0.0})
    )

    df_home_linha = (
        get_variation_with_flow(
            df_home_linha_base,
            value_column="valor_total",
            flow_column="aporte",
            period_column="data_apuracao",
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

    # -------------------------------------------------------------------------
    # 2/10: Home Botões (Totais mais recentes para botões e cards)
    # -------------------------------------------------------------------------
    log_section_separator(logger)
    logger.info("🥇 GOLD [2/10] | Building home buttons")
    latest_home_window = Window.partitionBy("instituicao_fin").orderBy(F.col("data_apuracao").desc())
    df_home_botoes = (
        df_home_linha
        .withColumn("rn", F.row_number().over(latest_home_window))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )
    saved_paths.append(handler_partitions(df_home_botoes, "gold", "home_botoes"))

    # -------------------------------------------------------------------------
    # 3/10: Home Barras (Evolução Anual com Ajuste de Fluxo)
    # -------------------------------------------------------------------------
    log_section_separator(logger)
    logger.info("🥇 GOLD [3/10] | Building home bars")
    annual_home_window = Window.partitionBy("ano", "instituicao_fin").orderBy(F.col("data_apuracao").desc())
    df_home_barras_raw = (
        df_home_linha
        .withColumn("rn", F.row_number().over(annual_home_window))
        .filter(F.col("rn") == 1)
        .drop("rn")
        .select(
            F.col("data_apuracao"),
            F.col("ano"),
            F.col("instituicao_fin"),
            F.col("valor_total"),
            F.lit("HOME").alias("tipo_escopo"),
            F.lit("valor_total_anual").alias("nome_metrica"),
        )
        .join(df_aportes_ano_lookup, on=["ano", "instituicao_fin"], how="left")
        .fillna({"aporte_anual": 0.0})
    )
    df_home_barras = (
        get_variation_with_flow(
            df_home_barras_raw,
            value_column="valor_total",
            flow_column="aporte_anual",
            period_column="data_apuracao",
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

    # -------------------------------------------------------------------------
    # 4/10: Aportes Linha (Evolução Mensal dos Aportes)
    # -------------------------------------------------------------------------
    log_section_separator(logger)
    logger.info("🥇 GOLD [4/10] | Building contributions line")
    df_aportes_instituicao = (
        df_aportes_base
        .groupBy("data_apuracao", "ano", "mes_num", "mes", "instituicao_fin")
        .agg(F.sum(F.col("aporte")).alias("valor_total"))
        .select(
            F.col("data_apuracao"),
            F.col("ano"),
            F.col("mes_num"),
            F.col("mes"),
            F.lit("INSTITUICAO").alias("tipo_escopo"),
            F.col("instituicao_fin"),
            F.lit("valor_aporte").alias("nome_metrica"),
            F.round(F.col("valor_total"), 2).alias("valor_total"),
        )
    )

    df_aportes_total = (
        df_aportes_base
        .groupBy("data_apuracao", "ano", "mes_num", "mes")
        .agg(F.sum(F.col("aporte")).alias("valor_total"))
        .select(
            F.col("data_apuracao"),
            F.col("ano"),
            F.col("mes_num"),
            F.col("mes"),
            F.lit("APORTES").alias("tipo_escopo"),
            F.lit("ALL").alias("instituicao_fin"),
            F.lit("valor_aporte").alias("nome_metrica"),
            F.round(F.col("valor_total"), 2).alias("valor_total"),
        )
    )

    df_aportes_linha = (
        df_aportes_instituicao.unionByName(df_aportes_total)
        .select(
            "data_apuracao",
            "ano",
            "mes_num",
            "mes",
            "tipo_escopo",
            "instituicao_fin",
            "nome_metrica",
            "valor_total",
        )
    )
    saved_paths.append(handler_partitions(df_aportes_linha, "gold", "aportes_linha"))

    # -------------------------------------------------------------------------
    # 5/10: Aportes Barras (Evolução Anual dos Aportes)
    # -------------------------------------------------------------------------
    log_section_separator(logger)
    logger.info("🥇 GOLD [5/10] | Building contributions bars")
    df_aportes_instituicao_barras = (
        df_aportes_base
        .groupBy("ano", "instituicao_fin")
        .agg(
            F.max(F.col("data_apuracao")).alias("data_apuracao"),
            F.sum(F.col("aporte")).alias("valor_total"),
        )
        .select(
            F.col("data_apuracao"),
            F.col("ano"),
            F.col("instituicao_fin"),
            F.round(F.col("valor_total"), 2).alias("valor_total"),
            F.lit("APORTES").alias("tipo_escopo"),
            F.lit("valor_aporte_anual").alias("nome_metrica"),
        )
    )

    df_aportes_total_barras = (
        df_aportes_base
        .groupBy("ano")
        .agg(
            F.max(F.col("data_apuracao")).alias("data_apuracao"),
            F.sum(F.col("aporte")).alias("valor_total"),
        )
        .select(
            F.col("data_apuracao"),
            F.col("ano"),
            F.lit("ALL").alias("instituicao_fin"),
            F.round(F.col("valor_total"), 2).alias("valor_total"),
            F.lit("APORTES").alias("tipo_escopo"),
            F.lit("valor_aporte_anual").alias("nome_metrica"),
        )
    )

    df_aportes_barras = (
        df_aportes_instituicao_barras.unionByName(df_aportes_total_barras)
        .select(
            "data_apuracao",
            "ano",
            "instituicao_fin",
            "valor_total",
            "tipo_escopo",
            "nome_metrica",
        )
    )
    saved_paths.append(handler_partitions(df_aportes_barras, "gold", "aportes_barras"))

    # -------------------------------------------------------------------------
    # 6/10: Pizza Exposição (Distribuição Geográfica da Última Foto)
    # -------------------------------------------------------------------------
    log_section_separator(logger)
    logger.info("🥇 GOLD [6/10] | Building exposure pie")
    logger.info("🥇 GOLD | Preparing latest positions (full photo by institution)")
    # Foto full: seleciona apenas as posições da data mais recente de cada instituição
    latest_inst_window = Window.partitionBy("instituicao_fin").orderBy(
        F.col("data_apuracao").desc()
    )
    df_dados_atuais = (
        df_silver
        .withColumn("_rank", F.dense_rank().over(latest_inst_window))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
    )

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

    # -------------------------------------------------------------------------
    # 7/10: Pizza Tipo (Distribuição por Tipo de Ativo da Última Foto)
    # -------------------------------------------------------------------------
    log_section_separator(logger)
    logger.info("🥇 GOLD [7/10] | Building type pie")
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

    # -------------------------------------------------------------------------
    # 8/10: Instituição Linha (Evolução Mensal por Ativo e Consolidado da Instituição)
    # -------------------------------------------------------------------------
    log_section_separator(logger)
    logger.info("🥇 GOLD [8/10] | Building institution line")

    # Ativos individuais (nome != 'ALL'): variação mensal calculada sobre a cota/preço unitário
    df_instituicao_ativos_raw = (
        df_silver
        .groupBy("data_apuracao", "ano", "mes", "instituicao_fin", "nome")
        .agg(
            F.sum(F.col("valor_total")).alias("valor_total"),
            F.first(F.col("preco_atual")).alias("preco_atual"),
        )
        .select(
            F.col("data_apuracao"),
            F.col("ano"),
            F.col("mes"),
            F.col("instituicao_fin"),
            F.col("nome"),
            F.round(F.col("valor_total"), 2).alias("valor_total"),
            F.col("preco_atual"),
            F.lit("valor_total").alias("nome_metrica"),
            F.lit("INSTITUICAO").alias("tipo_escopo"),
        )
    )

    df_instituicao_ativos = (
        get_variation(
            df_instituicao_ativos_raw,
            value_column="preco_atual",
            period_column="data_apuracao",
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

    # Consolidado da instituição (nome == 'ALL'): rentabilidade Opção A com o aporte da instituição
    df_instituicao_total_raw = (
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
        .join(df_aportes_inst_sum, on=["data_apuracao", "instituicao_fin"], how="left")
        .fillna({"aporte": 0.0})
    )

    df_instituicao_total = (
        get_variation_with_flow(
            df_instituicao_total_raw,
            value_column="valor_total",
            flow_column="aporte",
            period_column="data_apuracao",
            partition_columns=["instituicao_fin"],
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

    df_instituicao_linha = df_instituicao_ativos.unionByName(df_instituicao_total)
    saved_paths.append(handler_partitions(df_instituicao_linha, "gold", "instituicao_linha"))

    # -------------------------------------------------------------------------
    # 9/10: Instituição Labels (Tabela Detalhada com Posição da Última Foto)
    # -------------------------------------------------------------------------
    log_section_separator(logger)
    logger.info("🥇 GOLD [9/10] | Building institution labels")
    # Filtra as métricas atuais apenas para a última data apurada de cada instituição
    latest_inst_linha_window = Window.partitionBy("instituicao_fin").orderBy(F.col("data_apuracao").desc())
    df_val_atual_page = (
        df_instituicao_linha
        .withColumn("_rank", F.dense_rank().over(latest_inst_linha_window))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
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
            F.round(F.col("preco_medio"), 8).alias("preco_medio"),
            F.round(F.col("preco_entrada"), 2).alias("preco_entrada"),
            F.round(F.col("preco_atual"), 8).alias("preco_atual"),
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

    # -------------------------------------------------------------------------
    # 10/10: Instituição Barras (Evolução Anual por Ativo e Instituição)
    # -------------------------------------------------------------------------
    log_section_separator(logger)
    logger.info("🥇 GOLD [10/10] | Building institution bars")
    annual_institution_window = Window.partitionBy("ano", "instituicao_fin", "nome").orderBy(
        F.col("data_apuracao").desc()
    )
    df_instituicao_barras_raw = (
        df_instituicao_linha
        .withColumn("rn", F.row_number().over(annual_institution_window))
        .filter(F.col("rn") == 1)
        .drop("rn")
        .select(
            F.col("data_apuracao"),
            F.col("ano"),
            F.col("instituicao_fin"),
            F.col("nome"),
            F.col("valor_total"),
            F.lit("valor_total_anual").alias("nome_metrica"),
            F.lit("INSTITUICAO").alias("tipo_escopo"),
        )
    )

    df_inst_barras_ativos = (
        get_variation(
            df_instituicao_barras_raw.filter(F.col("nome") != "ALL"),
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

    df_inst_barras_total = (
        df_instituicao_barras_raw.filter(F.col("nome") == "ALL")
        .join(df_aportes_ano_inst_sum, on=["ano", "instituicao_fin"], how="left")
        .fillna({"aporte_anual": 0.0})
    )
    df_inst_barras_total = (
        get_variation_with_flow(
            df_inst_barras_total,
            value_column="valor_total",
            flow_column="aporte_anual",
            period_column="data_apuracao",
            partition_columns=["instituicao_fin"],
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

    df_instituicao_barras = df_inst_barras_ativos.unionByName(df_inst_barras_total)
    saved_paths.append(handler_partitions(df_instituicao_barras, "gold", "instituicao_barras"))

    log_section_separator(logger)
    logger.info("🥇 GOLD | Pipeline finished | files=%s", len(saved_paths))
    return saved_paths
