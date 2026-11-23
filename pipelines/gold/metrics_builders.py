from __future__ import annotations

import logging

from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.window import Window

from pipelines.shared.logging_utils import log_section_separator

logger = logging.getLogger(__name__)


def get_variation(
    df: DataFrame,
    value_column: str,
    period_column: str,
    partition_columns: list[str],
) -> DataFrame:
    """
    Calcula a variação percentual simples entre períodos consecutivos usando lag.
    Fórmula: ((valor_atual - valor_anterior) / valor_anterior) * 100.

    Parâmetros:
        df: DataFrame PySpark de entrada.
        value_column: Coluna numérica avaliada.
        period_column: Coluna temporal de ordenação da janela.
        partition_columns: Lista de colunas de particionamento da janela.

    Retorna:
        DataFrame PySpark com a coluna 'variacao_percentual'.
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
    Calcula a rentabilidade percentual deduzindo novos aportes.
    Fórmula: ((valor_atual - valor_anterior - aporte) / valor_anterior) * 100.

    Parâmetros:
        df: DataFrame PySpark de entrada.
        value_column: Coluna de patrimônio / saldo.
        flow_column: Coluna contendo o valor do aporte no período.
        period_column: Coluna temporal de ordenação da janela.
        partition_columns: Lista de colunas de particionamento da janela.

    Retorna:
        DataFrame PySpark com a coluna 'variacao_percentual' ajustada pelo fluxo.
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
                    )
                    * 100.0,
                    2,
                )
            ),
        )
        .drop("previous_value")
    )
    return df_result


def get_cumulative_capital(
    df_totals: DataFrame,
    df_contributions: DataFrame,
    partition_columns: list[str],
    date_column: str = "data_apuracao",
    value_column: str = "valor_total",
    flow_column: str = "aporte",
) -> DataFrame:
    """
    Calcula a evolução do capital investido acumulado (Escada de Aportes - Opção 2).

    Fórmula da Escada de Aportes:
        Capital Aportado_0 = Valor Inicial_0 (patrimônio no mês de partida)
        Capital Aportado_t = Capital Aportado_{t-1} + Aporte_t  (para t >= 1)

    Premissa:
        O capital investido parte do patrimônio total apurado no primeiro mês histórico (t=0).
        A cada mês subsequente (t >= 1), o capital acumulado é incrementado exclusivamente pelo
        aporte realizado naquele período. A variação percentual para esta série é definida como None,
        pois a métrica representa a régua de custo de capital investido e não a rentabilidade do período.

    Parâmetros:
        df_totals: DataFrame PySpark contendo os valores consolidados de patrimônio.
        df_contributions: DataFrame PySpark contendo os aportes por período e partição.
        partition_columns: Lista de colunas de particionamento (ex: [] para total geral, ou ['instituicao_fin']).
        date_column: Nome da coluna temporal (padrão 'data_apuracao').
        value_column: Nome da coluna de valor patrimonial (padrão 'valor_total').
        flow_column: Nome da coluna de aporte (padrão 'aporte').

    Retorna:
        DataFrame PySpark com a coluna 'valor_total' atualizada para o capital aportado acumulado
        e 'variacao_percentual' preenchida como None.
    """
    logger.info(
        "🥇 GOLD | Calculating cumulative invested capital (Option 2) | value=%s | flow=%s | date=%s | partition=%s",
        value_column,
        flow_column,
        date_column,
        partition_columns,
    )
    if partition_columns:
        join_keys = [*partition_columns, date_column]
        window = Window.partitionBy(*partition_columns).orderBy(F.col(date_column).asc())
    else:
        join_keys = [date_column]
        window = Window.orderBy(F.col(date_column).asc())

    window_frame = window.rowsBetween(Window.unboundedPreceding, Window.currentRow)

    df_joined = (
        df_totals
        .join(df_contributions, on=join_keys, how="left")
        .fillna({flow_column: 0.0})
    )

    df_result = (
        df_joined
        .withColumn("_rn", F.row_number().over(window))
        .withColumn(
            "_aporte_efetivo",
            F.when(F.col("_rn") == 1, F.lit(0.0)).otherwise(F.coalesce(F.col(flow_column), F.lit(0.0))),
        )
        .withColumn("_initial_value", F.first(F.col(value_column)).over(window_frame))
        .withColumn(
            "_cum_aporte",
            F.sum(F.col("_aporte_efetivo")).over(window_frame),
        )
        .withColumn(value_column, F.round(F.col("_initial_value") + F.col("_cum_aporte"), 2))
        .withColumn("variacao_percentual", F.lit(None).cast("double"))
        .drop("_rn", "_aporte_efetivo", "_initial_value", "_cum_aporte", flow_column)
    )

    return df_result


def get_annual_variation_with_flow(
    df_monthly: DataFrame,
    df_contributions: DataFrame,
    partition_columns: list[str],
    date_column: str = "data_apuracao",
    year_column: str = "ano",
    value_column: str = "valor_total",
    flow_column: str = "aporte",
) -> DataFrame:
    """
    Calcula a variação anual e YTD de patrimônio deduzindo o fluxo financeiro (aportes).

    Regra de Negócio:
        - Para anos com ano anterior no histórico: a base é o valor de fechamento do ano anterior
          e o fluxo deduzido compreende todos os aportes do ano avaliado.
        - Para o ano de início de histórico (sem ano anterior): a base é o primeiro snapshot
          registrado naquele ano e o fluxo deduzido compreende os aportes realizados
          estritamente após a primeira data de apuração daquele ano.
        - Para partições com apenas uma data de apuração em todo o histórico: retorna None.

    Parâmetros:
        df_monthly: DataFrame PySpark com as posições mensais consolidadas.
        df_contributions: DataFrame PySpark contendo os aportes por data e partição.
        partition_columns: Lista de colunas de partição (ex: ['instituicao_fin'] ou [] para total).
        date_column: Nome da coluna de data (padrão 'data_apuracao').
        year_column: Nome da coluna de ano (padrão 'ano').
        value_column: Nome da coluna de valor patrimonial (padrão 'valor_total').
        flow_column: Nome da coluna de aporte (padrão 'aporte').

    Retorna:
        DataFrame PySpark com uma linha por (partição, ano) com a data de fechamento do ano,
        o valor apurado no fechamento e a variação percentual anual/YTD ajustada pelo fluxo.
    """
    logger.info(
        "🥇 GOLD | Calculating annual & YTD flow-adjusted variation | value=%s | flow=%s | partitions=%s",
        value_column,
        flow_column,
        partition_columns,
    )
    if partition_columns:
        group_keys = [*partition_columns, year_column]
        window_year_desc = Window.partitionBy(*group_keys).orderBy(F.col(date_column).desc())
        window_year_asc = Window.partitionBy(*group_keys).orderBy(F.col(date_column).asc())
        window_partition_ano = Window.partitionBy(*partition_columns).orderBy(F.col(year_column).asc())
    else:
        group_keys = [year_column]
        window_year_desc = Window.partitionBy(year_column).orderBy(F.col(date_column).desc())
        window_year_asc = Window.partitionBy(year_column).orderBy(F.col(date_column).asc())
        window_partition_ano = Window.orderBy(F.col(year_column).asc())

    df_year_summary = (
        df_monthly
        .withColumn("_rn_last", F.row_number().over(window_year_desc))
        .withColumn("_rn_first", F.row_number().over(window_year_asc))
        .groupBy(*group_keys)
        .agg(
            F.max(F.when(F.col("_rn_last") == 1, F.col(date_column))).alias(date_column),
            F.max(F.when(F.col("_rn_last") == 1, F.col(value_column))).alias(value_column),
            F.max(F.when(F.col("_rn_first") == 1, F.col(date_column))).alias("_first_date"),
            F.max(F.when(F.col("_rn_first") == 1, F.col(value_column))).alias("_first_value"),
        )
    )

    df_contrib_prepared = df_contributions
    if year_column not in df_contrib_prepared.columns:
        df_contrib_prepared = df_contrib_prepared.withColumn(year_column, F.year(F.col(date_column)))

    df_contrib_agg = (
        df_contrib_prepared
        .join(df_year_summary.select(*group_keys, "_first_date"), on=group_keys, how="inner")
        .groupBy(*group_keys)
        .agg(
            F.sum(F.col(flow_column)).alias("_aporte_anual"),
            F.sum(
                F.when(F.col(date_column) > F.col("_first_date"), F.col(flow_column)).otherwise(F.lit(0.0))
            ).alias("_aporte_post_first"),
        )
    )

    df_joined = (
        df_year_summary
        .join(df_contrib_agg, on=group_keys, how="left")
        .fillna({"_aporte_anual": 0.0, "_aporte_post_first": 0.0})
    )

    df_with_prev = (
        df_joined
        .withColumn("_prev_closing_value", F.lag(F.col(value_column)).over(window_partition_ano))
    )

    df_result = (
        df_with_prev
        .withColumn(
            "_base_value",
            F.when(F.col("_prev_closing_value").isNotNull(), F.col("_prev_closing_value"))
            .when(F.col(date_column) > F.col("_first_date"), F.col("_first_value"))
            .otherwise(F.lit(None).cast("double")),
        )
        .withColumn(
            "_effective_flow",
            F.when(F.col("_prev_closing_value").isNotNull(), F.col("_aporte_anual"))
            .otherwise(F.col("_aporte_post_first")),
        )
        .withColumn(
            "variacao_percentual",
            F.when(
                F.col("_base_value").isNotNull() & (F.col("_base_value") > 0),
                F.round(
                    ((F.col(value_column) - F.col("_base_value") - F.coalesce(F.col("_effective_flow"), F.lit(0.0)))
                     / F.col("_base_value")) * 100.0,
                    2,
                ),
            ).otherwise(F.lit(None).cast("double")),
        )
        .drop(
            "_first_date",
            "_first_value",
            "_aporte_anual",
            "_aporte_post_first",
            "_prev_closing_value",
            "_base_value",
            "_effective_flow",
        )
    )

    return df_result


def get_annual_asset_variation(
    df_monthly_assets: DataFrame,
    partition_columns: list[str],
    date_column: str = "data_apuracao",
    year_column: str = "ano",
    value_column: str = "valor_total",
    price_column: str = "preco_atual",
) -> DataFrame:
    """
    Calcula a variação anual e YTD de ativos individuais com base na cota/preço unitário.

    Regra de Negócio:
        - Para anos com ano anterior no histórico: a base é o preço unitário de fechamento do ano anterior.
        - Para o ano de início de histórico (sem ano anterior): a base é o primeiro preço unitário registrado
          naquele ano (se houver mais de uma data de apuração).
        - Para ativos com apenas uma data de apuração em todo o histórico: retorna None.

    Parâmetros:
        df_monthly_assets: DataFrame PySpark com as posições mensais por ativo.
        partition_columns: Lista de colunas de partição (ex: ['instituicao_fin', 'nome']).
        date_column: Nome da coluna de data (padrão 'data_apuracao').
        year_column: Nome da coluna de ano (padrão 'ano').
        value_column: Nome da coluna de valor patrimonial (padrão 'valor_total').
        price_column: Nome da coluna de cota/preço (padrão 'preco_atual').

    Retorna:
        DataFrame PySpark com uma linha por (partição, ano) com 'valor_total' no fechamento do ano
        e 'variacao_percentual' da cota anual/YTD.
    """
    logger.info(
        "🥇 GOLD | Calculating annual & YTD asset price variation | price=%s | partitions=%s",
        price_column,
        partition_columns,
    )
    group_keys = [*partition_columns, year_column]
    window_year_desc = Window.partitionBy(*group_keys).orderBy(F.col(date_column).desc())
    window_year_asc = Window.partitionBy(*group_keys).orderBy(F.col(date_column).asc())
    window_partition_ano = Window.partitionBy(*partition_columns).orderBy(F.col(year_column).asc())

    df_year_summary = (
        df_monthly_assets
        .withColumn("_rn_last", F.row_number().over(window_year_desc))
        .withColumn("_rn_first", F.row_number().over(window_year_asc))
        .groupBy(*group_keys)
        .agg(
            F.max(F.when(F.col("_rn_last") == 1, F.col(date_column))).alias(date_column),
            F.max(F.when(F.col("_rn_last") == 1, F.col(value_column))).alias(value_column),
            F.max(F.when(F.col("_rn_last") == 1, F.col(price_column))).alias("_last_price"),
            F.max(F.when(F.col("_rn_first") == 1, F.col(date_column))).alias("_first_date"),
            F.max(F.when(F.col("_rn_first") == 1, F.col(price_column))).alias("_first_price"),
        )
    )

    df_with_prev = (
        df_year_summary
        .withColumn("_prev_closing_price", F.lag(F.col("_last_price")).over(window_partition_ano))
    )

    df_result = (
        df_with_prev
        .withColumn(
            "_base_price",
            F.when(F.col("_prev_closing_price").isNotNull(), F.col("_prev_closing_price"))
            .when(F.col(date_column) > F.col("_first_date"), F.col("_first_price"))
            .otherwise(F.lit(None).cast("double")),
        )
        .withColumn(
            "variacao_percentual",
            F.when(
                F.col("_base_price").isNotNull() & (F.col("_base_price") > 0),
                F.round(
                    ((F.col("_last_price") - F.col("_base_price")) / F.col("_base_price")) * 100.0,
                    2,
                ),
            ).otherwise(F.lit(None).cast("double")),
        )
        .drop("_last_price", "_first_date", "_first_price", "_prev_closing_price", "_base_price")
    )

    return df_result


def get_latest_portfolio_snapshot(df_silver: DataFrame) -> DataFrame:
    """
    Extrai a última foto de posições da carteira particionando estritamente por instituição.
    Garante a exclusão de ativos descontinuados ou vendidos em meses passados (ativos fantasmas).

    Parâmetros:
        df_silver: DataFrame PySpark da camada Silver.

    Retorna:
        DataFrame PySpark contendo apenas os ativos vigentes na data mais recente de cada instituição.
    """
    logger.info("🥇 GOLD | Preparing latest positions (full photo by institution)")
    latest_inst_window = Window.partitionBy("instituicao_fin").orderBy(
        F.col("data_apuracao").desc()
    )
    df_dados_atuais = (
        df_silver
        .withColumn("_rank", F.dense_rank().over(latest_inst_window))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
    )
    return df_dados_atuais


def build_contributions_context(df_silver: DataFrame) -> dict[str, DataFrame]:
    """
    Gera tabelas de lookup de aportes desduplicadas por linha Bronze,
    agregando valores mensais e anuais por instituição e consolidado geral (ALL).

    Parâmetros:
        df_silver: DataFrame PySpark da camada Silver.

    Retorna:
        Dicionário com DataFrames auxiliares de aportes mensais e anuais.
    """
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

    context = {
        "df_aportes_base": df_aportes_base,
        "df_aportes_inst_sum": df_aportes_inst_sum,
        "df_aportes_total_sum": df_aportes_total_sum,
        "df_aportes_lookup": df_aportes_lookup,
        "df_aportes_ano_inst_sum": df_aportes_ano_inst_sum,
        "df_aportes_ano_lookup": df_aportes_ano_lookup,
    }
    return context


def build_home_metrics(
    df_silver: DataFrame,
    contributions_ctx: dict[str, DataFrame],
) -> dict[str, DataFrame]:
    """
    Gera os datasets analíticos da tela inicial (Home): evolução temporal mensal,
    cartões com últimas posições e barras anuais deduzindo aportes.

    Parâmetros:
        df_silver: DataFrame PySpark da camada Silver.
        contributions_ctx: Dicionário contendo as tabelas base de aportes.

    Retorna:
        Dicionário com 'home_linha', 'home_botoes' e 'home_barras'.
    """
    log_section_separator(logger)
    logger.info("🥇 GOLD | Building home metrics (linha, botoes, barras)")

    # 1. Home Linha
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
        .join(contributions_ctx["df_aportes_lookup"], on=["data_apuracao", "instituicao_fin"], how="left")
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

    # 2. Home Botoes
    latest_home_window = Window.partitionBy("instituicao_fin").orderBy(F.col("data_apuracao").desc())
    df_home_botoes = (
        df_home_linha
        .withColumn("rn", F.row_number().over(latest_home_window))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

    # 3. Home Barras
    max_ano = df_silver.agg(F.max("ano")).first()[0]
    df_home_barras = (
        get_annual_variation_with_flow(
            df_monthly=df_home_linha,
            df_contributions=contributions_ctx["df_aportes_lookup"],
            partition_columns=["instituicao_fin"],
        )
        .withColumn("tipo_escopo", F.lit("HOME"))
        .withColumn("nome_metrica", F.lit("valor_total_anual"))
        .withColumn(
            "rotulo_ano",
            F.when(F.col("ano") == max_ano, F.lit("YTD")).otherwise(F.col("ano").cast("string")),
        )
        .select(
            "data_apuracao",
            "ano",
            "rotulo_ano",
            "instituicao_fin",
            "valor_total",
            "tipo_escopo",
            "nome_metrica",
            "variacao_percentual",
        )
    )

    # 4. Série Adicional: Capital Investido (Escada de Aportes - Opção 2) para gráficos de absolutos na Home
    df_aportes_total_only = (
        contributions_ctx["df_aportes_total_sum"]
        .select("data_apuracao", "aporte")
    )
    df_home_capital_aportado = (
        get_cumulative_capital(
            df_totals=df_total,
            df_contributions=df_aportes_total_only,
            partition_columns=[],
            date_column="data_apuracao",
            value_column="valor_total",
            flow_column="aporte",
        )
        .withColumn("instituicao_fin", F.lit("Capital Investido"))
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
    df_home_linha_final = df_home_linha.unionByName(df_home_capital_aportado)

    metrics = {
        "home_linha": df_home_linha_final,
        "home_botoes": df_home_botoes,
        "home_barras": df_home_barras,
    }
    return metrics


def build_contribution_metrics(
    contributions_ctx: dict[str, DataFrame],
) -> dict[str, DataFrame]:
    """
    Gera os datasets da página de aportes: histórico mensal e barras anuais
    por instituição e consolidado geral (ALL).

    Parâmetros:
        contributions_ctx: Dicionário contendo as tabelas base de aportes.

    Retorna:
        Dicionário com 'aportes_linha' e 'aportes_barras'.
    """
    log_section_separator(logger)
    logger.info("🥇 GOLD | Building contribution metrics (linha, barras)")
    df_aportes_base = contributions_ctx["df_aportes_base"]

    # 1. Aportes Linha
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

    # 2. Aportes Barras
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

    metrics = {
        "aportes_linha": df_aportes_linha,
        "aportes_barras": df_aportes_barras,
    }
    return metrics


def build_distribution_metrics(df_dados_atuais: DataFrame) -> dict[str, DataFrame]:
    """
    Gera os datasets de distribuição percentual da carteira atual (gráficos de pizza):
    exposição geográfica (nacional vs internacional) e classe de ativo.

    Parâmetros:
        df_dados_atuais: DataFrame PySpark contendo a última foto particionada por instituição.

    Retorna:
        Dicionário com 'pizza_expo' e 'pizza_tipo'.
    """
    log_section_separator(logger)
    logger.info("🥇 GOLD | Building distribution metrics (pizza_expo, pizza_tipo)")

    # 1. Pizza Exposição
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

    # 2. Pizza Tipo
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

    metrics = {
        "pizza_expo": df_exposicao,
        "pizza_tipo": df_tipo,
    }
    return metrics


def build_institution_metrics(
    df_silver: DataFrame,
    df_dados_atuais: DataFrame,
    contributions_ctx: dict[str, DataFrame],
) -> dict[str, DataFrame]:
    """
    Gera os datasets analíticos específicos de cada instituição financeira:
    evolução mensal por ativo e total, tabela detalhada com retorno acumulado vs preço médio
    e evolução anual em barras.

    Parâmetros:
        df_silver: DataFrame PySpark da camada Silver.
        df_dados_atuais: DataFrame PySpark contendo a última foto por instituição.
        contributions_ctx: Dicionário contendo as tabelas base de aportes.

    Retorna:
        Dicionário com 'instituicao_linha', 'instituicao_label' e 'instituicao_barras'.
    """
    log_section_separator(logger)
    logger.info("🥇 GOLD | Building institution metrics (linha, label, barras)")

    # 1. Instituição Linha
    # Ativos individuais: variação mensal calculada sobre a cota/preço unitário
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

    # Consolidado da instituição (nome == 'ALL'): rentabilidade ponderada dos ativos da instituição
    # Pondera a oscilação da cota de cada ativo pelo seu patrimônio no período anterior (t-1)
    asset_window = Window.partitionBy("instituicao_fin", "nome").orderBy(F.col("data_apuracao").asc())
    df_ativos_weights = (
        df_instituicao_ativos_raw
        .withColumn("_prev_preco", F.lag(F.col("preco_atual")).over(asset_window))
        .withColumn("_prev_valor", F.lag(F.col("valor_total")).over(asset_window))
        .withColumn(
            "_var_cota",
            F.when(
                F.col("_prev_preco").isNotNull() & (F.col("_prev_preco") > 0),
                ((F.col("preco_atual") - F.col("_prev_preco")) / F.col("_prev_preco")) * 100.0,
            ).otherwise(F.lit(None)),
        )
        .withColumn(
            "_weighted_gain",
            F.when(
                F.col("_var_cota").isNotNull() & F.col("_prev_valor").isNotNull(),
                F.col("_prev_valor") * F.col("_var_cota"),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "_base_weight",
            F.when(
                F.col("_var_cota").isNotNull() & F.col("_prev_valor").isNotNull(),
                F.col("_prev_valor"),
            ).otherwise(F.lit(0.0)),
        )
    )

    df_instituicao_total = (
        df_ativos_weights
        .groupBy("data_apuracao", "ano", "mes", "instituicao_fin")
        .agg(
            F.round(F.sum("valor_total"), 2).alias("valor_total"),
            F.sum("_weighted_gain").alias("_total_gain"),
            F.sum("_base_weight").alias("_total_weight"),
        )
        .withColumn(
            "variacao_percentual",
            F.when(
                F.col("_total_weight") > 0,
                F.round(F.col("_total_gain") / F.col("_total_weight"), 2),
            ).otherwise(F.lit(None)),
        )
        .select(
            F.col("data_apuracao"),
            F.col("ano"),
            F.col("mes"),
            F.col("instituicao_fin"),
            F.lit("ALL").alias("nome"),
            F.col("valor_total"),
            F.lit("valor_total").alias("nome_metrica"),
            F.lit("INSTITUICAO").alias("tipo_escopo"),
            F.col("variacao_percentual"),
        )
    )

    df_instituicao_linha = df_instituicao_ativos.unionByName(df_instituicao_total)

    # 2. Instituição Labels
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

    # 3. Instituição Barras
    df_inst_barras_ativos = (
        get_annual_asset_variation(
            df_monthly_assets=df_instituicao_ativos_raw,
            partition_columns=["instituicao_fin", "nome"],
        )
        .withColumn("nome_metrica", F.lit("valor_total_anual"))
        .withColumn("tipo_escopo", F.lit("INSTITUICAO"))
    )

    df_inst_barras_total = (
        get_annual_variation_with_flow(
            df_monthly=df_instituicao_total,
            df_contributions=contributions_ctx["df_aportes_inst_sum"],
            partition_columns=["instituicao_fin"],
        )
        .withColumn("nome", F.lit("ALL"))
        .withColumn("nome_metrica", F.lit("valor_total_anual"))
        .withColumn("tipo_escopo", F.lit("INSTITUICAO"))
    )

    max_ano = df_silver.agg(F.max("ano")).first()[0]
    df_instituicao_barras = (
        df_inst_barras_ativos.unionByName(df_inst_barras_total)
        .withColumn(
            "rotulo_ano",
            F.when(F.col("ano") == max_ano, F.lit("YTD")).otherwise(F.col("ano").cast("string")),
        )
        .select(
            "data_apuracao",
            "ano",
            "rotulo_ano",
            "instituicao_fin",
            "nome",
            "valor_total",
            "nome_metrica",
            "tipo_escopo",
            "variacao_percentual",
        )
    )

    # 4. Série Adicional: Capital Investido por Instituição (Escada de Aportes - Opção 2) para gráficos de absolutos
    df_aportes_inst_only = (
        contributions_ctx["df_aportes_inst_sum"]
        .select("data_apuracao", "instituicao_fin", "aporte")
    )
    df_inst_total_base = (
        df_instituicao_total
        .select(
            "data_apuracao",
            "ano",
            "mes",
            "instituicao_fin",
            "valor_total",
            "nome_metrica",
            "tipo_escopo",
        )
    )
    df_inst_capital_aportado = (
        get_cumulative_capital(
            df_totals=df_inst_total_base,
            df_contributions=df_aportes_inst_only,
            partition_columns=["instituicao_fin"],
            date_column="data_apuracao",
            value_column="valor_total",
            flow_column="aporte",
        )
        .withColumn("nome", F.lit("Capital Investido"))
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
    df_instituicao_linha_final = df_instituicao_linha.unionByName(df_inst_capital_aportado)

    metrics = {
        "instituicao_linha": df_instituicao_linha_final,
        "instituicao_label": df_instituicao_label,
        "instituicao_barras": df_instituicao_barras,
    }
    return metrics
