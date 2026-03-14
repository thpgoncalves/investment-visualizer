from __future__ import annotations

from typing import Sequence

import pandas as pd
import plotly.graph_objects as go


# -------------------------------------------------------------------
# Esta função existe para validar se o DataFrame recebido possui
# as colunas mínimas necessárias para o gráfico.
#
# Por que isso é útil?
# - porque você quer que o componente de gráfico seja reutilizável;
# - então, se um DataFrame vier sem a coluna esperada, o erro fica claro;
# - isso evita um erro confuso mais para frente no Plotly.
#
# Importante:
# - esta função NÃO transforma dado;
# - ela só verifica a existência das colunas.
# -------------------------------------------------------------------
def validate_required_columns(
    df: pd.DataFrame,
    required_columns: Sequence[str],
    chart_name: str,
) -> None:
    """
    Valida se o DataFrame contém todas as colunas exigidas.

    Argumentos:
    - df:
      DataFrame já pronto, vindo da camada gold ou equivalente.

    - required_columns:
      sequência com os nomes das colunas obrigatórias.

    - chart_name:
      nome textual do gráfico.
      Serve apenas para melhorar a mensagem de erro.

    Retorno:
    - nenhum.
      Se faltar coluna, a função lança ValueError.
    """

    missing_columns = [column for column in required_columns if column not in df.columns]

    if missing_columns:
        raise ValueError(
            f"Missing required columns for {chart_name}: {missing_columns}. "
            f"Available columns: {list(df.columns)}"
        )


# -------------------------------------------------------------------
# GRÁFICO DE PIZZA
# -------------------------------------------------------------------
def build_pie_chart(
    df: pd.DataFrame,
    title: str,
    label_col: str,
    value_col: str,
) -> go.Figure:
    """
    Monta um gráfico de pizza lendo diretamente um DataFrame já pronto.

    Ideia de uso:
    - usar esta função para os gráficos de distribuição do topo;
    - exemplo: distribuição por tipo, por moeda, por instituição etc.

    O que o DataFrame precisa ter:
    - uma coluna com os rótulos da pizza;
    - uma coluna com os valores numéricos da pizza.

    Exemplo de DataFrame esperado:
        category          amount
        Ações             24000
        Renda Fixa        27000
        Cripto            16000
        Tesouro           33000

    Nesse caso:
    - label_col = "category"
    - value_col = "amount"

    Importante:
    - esta função NÃO agrega dados;
    - ela NÃO faz groupby;
    - ela NÃO calcula percentual;
    - ela apenas lê linha a linha do DataFrame recebido.
    - o percentual mostrado na pizza é calculado visualmente pelo Plotly
      a partir dos valores recebidos, o que é comportamento padrão do gráfico,
      não uma regra de negócio do seu sistema.

    Argumentos:
    - df:
      DataFrame pronto para consumo.
      Cada linha representa uma fatia da pizza.

    - title:
      título do gráfico.

    - label_col:
      nome da coluna que contém o texto de cada fatia.

    - value_col:
      nome da coluna que contém o valor numérico de cada fatia.

    Retorno:
    - Figure do Plotly.
    """

    validate_required_columns(
        df=df,
        required_columns=[label_col, value_col],
        chart_name="pie chart",
    )

    figure = go.Figure()

    figure.add_trace(
        go.Pie(
            labels=df[label_col],
            values=df[value_col],
            textinfo="label+percent",
            hole=0,
        )
    )

    figure.update_layout(
        title=title,
        height=280,
        margin=dict(l=20, r=20, t=50, b=20),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
    )

    return figure


# -------------------------------------------------------------------
# GRÁFICO DE LINHA
# -------------------------------------------------------------------
def build_line_chart(
    df: pd.DataFrame,
    title: str,
    x_col: str,
    y_col: str,
    series_col: str,
    y_axis_title: str = "Value",
) -> go.Figure:
    """
    Monta um gráfico de linha a partir de um DataFrame já pronto.

    Ideia de uso:
    - serve tanto para gráfico de absolutos quanto para gráfico de variação %;
    - a diferença vai estar no DataFrame recebido e no y_col usado.

    Estrutura esperada do DataFrame:
    - uma coluna de eixo X
    - uma coluna de valores numéricos (Y)
    - uma coluna que identifica a série/linha

    Exemplo de DataFrame esperado:
        month    series    value
        Jan      Total     100000
        Fev      Total     102000
        Mar      Total     101000
        Jan      XP        15000
        Fev      XP        16000
        Mar      XP        17000

    Nesse caso:
    - x_col = "month"
    - y_col = "value"
    - series_col = "series"

    Importante:
    - esta função NÃO ordena os dados;
    - ela NÃO faz pivot;
    - ela NÃO calcula valor absoluto;
    - ela NÃO calcula percentual;
    - ela apenas separa visualmente as linhas com base em series_col.
    - portanto, o DataFrame deve chegar pronto e, de preferência,
      já na ordem correta para plotagem.

    Sobre a ordenação:
    - se você quiser que os meses apareçam em ordem correta,
      envie o DataFrame já ordenado antes de chamar esta função.

    Argumentos:
    - df:
      DataFrame pronto, onde cada linha representa um ponto de uma série.

    - title:
      título do gráfico.

    - x_col:
      nome da coluna usada no eixo X.
      Exemplo: "month", "reference_date", "period_label".

    - y_col:
      nome da coluna usada no eixo Y.
      Exemplo: "absolute_value" ou "percentage_change".

    - series_col:
      nome da coluna que identifica a qual linha aquele ponto pertence.
      Exemplo: "series", "institution", "metric_name".

    - y_axis_title:
      texto do eixo Y.
      Exemplo: "R$" ou "%".

    Retorno:
    - Figure do Plotly.
    """

    validate_required_columns(
        df=df,
        required_columns=[x_col, y_col, series_col],
        chart_name="line chart",
    )

    figure = go.Figure()

    # ----------------------------------------------------------------
    # Aqui não estamos transformando o dado.
    # Apenas percorremos os nomes distintos das séries para desenhar
    # uma linha por série.
    #
    # Exemplo:
    # - se series_col tiver "Total", "XP", "Nubank"
    #   então teremos 3 linhas.
    # ----------------------------------------------------------------
    series_names = df[series_col].dropna().unique()

    for series_name in series_names:
        series_df = df[df[series_col] == series_name]

        figure.add_trace(
            go.Scatter(
                x=series_df[x_col],
                y=series_df[y_col],
                mode="lines+markers",
                name=str(series_name),
            )
        )

    figure.update_layout(
        title=title,
        height=320,
        margin=dict(l=20, r=20, t=50, b=20),
        xaxis_title="Período",
        yaxis_title=y_axis_title,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        legend_title="Séries",
    )

    # acho q vale refatorar pro total calcular aqui, vai dar menos trabalho do q adicionar a linha com tratamento no df

    return figure



# -------------------------------------------------------------------
# GRÁFICO DE BARRAS AGRUPADAS
# -------------------------------------------------------------------
def build_grouped_bar_chart(
    df: pd.DataFrame,
    title: str,
    x_col: str,
    y_col: str,
    series_col: str,
    y_axis_title: str = "Value",
) -> go.Figure:
    """
    Monta um gráfico de barras agrupadas a partir de um DataFrame já pronto.

    Ideia de uso:
    - serve para os gráficos comparativos da direita;
    - por exemplo: 2024 x 2025 x YTD;
    - tanto para valores absolutos quanto para percentuais.

    Estrutura esperada do DataFrame:
    - uma coluna para o eixo X;
    - uma coluna com o valor numérico;
    - uma coluna identificando a série.

    Exemplo de DataFrame esperado:
        period    series    value
        2024      Total     100000
        2025      Total     120000
        YTD       Total     95000
        2024      XP        25000
        2025      XP        28000
        YTD       XP        22000

    Nesse caso:
    - x_col = "period"
    - y_col = "value"
    - series_col = "series"

    Importante:
    - esta função NÃO agrupa nem soma;
    - ela NÃO calcula o YTD;
    - ela NÃO calcula as variações;
    - ela só lê as linhas prontas e monta os grupos visuais.
    - então o DataFrame gold precisa chegar com esses números já definidos.

    Argumentos:
    - df:
      DataFrame pronto, onde cada linha representa uma barra.

    - title:
      título do gráfico.

    - x_col:
      coluna das categorias no eixo X.
      Exemplo: "period", "year_label", "comparison_group".

    - y_col:
      coluna do valor numérico da barra.
      Exemplo: "absolute_value", "percentage_value".

    - series_col:
      coluna que define a série.
      Exemplo: "series", "institution", "asset_group".

    - y_axis_title:
      rótulo do eixo Y.

    Retorno:
    - Figure do Plotly.
    """

    validate_required_columns(
        df=df,
        required_columns=[x_col, y_col, series_col],
        chart_name="grouped bar chart",
    )

    figure = go.Figure()

    # ----------------------------------------------------------------
    # Mesmo raciocínio do gráfico de linha:
    # - cada série distinta vira um conjunto de barras.
    # - não há cálculo aqui, apenas leitura e separação visual.
    # ----------------------------------------------------------------
    series_names = df[series_col].dropna().unique()

    for series_name in series_names:
        series_df = df[df[series_col] == series_name]

        figure.add_trace(
            go.Bar(
                x=series_df[x_col],
                y=series_df[y_col],
                name=str(series_name),
            )
        )

    figure.update_layout(
        title=title,
        height=320,
        margin=dict(l=20, r=20, t=50, b=20),
        xaxis_title="Categoria",
        yaxis_title=y_axis_title,
        barmode="group",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        legend_title="Séries",
    )

    return figure