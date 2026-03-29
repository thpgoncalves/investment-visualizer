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


def build_base_layout(title: str) -> dict:
    return dict(
        title=dict(
            text=title,
            x=0.5,
            xanchor="center",
        ),
        margin=dict(l=20, r=20, t=50, b=50),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        legend=dict(
            title=None,
            orientation="h",
            x=0.5,
            xanchor="center",
            y=-0.2,
            yanchor="top",
            font=dict(size=11),
        ),
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
        **build_base_layout(title),
        height=280
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
    percentual: True | False = False
) -> go.Figure:

    validate_required_columns(
        df=df,
        required_columns=[x_col, y_col, series_col],
        chart_name="line chart",
    )

    y_min = df[y_col].min()
    y_max = df[y_col].max()
    y_padding = (y_max - y_min) * 0.15 if y_max != y_min else 1

    figure = go.Figure()

    series_names = df[series_col].dropna().unique()

    for series_name in series_names:
        series_df = df[df[series_col] == series_name]

        if percentual == False:
            figure.add_trace(
                go.Scatter(
                    x=series_df[x_col],
                    y=series_df[y_col],
                    mode="lines+markers+text",
                    name=str(series_name),
                    text=[""] * (len(series_df) - 1) + [f"R${series_df[y_col].iloc[-1]:,.0f}"],
                    textposition="top right",
                    textfont=dict(size=11),
                )
            )

            figure.update_layout(
                **build_base_layout(title),
                height=320,
                yaxis=dict(
                    range=[0, y_max + y_padding],
                ),
            )


        else:
            figure.add_trace(
                go.Scatter(
                    x=series_df[x_col],
                    y=series_df[y_col],
                    mode="lines+markers+text",
                    name=str(series_name),
                    text=[""] * (len(series_df) - 1) + [f"{series_df[y_col].iloc[-1]:,.2f}%"],
                    textposition="top right",
                    textfont=dict(size=11),
                )
            )

            figure.update_layout(
                **build_base_layout(title),
                height=320,
                yaxis=dict(
                    range=[y_min - y_padding, y_max + y_padding],
                ),
            )

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
    percentual: True | False = False
) -> go.Figure:

    validate_required_columns(
        df=df,
        required_columns=[x_col, y_col, series_col],
        chart_name="grouped bar chart",
    )

    figure = go.Figure()

    series_names = df[series_col].dropna().unique()

    for series_name in series_names:
        series_df = df[df[series_col] == series_name]

        if percentual == False:
            figure.add_trace(
                go.Bar(
                x=series_df[x_col],
                y=series_df[y_col],
                name=str(series_name),
                text=[f"R${value:,.0f}" for value in series_df[y_col]],
                textposition="outside",
                textfont=dict(size=10),
                cliponaxis=False,
                )
            )

        else:
            figure.add_trace(
                go.Bar(
                x=series_df[x_col],
                y=series_df[y_col],
                name=str(series_name),
                text=[f"{value:,.2f}%" for value in series_df[y_col]],
                textposition="outside",
                textfont=dict(size=10),
                cliponaxis=False,
                )
            )

    figure.update_layout(
    **build_base_layout(title),
    height=320
    )

    return figure