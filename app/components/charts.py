from __future__ import annotations

from typing import Sequence

import pandas as pd
import plotly.graph_objects as go

CHART_COLORS = [
    "#cb785c",  # terracotta, theme primary
    "#2f6f73",  # muted teal
    "#6f8f5a",  # moss
    "#c59a43",  # soft ochre
    "#8b6f9f",  # muted violet
    "#607580",  # blue gray
    "#a65f46",  # clay
    "#8f8a78",  # warm gray
]


def get_chart_color(index: int) -> str:
    return CHART_COLORS[index % len(CHART_COLORS)]


def get_chart_colors(count: int) -> list[str]:
    return [get_chart_color(index) for index in range(count)]


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
        colorway=CHART_COLORS,
        font=dict(color="#3d3a2a"),
        legend=dict(
            title=None,
            orientation="h",
            x=0.5,
            xanchor="center",
            y=-0.2,
            yanchor="top",
            font=dict(size=11),
        ),
        xaxis=dict(
            gridcolor="#e6e2d8",
            zerolinecolor="#d3d2ca",
        ),
        yaxis=dict(
            gridcolor="#e6e2d8",
            zerolinecolor="#d3d2ca",
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
            marker=dict(colors=get_chart_colors(len(df))),
        )
    )

    layout = build_base_layout(title)
    layout["height"] = 300
    layout["margin"] = dict(l=30, r=30, t=76, b=46)
    layout["title"] = dict(
        text=title,
        x=0.5,
        xanchor="center",
        y=0.98,
    )

    figure.update_layout(**layout)

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
    y_range = y_max - y_min
    y_padding = max(y_range * 0.2, abs(y_max) * 0.12, 1)

    figure = go.Figure()

    series_names = df[series_col].dropna().unique()

    for index, series_name in enumerate(series_names):
        series_df = df[df[series_col] == series_name]
        color = get_chart_color(index)

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
                    line=dict(color=color),
                    marker=dict(color=color),
                )
            )

            layout = build_base_layout(title)
            layout["height"] = 320
            layout["yaxis"] = {
                **layout["yaxis"],
                "range": [0, y_max + y_padding],
            }
            figure.update_layout(**layout)


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
                    line=dict(color=color),
                    marker=dict(color=color),
                )
            )

            layout = build_base_layout(title)
            layout["height"] = 320
            layout["yaxis"] = {
                **layout["yaxis"],
                "range": [y_min - y_padding, y_max + y_padding],
            }
            figure.update_layout(**layout)

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

    for index, series_name in enumerate(series_names):
        series_df = df[df[series_col] == series_name]
        color = get_chart_color(index)

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
                marker_color=color,
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
                marker_color=color,
                )
            )

    figure.update_layout(
    **build_base_layout(title),
    height=320
    )

    return figure
