from pathlib import Path

import pandas as pd
import streamlit as st

from app.components.charts import build_grouped_bar_chart, build_line_chart
from app.components.commons import (
    build_aportes_table_display,
    build_aportes_table_column_config,
    inject_page_css,
    render_total_block,
    render_value_block,
)


inject_page_css()

st.markdown(
    """
    <style>
        .st-key-aportes-average-block .big-total,
        .st-key-aportes-total-block .big-total {
            margin-top: 3.6rem;
        }
    </style>
    """,
    unsafe_allow_html=True,
)

selected_yyyymm = st.session_state["selected_yyyymm"]

if selected_yyyymm is None:
    st.info("Nenhuma partição mensal foi encontrada em data/gold.")
    st.stop()


def build_gold_path(file_name: str) -> Path:
    return Path("data") / "gold" / selected_yyyymm / f"{selected_yyyymm}_gold_{file_name}_snapshot.csv"


aportes_linha_path = build_gold_path("aportes_linha")
aportes_barras_path = build_gold_path("aportes_barras")
missing_paths = [
    path
    for path in [aportes_linha_path, aportes_barras_path]
    if not path.exists()
]

if missing_paths:
    st.info("Os arquivos Gold de aportes ainda não foram gerados. Rode a pipeline para atualizar data/gold.")
    st.stop()


def build_aportes_matrix(df: pd.DataFrame) -> pd.DataFrame:
    df_matrix = df[df["instituicao_fin"] != "ALL"].copy()
    df_matrix["ano_label"] = df_matrix["ano"].astype(str)

    year_order = (
        df_matrix[["ano", "ano_label"]]
        .drop_duplicates()
        .sort_values("ano")["ano_label"]
        .tolist()
    )

    matrix = df_matrix.pivot_table(
        index="instituicao_fin",
        columns="ano_label",
        values="valor_total",
        aggfunc="sum",
        fill_value=0,
    )
    matrix = matrix.reindex(columns=year_order, fill_value=0)
    matrix["total"] = matrix.sum(axis=1)

    row_order = sorted(matrix.index)
    return matrix.loc[row_order].reset_index()


df_aportes_linha = pd.read_csv(aportes_linha_path)
df_aportes_linha["data_apuracao"] = pd.to_datetime(df_aportes_linha["data_apuracao"])
df_aportes_linha = df_aportes_linha.sort_values(["data_apuracao", "instituicao_fin"])

df_aportes_barras = pd.read_csv(aportes_barras_path)
df_aportes_barras["data_apuracao"] = pd.to_datetime(df_aportes_barras["data_apuracao"])
df_aportes_barras = df_aportes_barras.sort_values(["ano", "instituicao_fin"])

df_aportes_matriz = build_aportes_matrix(df_aportes_linha)

df_aportes_total = df_aportes_linha[df_aportes_linha["instituicao_fin"] == "ALL"]
total_aportado = df_aportes_total["valor_total"].sum()
media_mensal = (
    df_aportes_total["valor_total"].mean()
    if not df_aportes_total.empty
    else 0
)

column_config = build_aportes_table_column_config(df_aportes_matriz.columns)
df_aportes_matriz_display = build_aportes_table_display(df_aportes_matriz)

with st.container(border=True):
    st.markdown(
        """
        <div class="institution-heading">
            <span class="institution-name">Aportes</span>
        </div>
        """,
        unsafe_allow_html=True,
    )

    left_col, center_col, right_col = st.columns([3.8, 1.7, 1.7], gap="large")

    with left_col:
        st.markdown(
            '<div class="section-title" style="font-size: 24px;">Aportes por Ano</div>',
            unsafe_allow_html=True,
        )
        st.dataframe(
            df_aportes_matriz_display,
            hide_index=True,
            width="stretch",
            column_config=column_config,
        )

    with center_col:
        with st.container(border=False, key="aportes-average-block"):
            render_value_block("Média Mensal", media_mensal)

    with right_col:
        with st.container(border=False, key="aportes-total-block"):
            render_total_block(total_aportado)

    st.write("")


st.markdown("""
<style>
    .st-key-absolute-section {
        background: linear-gradient(
            180deg,
            rgba(253, 253, 248, 0.96) 0%,
            rgba(245, 243, 235, 0.98) 15%
        );
        border: 0px solid rgba(211, 210, 202, 0.95);
        border-radius: 0.75rem;
        padding: 1.2rem 1.2rem 1rem 1.2rem;
        margin-bottom: 1rem;
    }
</style>
""", unsafe_allow_html=True)

with st.container(border=True):
    st.markdown('<div class="section-title" style="font-size: 32px;">Evolução Temporal</div>', unsafe_allow_html=True)
    st.write("")
    st.write("")

    with st.container(border=False, key="absolute-section"):
        st.markdown('<div class="section-title" style="font-size: 30px;">Absolutos</div>', unsafe_allow_html=True)
        st.write("")
        st.write("")
        abs_left_col, abs_right_col = st.columns([4, 2.5], gap="large")

        with abs_left_col:
            st.plotly_chart(
                build_line_chart(
                    df=df_aportes_linha,
                    title="Evolução Mensal",
                    x_col="mes",
                    y_col="valor_total",
                    series_col="instituicao_fin",
                ),
                width="stretch",
            )

        with abs_right_col:
            st.plotly_chart(
                build_grouped_bar_chart(
                    df=df_aportes_barras,
                    title="Evolução Anual",
                    x_col="ano",
                    y_col="valor_total",
                    series_col="instituicao_fin",
                ),
                width="stretch",
            )
    st.write("")
    st.write("")
