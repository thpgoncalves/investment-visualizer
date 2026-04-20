import streamlit as st
import pandas as pd

from app.config.pages import PAGES
from app.components.charts import (
    build_grouped_bar_chart,
    build_line_chart,
    build_pie_chart,
)
from app.components.commons import render_total_block, inject_page_css


inject_page_css()


# Variaveis "Globais"
PAGE_CONFIG = PAGES["page_1"]

PAGE_TITLE = PAGE_CONFIG["title"]
PAGE_SCOPE_TYPE = PAGE_CONFIG["scope_type"]
PAGE_SCOPE_VALUE = PAGE_CONFIG["scope_value"]

selected_yyyymm = st.session_state["selected_yyyymm"]


# adc valor atual da acao no df e montar table baseado nessa q ta montanda mas com as estilizacoes de nome de coluna, R$ e %. 


investimentos_path = f"data/gold/{selected_yyyymm}/{selected_yyyymm}_gold_instituicao_label_snapshot.csv"
df_investimentos = pd.read_csv(investimentos_path)

df_investimentos_filtrado = df_investimentos[(df_investimentos['instituicao_fin'] == PAGE_SCOPE_VALUE) & (df_investimentos['nome'] != 'ALL')]
page_total = df_investimentos.loc[((df_investimentos['instituicao_fin'] == PAGE_SCOPE_VALUE) & (df_investimentos['nome'] == 'ALL')), 'valor_total'].item()

pizza_tipo_path = f"data/gold/{selected_yyyymm}/{selected_yyyymm}_gold_pizza_tipo_snapshot.csv"
df_pizza_tipo = pd.read_csv(pizza_tipo_path)

df_pizza_tipo_filtrado = df_pizza_tipo[df_pizza_tipo['instituicao_fin'] == PAGE_SCOPE_VALUE]

pizza_expo_path = f"data/gold/{selected_yyyymm}/{selected_yyyymm}_gold_pizza_expo_snapshot.csv"
df_pizza_expo = pd.read_csv(pizza_expo_path)

df_pizza_expo_filtrado = df_pizza_expo[df_pizza_expo['instituicao_fin'] == PAGE_SCOPE_VALUE]

instituicao_linha_path = f"data/gold/{selected_yyyymm}/{selected_yyyymm}_gold_instituicao_linha_snapshot.csv"
df_instituicao_linha = pd.read_csv(instituicao_linha_path)

df_instituicao_linha_filtrado = df_instituicao_linha[df_instituicao_linha['instituicao_fin'] == PAGE_SCOPE_VALUE]

home_barra_path = f"data/gold/{selected_yyyymm}/{selected_yyyymm}_gold_instituicao_linha_snapshot.csv"
df_home_barra = pd.read_csv(home_barra_path)

df_home_barra_filtrado = df_home_barra[df_home_barra['instituicao_fin'] == PAGE_SCOPE_VALUE]

st.title(f"{PAGE_TITLE}")

with st.container(border=True):
    left_col, center_col, right_col = st.columns([3.8, 1.7, 3.5], gap="large")
    
    with left_col:
        st.markdown('<div class="section-title" style="font-size: 24px;">Investimentos</div>', unsafe_allow_html=True)

        st.dataframe(df_investimentos_filtrado[['nome', 'qtd', 'preco_medio', 'preco_atual', 'variacao_percentual', 'valor_total',]], hide_index=True)

    with center_col:
        render_total_block(page_total)


    with right_col:
        pie_col_1, pie_col_2 = st.columns(2, gap="medium")

        with pie_col_1:
            st.plotly_chart(
                build_pie_chart(
                    df =df_pizza_tipo_filtrado, 
                    title="Distribuição Investimentos (Tipo)",
                    label_col='tipo',
                    value_col='valor_total'
                ),
                width='stretch',
            )

        with pie_col_2:
            st.plotly_chart(
                build_pie_chart(
                    df=df_pizza_expo_filtrado,
                    title="Distribuição Investimentos (Exposicao)",
                    label_col='exposicao',
                    value_col='valor_total'
                ),
                width='stretch',
            )
    st.write("")

#variacoes

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
            
    .st-key-percentage-section {
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

# -------------------------------------------------------------------
# SEÇÃO DE ABSOLUTOS
# -------------------------------------------------------------------
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
                    df=df_instituicao_linha_filtrado,
                    title="Evolução Mensal",
                    x_col='mes',
                    y_col='valor_total',
                    series_col='nome'
                ),
                width='stretch',
            )

        # avaliar como realmente ficaria essa parte aqui pra calculo de ano e ytd
        with abs_right_col:
            st.plotly_chart(
                build_grouped_bar_chart(
                    df=df_home_barra_filtrado,
                    title="Evolução Anual",
                    x_col='ano', 
                    y_col='valor_total',
                    series_col='nome'
                ),
                width='stretch',
            )
    st.write("")
    st.write("")


# -------------------------------------------------------------------
# SEÇÃO DE VARIAÇÕES PERCENTUAIS
# -------------------------------------------------------------------
    with st.container(border=False, key="percentage-section"):
        st.markdown('<div class="section-title" style="font-size: 30px;">Variações Percentuais</div>', unsafe_allow_html=True)
        st.write("")
        st.write("")
        pct_left_col, pct_right_col = st.columns([4, 2.5], gap="large")

        with pct_left_col:
            st.plotly_chart(
                build_line_chart(
                    df=df_instituicao_linha_filtrado,
                    title="Evolução Mensal",
                    x_col='mes',
                    y_col='variacao_percentual',
                    series_col='nome',
                    percentual=True
                ),
                width='stretch',
            )

        # avaliar como realmente ficaria essa parte aqui pra calculo de ano e ytd
        with pct_right_col:
            st.plotly_chart(
                build_grouped_bar_chart(
                    df=df_home_barra_filtrado,
                    title="Evolução Anual",
                    x_col='ano', 
                    y_col='variacao_percentual',
                    series_col='nome',
                    percentual=True
                ),
                width='stretch',
            )

# -------------------------------------------------------------------
# st.page_link cria um link para outra página do app.
# Aqui estamos usando para voltar para a home.
# -------------------------------------------------------------------
st.page_link(
    "pages/home.py",
    label="Voltar para Principal",
    icon=":material/arrow_back:",
)


st.write("")
