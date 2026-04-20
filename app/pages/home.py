import streamlit as st
import pandas as pd

from app.config.pages import PAGES
from app.components.charts import (
    build_grouped_bar_chart,
    build_line_chart,
    build_pie_chart,
)
from app.components.commons import (
    inject_page_css,
    render_navigation_button,
    render_total_block
)


# -------------------------------------------------------------------
# Esta função injeta CSS leve para melhorar a aparência do esqueleto.
#
# Por que usar CSS aqui?
# - porque o Streamlit resolve boa parte do layout;
# - mas alguns ajustes simples ajudam a aproximar do desenho;
# - como ainda é protótipo, estamos só refinando o visual básico.
# -------------------------------------------------------------------

# -------------------------------------------------------------------
# CSS leve da página.
# -------------------------------------------------------------------
inject_page_css()

# carregando variavel global selecionada
selected_yyyymm = st.session_state["selected_yyyymm"] 

home_botao_path = f"data/gold/{selected_yyyymm}/{selected_yyyymm}_gold_home_botoes_snapshot.csv"
pizza_tipo_path = f"data/gold/{selected_yyyymm}/{selected_yyyymm}_gold_pizza_tipo_snapshot.csv"
pizza_expo_path = f"data/gold/{selected_yyyymm}/{selected_yyyymm}_gold_pizza_expo_snapshot.csv"
home_linha_path = f"data/gold/{selected_yyyymm}/{selected_yyyymm}_gold_home_linha_snapshot.csv" 
home_barra_path = f"data/gold/{selected_yyyymm}/{selected_yyyymm}_gold_home_barras_snapshot.csv" 

df_botoes = pd.read_csv(home_botao_path, sep=',')

df_pizza_tipo = pd.read_csv(pizza_tipo_path, sep=',')
df_pizza_tipo = df_pizza_tipo[df_pizza_tipo['instituicao_fin'] == 'ALL']

df_pizza_expo = pd.read_csv(pizza_expo_path, sep=',')
df_pizza_expo = df_pizza_expo[df_pizza_expo['instituicao_fin'] == 'ALL']

df_home_linha = pd.read_csv(home_linha_path, sep=',')

df_home_barra = pd.read_csv(home_barra_path, sep=',')

page_home = PAGES['home']
page_1    = PAGES['page_1']
page_2    = PAGES['page_2']
page_3    = PAGES['page_3']
# page_4    = PAGES['page_4']


# jogar tratamento dentro da funcao para adicionar R$ . para separacao e , nos decimais.
val_atual_home = df_botoes.loc[df_botoes['instituicao_fin'] == page_home['scope_value'], 'valor_total'].item()
val_atual_page_1 = df_botoes.loc[df_botoes['instituicao_fin'] == page_1['scope_value'], 'valor_total'].item() 
val_atual_page_2 = df_botoes.loc[df_botoes['instituicao_fin'] == page_2['scope_value'], 'valor_total'].item()
val_atual_page_3 = df_botoes.loc[df_botoes['instituicao_fin'] == page_3['scope_value'], 'valor_total'].item()
# val_atual_page_4 = df_botoes.loc[df_botoes['instituicao_fin'] == page_4['scope_value'], 'valor_total'].item()

# -------------------------------------------------------------------
# PRIMEIRA LINHA DA TELA
#
# Vamos dividir em 3 áreas:
# - esquerda: botões das instituições
# - centro: total consolidado
# - direita: 2 gráficos de pizza
#
# Os números das columns são proporcionais, não são pixels.
# -------------------------------------------------------------------
with st.container(border=True):
    left_col, center_col, right_col = st.columns([1, 1, 2], gap="large")
    
    with left_col:
        st.markdown('<div class="section-title" style="font-size: 24px;">Instituições Financeiras</div>', unsafe_allow_html=True)
        # st.caption("Atalhos de navegação para páginas reais placeholder")

        st.write("")
        render_navigation_button(val_atual_page_1, page_1)
        st.write("")
        render_navigation_button(val_atual_page_2, page_2) 
        st.write("")
        render_navigation_button(val_atual_page_3, page_3) 
        st.write("")
        #render_navigation_button(val_atual_page_4, page_4) 

    with center_col:
        render_total_block(val_atual_home)

    with right_col:
        pie_col_1, pie_col_2 = st.columns(2, gap="medium")

        with pie_col_1:
            st.plotly_chart(
                build_pie_chart(
                    df =df_pizza_tipo, 
                    title="Distribuição Investimentos (Tipo)",
                    label_col='tipo',
                    value_col='valor_total'
                ),
                width='stretch',
            )

        with pie_col_2:
            st.plotly_chart(
                build_pie_chart(
                    df=df_pizza_expo,
                    title="Distribuição Investimentos (Exposicao)",
                    label_col='exposicao',
                    value_col='valor_total'
                ),
                width='stretch',
            )
    st.write("")


# -------------------------------------------------------------------
# ESTILO VISUAL DAS SUBSEÇÕES INTERNAS
# Estratégia: contraste suave de fundo entre "Absolutos" e
# "Variações Percentuais", mantendo tudo no mesmo container principal.
# -------------------------------------------------------------------
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
#
# Estrutura:
# - gráfico de linha mais largo à esquerda
# - gráfico de barras à direita
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
                    df=df_home_linha,
                    title="Evolução Mensal",
                    x_col='mes',
                    y_col='valor_total',
                    series_col='instituicao_fin'
                ),
                width='stretch',
            )

        # avaliar como realmente ficaria essa parte aqui pra calculo de ano e ytd
        with abs_right_col:
            st.plotly_chart(
                build_grouped_bar_chart(
                    df=df_home_barra,
                    title="Evolução Anual",
                    x_col='ano', 
                    y_col='valor_total',
                    series_col='instituicao_fin'
                ),
                width='stretch',
            )
    st.write("")
    st.write("")


# -------------------------------------------------------------------
# SEÇÃO DE VARIAÇÕES PERCENTUAIS
#
# Mesma ideia estrutural da seção acima.
# Isso é bom porque mantém consistência visual.
# -------------------------------------------------------------------
    with st.container(border=False, key="percentage-section"):
        st.markdown('<div class="section-title" style="font-size: 30px;">Variações Percentuais</div>', unsafe_allow_html=True)
        st.write("")
        st.write("")
        pct_left_col, pct_right_col = st.columns([4, 2.5], gap="large")

        with pct_left_col:
            st.plotly_chart(
                build_line_chart(
                    df=df_home_linha,
                    title="Evolução Mensal",
                    x_col='mes',
                    y_col='variacao_percentual',
                    series_col='instituicao_fin',
                    percentual=True
                ),
                width='stretch',
            )

        # avaliar como realmente ficaria essa parte aqui pra calculo de ano e ytd
        with pct_right_col:
            st.plotly_chart(
                build_grouped_bar_chart(
                    df=df_home_barra,
                    title="Evolução Anual",
                    x_col='ano', 
                    y_col='variacao_percentual',
                    series_col='instituicao_fin',
                    percentual=True
                ),
                width='stretch',
            )