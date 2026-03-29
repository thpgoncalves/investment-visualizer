import streamlit as st
import pandas as pd
from app.config.pages import PAGES

from app.components.charts import (
    build_grouped_bar_chart,
    build_line_chart,
    build_pie_chart,
)


# -------------------------------------------------------------------
# Esta função injeta CSS leve para melhorar a aparência do esqueleto.
#
# Por que usar CSS aqui?
# - porque o Streamlit resolve boa parte do layout;
# - mas alguns ajustes simples ajudam a aproximar do desenho;
# - como ainda é protótipo, estamos só refinando o visual básico.
# -------------------------------------------------------------------
def inject_page_css() -> None:
    st.markdown(
        """
        <style>
            .block-container {
                padding-top: 1rem;
                padding-bottom: 2rem;
            }

            .page-title {
                text-align: right;
                font-size: 0.95rem;
                opacity: 0.9;
                margin-bottom: 0.5rem;
            }

            .big-total {
                font-size: 2rem;
                font-weight: 700;
                text-align: center;
                margin-top: 3rem;
            }

            .section-title {
                text-align: center;
                font-size: 1.1rem;
                font-weight: 600;
                margin-top: 0.5rem;
                margin-bottom: 0.5rem;
            }

            .small-helper {
                font-size: 0.85rem;
                opacity: 0.8;
            }
        </style>
        """,
        unsafe_allow_html=True,
    )


# -------------------------------------------------------------------
# Esta função centraliza a lógica do botão que navega para outra página.
#
# Por que colocar isso em função?
# - porque o comportamento se repete;
# - porque assim o código da home fica mais legível;
# - porque, se você quiser trocar a forma de navegação depois,
#   muda num lugar só.
#
# Argumentos:
# - label:
#   texto do botão.
#
# - page_path:
#   caminho da página para onde o botão vai levar.
#
# Observação:
# - st.switch_page só funciona com páginas reconhecidas pelo app multipage.
# - como essas páginas placeholder existem de verdade, a navegação é real.
# -------------------------------------------------------------------
def render_navigation_button(val: str | float | int, page: dict) -> None:
    def _normalize_label(val: str | float | int) -> str:
        numeric_value = float(str(val).replace(",", "."))
        formatted_value = f"{numeric_value:,.2f}"
        formatted_value = (
            formatted_value
            .replace(",", "_")
            .replace(".", ",")
            .replace("_", ".")
        )
        return f"{page['title']} - R$ {formatted_value}"

    if st.button(_normalize_label(val), width='stretch'):
        st.switch_page(page['page_path'])

# -------------------------------------------------------------------
# Esta função renderiza o bloco do total.
#
# Argumento:
# - total_value_label:
#   string pronta para exibição.
#
# Por que string e não float neste momento?
# - porque você pediu esqueleto;
# - então ainda não estamos nos preocupando com leitura nem formatação real.
# -------------------------------------------------------------------
def render_total_block(val: str | float | int) -> None:
    def _normalize_label(val: str | float | int) -> str:
        numeric_value = float(str(val).replace(",", "."))
        formatted_value = f"{numeric_value:,.2f}"
        formatted_value = (
            formatted_value
            .replace(",", "_")
            .replace(".", ",")
            .replace("_", ".")
        )
        return f"R$ {formatted_value}"
    
    st.markdown("#### Total")
    st.markdown(
        f'<div class="big-total">{_normalize_label(val)}</div>',
        unsafe_allow_html=True,
    )


# -------------------------------------------------------------------
# CSS leve da página.
# -------------------------------------------------------------------
inject_page_css()


# -------------------------------------------------------------------
# Estas variáveis representam os lugares onde os dados reais entrarão depois.
#
# A ideia aqui é:
# - deixar explícito onde vai entrar a leitura do teu arquivo;
# - mas sem implementar a leitura agora.
#
# Depois você pode preencher isso com dados vindos de:
# - parquet
# - csv
# - json
# - output do pipeline em data/gold/metrics
# -------------------------------------------------------------------
portfolio_snapshot = None

allocation_by_type = None
allocation_by_currency = None

absolute_history = None # grafico de linhas
absolute_comparison = None # grafico de barra

percentage_history = None # grafico de linhas
percentage_comparison = None # grafico de barra

total_value_label = "R$ XX.XXX,XX" #subistituir por um soma ou algo do tipo, criar funcao para somar e pegar total e se possivel tbm pegar ytd

home_botao_path = "data/gold/202603/202603_gold_home_botoes_snapshot.csv"
pizza_tipo_path = "data/gold/202603/202603_gold_pizza_tipo_snapshot.csv"
pizza_expo_path = "data/gold/202603/202603_gold_pizza_expo_snapshot.csv"
home_linha_path = "data/gold/202603/202603_gold_home_linha_snapshot.csv" 
home_barra_path = "data/gold/202603/202603_gold_home_barras_snapshot.csv" 

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
# Título da página.
# -------------------------------------------------------------------
st.markdown('<div class="page-title">Janela 1 (Principal)</div>', unsafe_allow_html=True)


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
        st.markdown("**Instituições Financeiras**")
        # st.caption("Atalhos de navegação para páginas reais placeholder")

        render_navigation_button(val_atual_page_1, page_1)
        render_navigation_button(val_atual_page_2, page_2) 
        render_navigation_button(val_atual_page_3, page_3) 
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
# SEÇÃO DE ABSOLUTOS
#
# Estrutura:
# - gráfico de linha mais largo à esquerda
# - gráfico de barras à direita
# -------------------------------------------------------------------
with st.container(border=True):
    st.markdown('<div class="section-title">Absolutos</div>', unsafe_allow_html=True)
    abs_left_col, abs_right_col = st.columns([2.2, 1.0], gap="large")
    
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
    st.markdown('<div class="section-title">Variações Percentuais</div>', unsafe_allow_html=True)

    pct_left_col, pct_right_col = st.columns([2.2, 1.0], gap="large")

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
