import streamlit as st

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
def render_navigation_button(label: str, page_path: str) -> None:
    if st.button(label, use_container_width=True):
        st.switch_page(page_path)


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
def render_total_block(total_value_label: str) -> None:
    st.markdown("#### Total")
    st.markdown(
        f'<div class="big-total">{total_value_label}</div>',
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
left_col, center_col, right_col = st.columns([1.5, 1.0, 1.8], gap="large")

with left_col:
    with st.container(border=True):
        st.markdown("**Instituições**")
        st.caption("Atalhos de navegação para páginas reais placeholder")

        render_navigation_button("XP - R$ XX.XXX,XX", "pages/page_xp.py") # adicionar funcao q traz o total pra trazer nesse formato ai de string formatada
        render_navigation_button("Nubank - R$ XX.XXX,XX", "pages/page_nubank.py")  # adicionar funcao q traz o total pra trazer nesse formato ai de string formatada
        render_navigation_button("Clear - R$ XX.XXX,XX", "pages/page_clear.py")  # adicionar funcao q traz o total pra trazer nesse formato ai de string formatada
        render_navigation_button("Binance - R$ XX.XXX,XX", "pages/page_binance.py")  # adicionar funcao q traz o total pra trazer nesse formato ai de string formatada

with center_col:
    with st.container(border=True, height=280):
        render_total_block(total_value_label)

with right_col:
    pie_col_1, pie_col_2 = st.columns(2, gap="medium")

    with pie_col_1:
        with st.container(border=True):
            st.plotly_chart(
                build_pie_chart(
                    title="Distribuição Investimento (Tipo)",
                    labels=None if allocation_by_type is None else allocation_by_type["tipo"], # trocar nome das variaveis
                    values=None if allocation_by_type is None else allocation_by_type["preco_atual"], # trocar nome das variaveis
                ),
                use_container_width=True,
            )

    with pie_col_2:
        with st.container(border=True):
            st.plotly_chart(
                build_pie_chart(
                    title="Distribuição Investimentos (Moeda)",
                    labels=None if allocation_by_currency is None else allocation_by_currency["exposicao"],  # trocar nome das variaveis
                    values=None if allocation_by_currency is None else allocation_by_currency["preco_atual"], # trocar nome das variaveis
                ),
                use_container_width=True,
            )


st.write("")


# -------------------------------------------------------------------
# SEÇÃO DE ABSOLUTOS
#
# Estrutura:
# - gráfico de linha mais largo à esquerda
# - gráfico de barras à direita
# -------------------------------------------------------------------
st.markdown('<div class="section-title">Absolutos</div>', unsafe_allow_html=True)

abs_left_col, abs_right_col = st.columns([2.2, 1.0], gap="large")

with abs_left_col:
    with st.container(border=True):
        st.plotly_chart(
            build_line_chart(
                title="Evolução Mensal",
                x_col=None if absolute_history is None else absolute_history["mes"], # trocar nome das variaveis
                y_col=None if absolute_history is None else absolute_history["preco_atual"], # trocar nome das variaveis
                series_col=None if absolute_history is None else absolute_history["instituicao_fin"], # trocar nome das variaveis
                y_axis_title="R$",
            ),
            use_container_width=True,
        )

# avaliar como realmente ficaria essa parte aqui pra calculo de ano e ytd
with abs_right_col:
    with st.container(border=True):
        st.plotly_chart(
            build_grouped_bar_chart(
                title="2024 x 2025 x YTD",
                categories=None if absolute_comparison is None else absolute_comparison["categories"], 
                series=None if absolute_comparison is None else absolute_comparison["series"],
                y_axis_title="R$",
            ),
            use_container_width=True,
        )


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
    with st.container(border=True):
        st.plotly_chart(
            build_line_chart(
                title="Variação Mensal (%)",
                x_col=None if percentage_history is None else percentage_history["mes"], # trocar nome das variaveis
                y_col=None if percentage_history is None else percentage_history["preco_atual"], # trocar nome das variaveis
                series_col=None if percentage_history is None else percentage_history["instituicao_fin"], # trocar nome das variaveis
                y_axis_title="%",
            ),
            use_container_width=True,
        )

# avaliar como realmente ficaria essa parte aqui pra calculo de ano e ytd
with pct_right_col:
    with st.container(border=True):
        st.plotly_chart(
            build_grouped_bar_chart(
                title="Comparação Percentual",
                categories=None if percentage_comparison is None else percentage_comparison["categories"],
                series=None if percentage_comparison is None else percentage_comparison["series"],
                y_axis_title="%",
            ),
            use_container_width=True,
        )
