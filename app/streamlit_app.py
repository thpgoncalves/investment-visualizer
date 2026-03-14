from pathlib import Path

import streamlit as st


# -------------------------------------------------------------------
# Este arquivo é o "entrypoint" da aplicação.
# É ele que você vai rodar com:
# streamlit run app/streamlit_app.py
#
# Como estamos usando multipage real com st.Page + st.navigation,
# este arquivo funciona como o roteador principal da aplicação.
# -------------------------------------------------------------------


# -------------------------------------------------------------------
# set_page_config:
# - define configurações globais da janela/página;
# - layout="wide" é importante porque o teu desenho é horizontal;
# - precisa ser chamado no começo do app.
# -------------------------------------------------------------------
st.set_page_config(
    page_title="Investment Visualizer",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed",
)


# -------------------------------------------------------------------
# Aqui pegamos o diretório atual do arquivo.
# Por que fazer isso?
# - para montar caminhos de páginas de forma mais segura;
# - evita ficar espalhando strings soltas por todo o código.
# -------------------------------------------------------------------
APP_DIR = Path(__file__).resolve().parent
PAGES_DIR = APP_DIR / "pages"


# -------------------------------------------------------------------
# Cada st.Page representa uma página real da aplicação.
#
# Observações:
# - path: caminho do arquivo Python da página;
# - title: nome exibido na navegação;
# - icon: ícone da navegação;
# - default=True: define a página inicial.
# -------------------------------------------------------------------
home_page = st.Page(
    str(PAGES_DIR / "home.py"),
    title="Principal",
    icon=":material/home:",
    default=True,
)

xp_page = st.Page(
    str(PAGES_DIR / "page_xp.py"),
    title="XP",
    icon=":material/account_balance:",
)

nubank_page = st.Page(
    str(PAGES_DIR / "page_nubank.py"),
    title="Nubank",
    icon=":material/account_balance_wallet:",
)

clear_page = st.Page(
    str(PAGES_DIR / "page_clear.py"),
    title="Clear",
    icon=":material/show_chart:",
)

binance_page = st.Page(
    str(PAGES_DIR / "page_binance.py"),
    title="Binance",
    icon=":material/currency_bitcoin:",
)


# -------------------------------------------------------------------
# st.navigation cria a navegação multipage.
#
# position="top":
# - coloca a navegação no topo da tela, que é exatamente o que você quer.
#
# expanded=True:
# - tenta deixar a navegação expandida quando aplicável.
#
# O retorno é a página atualmente selecionada.
# Depois precisamos chamar .run() nela.
# -------------------------------------------------------------------
current_page = st.navigation(
    [
        home_page,
        xp_page,
        nubank_page,
        clear_page,
        binance_page,
    ],
    position="top",
    expanded=True,
)


# -------------------------------------------------------------------
# Executa a página atual.
# Sem isso, a navegação existiria, mas a página selecionada não renderizaria.
# -------------------------------------------------------------------
current_page.run()