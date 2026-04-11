from pathlib import Path

import streamlit as st
from app.config.pages import PAGES

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
PROJECT_ROOT = APP_DIR.parent
GOLD_DIR = PROJECT_ROOT / "data" / "gold"


def is_valid_month_partition(path: Path) -> bool:
    return path.is_dir() and path.name.isdigit() and len(path.name) == 6


def list_available_months(base_dir: Path) -> list[str]:
    if not base_dir.exists():
        return []

    return sorted(
        [
            path.name
            for path in base_dir.iterdir()
            if is_valid_month_partition(path)
        ],
        reverse=True,
    )


def format_month_label(month_id: str) -> str:
    year = month_id[:4]
    month = month_id[4:6]
    return f"{month}/{year}"


def build_month_dir(month_id: str) -> Path:
    return GOLD_DIR / month_id


def initialize_month_state() -> None:
    available_months = list_available_months(GOLD_DIR)
    st.session_state["available_months"] = available_months

    if not available_months:
        st.session_state["selected_yyyymm"] = None
        st.session_state["selected_gold_month_dir"] = None
        return

    current_selected_month = st.session_state.get("selected_yyyymm")

    if current_selected_month not in available_months:
        st.session_state["selected_yyyymm"] = available_months[0]

    update_month_context()


def update_month_context() -> None:
    selected_month = st.session_state.get("selected_yyyymm")

    if not selected_month:
        st.session_state["selected_gold_month_dir"] = None
        return

    st.session_state["selected_gold_month_dir"] = build_month_dir(selected_month)


# -------------------------------------------------------------------
# Inicializa o estado antes dos widgets.
# -------------------------------------------------------------------
initialize_month_state()


page_home = PAGES["home"]
page_1 = PAGES["page_1"]
page_2 = PAGES["page_2"]
page_3 = PAGES["page_3"]
# page_4 = PAGES['page_4']

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
    str(APP_DIR / page_home["page_path"]),
    title="Principal",
    icon=":material/home:",
    default=True,
)

xp_page = st.Page(
    str(APP_DIR / page_1["page_path"]),
    title="XP",
    icon=":material/account_balance:",
)

nubank_page = st.Page(
    str(APP_DIR / page_2["page_path"]),
    title="Nubank",
    icon=":material/account_balance_wallet:",
)

clear_page = st.Page(
    str(APP_DIR / page_3["page_path"]),
    title="Clear",
    icon=":material/show_chart:",
)

# binance_page = st.Page(
#     str(APP_DIR / page_4['page_path']),
#     title="Binance",
#     icon=":material/currency_bitcoin:",
# )

# -------------------------------------------------------------------
# st.navigation cria a navegação multipage.
# -------------------------------------------------------------------
current_page = st.navigation(
    [
        home_page,
        xp_page,
        nubank_page,
        clear_page,
        # binance_page,
    ],
    position="top",
    expanded=True,
)


# -------------------------------------------------------------------
# Linha de controle global logo abaixo da navegação.
# Não fica "dentro" do menu nativo do Streamlit, mas fica no topo
# do app e serve como filtro global compartilhado entre páginas.
# -------------------------------------------------------------------
available_months = st.session_state["available_months"]

if available_months:
    control_col_left, control_col_right = st.columns([8, 2])

    with control_col_right:
        st.selectbox(
            "Mês de referência",
            options=available_months,
            format_func=format_month_label,
            key="selected_yyyymm",
            help="Por padrão, a aplicação abre no mês mais recente disponível.",
        )

    update_month_context()
else:
    st.info("Nenhuma partição mensal foi encontrada em data/gold.")


# -------------------------------------------------------------------
# Executa a página atual.
# -------------------------------------------------------------------
current_page.run()


###################
### pra ler dps ###
###################

# from pathlib import Path
# import streamlit as st
# import pandas as pd

# selected_yyyymm = st.session_state["selected_yyyymm"]
# gold_month_dir = Path(st.session_state["selected_gold_month_dir"])

# df_summary = pd.read_csv(gold_month_dir / f"{selected_yyyymm}_gold_summary.csv")
# df_xp = pd.read_csv(gold_month_dir / f"{selected_yyyymm}_gold_xp.csv")