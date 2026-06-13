from pathlib import Path
from typing import Literal, Sequence
import pandas as pd
import streamlit as st

ColumnKind = Literal["text", "currency", "percent", "float", "integer"]
APP_DIR = Path(__file__).resolve().parents[1]
TEMPORAL_FILTER_OPTIONS = ("YTD", "6 meses", "12 meses", "24 meses")
TEMPORAL_FILTER_MONTHS = {
    "6 meses": 6,
    "12 meses": 12,
    "24 meses": 24,
}
INVESTMENTS_TABLE_COLUMNS = [
    "nome",
    "qtd",
    "preco_medio",
    "preco_atual",
    "variacao_percentual",
    "valor_total",
]

def format_currency_label(val: str | float | int) -> str:
    numeric_value = float(str(val).replace(",", "."))
    formatted_value = f"{numeric_value:,.2f}"
    formatted_value = (
        formatted_value
        .replace(",", "_")
        .replace(".", ",")
        .replace("_", ".")
    )
    return f"R$ {formatted_value}"


def render_value_block(title: str, val: str | float | int) -> None:
    st.markdown(
        f'<div class="section-title" style="font-size: 24px;">{title}</div>',
        unsafe_allow_html=True,
    )
    st.markdown(
        f'<div class="big-total">{format_currency_label(val)}</div>',
        unsafe_allow_html=True,
    )


def render_total_block(val: str | float | int) -> None:
    render_value_block("Total", val)


def get_dataframe_height(df: pd.DataFrame) -> int:
    return 42 + (len(df) * 35)


def render_temporal_filter(page_key: str) -> str:
    st.markdown(
        """
        <style>
            .temporal-filter-label {
                color: rgba(61, 58, 42, 0.72);
                font-size: 0.78rem;
                font-weight: 600;
                letter-spacing: 0;
                margin-bottom: 0.1rem;
                text-align: left;
            }

            div[class*="st-key-temporal_filter_"] {
                display: flex;
                justify-content: flex-start;
                margin-bottom: 0.3rem;
            }

            div[class*="st-key-temporal_filter_"] [role="radiogroup"] {
                align-items: center;
                gap: 1.05rem;
                justify-content: flex-start;
            }

            div[class*="st-key-temporal_filter_"] label {
                color: rgba(61, 58, 42, 0.82);
                font-size: 0.84rem;
                font-weight: 500;
            }
        </style>
        <div class="temporal-filter-label">Temporalidade</div>
        """,
        unsafe_allow_html=True,
    )

    return st.radio(
        "Temporalidade",
        TEMPORAL_FILTER_OPTIONS,
        index=0,
        horizontal=True,
        key=f"temporal_filter_{page_key}",
        label_visibility="collapsed",
    )


def filter_temporal_window(
    df: pd.DataFrame,
    temporal_filter: str,
    date_col: str = "data_apuracao",
) -> pd.DataFrame:
    if df.empty or date_col not in df.columns:
        return df.copy()

    filtered_df = df.copy()
    filtered_df[date_col] = pd.to_datetime(filtered_df[date_col])
    available_periods = filtered_df[date_col].dt.to_period("M")
    reference_period = available_periods.max()

    if temporal_filter == "YTD":
        start_period = pd.Period(year=reference_period.year, month=1, freq="M")
    else:
        months = TEMPORAL_FILTER_MONTHS.get(temporal_filter, 12)
        start_period = reference_period - (months - 1)

    return (
        filtered_df[
            (available_periods >= start_period)
            & (available_periods <= reference_period)
        ]
        .sort_values(date_col)
        .copy()
    )


def format_compact_number(val: str | float | int, *, max_decimals: int = 8) -> str:
    if pd.isna(val):
        return ""

    numeric_value = float(str(val).replace(",", "."))
    abs_value = abs(numeric_value)

    if abs_value >= 1_000_000_000:
        return f"{numeric_value / 1_000_000_000:.1f}B"

    if abs_value >= 1_000_000:
        return f"{numeric_value / 1_000_000:.1f}M"

    if abs_value >= 1_000:
        return f"{numeric_value / 1_000:.1f}K"

    if abs_value == 0:
        return "0"

    if abs_value >= 1:
        formatted_value = f"{numeric_value:.2f}"
    else:
        formatted_value = f"{numeric_value:.{max_decimals}f}"

    return formatted_value.rstrip("0").rstrip(".")


def format_table_currency(val: str | float | int) -> str:
    if pd.isna(val):
        return ""

    numeric_value = float(str(val).replace(",", "."))

    if numeric_value == 0:
        return "R$ 0.00"

    if abs(numeric_value) >= 1_000:
        return f"R$ {format_compact_number(numeric_value)}"

    if abs(numeric_value) >= 1:
        return f"R$ {numeric_value:.2f}"

    return f"R$ {format_compact_number(numeric_value)}"


def style_variation_cell(val: float | int) -> str:
    if pd.isna(val) or val == 0:
        return ""

    if val > 0:
        return (
            "color: #166534; "
            "background-color: #dcfce7; "
            "font-weight: 600;"
        )

    return (
        "color: #991b1b; "
        "background-color: #fee2e2; "
        "font-weight: 600;"
    )


def build_investments_table_display(df: pd.DataFrame):
    display_df = df[INVESTMENTS_TABLE_COLUMNS].copy()
    display_df["qtd"] = display_df["qtd"].map(format_compact_number)
    display_df["preco_medio"] = display_df["preco_medio"].map(format_table_currency)
    display_df["preco_atual"] = display_df["preco_atual"].map(format_table_currency)
    display_df["variacao_percentual"] = pd.to_numeric(
        display_df["variacao_percentual"],
        errors="coerce",
    )
    display_df["valor_total"] = display_df["valor_total"].map(format_table_currency)

    return (
        display_df.style
        .map(lambda _: "font-weight: 600;", subset=["nome", "valor_total"])
        .map(style_variation_cell, subset=["variacao_percentual"])
    )


def build_aportes_table_display(df: pd.DataFrame) -> pd.DataFrame:
    display_df = df.copy()

    for column in display_df.columns:
        if column == "instituicao_fin":
            continue

        display_df[column] = display_df[column].map(format_table_currency)

    return display_df


def build_investments_table_column_config() -> dict:
    return {
        "nome": st.column_config.TextColumn("Investimento", width="medium"),
        "qtd": st.column_config.TextColumn("Qtd", width="small"),
        "preco_medio": st.column_config.TextColumn("Preço Médio", width="small"),
        "preco_atual": st.column_config.TextColumn("Preço Atual", width="small"),
        "variacao_percentual": st.column_config.NumberColumn(
            "Var. %",
            width="small",
            format="%.2f%%",
        ),
        "valor_total": st.column_config.TextColumn("Valor Total", width="small"),
    }


def build_aportes_table_column_config(columns: Sequence[str]) -> dict:
    column_config = {
        "instituicao_fin": st.column_config.TextColumn("Instituição", width="medium"),
    }

    for column in columns:
        if column == "instituicao_fin":
            continue

        column_label = "Total" if column == "total" else str(column)
        column_config[column] = st.column_config.TextColumn(
            column_label,
            width="small",
        )

    return column_config



def render_navigation_button(val: str | float | int, page: dict) -> None:
    button_key = f"nav_button_{page['scope_value'].lower()}"

    if st.button(f"{page['title']} - {format_currency_label(val)}", key=button_key, width='stretch'):
        st.switch_page(str(APP_DIR / page['page_path']))

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
                margin-top: 0;
            }

            .section-title {
                text-align: center;
                font-size: 1.1rem;
                font-weight: 600;
                margin-top: 0.5rem;
                margin-bottom: 0.5rem;
            }

            .institution-heading {
                display: flex;
                margin: 0.1rem 0 0.75rem 0;
            }

            .institution-name {
                font-size: 1.65rem;
                font-weight: 700;
                line-height: 1;
            }

            div[class*="st-key-nav_button_"] button {
                background: rgba(244, 241, 231, 0.82);
                border: 1px solid rgba(197, 194, 181, 0.95);
                color: rgb(38, 43, 39);
                font-family: inherit;
                font-size: 1.02rem;
                font-weight: 500;
                min-height: 3.05rem;
                border-radius: 999px;
                box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.65);
                transition:
                    background-color 140ms ease,
                    border-color 140ms ease,
                    box-shadow 140ms ease,
                    transform 140ms ease;
            }

            div[class*="st-key-nav_button_"] button:hover {
                background: rgba(232, 226, 211, 0.98);
                border-color: rgba(172, 166, 148, 1);
                color: rgb(31, 36, 32);
                box-shadow:
                    inset 0 1px 0 rgba(255, 255, 255, 0.75),
                    0 2px 8px rgba(50, 45, 34, 0.08);
                transform: translateY(-1px);
            }

            div[class*="st-key-nav_button_"] button:active {
                transform: translateY(0);
                background: rgba(222, 215, 198, 1);
            }

            .small-helper {
                font-size: 0.85rem;
                opacity: 0.8;
            }
        </style>
        """,
        unsafe_allow_html=True,
    )
