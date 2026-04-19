from typing import Mapping, Sequence, Literal
import pandas as pd
import streamlit as st

ColumnKind = Literal["text", "currency", "percent", "float", "integer"]


def build_column_config(
    column_labels: Mapping[str, str],
    column_kinds: Mapping[str, ColumnKind],
) -> dict:
    def resolve_column_config(column_name: str):
        label = column_labels.get(column_name, column_name)
        kind = column_kinds.get(column_name, "text")

        if kind == "currency":
            return st.column_config.NumberColumn(
                label=label,
                format="R$ %.2f",
            )

        if kind == "percent":
            return st.column_config.NumberColumn(
                label=label,
                format="%.2f%%",
            )

        if kind == "float":
            return st.column_config.NumberColumn(
                label=label,
                format="%.2f",
            )

        if kind == "integer":
            return st.column_config.NumberColumn(
                label=label,
                format="%d",
            )

        return st.column_config.TextColumn(label=label)

    return {
        column_name: resolve_column_config(column_name)
        for column_name in column_labels.keys()
    }


def prepare_table_dataframe(
    df: pd.DataFrame,
    column_order: Sequence[str],
) -> pd.DataFrame:
    available_columns = [column for column in column_order if column in df.columns]
    return df.loc[:, available_columns].copy()


def calculate_table_height(
    row_count: int,
    base_height: int = 56,
    row_height: int = 35,
    max_height: int = 700,
) -> int:
    return min(base_height + (row_count * row_height), max_height)


def render_investments_table(
    df: pd.DataFrame,
    title: str = "Posição dos investimentos",
    subtitle: str | None = None,
    column_order: Sequence[str] = (
        "investment_name",
        "entry_price",
        "current_value",
        "variation_pct",
    ),
    column_labels: Mapping[str, str] | None = None,
    column_kinds: Mapping[str, ColumnKind] | None = None,
) -> None:
    resolved_labels = dict(
        column_labels
        or {
            "investment_name": "Investimento",
            "entry_price": "Preço entrada",
            "current_value": "Valor Atual",
            "variation_pct": "Var. %",
        }
    )

    resolved_kinds = dict(
        column_kinds
        or {
            "investment_name": "text",
            "entry_price": "currency",
            "current_value": "currency",
            "variation_pct": "percent",
        }
    )

    table_df = prepare_table_dataframe(
        df=df,
        column_order=column_order,
    )

    if table_df.empty:
        st.info("Nenhum investimento encontrado.")
        return

    st.subheader(title)

    if subtitle:
        st.caption(subtitle)
    else:
        st.caption(f"{len(table_df)} investimento(s) encontrado(s)")

    st.dataframe(
        table_df,
        hide_index=True,
        use_container_width=True,
        height=calculate_table_height(len(table_df)),
        column_config=build_column_config(
            column_labels=resolved_labels,
            column_kinds=resolved_kinds,
        ),
    )