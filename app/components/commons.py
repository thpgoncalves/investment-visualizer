from typing import Mapping, Sequence, Literal
import pandas as pd
import streamlit as st

ColumnKind = Literal["text", "currency", "percent", "float", "integer"]

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
    
    st.markdown('<div class="section-title" style="font-size: 24px;">Total</div>', unsafe_allow_html=True)
    st.markdown(
        f'<div class="big-total">{_normalize_label(val)}</div>',
        unsafe_allow_html=True,
    )



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
