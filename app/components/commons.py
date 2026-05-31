from pathlib import Path
from typing import Mapping, Sequence, Literal
import pandas as pd
import streamlit as st

ColumnKind = Literal["text", "currency", "percent", "float", "integer"]
APP_DIR = Path(__file__).resolve().parents[1]

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

    button_key = f"nav_button_{page['scope_value'].lower()}"

    if st.button(_normalize_label(val), key=button_key, width='stretch'):
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
                margin-top: 3rem;
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
                font-size: 0.96rem;
                font-weight: 500;
                min-height: 2.55rem;
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
