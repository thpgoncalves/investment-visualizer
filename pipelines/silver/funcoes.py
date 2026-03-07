import pandas as pd
import yfinance as yf
from typing import List, Tuple, Optional, Callable


def yfinance_ticker_normalization (raw: str) -> str:
    """
    Normaliza tickers brasileiros:
    - remove espaços (ex.: 'IVVB 11' -> 'IVVB11')
    - upper
    - adiciona '.SA' se não houver sufixo (sem '.')
    """
    s = str(raw).strip().upper().replace(" ", "")
    if s == "":
        return s
    return s if "." in s else f"{s}.SA"

# adaptar pra a regex q sera usada para tratar o campo da tabela
def _normalize_tickers(df: pd.DataFrame) -> List[str]:
    df_normalizer = df.copy() 
    
    normalized_list =  (
        df_normalizer
        .dropna()
        .filter(where tipo = "Ação") # algo do tipo ou adicionar variavel na funcao
        .astype(str)
        .map(yfinance_ticker_normalization) # confirmar
        .loc[lambda s: s.ne("")] # confirmar
        .unique()
        .tolist()
    )

    return normalized_list
    

# ajustar para retornar um df
def _download_closes (tickers: List[str], period: str, interval: str) -> pd.Series:
    data = yf.download(
        tickers=tickers,
        period=period,
        interval=interval,
        group_by="ticker", # confirmar
        auto_adjust=False, # confirmar
        threads=True, # confirmar
        progress=False, # confirmar
    )

    if isinstance(data.columns, pd.Index) and "Close" in data.columns:
        closes = data["Close"].dropna()
        last = closes.iloc[-1] if not closes.empty else float("nan")
        return pd.Series({tickers[0]: last}, name="last_price") # ajustar para retornar um df
    
    if isinstance(data.columns, pd.MultiIndex):
        close_panel = data.xs("Close", axis=1, level=1, drop_level=False) # confirmar
        close_df = close_panel.droplevel(1, axis=1) # confirmar

        last_prices = close_df.apply(
            lambda s: s.dropna().iloc[-1] if s.dropna().size else float("nan") # confirmar
        )
        last_prices.name = "last_price"
        return last_prices # ajustar para retornar um df
    
    return pd.Series({t: float("nan") for t in tickers}, name="last_price") # ajustar para retornar um df

def latest_prices_with_fallback(
    df: pd.DataFrame,
    fallback_chain: Optional[List[Tuple[str, str]]] = None
) -> pd.DataFrame :
    tickers = _normalize_tickers(df)
    if not tickers:
        return pd.DataFrame(columns=["ticker_col", "last_price"]) # verificar como q vai ficar "ticker_col" # confirmar
    
    chain = fallback_chain or [
        ("1d", "1m"),
        ("5d", "5m"),
        ("1mo", "1h"),
        ("3mo", "1d"),
    ]

    prices = pd.Series({t: float("nan") for t in tickers}, name="last_price") # confirmar
    remaining = tickers

    for period, interval in chain:
        if not remaining:
            break

        attempt = _download_closes(remaining, period=period, interval=interval)

        filled_mask = attempt.notna() # confirmar
        prices.loc[attempt.index[filled_mask]] = attempt.loc[filled_mask] # confirmar
        remaining = prices[prices.isna()].index.to_list # confirmar

    prices_df = prices.reset_index # confirmar
    prices_df.columns = ["ticker_col", "last_price"] # verificar como q vai ficar "ticker_col"