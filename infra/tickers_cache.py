from datetime import date, datetime, timedelta
from pathlib import Path
import logging

import yfinance as yf
import pandas as pd

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql import types as T
from pyspark.sql.dataframe import DataFrame

logger = logging.getLogger(__name__)

def get_tickers_price(df: DataFrame, lookback_days: int = 7) -> DataFrame:
    """
    - Recebe DF Silver (granularidade por investimento) com colunas: tipo, nome (ticker)
    - Filtra tipo == "stock"
    - Coleta tickers distintos para o driver (assumindo poucos tickers)
    - Para Yahoo Finance: adiciona sufixo ".SA" (B3) quando não houver sufixo
      Ex: "BERK34" -> "BERK34.SA"
    - Busca no Yahoo o preço de fechamento mais recente disponível dentro do range:
        cutoff = today - 1
        start = cutoff - lookback_days
        end   = cutoff + 1
    - Retorna um DF Spark com: (ticker, price_date, close, extracted_at)
      Onde ticker é o ticker original (B3, sem sufixo), e o Yahoo é usado internamente.
    """
    
    spark = df.sparkSession

    cutoff = date.today() - timedelta(days=1)
    start = cutoff - timedelta(days=lookback_days)
    
    df_filtrado = (df
                   .filter(F.col('tipo') == F.lit("stock"))
                   .select(F.col('nome').alias('ticker'))
                   .filter(F.col('ticker').isNotNull() & (F.col('ticker') != ''))
                   .distinct()
    )

    schema = T.StructType([
            T.StructField("ticker", T.StringType(), False),
            T.StructField("data_preco", T.DateType(), False),
            T.StructField("close", T.DoubleType(), True),
            T.StructField("extracted_at", T.TimestampType(), False)
        ])

    tickers_list = [r['ticker'] for r in df_filtrado.collect()]
    if not tickers_list:
        return spark.createDataFrame([], schema)
    
    def _to_yahoo_ticker(ticker:str) -> str:
        if "." in ticker:
            return ticker
        
        return f"{ticker}.SA"
    
    yahoo_map = {_to_yahoo_ticker(t): t for t in tickers_list}
    yahoo_ticker_list = list(yahoo_map.keys())

    data = yf.download(
        tickers=yahoo_ticker_list,
        start=start,
        end=cutoff,
        interval="1d",
        auto_adjust=True,
        threads=False
    )

    extracted_at = datetime.now()

    # Função interna: extrai a série de "Close" para um ticker, lidando com retorno 1-ticker vs multi-ticker
    def _close_series_for_ticker(yahoo_t: str):
        """
        Retorna um pandas.DataFrame com colunas:
        date | ticker | close
        """
        try:
            if len(yahoo_ticker_list) == 1:
                close_s = data['Close']
            else:
                close_s = data[yahoo_t]["Close"]

            
            df_close = close_s.reset_index()
            df_close.columns = ["data_preco", "close"]
            df_close["ticker"] = yahoo_t
            df_close["extracted_at"] = extracted_at

            return df_close[["data_preco", "ticker", "close", "extracted_at"]]
        except Exception:
            return None
        
    dfs = []
    for yahoo_t in yahoo_ticker_list:
        df_ticker = _close_series_for_ticker(yahoo_t)

        if df_ticker is not None:
            dfs.append(df_ticker)

    if not dfs:
        return spark.createDataFrame([], schema)

    df_concat = pd.concat(dfs, ignore_index=True)

    # voltando os tickers ao nome original
    df_concat["ticker"] = df_concat["ticker"].map(yahoo_map)

    def build_latest_prices_spark_df(df, spark, schema) -> DataFrame:
        df = df.copy()

        # garante tipos compatíveis no pandas antes da conversão
        df["data_preco"] = pd.to_datetime(df["data_preco"], errors="coerce").dt.date
        df["extracted_at"] = pd.to_datetime(df["extracted_at"], errors="coerce")
        df["ticker"] = df["ticker"].astype("string")
        df["close"] = pd.to_numeric(df["close"], errors="coerce")

        spark_df = spark.createDataFrame(df, schema)
        spark_df.createOrReplaceTempView("ticker_prices_raw")

        df_final = spark.sql("""
            WITH 
                ranked as (
                    SELECT 
                        ticker,
                        data_preco,
                        close,
                        extracted_at,
                        ROW_NUMBER() OVER(PARTITION BY ticker ORDER BY data_preco DESC) AS rn
                    FROM ticker_prices_raw
                    WHERE close IS NOT NULL AND close > 0
                )
            SELECT 
                ticker,
                data_preco,
                close,
                extracted_at 
            FROM ranked WHERE rn = 1
        """)

        return df_final
    
    df_final = build_latest_prices_spark_df(df_concat, spark, schema)

    return df_final

def update_tickers_cache(df_prices: DataFrame, cache_dir: str | None = None) -> DataFrame:
    """
    - Recebe DF (ticker, price_date, close, extracted_at)
    - Salva/atualiza cache em: <raiz_do_projeto>/data/silver/tickers_cache (por padrão)
      (raiz = uma pasta acima de 'infra/')
    - Se existir cache, faz union + dedup por (ticker, price_date), mantendo extracted_at mais recente
    - Persiste em Parquet (dataset) dentro do diretório de cache
    - Retorna DF final deduplicado
    """
    spark = df_prices.sparkSession

    # Default path: project_root/data/silver/tickers_cache
    if cache_dir is None:
        project_root = Path(__file__).resolve().parents[1] # retorna 1 pagina para a raiz do projeto
        cache_path = project_root / "data" / "silver" / "tickers_cache"
    else:
        cache_path = Path(cache_dir)

    cache_path.mkdir(parents=True, exist_ok=True)
    parquet_path = str(cache_path)

    new_count = df_prices.count()
    logger.info("Updating ticker prices cache. New batch rows=%s path=%s", new_count, parquet_path)

    try:
        df_cache = spark.read.parquet(parquet_path)
        old_count = df_cache.count()
        logger.info("Existing cache found. Cached rows=%s", old_count)
        df_all = df_cache.unionByName(df_prices)
    except Exception:
        old_count = 0
        logger.info("No existing cache found. Creating new cache at %s", parquet_path)
        df_all = df_prices

    w = Window.partitionBy("ticker", "data_preco").orderBy(F.col("extracted_at").desc())

    df_dedup = (
        df_all
        .withColumn('rn', F.row_number().over(w))
        .filter(F.col('rn') == 1)
        .drop("rn")
    )

    logger.info("Writing cache (overwrite) to %s", parquet_path)
    df_dedup.write.mode("overwrite").parquet(parquet_path)
    logger.info("Cache write finished: %s", parquet_path)
    
    return df_dedup