from datetime import date, datetime, timedelta
import logging

import yfinance as yf

from pathlib import Path

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

    cutoff = date.today()
    start = cutoff - timedelta(days=lookback_days)
    end = cutoff + timedelta(days=1)
    
    df_filtrado = (df
                   .filter(F.col('tipo') == F.lit("stock")) # confirmar o pq do li
                   .select(F.col('nome').alias('ticker'))
                   .filter(F.col('ticker').isNotNull() & F.col('ticker') != '' )
                   .distinct()
    )

    tickers_list = [r['tickers'] for r in df_filtrado.collect()]
    if not tickers_list:
        schema = T.StructType([
            T.StructField("ticker", T.StringType(), False),
            T.StructField("data_preco", T.DateType(), False),
            T.StructField("close", T.DoubleType(), False),
            T.StructField("extracted_at", T.TimestampType(), False)
        ])
        return spark.createDataFrame([], schema)
    
    def _to_yahoo_ticker(ticker:str) -> str:
        if "." in ticker:
            return ticker
        
        return f"{ticker}.SA"
    
    yahoo_map = {t: _to_yahoo_ticker(t) for t in tickers_list}
    yahoo_ticker_list = list(yahoo_map.values())

    data = yf.download(
        tickers=yahoo_ticker_list,
        start=start,
        end=end,
        interval="1d",
        auto_adjust=True,
        threads=True
    )

    extracted_at = datetime.now()

    # Função interna: extrai a série de "Close" para um ticker, lidando com retorno 1-ticker vs multi-ticker
    def _close_series_for_ticker(yahoo_t: str):
        """
        Retorna um pandas.Series com index datetime (dias) e valores de Close, ou Series vazia.
        """
        try:
            if len(yahoo_ticker_list) == 1:
                close_s = data['Close']
            else:
                close_s = data[yahoo_t]["Close"]
            return close_s
        except:
            return None
        
    rows = []
    for ticker, yahoo_ticker in yahoo_map.items():
        close_s = _close_series_for_ticker(yahoo_ticker)

        if close_s is None:
            rows.append((ticker, cutoff, None, extracted_at))
            continue

        close_s = close_s.dropna()

        if close_s.empty:
            rows.append((ticker, cutoff, None, extracted_at))
            continue
        
        close = float(close_s.iloc[-1])
        data_preco = close_s.index[-1].date()

        rows.append((ticker, data_preco, close, extracted_at))

    schema = T.StructType([
            T.StructField("ticker", T.StringType(), False),
            T.StructField("data_preco", T.DateType(), False),
            T.StructField("close", T.DoubleType(), False),
            T.StructField("extracted_at", T.TimestampType(), False)
        ])
    return spark.createDataFrame(rows, schema) 

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