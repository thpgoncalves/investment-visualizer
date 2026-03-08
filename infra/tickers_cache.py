from datetime import date, datetime, timedelta
from pathlib import Path
import logging
import shutil

import yfinance as yf
import pandas as pd

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql import types as T
from pyspark.sql.dataframe import DataFrame

logger = logging.getLogger(__name__)

def get_tickers_price(df: DataFrame, lookback_days: int = 7) -> DataFrame:
    """
        Busca no Yahoo Finance o preço de fechamento mais recente disponível para cada ticker de ação presente no DataFrame de entrada. 
        A função considera apenas registros com `tipo = "stock"`, extrai os tickers distintos da coluna `nome` e adiciona o sufixo `.SA` 
        quando necessário para consulta no Yahoo.

        A busca é feita no intervalo entre a data atual menos `lookback_days` e a data atual. 
        Para cada ticker, é selecionado o fechamento mais recente com valor válido (`close` não nulo e maior que zero), 
        preservando no retorno o ticker original do DataFrame de entrada, sem o sufixo usado internamente na consulta.

        Retorna um DataFrame Spark com as colunas `data_preco`, `ticker`, `close`, `extracted_at` e `data_apuracao`. 
        Assume que a quantidade de tickers distintos é pequena o suficiente para ser coletada no driver.
    """
    
    spark = df.sparkSession

    cutoff = date.today()
    start = cutoff - timedelta(days=lookback_days)
    
    df_filtrado = (df
                   .filter(F.col('tipo') == F.lit("stock"))
                   .select(F.col('nome').alias('ticker'))
                   .filter(F.col('ticker').isNotNull() & (F.col('ticker') != ''))
                   .distinct()
    )


    schema = T.StructType([
            T.StructField("data_preco", T.DateType(), False),
            T.StructField("ticker", T.StringType(), False),
            T.StructField("close", T.DoubleType(), True),
            T.StructField("extracted_at", T.TimestampType(), False)
        ])

    tickers_list = [r['ticker'] for r in df_filtrado.collect()]
    if not tickers_list:
        return spark.createDataFrame([], schema)
    
    def _to_yahoo_ticker(ticker:str) -> str:
        """
            Converte o ticker para o formato esperado pelo Yahoo Finance. 
            Caso o valor já possua um sufixo, ele é mantido; caso contrário, é adicionado `.SA`, assumindo negociação na B3.
        """
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
            Extrai a série de fechamento (`Close`) de um ticker a partir do retorno do Yahoo Finance, 
            tratando tanto cenários com um único ticker quanto com múltiplos tickers. 
            Organiza o resultado em um DataFrame pandas com as colunas `data_preco`, `ticker`, `close` e `extracted_at`.

            Em caso de erro no processamento do ticker, registra a mensagem e retorna `None`.
        """
        try:
            if isinstance(data.columns, pd.MultiIndex):
                close_s = data["Close"][yahoo_t]
            else:
                close_s = data["Close"]

            df_close = close_s.reset_index()
            df_close.columns = ["data_preco", "close"]
            df_close["ticker"] = yahoo_t
            df_close["extracted_at"] = extracted_at

            return df_close[["data_preco", "ticker", "close", "extracted_at"]]
        except Exception as e:
            print(f"Erro ao processar {yahoo_t}: {e}")
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
        """
            Normaliza os tipos do DataFrame pandas recebido e o converte para um DataFrame Spark com o schema informado. 
            Em seguida, seleciona para cada ticker o preço de fechamento mais recente com valor válido, 
            considerando apenas registros com `close` não nulo e maior que zero.

            Retorna um DataFrame Spark final com `data_preco`, `ticker`, `close`, `extracted_at` e `data_apuracao`.
        """
        df = df.copy()

        # garante tipos compatíveis no pandas antes da conversão
        df["data_preco"] = pd.to_datetime(df["data_preco"], errors="coerce").dt.date
        df["ticker"] = df["ticker"].astype("string")
        df["close"] = pd.to_numeric(df["close"], errors="coerce")
        df["extracted_at"] = pd.to_datetime(df["extracted_at"], errors="coerce")

        spark_df = spark.createDataFrame(df, schema)
        spark_df.createOrReplaceTempView("ticker_prices_raw")

        df_final = spark.sql("""
            WITH 
                ranked as (
                    SELECT 
                        data_preco,
                        ticker,
                        ROUND(close, 2) as close,
                        extracted_at,
                        ROW_NUMBER() OVER(PARTITION BY ticker ORDER BY data_preco DESC) AS rn
                    FROM ticker_prices_raw
                    WHERE close IS NOT NULL AND close > 0
                )
            SELECT 
                data_preco,
                ticker,
                close,
                extracted_at,
                CAST(extracted_at as date) as data_apuracao 
            FROM ranked WHERE rn = 1
        """)

        return df_final
    
    df_final = build_latest_prices_spark_df(df_concat, spark, schema)

    return df_final

def handler_tickers_cache(df_prices: DataFrame, cache_dir: str | None = None) -> DataFrame:
    """
        Atualiza o cache parquet e csv de preços de tickers. 
        Se já existir cache, faz union com os dados novos e deduplica por `ticker` e `data_preco`, 
        mantendo o registro com `extracted_at` mais recente. 
        Ao final, grava o resultado em uma pasta temporária, substitui a pasta oficial e remove a temporária.
    """
    spark = df_prices.sparkSession

    # Default path: project_root/data/silver/tickers_cache
    if cache_dir is None:
        project_root = Path(__file__).resolve().parents[1]
        final_dir = project_root / "data" / "silver" / "tickers_cache"
    else:
        final_dir = Path(cache_dir)

    temp_dir = final_dir.parent / f"{final_dir.name}_temp"

    final_parquet_file = final_dir / "tickers_cache.parquet"
    temp_parquet_file = temp_dir / "tickers_cache.parquet"
    temp_csv_file = temp_dir / "tickers_cache.csv"

    final_dir.parent.mkdir(parents=True, exist_ok=True)

    logger.info("Updating ticker cache at %s", final_dir)

    df_new = df_prices.toPandas().copy()

    df_new["data_preco"] = pd.to_datetime(df_new["data_preco"], errors="coerce").dt.date
    df_new["extracted_at"] = pd.to_datetime(df_new["extracted_at"], errors="coerce")
    df_new["close"] = pd.to_numeric(df_new["close"], errors="coerce")

    if final_parquet_file.exists():
        logger.info("Existing cache found. Merging with new batch.")
        df_cache = pd.read_parquet(final_parquet_file)

        df_cache["data_preco"] = pd.to_datetime(df_cache["data_preco"], errors="coerce").dt.date
        df_cache["extracted_at"] = pd.to_datetime(df_cache["extracted_at"], errors="coerce")
        df_cache["close"] = pd.to_numeric(df_cache["close"], errors="coerce")

        df_all = pd.concat([df_cache, df_new], ignore_index=True)

    else:
        logger.info("No existing cache found. Creating a new one.")
        df_all = df_new

    # logica de dedup e escrita do parquet feito com pandas para evitar necessidade de configuracao de ambiente hadoop
    df_dedup = (
        df_all
        .sort_values(["ticker", "data_preco", "extracted_at"], ascending=[True, True, False])
        .drop_duplicates(subset=["ticker", "data_preco"], keep="first")
        .reset_index(drop=True)
    )

    if temp_dir.exists():
        shutil.rmtree(temp_dir)

    temp_dir.mkdir(parents=True, exist_ok=True)
    
    logger.info("Writing temporary cache to %s", temp_parquet_file)
    df_dedup.to_parquet(temp_parquet_file, index=False)
    df_dedup.to_csv(temp_csv_file, index=False)

    if final_dir.exists():
        shutil.rmtree(final_dir)

    shutil.move(str(temp_dir), str(final_dir))

    logger.info("Ticker cache updated successfully at %s", final_dir)

    return spark.createDataFrame(df_dedup)

def handler_partitions(df: DataFrame, location: str) -> DataFrame:
    """
        Salva um snapshot completo do DataFrame em CSV dentro da camada informada em `location`,
        como `silver` ou `gold`. A partição de destino é definida a partir da maior data presente
        na coluna `data_apuracao`, no formato `YYYY-MM`.

        Lógica de reprocessamento:
            - Se a pasta da partição já existir, ela é removida antes da nova gravação, garantindo que
                o mês contenha sempre a versão mais atual do snapshot. 
            - Se não existir, a pasta é criada normalmente e o arquivo é salvo dentro dela.

        Retorna uma mensagem com o caminho final do arquivo gerado.
    """
    row = (df
           .agg(
               F.year(F.max("data_apuracao")).alias("ano"),
               F.month(F.max("data_apuracao")).alias("mes")
            )
           .first()
    )
    ano = row["ano"]
    mes = row["mes"]

    project_root = Path(__file__).resolve().parents[1]
    partition_ref = f"{ano}-{mes:02d}"
    location_dir = project_root / "data" / location / "snapshots" / partition_ref
    final_file = location_dir / f"{partition_ref}_snapshot.csv"

    df_final = df.toPandas().copy()

    if location_dir.exists():
        logger.info("Existing partition found. Deleting before saving new file.")
        shutil.rmtree(location_dir)

    location_dir.mkdir(parents=True, exist_ok=True)

    df_final.to_csv(final_file, index=False)

    return f"Success file saved at: {final_file}"