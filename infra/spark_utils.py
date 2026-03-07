import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.column import Column

def build_spark(app_name: str = 'local_finance_pipeline') -> SparkSession:
    python_executable = sys.executable

    os.environ["PYSPARK_PYTHON"] = python_executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_executable
    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
    
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[1]") # usa todos 1 cores, para usar todos usar "local[*]"
        .config("spark.pyspark.python", python_executable)
        .config("spark.pyspark.driver.python", python_executable)
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        # Performance/estabilidade local
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        # Memória (exemplo conservador; ajuste)
        .config("spark.driver.memory", "4g")
        .config("spark.driver.maxResultSize", "1g")
        .getOrCreate()
    )

def normalize_ptbr_number(col: Column) -> Column:
    trimmed = F.trim(col)

    ptbr_like = F.regexp_replace(trimmed, r"\.", "")
    ptbr_like = F.regexp_replace(ptbr_like, r",", ".")

    normalized = (
        F.when(trimmed.isNull() | (trimmed==""), F.lit(None))
        .when(trimmed.contains(','), ptbr_like)
        .otherwise(trimmed)
    )

    return normalized