from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.column import Column

def build_spark(app_name: str = 'local_finance_pipeline') -> SparkSession:
    return (
        SparkSession.builder
            .appName(app_name)
            .master("local[4]") # usa todos 4 cores, para usar todos usar "local[*]"
            # Performance/estabilidade local
            .config("spark.sql.adaptive.enabled", "true")  # AQE: melhora joins/shuffles automaticamente
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.sql.shuffle.partitions", "16")  # default 200 é ruim pra dataset pequeno
            .config("spark.default.parallelism", "16")
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