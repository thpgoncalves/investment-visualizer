from infra.spark_utils import build_spark
from infra.tickers_cache import handler_partitions

from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = build_spark()

############################################################################################
### CRIAR LOGICA DE LEITURA DINAMICA PARA PEGAR SEMPRE O ARQUIVO CORRETO/ULTIMA PARTICAO ###
############################################################################################

file_path = "../data/silver/snapshots/202603_snapshot.csv"

df_silver = spark.read.csv(
    path = file_path,
    sep = ",", 
    header=True,
)

# dfs graficos de linha de comparacao, de barra e valores atuais para botoes HOME
df_instituicao = (
    df_silver
    .groupBy("data_apuracao","ano", "mes", "instituicao_fin")
    .agg(F.sum(F.col("valor_total")).alias("valor_total"))
    .select(
        F.col("data_apuracao"),
        F.col("ano"),
        F.col("mes"),
        F.lit("INSTITUICAO").alias("tipo_escopo"),
        F.col("instituicao_fin"),
        F.lit("valor_total").alias("nome_metrica"),
        F.col("valor_total"),
    )
)

df_total = (
    df_silver
    .groupBy("data_apuracao", "ano", "mes")
    .agg(F.sum(F.col("valor_total")).alias("valor_total"))
    .select(
        F.col("data_apuracao"),
        F.col("ano"),
        F.col("mes"),
        F.col("valor_total"),
        F.lit("HOME").alias("tipo_escopo"),
        F.lit("ALL").alias("instituicao_fin"),
        F.lit("valor_total").alias("nome_metrica"),
    )
)

df_evolucao_ano_mes = df_instituicao.unionByName(df_total)
df_evolucao_ano_mes = df_evolucao_ano_mes.withColumn("valor_total", F.round(F.col("valor_total"), 2)) # garantia de 2 casas 

# grafico de linha 
# df_evolucao_ano_mes.show()

print(handler_partitions(df_evolucao_ano_mes, 'gold', 'home_linha'))


latest_value_window = Window.partitionBy("instituicao_fin").orderBy(F.col("data_apuracao").desc())

df_val_atual = (
    df_evolucao_ano_mes
    .withColumn("rn", F.row_number().over(latest_value_window))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

# botoes
# df_val_atual.show()
print(handler_partitions(df_val_atual, 'gold', 'home_botoes'))

df_evolucao_ano = (
    df_evolucao_ano_mes
    .groupBy("data_apuracao","ano", "instituicao_fin")
    .agg(F.sum("valor_total").alias("valor_total"))
    .select(
        F.col("data_apuracao"),
        F.col("ano"),
        F.col("instituicao_fin"),
        F.round(F.col("valor_total"), 2).alias("valor_total"),
        F.lit("HOME").alias("tipo_escopo"),
        F.lit("valor_total_anual").alias("nome_metrica"),
    )
)

# grafico de barras
# df_evolucao_ano.show()
print(handler_partitions(df_evolucao_ano, 'gold', 'home_barras'))

# dfs graficos de pizza HOME e INSTITUICAO
latest_value_window_name = Window.partitionBy("instituicao_fin", "tipo", "nome").orderBy(F.col("data_apuracao").desc())

df_dados_atuais = (
    df_silver
    .withColumn("rn", F.row_number().over(latest_value_window_name))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

df_exposicao_all = (
    df_dados_atuais
    .groupBy('data_apuracao', 'exposicao')
    .agg(F.sum(F.col("valor_total")).alias("valor_total"))
    .select(
        F.col('data_apuracao')
        , F.col('exposicao')
        , F.col("valor_total")
        , F.lit("ALL").alias("instituicao_fin")
    )
)

df_exposicao_instituicao = (
    df_dados_atuais
    .groupBy('data_apuracao', 'exposicao', 'instituicao_fin')
    .agg(F.sum(F.col("valor_total")).alias("valor_total"))
    .select(
        F.col('data_apuracao')
        , F.col('instituicao_fin')
        , F.col('exposicao')
        , F.col("valor_total")
    )
)

df_exposicao = df_exposicao_all.unionByName(df_exposicao_instituicao)

# grafico pizza exposicao HOME e INSTITUTICAO
# df_exposicao.show()
print(handler_partitions(df_exposicao, 'gold', 'pizza_expo'))

df_tipo_all = (
    df_dados_atuais
    .groupBy('data_apuracao', 'tipo')
    .agg(F.sum(F.col("valor_total")).alias("valor_total"))
    .select(
        F.col('data_apuracao')
        , F.col('tipo')
        , F.col("valor_total")
        , F.lit("ALL").alias("instituicao_fin")
    )
)

df_tipo_instituicao = (
    df_dados_atuais
    .groupBy('data_apuracao', 'tipo', 'instituicao_fin')
    .agg(F.sum(F.col("valor_total")).alias("valor_total"))
    .select(
        F.col('data_apuracao')
        , F.col('instituicao_fin')
        , F.col('tipo')
        , F.col("valor_total")
    )
)

df_tipo = df_tipo_all.unionByName(df_tipo_instituicao)
df_tipo = df_tipo.withColumn('valor_total', F.round(F.col("valor_total"), 2))

# grafico pizza tipo HOME e INSTITUTICAO
# df_tipo.show()
print(handler_partitions(df_tipo, 'gold', 'pizza_tipo'))

# dfs graficos de linha de comparacao, de barra e valores atuais INSTITUICAO
df_instituicao_page = (
    df_silver
    .groupBy("data_apuracao","ano", "mes", "instituicao_fin", 'nome')
    .agg(F.sum(F.col("valor_total")).alias("valor_total"))
    .select(
        F.col("data_apuracao"),
        F.col("ano"),
        F.col("mes"),
        F.col("instituicao_fin"),
        F.col("nome"),
        F.col("valor_total"),
        F.lit("valor_total").alias("nome_metrica"),
        F.lit("INSTITUICAO").alias("tipo_escopo"),
    )
)

df_total_page = (
    df_silver
    .groupBy("data_apuracao", "ano", "mes", 'instituicao_fin')
    .agg(F.sum(F.col("valor_total")).alias("valor_total"))
    .select(
        F.col("data_apuracao"),
        F.col("ano"),
        F.col("mes"),
        F.col("instituicao_fin"),
        F.lit('ALL').alias('nome'),
        F.col("valor_total"),
        F.lit("valor_total").alias("nome_metrica"),
        F.lit("INSTITUICAO").alias("tipo_escopo"),
    )
)


df_evolucao_ano_mes_page = df_instituicao_page.unionByName(df_total_page)
df_evolucao_ano_mes_page = df_evolucao_ano_mes_page.withColumn("valor_total", F.round(F.col("valor_total"), 2)) # garantia de 2 casas 

# grafico de linha 
# df_evolucao_ano_mes_page.show()
print(handler_partitions(df_evolucao_ano_mes_page, 'gold', 'instituicao_linha'))


latest_value_window = Window.partitionBy("instituicao_fin", "nome").orderBy(F.col("data_apuracao").desc())

df_val_atual_page = (
    df_evolucao_ano_mes_page
    .withColumn("rn", F.row_number().over(latest_value_window))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

# labels
# df_val_atual_page.show()
print(handler_partitions(df_val_atual_page, 'gold', 'instituicao_label'))

df_evolucao_ano_page = (
    df_evolucao_ano_mes_page
    .groupBy("data_apuracao","ano", "instituicao_fin", "nome")
    .agg(F.sum("valor_total").alias("valor_total"))
    .select(
        F.col("data_apuracao"),
        F.col("ano"),
        F.col("instituicao_fin"),
        F.col("nome"),
        F.round(F.col("valor_total"), 2).alias("valor_total"),
        F.lit("valor_total_anual").alias("nome_metrica"),
        F.lit("HOME").alias("tipo_escopo"),
    )
)

# grafico de barras
# df_evolucao_ano.show()
print(handler_partitions(df_val_atual_page, 'gold', 'instituicao_barras'))