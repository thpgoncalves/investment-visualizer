import logging

from pathlib import Path
from typing import Literal

from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame


logger = logging.getLogger(__name__)

def handler_partitions(df: DataFrame, layer: Literal["silver", "gold"], file_name: str | None = None) -> str:
    """
        Salva um snapshot completo do DataFrame em CSV na camada informada.

        A partição de referência é definida a partir da maior data presente na coluna
        `data_apuracao`, no formato `YYYYMM`.

        Regras de gravação por camada:
            - silver:
                salva o arquivo em `data/silver/snapshots`.
                Se o snapshot da mesma partição já existir, apenas esse arquivo é
                removido antes da nova gravação, preservando snapshots de outros meses.
            - gold:
                salva o arquivo em `data/gold/<partition_ref>`.
                Se o arquivo de destino já existir, apenas esse arquivo é removido antes
                da nova gravação, preservando os demais arquivos da mesma partição.

        Parâmetros:
            df:
                DataFrame Spark que será convertido e salvo em CSV.
            layer:
                Camada de destino. Aceita apenas `"silver"` ou `"gold"`.
            file_name:
                Nome lógico do arquivo para a camada gold. Obrigatório quando
                `layer="gold"`.

        Retorna:
            Uma string com o caminho final do arquivo gerado.

        Raises:
            ValueError:
                Quando `layer="gold"` e `file_name` não for informado.
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

    project_root = Path(__file__).resolve().parents[2]
    partition_ref = f"{ano}{mes:02d}"
    df_final = df.toPandas().copy()
    logical_name = file_name or "silver_snapshot"
    layer_emoji = "🥈" if layer == "silver" else "🥇"
    layer_label = layer.upper()

    logger.info(
        "%s %s | Snapshot preparation | table=%s | partition=%s | rows=%s | columns=%s",
        layer_emoji,
        layer_label,
        logical_name,
        partition_ref,
        len(df_final),
        len(df_final.columns),
    )

    if layer == "silver":
        location_dir = project_root / "data" / "silver" / "snapshots"
        final_file = location_dir / f"{partition_ref}_silver_snapshot.csv"

        location_dir.mkdir(parents=True, exist_ok=True)

        if final_file.exists():
            logger.info("🥈 SILVER | Replacing existing snapshot | path=%s", final_file)
            final_file.unlink()

        df_final.to_csv(final_file, index=False)

        logger.info(
            "🥈 SILVER | Snapshot saved | table=%s | rows=%s | path=%s",
            logical_name,
            len(df_final),
            final_file,
        )
        return str(final_file)

    if file_name is None:
        raise ValueError("`file_name` is required when layer='gold'.")
    
    location_dir = project_root / "data" / "gold" / partition_ref
    final_file = location_dir / f"{partition_ref}_gold_{file_name}_snapshot.csv"

    location_dir.mkdir(parents=True, exist_ok=True)


    if final_file.exists():
        logger.info("🥇 GOLD | Replacing existing snapshot | path=%s", final_file)
        final_file.unlink()

    df_final.to_csv(final_file, index=False)

    logger.info(
        "🥇 GOLD | Snapshot saved | table=%s | rows=%s | path=%s",
        logical_name,
        len(df_final),
        final_file,
    )
    return str(final_file)
