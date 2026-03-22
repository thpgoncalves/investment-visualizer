import logging
import shutil

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
                Se a pasta de destino já existir, ela é removida completamente antes
                da nova gravação, garantindo um reprocessamento completo da camada.
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

    project_root = Path(__file__).resolve().parents[1]
    partition_ref = f"{ano}{mes:02d}"
    df_final = df.toPandas().copy()

    if layer == "silver":
        location_dir = project_root / "data" / "silver" / "snapshots"
        final_file = location_dir / f"{partition_ref}_silver_snapshot.csv"

        if location_dir.exists():
            logger.info("Existing silver snapshot directory found. Deleting before saving new file.")
            shutil.rmtree(location_dir)

        location_dir.mkdir(parents=True, exist_ok=True)
        df_final.to_csv(final_file, index=False)

        return f"Success file saved at: {final_file}"

    if file_name is None:
        raise ValueError("`file_name` is required when layer='gold'.")
    
    location_dir = project_root / "data" / "gold" / partition_ref
    final_file = location_dir / f"{partition_ref}_gold_{file_name}_snapshot.csv"

    location_dir.mkdir(parents=True, exist_ok=True)


    if final_file.exists():
        logger.info("Existing gold snapshot file found. Deleting before saving new file.")
        final_file.unlink()

    df_final.to_csv(final_file, index=False)

    return f"Success file saved at: {final_file}"