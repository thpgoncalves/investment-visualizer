import logging
import shutil
from pathlib import Path
from typing import Literal

import pandas as pd

from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame


logger = logging.getLogger(__name__)


def _build_partition_ref(year: int, month: int) -> str:
    return f"{year}{month:02d}"


def handler_partitions(
    data: DataFrame | str | Path,
    layer: Literal["bronze", "silver", "gold"],
    file_name: str | None = None,
) -> str:
    """
        Salva ou arquiva os dados na camada informada.

        A partição de referência é definida a partir da maior data presente na coluna
        `data_apuracao`, no formato `YYYYMM`.

        Regras de gravação por camada:
            - bronze:
                move o CSV processado para `data/bronze/history`, adicionando a
                partição ao nome do arquivo.
            - silver:
                salva o arquivo em `data/silver/snapshots`.
                Se o snapshot da mesma partição já existir, apenas esse arquivo é
                removido antes da nova gravação, preservando snapshots de outros meses.
            - gold:
                salva o arquivo em `data/gold/<partition_ref>`.
                Se o arquivo de destino já existir, apenas esse arquivo é removido antes
                da nova gravação, preservando os demais arquivos da mesma partição.

        Parâmetros:
            data:
                Caminho do CSV para Bronze ou DataFrame Spark para Silver e Gold.
            layer:
                Camada de destino. Aceita `"bronze"`, `"silver"` ou `"gold"`.
            file_name:
                Nome lógico do arquivo para a camada gold. Obrigatório quando
                `layer="gold"`.

        Retorna:
            Uma string com o caminho final do arquivo gerado.

        Raises:
            ValueError:
                Quando `layer="gold"` e `file_name` não for informado.
    """
    
    if layer == "bronze":
        input_path = Path(data)
        dates = pd.to_datetime(
            pd.read_csv(input_path, usecols=["data_apuracao"])["data_apuracao"],
            format="%d/%m/%Y",
            errors="coerce",
        )
        max_date = dates.max()

        if pd.isna(max_date):
            raise ValueError(
                f"Could not identify a valid data_apuracao in Bronze file: {input_path}"
            )

        partition_ref = _build_partition_ref(max_date.year, max_date.month)
        history_dir = input_path.parent / "history"
        final_file = history_dir / f"{partition_ref}_{input_path.name}"

        logger.info(
            "🥉 BRONZE | Archive preparation | partition=%s | source=%s | destination=%s",
            partition_ref,
            input_path,
            final_file,
        )

        history_dir.mkdir(parents=True, exist_ok=True)

        if final_file.exists():
            logger.info("🥉 BRONZE | Replacing existing archive | path=%s", final_file)
            final_file.unlink()

        shutil.move(str(input_path), str(final_file))
        logger.info("🥉 BRONZE | Archive saved | path=%s", final_file)
        return str(final_file)

    df = data
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
    partition_ref = _build_partition_ref(ano, mes)
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
