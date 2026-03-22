"""
    FLUXO:
    -> read bronze
    -> transform silver
    -> enrich silver
    -> build silver snapshot
    -> build gold metrics
    -> save outputs    
    -> stop spark         # n esquecer

    
    EXEMPLO MENTAL
    def run_pipeline(df_bronze: DataFrame) -> None:
        bronze_df = ingest_forms(...)
        silver_df = transform_positions(bronze_df)
        enriched_df = enrich_prices(silver_df, ...)
        snapshot_df = build_snapshot(enriched_df, snapshot_month)
        gold_tables = build_gold_tables(snapshot_df)

        write_snapshot(snapshot_df, snapshot_month)

    return
"""