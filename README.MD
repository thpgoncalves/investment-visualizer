Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser

Remove-Item -Recurse -Force .venv

py -3.11 -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -e .
python -m ipykernel install --user --name investment-visualizer --display-name "Python (investment-visualizer)"


streamlit run app/streamlit_app.py

investment-visualizer/
  README.md
  pyproject.toml            # ou requirements.txt
  .gitignore

  config/
    settings.yaml           # paths, nomes de colunas, tickers internacionais, etc.

  app/
    streamlit_app.py        # entrada do Streamlit
    pages/                  # (opcional) multipáginas
    components/             # (opcional) gráficos/tabelas reutilizáveis

  pipelines/
    run_pipeline.py         # orquestra (bronze->silver->snapshot->gold)
    bronze/
      ingest_forms.py       # leitura do CSV + normalizações mínimas
    silver/
      transform_positions.py # split/explode/parse/tipagem
      enrich_prices.py      # busca preço + cache
    gold/
      build_metrics.py      # agregações/variações para dashboard

  infra/
    spark_session.py        # build_spark + configs locais
    io.py                   # helpers de leitura/escrita (parquet/csv), criação de pastas, etc.
    logging.py              # (opcional) logger simples

  domain/
    mappings.py             # listas/dicts: tickers internacionais, classes, aliases
    schemas.py              # (opcional) nomes de colunas + contratos de schema
    rules.py                # regras puras (ex: classificação nacional/internacional)

  data/
    bronze/
      forms/                # CSVs originais (imutável)
    silver/
      positions/            # datasets intermediários (opcional)
      prices_cache/
      snapshots/
        yyyy-mm/              # seus snapshots materializados por mês
    gold/
      metrics/              # tabelas prontas para o app

  notebooks/
    01_exploration.ipynb
    02_silver_tests.ipynb