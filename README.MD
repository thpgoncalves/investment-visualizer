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
  pyproject.toml            
  .gitignore

  .streamlit/
    config.toml             # tema da aplicação

  app/
    streamlit_app.py        # "entrypoint" da aplicação
    configs/                # configuracao de dicionario de paginas 
    pages/                  # multipáginas
    components/             # gráficos/tabelas reutilizáveis
    static/                 # fontes do tema

  pipelines/
    run_pipeline.py         # orquestra (bronze->silver->snapshot->gold)
    silver/
      transformations.py    # split/explode/parse/tipagem
      tickers.py            # busca preço + cache
    gold/
      gold_metrics.py       # agregações/variações para dashboard

  infra/
    spark_utils.py          # build_spark + configs locais

  data/
    bronze/
                            # CSVs originais (imutável)
    silver/
      tickers_cache/
      snapshots/
        YYYYMM_snapshot.csv # snapshots materializados por mês
    gold/
      YYYYMM/
        YYYYMM_gold_....csv # tabelas prontas para o streamlit ler e plotar os graficos

  notebooks/                # notebooks de testes e exploracao
    01_exploration.ipynb
    02_silver_tests.ipynb


- Corrigir paths de project_root após mover módulos.
  - pode fazer, realmente quero que fique salvo em data na raiz do projeto.

- Transformar gold_metrics.py em função reutilizável e dinâmica.
  - faça isso e deixe a maneira de construção dessa função similar a aplicada ao run_silver_pipeline
    - o que a camada gold deve fazer esta dentro do notebook gold_notebook_pyspark
  - Padronizar o schema gold que o Streamlit espera.


- Implementar run_pipeline.py de verdade.
  - use o passo a passo que sugeriu porém não chame o streamlit, quero deixar essa reponsabilidade para o comando

- Criar comandos no pyproject.toml.
  - crie o comando para rodar a pipeline completa, depois de executada rodar o streamlit e um comando só pro streamlit, pode definir os nomes como iv-run e iv-app


- Documentar tudo
