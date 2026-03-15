# Implementação da camada Gold Metrics para o dashboard

## Objetivo

A camada Gold Metrics existe para entregar ao Streamlit dados prontos para visualização, evitando cálculos dentro da aplicação.

A regra é:

- Streamlit lê
- Streamlit filtra
- Streamlit renderiza

O Streamlit não deve:

- agregar
- calcular percentuais
- calcular YTD
- consolidar instituição
- consolidar moeda
- consolidar tipo de ativo

---

## Camadas do projeto

### Bronze
Dados brutos.

### Silver
Base analítica detalhada e enriquecida.
Essa é a principal fonte para geração da camada Gold Metrics.

### Gold Metrics
Dados moldados para os gráficos do dashboard.

---

## Premissa principal

Não criar um dataframe por página.

Criar poucos dataframes gold reutilizáveis, e cada página apenas filtra por escopo.

---

## Escopos

### Home
- `scope_type = "HOME"`
- `scope_value = "ALL"`

### Páginas de instituição
- `scope_type = "INSTITUTION"`
- `scope_value = nome da instituição`

Exemplos:
- `XP`
- `NUBANK`
- `CLEAR`
- `BINANCE`

---

## Arquivos sugeridos

```text
data/gold/metrics/
  summary_metrics.parquet
  allocation_metrics.parquet
  history_metrics.parquet
  comparison_metrics.parquet