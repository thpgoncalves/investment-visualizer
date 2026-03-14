import streamlit as st


# -------------------------------------------------------------------
# Esta é uma página placeholder real.
#
# Por que isso existe?
# - porque você quer multipage real desde já;
# - então a página precisa existir de verdade;
# - depois você troca este conteúdo pelo conteúdo final da página.
# -------------------------------------------------------------------


st.title("XP")
st.info("Placeholder da página XP. Aqui você vai construir a página real depois.")


# -------------------------------------------------------------------
# st.page_link cria um link para outra página do app.
# Aqui estamos usando para voltar para a home.
# -------------------------------------------------------------------
st.page_link(
    "pages/home.py",
    label="Voltar para Principal",
    icon=":material/arrow_back:",
)


st.write("")


# -------------------------------------------------------------------
# Comentários de orientação para quando você for evoluir esta página:
#
# 1) Criar variáveis de leitura:
#    position_data = ...
#    allocation_data = ...
#    performance_data = ...
#
# 2) Reaproveitar componentes:
#    - build_pie_chart(...)
#    - build_line_chart(...)
#    - build_grouped_bar_chart(...)
#
# 3) Separar layout em colunas e containers como foi feito na home.
#
# 4) Se a página crescer muito, você pode criar componentes novos em:
#    app/components/
# -------------------------------------------------------------------
st.caption("Página futura: app/pages/xp.py")