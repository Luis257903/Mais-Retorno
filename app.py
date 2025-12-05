import os
import duckdb
import pandas as pd
import streamlit as st
import plotly.graph_objects as go
import plotly.express as px

# ===============================
# CONFIGURAÇÃO STREAMLIT
# ===============================
st.set_page_config(layout="wide")
st.title("📊 Rentabilidade de Fundos por CNPJ (Ultra Rápido com DuckDB)")

# ===============================
# LER TABELA DE NOMES DOS FUNDOS
# ===============================
# Aqui você usa a tabela fundos2 que tem CNPJ + Nome
from Nome import fundos2

fundos_unicos = fundos2[["CNPJ", "Nome"]].dropna().drop_duplicates()
fundos_unicos["Label"] = fundos_unicos["Nome"] + " - " + fundos_unicos["CNPJ"]

options_dict = fundos_unicos.set_index("Label")["CNPJ"].to_dict()
labels_list = list(options_dict.keys())

# ===============================
# ENTRADA DO USUÁRIO
# ===============================
st.subheader("Selecione os fundos")
selected_labels = st.multiselect("Escolha um ou mais fundos:", labels_list)

# Converter labels → CNPJs
selected_cnpjs = [options_dict[label] for label in selected_labels]

# Escolha do período pelo usuário
st.subheader("Selecione o período")
default_start = pd.to_datetime("2000-01-01")
default_end = pd.to_datetime("today")

date_range = st.date_input(
    "Intervalo de datas",
    [default_start, default_end],
    min_value=default_start,
    max_value=default_end
)

start_date, end_date = pd.to_datetime(date_range[0]), pd.to_datetime(date_range[1])

# ===============================
# PROCESSAMENTO APENAS QUANDO HÁ FUNDO SELECIONADO
# ===============================
if selected_cnpjs:

    # ===============================
    # LEITURA EFICIENTE DOS PARQUETS
    # ===============================
    folder_path = os.path.join(os.path.dirname(__file__), "dados_parquet")
    parquet_pattern = os.path.join(folder_path, "*.parquet")

    # Para query IN ('a','b','c')
    cnpjs_sql = ",".join([f"'{c}'" for c in selected_cnpjs])

    query = f"""
        SELECT *
        FROM read_parquet('{parquet_pattern}')
        WHERE CNPJ IN ({cnpjs_sql})
        ORDER BY DATA
    """

    try:
        df = duckdb.query(query).df()

    except Exception as e:
        st.error(f"Erro ao consultar DuckDB: {e}")
        st.stop()

    if df.empty:
        st.warning("Nenhum dado encontrado para esses fundos.")
        st.stop()

    # Garantir formatação
    df["DATA"] = pd.to_datetime(df["DATA"])
    df = df[(df["DATA"] >= start_date) & (df["DATA"] <= end_date)]

    # ===============================
    # AJUSTAR DATA DE INÍCIO PARA A MAIS NOVA ENTRE OS FUNDOS
    # ===============================
    min_dates = df.groupby("CNPJ")["DATA"].min()
    true_start = min_dates.max()   # começa do fundo mais "novo"

    df = df[df["DATA"] >= true_start]

    st.info(f"⏳ Análise iniciando em **{true_start.date()}** (primeiro ponto comum entre os fundos).")

    # ===============================
    # CALCULAR RENTABILIDADE
    # ===============================
    tabela = df.pivot_table(index="DATA", columns="CNPJ", values="COTA")
    tabela = tabela.dropna()

    rent_mensal = tabela.pct_change().fillna(0)
    rent_acum = (rent_mensal + 1).cumprod() - 1

    # ===============================
    # GRÁFICO PLOTLY
    # ===============================
    st.subheader("📈 Rentabilidade Acumulada")

    fig = go.Figure()

    for cnpj in rent_acum.columns:
        serie = rent_acum[cnpj]
        nome = fundos_unicos.loc[fundos_unicos["CNPJ"] == cnpj, "Nome"].values[0]

        fig.add_trace(
            go.Scatter(
                x=serie.index,
                y=serie.values,
                mode="lines",
                name=nome,
                line=dict(width=3)
            )
        )

        # anotação ao final da linha
        fig.add_annotation(
            x=serie.index[-1],
            y=serie.values[-1],
            text=f"{serie.values[-1]*100:.2f}%",
            showarrow=False,
            bgcolor="black",
            font=dict(color="white", size=14)
        )

    fig.update_layout(
        height=600,
        template="plotly_white",
        yaxis_tickformat=".2%",
        xaxis_title="Data",
        yaxis_title="Rentabilidade Acumulada"
    )

    st.plotly_chart(fig, use_container_width=True)

    # ===============================
    # MOSTRAR TABELA FINAL
    # ===============================
    st.subheader("📋 Dados utilizados")
    st.dataframe(df)

else:
    st.info("Selecione um ou mais fundos para iniciar a análise.")
