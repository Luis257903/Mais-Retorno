import os
import duckdb
import pandas as pd
import streamlit as st
import plotly.graph_objects as go
import plotly.colors as pc
import random

# ============================
# CONFIGURAÇÃO DO APP
# ============================
st.set_page_config(layout="wide")

st.title("Rentabilidade de Fundos – Consulta Turbo 🚀")

# ============================
# CARREGAR BASE DE NOMES (fundos2)
# ============================
# Você já usa isso no dashboard anterior
from Nome import fundos2  

fundos_lista = fundos2[["CNPJ", "Nome"]].dropna().drop_duplicates()
fundos_lista["Label"] = fundos_lista["Nome"] + " — " + fundos_lista["CNPJ"]

opcoes = fundos_lista["Label"].tolist()
mapa_label_para_cnpj = dict(zip(fundos_lista["Label"], fundos_lista["CNPJ"]))

# ============================
# MULTISELECT com busca
# ============================
selecionados = st.multiselect(
    "Selecione um ou mais fundos pelo nome ou CNPJ:",
    options=opcoes,
    placeholder="Digite o nome do fundo..."
)

if not selecionados:
    st.info("Selecione pelo menos um fundo para análise.")
    st.stop()

cnpjs = [mapa_label_para_cnpj[label] for label in selecionados]

# ============================
# CAMINHO DOS PARQUETS
# ============================
folder_path = os.path.join(os.path.dirname(__file__), "dados_parquet")
parquet_pattern = os.path.join(folder_path, "*.parquet")

# ============================
# CONSULTA DUCKDB
# ============================
query = f"""
    SELECT *
    FROM read_parquet('{parquet_pattern}')
    WHERE CNPJ IN ({','.join("'" + c + "'" for c in cnpjs)})
    ORDER BY CNPJ, DATA
"""

df = duckdb.query(query).df()

if df.empty:
    st.warning("Nenhum dado encontrado para os fundos selecionados.")
    st.stop()

df["DATA"] = pd.to_datetime(df["DATA"])
df = df.sort_values(["CNPJ", "DATA"])

# ============================
# DEFINIR PERÍODO POSSÍVEL
# ============================
datas_inicio = df.groupby("CNPJ")["DATA"].min()
data_inicio_real = datas_inicio.max()     # fundo mais NOVO define o início
data_fim_real = df["DATA"].max()

periodo = st.date_input(
    "Selecione o período da análise:",
    [data_inicio_real, data_fim_real],
    min_value=data_inicio_real,
    max_value=data_fim_real
)

data_ini = pd.to_datetime(periodo[0])
data_fim = pd.to_datetime(periodo[1])

df = df[(df["DATA"] >= data_ini) & (df["DATA"] <= data_fim)]

if df.empty:
    st.warning("Nenhum dado no período selecionado.")
    st.stop()

# ============================
# CALCULAR RENTABILIDADE
# ============================
fundos_rent = {}

for cnpj, tabela in df.groupby("CNPJ"):

    tabela = tabela.sort_values("DATA").copy()
    tabela["RET"] = tabela["COTA"].pct_change().fillna(0)
    tabela["ACUM"] = (1 + tabela["RET"]).cumprod() - 1

    fundos_rent[cnpj] = tabela

# ============================
# GRÁFICO
# ============================
fig = go.Figure()

tons_escuros = [i / 100 for i in range(20, 100, 20)]  # tons de azul escuros

for i, (cnpj, tabela) in enumerate(fundos_rent.items()):

    ton = random.choice(tons_escuros)
    cor = pc.sample_colorscale("Blues", [ton])[0]

    nome = fundos_lista.loc[fundos_lista["CNPJ"] == cnpj, "Nome"].values[0]

    fig.add_trace(go.Scatter(
        x=tabela["DATA"],
        y=tabela["ACUM"],
        mode="lines",
        name=nome,
        line=dict(color=cor, width=3),
        line_shape="spline"
    ))

    fig.add_annotation(
        x=tabela["DATA"].iloc[-1],
        y=tabela["ACUM"].iloc[-1],
        text=f"{tabela['ACUM'].iloc[-1]*100:.2f}%",
        showarrow=False,
        bgcolor=cor,
        font=dict(color="white", size=14)
    )

fig.update_layout(
    title="Rentabilidade Acumulada dos Fundos",
    xaxis_title="Data",
    yaxis_title="Rentabilidade",
    yaxis_tickformat=".2%",
    template="plotly_white",
    height=600,
    xaxis=dict(showgrid=False),
    yaxis=dict(showgrid=False),
    plot_bgcolor="rgba(0,0,0,0)",
)

st.plotly_chart(fig, use_container_width=True)
