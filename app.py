import os
import duckdb
import pandas as pd
import streamlit as st
import plotly.graph_objects as go
import plotly.colors as pc
import random

st.title("Rentabilidade de Fundos (Múltiplos CNPJs) - DuckDB Turbo")

# === Caminho dos parquets ===
folder_path = os.path.join(os.path.dirname(__file__), "dados_parquet")
parquet_pattern = os.path.join(folder_path, "*.parquet")

# === Entrada múltipla de CNPJs ===
lista_input = st.text_area(
    "Digite um ou vários CNPJs (um por linha):",
    height=120,
    placeholder="Exemplo:\n34.431.415/0001-31\n12.345.678/0001-22"
)

if lista_input.strip():

    cnpjs = [c.strip() for c in lista_input.split("\n") if c.strip()]

    # ============================================
    # 1) Carregar todos os fundos via DuckDB
    # ============================================
    query = f"""
        SELECT *
        FROM read_parquet('{parquet_pattern}')
        WHERE CNPJ IN ({','.join("'" + c + "'" for c in cnpjs)})
        ORDER BY CNPJ, DATA
    """

    df = duckdb.query(query).df()

    if df.empty:
        st.warning("Nenhum dado encontrado para os CNPJs informados.")
        st.stop()

    df["DATA"] = pd.to_datetime(df["DATA"])
    df = df.sort_values(["CNPJ", "DATA"])

    # ============================================
    # 2) Determinar o período possível da análise
    # ============================================
    # Cada fundo tem sua data mínima, pegamos a MAIS RECENTE
    datas_inicio = df.groupby("CNPJ")["DATA"].min()
    data_inicio_real = datas_inicio.max()    # igual ao seu dashboard antigo
    data_fim_real = df["DATA"].max()

    # ============================================
    # 3) Seletor de período
    # ============================================
    periodo = st.date_input(
        "Selecione o período da análise:",
        [data_inicio_real, data_fim_real],
        min_value=data_inicio_real,
        max_value=data_fim_real
    )

    data_ini = pd.to_datetime(periodo[0])
    data_fim = pd.to_datetime(periodo[1])

    # Filtrar
    df_filtrado = df[(df["DATA"] >= data_ini) & (df["DATA"] <= data_fim)]

    if df_filtrado.empty:
        st.warning("Não há dados no intervalo escolhido.")
        st.stop()

    # ============================================
    # 4) Calcular rentabilidade por fundo
    # ============================================
    fundos_rent = {}

    for cnpj, tabela in df_filtrado.groupby("CNPJ"):

        tabela = tabela.sort_values("DATA").copy()
        tabela["RET"] = tabela["COTA"].pct_change().fillna(0)
        tabela["ACUM"] = (1 + tabela["RET"]).cumprod() - 1

        fundos_rent[cnpj] = tabela

    # ============================================
    # 5) GRÁFICO FINAL
    # ============================================
    fig = go.Figure()

    tons_escuros = [i / 100 for i in range(20, 100, 25)]  # tons escuros de blue

    legendas = []

    for i, (cnpj, tabela) in enumerate(fundos_rent.items()):

        ton = random.choice(tons_escuros)
        cor = pc.sample_colorscale("Blues", [ton])[0]

        fig.add_trace(go.Scatter(
            x=tabela["DATA"],
            y=tabela["ACUM"],
            mode="lines",
            name=cnpj,
            line=dict(color=cor, width=3),
            line_shape="spline"
        ))

        # anotação final
        fig.add_annotation(
            x=tabela["DATA"].iloc[-1],
            y=tabela["ACUM"].iloc[-1],
            text=f"{tabela['ACUM'].iloc[-1]*100:.2f}%",
            showarrow=False,
            bgcolor=cor,
            font=dict(color="white", size=14)
        )

        legendas.append(cnpj)

    fig.update_layout(
        title="Rentabilidade Acumulada dos Fundos",
        xaxis_title="Data",
        yaxis_title="Rentabilidade",
        yaxis_tickformat=".2%",
        template="plotly_white",
        height=600,
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        xaxis=dict(showgrid=False),
        yaxis=dict(showgrid=False),
    )

    st.plotly_chart(fig, use_container_width=True)

    st.success("Gráfico gerado com sucesso!")
