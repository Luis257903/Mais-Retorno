import os
import duckdb
import pandas as pd
import streamlit as st
import plotly.graph_objects as go
import plotly.colors as pc
import random

st.title("Consulta de Rentabilidade - Fundos por CNPJ (DuckDB Ultra Rápido)")

# Caminho da pasta dos parquets
folder_path = os.path.join(os.path.dirname(__file__), "dados_parquet")
parquet_pattern = os.path.join(folder_path, "*.parquet")

cnpj_input = st.text_input("Digite o CNPJ do fundo (formato igual ao parquet):")

if cnpj_input:

    # ================================
    # 1) CARREGA O FUNDO VIA DUCKDB
    # ================================
    query = f"""
        SELECT *
        FROM read_parquet('{parquet_pattern}')
        WHERE CNPJ = '{cnpj_input}'
        ORDER BY DATA
    """

    try:
        df = duckdb.query(query).df()

        if df.empty:
            st.warning("Nenhum registro encontrado para esse CNPJ.")
            st.stop()

        # Converter data corretamente
        df["DATA"] = pd.to_datetime(df["DATA"])
        df = df.sort_values("DATA")

        # ================================
        # 2) SELETOR DE INTERVALO DE DATAS
        # ================================
        data_min = df["DATA"].min()
        data_max = df["DATA"].max()

        periodo = st.date_input(
            "Selecione o período da análise:",
            [data_min, data_max],
            min_value=data_min,
            max_value=data_max
        )

        data_ini = pd.to_datetime(periodo[0])
        data_fim = pd.to_datetime(periodo[1])

        # Filtra o período escolhido
        df = df[(df["DATA"] >= data_ini) & (df["DATA"] <= data_fim)]

        if df.empty:
            st.warning("Não há dados para o intervalo selecionado.")
            st.stop()

        st.subheader("Primeiras linhas do fundo filtrado:")
        st.dataframe(df.head())

        # ================================
        # 3) CÁLCULO DA RENTABILIDADE
        # ================================
        df["RETORNO"] = df["COTA"].pct_change().fillna(0)
        df["ACUMULADO"] = (1 + df["RETORNO"]).cumprod() - 1

        # ================================
        # 4) GRÁFICO DE RENTABILIDADE
        # ================================
        fig = go.Figure()

        # Cor aleatória dentro do Blues
        tons_escuros = [i / 100 for i in range(20, 100, 25)]
        ton = random.choice(tons_escuros)
        cor_serie = pc.sample_colorscale("Blues", [ton])[0]

        fig.add_trace(go.Scatter(
            x=df["DATA"],
            y=df["ACUMULADO"],
            mode="lines",
            line=dict(color=cor_serie, width=4),
            name="Rentabilidade",
            line_shape="spline"
        ))

        # Anotação final
        fig.add_annotation(
            x=df["DATA"].iloc[-1],
            y=df["ACUMULADO"].iloc[-1],
            text=f"{df['ACUMULADO'].iloc[-1]*100:.2f}%",
            showarrow=False,
            font=dict(color='white', size=16),
            align='center',
            bgcolor=cor_serie,
            bordercolor=cor_serie,
            borderwidth=1,
            borderpad=4
        )

        fig.update_layout(
            title=f"Rentabilidade Acumulada - {cnpj_input}",
            xaxis_title="Data",
            yaxis_title="Rentabilidade",
            template="plotly_white",
            height=550,
            yaxis_tickformat=".2%",
            xaxis=dict(showgrid=False),
            yaxis=dict(showgrid=False),
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)"
        )

        st.plotly_chart(fig, use_container_width=True)

    except Exception as e:
        st.error(f"Erro ao consultar os dados: {e}")
