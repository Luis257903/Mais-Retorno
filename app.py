import os
import duckdb
import pandas as pd
import streamlit as st

st.title("Consulta ultra rápida de fundos por CNPJ")

# Caminho da pasta de Parquets
folder_path = os.path.join(os.path.dirname(__file__), "dados_parquet")

# Caminho wildcard para o DuckDB ler tudo de uma vez
parquet_pattern = os.path.join(folder_path, "*.parquet")

cnpj_input = st.text_input("Digite o CNPJ do fundo:")

if cnpj_input:

    query = f"""
        SELECT *
        FROM read_parquet('{parquet_pattern}')
        WHERE CNPJ = '{cnpj_input}'
        ORDER BY DATA
    """

    try:
        df = duckdb.query(query).df()   # vira pandas automaticamente

        if df.empty:
            st.warning("Nenhum registro encontrado para esse CNPJ.")
        else:
            st.dataframe(df.head(20))

    except Exception as e:
        st.error(f"Erro ao consultar os dados: {e}")
