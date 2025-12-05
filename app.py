import os
import duckdb
import streamlit as st

st.title("Consulta ultra rápida de fundos por CNPJ")

# Caminho da pasta
folder_path = os.path.join(os.path.dirname(__file__), "dados_parquet")
parquet_pattern = os.path.join(folder_path, "*.parquet")

# Cria a conexão apenas UMA vez (muito importante!)
@st.cache_resource
def get_connection():
    return duckdb.connect(database=':memory:')

con = get_connection()

cnpj_input = st.text_input("Digite o CNPJ do fundo:")

if cnpj_input:

    query = f"""
        SELECT *
        FROM read_parquet('{parquet_pattern}')
        WHERE CNPJ = '{cnpj_input}'
        ORDER BY DATA
    """

    try:
        # execute dentro da conexão fixa
        df = con.execute(query).fetchdf()

        if df.empty:
            st.warning("Nenhum registro encontrado para esse CNPJ.")
        else:
            st.dataframe(df)

    except Exception as e:
        st.error(f"Erro ao consultar os dados: {e}")

duckdb.sql("PRAGMA show_tables")
duckdb.sql("DESCRIBE SELECT * FROM nome_da_tabela LIMIT 5")
