import os
import duckdb
import pyarrow.parquet as pq
import streamlit as st

st.title("Consulta de Fundos por CNPJ")

folder_path = os.path.join(os.path.dirname(__file__), "dados_parquet")

arquivos_validos = []
arquivos_corrompidos = []

for f in os.listdir(folder_path):
    if f.endswith(".parquet"):
        caminho = os.path.join(folder_path, f)

        # Testa se o parquet é válido
        try:
            pq.ParquetFile(caminho)  # <- só abre o metadata
            arquivos_validos.append(caminho)
        except Exception:
            arquivos_corrompidos.append(caminho)

# Mostrar arquivos problemáticos
if arquivos_corrompidos:
    st.warning("Arquivos corrompidos detectados:")
    for arq in arquivos_corrompidos:
        st.write(f"- {arq}")

if not arquivos_validos:
    st.error("Nenhum arquivo parquet válido encontrado.")
    st.stop()

# Junta arquivos válidos para consulta
arquivos_str = "', '".join(arquivos_validos)

cnpj_input = st.text_input("Digite o CNPJ do fundo:")

if cnpj_input:

    query = f"""
        SELECT *
        FROM read_parquet(['{arquivos_str}'])
        WHERE CNPJ = '{cnpj_input}'
        ORDER BY DATA
    """

    try:
        df = duckdb.query(query).df()

        if df.empty:
            st.warning("Nenhum registro encontrado para esse CNPJ.")
        else:
            st.dataframe(df.head(20))

    except Exception as e:
        st.error(f"Erro ao consultar os dados: {e}")
