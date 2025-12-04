import os
import pandas as pd
import pyarrow.parquet as pq
import streamlit as st

st.title("Consulta de Fundos por CNPJ")

# Caminho da pasta de dados
folder_path = os.path.join(os.path.dirname(__file__), "dados_parquet")

# Lista todos os arquivos .parquet
arquivos = sorted([
    os.path.join(folder_path, f)
    for f in os.listdir(folder_path)
    if f.endswith(".parquet")
])

st.write(f"Arquivos parquet encontrados: {len(arquivos)}")

# Input do usuário
cnpj_input = st.text_input("Digite o CNPJ do fundo:")

if cnpj_input:

    lista_df = []  # armazenará apenas registros do CNPJ escolhido

    for arquivo in arquivos:
        try:
            table = pq.read_table(arquivo)
            df = table.to_pandas()

            # Garantir que DATA é datetime
            if "DATA" in df.columns:
                df["DATA"] = pd.to_datetime(df["DATA"])

            # Filtrar somente o CNPJ desejado
            filtrado = df[df["CNPJ"] == cnpj_input]

            if not filtrado.empty:
                lista_df.append(filtrado)

        except Exception as e:
            st.warning(f"Erro ao ler {arquivo}: {e}")

    # Junta tudo
    if lista_df:
        df_final = pd.concat(lista_df, ignore_index=True)
        df_final = df_final.sort_values("DATA")

        st.subheader("Resultados encontrados")
        st.dataframe(df_final.head(20))
    else:
        st.error("Nenhum registro encontrado para esse CNPJ.")
