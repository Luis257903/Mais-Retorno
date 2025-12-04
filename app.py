import streamlit as st
import pandas as pd
import os

st.title("Consulta de Fundos por CNPJ")

# ⬇️ Input do usuário
cnpj = st.text_input("Digite o CNPJ do fundo (somente números):")

@st.cache_data(show_spinner=True)
def carregar_cnpj(cnpj_alvo: str):
    pasta = "dados_parquet"

    dfs = []

    # Lista todos os arquivos parquet
    arquivos = sorted([f for f in os.listdir(pasta) if f.endswith(".parquet")])

    for arquivo in arquivos:
        caminho = os.path.join(pasta, arquivo)

        # Lê apenas colunas necessárias (mais rápido!)
        try:
            df = pd.read_parquet(
                caminho,
                columns=["cnpj_fundo", "dt_comptc", "vl_quota"]
            )
        except:
            continue

        # Filtra só o CNPJ desejado
        recorte = df[df["cnpj_fundo"] == cnpj_alvo]

        if not recorte.empty:
            dfs.append(recorte)

    if not dfs:
        return None

    # Junta tudo e ordena
    df_final = pd.concat(dfs).sort_values("dt_comptc").reset_index(drop=True)

    return df_final


# Executa após digitar CNPJ
if cnpj:
    with st.spinner("Carregando dados do fundo..."):
        dados = carregar_cnpj(cnpj)

    if dados is None:
        st.warning("❗ Nenhum registro encontrado para este CNPJ.")
    else:
        st.success(f"Registros encontrados: {len(dados)}")

        # ⚠️ Mostra só as 20 primeiras linhas
        st.subheader("Primeiras 20 linhas:")
        st.dataframe(dados.head(20))
