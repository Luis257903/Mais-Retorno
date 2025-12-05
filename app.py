import os
import requests
import duckdb
import pandas as pd
import streamlit as st
import plotly.graph_objects as go

# ===============================
# IMPORTAÇÃO MANTENDO Nome.py
# ===============================
import Nome   # Mantido exatamente como está no seu projeto

# ===============================
# STREAMLIT CONFIG
# ===============================
st.set_page_config(layout="wide")
st.title("📈 Rentabilidade de Fundos (via DuckDB) + CDI Diário")

# ===============================
# BANCO: DOWNLOAD AUTOMÁTICO
# ===============================
DB_PATH = "base.duckdb"
DB_RELEASE_URL = (
    "https://github.com/Luis257903/Mais-Retorno/releases/download/banco-latest/base.duckdb"
)

if not os.path.exists(DB_PATH):
    st.warning("Baixando banco de dados inicial...")
    content = requests.get(DB_RELEASE_URL).content
    open(DB_PATH, "wb").write(content)
    st.success("Banco carregado!")

con = duckdb.connect(DB_PATH, read_only=True)

# ===============================
# CARREGAR LISTA DE FUNDOS (nome + CNPJ)
# mantém Nome.py por enquanto
# ===============================
@st.cache_data(show_spinner=False)
def load_fundos2():
    return Nome.fundos2

fundos2 = load_fundos2()

# Interface usuário
fundos2["Label"] = fundos2["Nome"] + " - " + fundos2["CNPJ"]
options_dict = fundos2.set_index("Label")["CNPJ"].to_dict()

selected_labels = st.multiselect(
    "Selecione os fundos:",
    list(options_dict.keys())
)

selected_cnpjs = [options_dict[x] for x in selected_labels]

# Intervalo
date_range = st.date_input(
    "Intervalo de análise:",
    [pd.to_datetime("2000-01-01"), pd.to_datetime("today")]
)
start_date, end_date = pd.to_datetime(date_range[0]), pd.to_datetime(date_range[1])

# ===============================
# PRINCIPAL
# ===============================
if selected_cnpjs:

    # Fundos selecionados → SQL
    cnpjs_sql = ",".join([f"'{c}'" for c in selected_cnpjs])

    # ===============================
    # CONSULTA DE FUNDOS VIA DUCKDB
    # ===============================
    query_fundos = f"""
        SELECT *
        FROM fundos
        WHERE CNPJ IN ({cnpjs_sql})
        AND DATA BETWEEN '{start_date.date()}' AND '{end_date.date()}'
        ORDER BY DATA;
    """

    df = con.execute(query_fundos).df()
    df["DATA"] = pd.to_datetime(df["DATA"])

    if df.empty:
        st.warning("Nenhum dado retornado.")
        st.stop()

    # ===============================
    # AJUSTE – Primeira data comum
    # ===============================
    min_dates = df.groupby("CNPJ")["DATA"].min()
    true_start = min_dates.max()

    st.info(
        f"📌 A análise começa em **{true_start.date()}**, "
        "primeira data em comum entre os fundos."
    )

    df = df[df["DATA"] >= true_start]

    # ===============================
    # COTAS → Tabela pivot
    # ===============================
    tabela = df.pivot_table(index="DATA", columns="CNPJ", values="COTA").dropna()
    rent_diaria = tabela.pct_change().fillna(0)
    rent_acum = (rent_diaria + 1).cumprod() - 1

    # ===============================
    # PUXAR O CDI DO BANCO
    # ===============================
    query_cdi = """
        SELECT Data, CDI
        FROM indicadores_bcb
        WHERE CDI IS NOT NULL
        ORDER BY Data;
    """

    cdi = con.execute(query_cdi).df()
    cdi["Data"] = pd.to_datetime(cdi["Data"])
    cdi = cdi.set_index("Data")

    # Filtrar período usado
    cdi = cdi[(cdi.index >= true_start) & (cdi.index <= end_date)]

    # ===============================
    # CDI DIÁRIO — cálculo exato
    # ===============================
    all_days = tabela.index
    daily_list = []

    for month in cdi.index:
        month_str = month.strftime("%Y-%m")
        cdi_mes = cdi.loc[month, "CDI"]

        dias_mes = all_days[all_days.strftime("%Y-%m") == month_str]
        if len(dias_mes) == 0:
            continue

        cdi_daily = (1 + cdi_mes) ** (1 / len(dias_mes)) - 1
        serie_mes = pd.Series([cdi_daily] * len(dias_mes), index=dias_mes)
        daily_list.append(serie_mes)

    cdi_daily = pd.concat(daily_list).sort_index()
    cdi_daily.iloc[0] = 0

    cdi_acum = (cdi_daily + 1).cumprod() - 1

    # ===============================
    # GRÁFICO
    # ===============================
    st.subheader("📊 Rentabilidade Acumulada — Fundos vs CDI Diário")

    fig = go.Figure()

    # Fundos
    for cnpj in rent_acum.columns:
        nome = fundos2.loc[fundos2["CNPJ"] == cnpj, "Nome"].values[0]
        fig.add_trace(
            go.Scatter(
                x=rent_acum.index,
                y=rent_acum[cnpj],
                mode="lines",
                name=nome,
                line=dict(width=3)
            )
        )

    # CDI
    fig.add_trace(
        go.Scatter(
            x=cdi_acum.index,
            y=cdi_acum.values,
            mode="lines",
            name="CDI Diário",
            line=dict(width=4, dash="dash", color="black")
        )
    )

    fig.update_layout(
        height=650,
        template="plotly_white",
        yaxis_tickformat=".2%",
        xaxis_title="Data",
        yaxis_title="Rentabilidade Acumulada"
    )

    st.plotly_chart(fig, use_container_width=True)

    # ===============================
    # Dados brutos
    # ===============================
    st.subheader("📋 Dados Brutos")
    st.dataframe(df)

else:
    st.info("Selecione pelo menos um fundo.")
