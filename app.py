import os
import duckdb
import pandas as pd
import streamlit as st
import plotly.graph_objects as go

# =======================================
# IMPORTAÇÃO INTELIGENTE (lazy load)
# =======================================
import Nome     # agora não carrega arquivos pesados no startup
from cdi import CDI_M_Correto  # CDI mensal (index = datas)

# =======================================
# STREAMLIT CONFIG
# =======================================
st.set_page_config(layout="wide")
st.title("📈 Rentabilidade de Fundos + CDI Diário (via DuckDB)")

# =======================================
# LAZY LOAD DOS FUNDOS — rápido e cacheado
# =======================================
@st.cache_data(show_spinner=False)
def load_fundos2():
    return Nome.fundos2     # só carrega quando necessário

fundos2 = load_fundos2()

# =======================================
# INTERFACE — seleção dos fundos
# =======================================
fundos_unicos = fundos2[["CNPJ", "Nome"]].dropna().drop_duplicates()
fundos_unicos["Label"] = fundos_unicos["Nome"] + " - " + fundos_unicos["CNPJ"]

options_dict = fundos_unicos.set_index("Label")["CNPJ"].to_dict()

selected_labels = st.multiselect("Selecione os fundos:", list(options_dict.keys()))
selected_cnpjs = [options_dict[x] for x in selected_labels]

date_range = st.date_input(
    "Intervalo de análise:",
    [pd.to_datetime("2000-01-01"), pd.to_datetime("today")]
)

start_date, end_date = pd.to_datetime(date_range[0]), pd.to_datetime(date_range[1])

# =======================================
# PROCESSAMENTO PRINCIPAL
# =======================================
if selected_cnpjs:

    # Caminho dos parquets
    folder_path = os.path.join(os.path.dirname(__file__), "dados_parquet")
    parquet_pattern = os.path.join(folder_path, "*.parquet")

    # Lista de CNPJs → SQL
    cnpjs_sql = ",".join([f"'{c}'" for c in selected_cnpjs])

    # CONSULTA DUCKDB — ultra rápida
    query = f"""
        SELECT *
        FROM read_parquet('{parquet_pattern}')
        WHERE CNPJ IN ({cnpjs_sql})
        ORDER BY DATA
    """

    df = duckdb.query(query).df()
    df["DATA"] = pd.to_datetime(df["DATA"])

    df = df[(df["DATA"] >= start_date) & (df["DATA"] <= end_date)]

    if df.empty:
        st.warning("Nenhum dado encontrado.")
        st.stop()

    # =======================================
    # DEFINIR A PRIMEIRA DATA COMUM ENTRE FUNDOS
    # =======================================
    min_dates = df.groupby("CNPJ")["DATA"].min()
    true_start = min_dates.max()    # lógica correta

    st.info(f"📌 A análise inicia automaticamente em **{true_start.date()}**, "
            f"que é o primeiro ponto em comum entre os fundos selecionados.")

    df = df[df["DATA"] >= true_start]

    # =======================================
    # TABELA DE COTAS
    # =======================================
    tabela = df.pivot_table(index="DATA", columns="CNPJ", values="COTA").dropna()

    # Retorno mensal
    rent_mensal = tabela.pct_change().fillna(0)

    # Rentabilidade acumulada dos fundos
    rent_acum = (rent_mensal + 1).cumprod() - 1

    # =======================================
    # CDI DIÁRIO — DERIVADO DO CDI MENSAL
    # =======================================

    # CDI_M_Correto já tem índice = datas mensais
    cdi = CDI_M_Correto.copy()
    cdi.index = pd.to_datetime(cdi.index)
    cdi = cdi.sort_index()

    # Filtrar pelo intervalo real da análise
    cdi = cdi[(cdi.index >= true_start) & (cdi.index <= end_date)]

    all_days = tabela.index
    cdi_daily_list = []

    for month in cdi.index:

        month_str = month.strftime("%Y-%m")
        cdi_mes = cdi.loc[month, "CDI"]

        # Dias do mês presentes nos dados dos fundos
        mask = all_days.strftime("%Y-%m") == month_str
        dias_mes = all_days[mask]

        if len(dias_mes) == 0:
            continue

        # CDI diário calculado
        cdi_daily_rate = (1 + cdi_mes) ** (1 / len(dias_mes)) - 1

        serie_mes = pd.Series([cdi_daily_rate] * len(dias_mes), index=dias_mes)
        cdi_daily_list.append(serie_mes)

    cdi_daily = pd.concat(cdi_daily_list).sort_index()

    # Primeiro dia deve ser 0
    cdi_daily.iloc[0] = 0

    # CDI acumulado
    cdi_acum = (cdi_daily + 1).cumprod() - 1

    # =======================================
    # GRÁFICO FINAL
    # =======================================
    st.subheader("📊 Rentabilidade Acumulada — Fundos vs CDI Diário")

    fig = go.Figure()

    # Fundos
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

    # CDI
    fig.add_trace(
        go.Scatter(
            x=cdi_acum.index,
            y=cdi_acum.values,
            mode="lines",
            name="CDI Diário (via CDI Mensal)",
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

    st.subheader("📋 Dados Brutos (Fundos)")
    st.dataframe(df)

else:
    st.info("Selecione pelo menos um fundo para iniciar.")
