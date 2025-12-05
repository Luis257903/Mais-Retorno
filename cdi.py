import duckdb
import pandas as pd
import requests
from io import StringIO
from datetime import datetime

DB_PATH = "base.duckdb"

def get_bcb_series(series_id):
    url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{series_id}/dados?formato=csv"

    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        r.encoding = "utf-8-sig"
    except Exception as e:
        print(f"[ERRO] Não conseguiu baixar série {series_id}: {e}")
        return pd.DataFrame()

    primeira_linha = r.text.split("\n", 1)[0].lower().replace('"', "")
    if not primeira_linha.startswith("data;valor"):
        print(f"[ERRO] Resposta inesperada para série {series_id}.")
        return pd.DataFrame()

    df = pd.read_csv(StringIO(r.text), sep=";", decimal=",")
    df["data"] = pd.to_datetime(df["data"], dayfirst=True, errors="coerce")
    df["valor"] = pd.to_numeric(df["valor"], errors="coerce")
    df = df.dropna(subset=["data"]).set_index("data")
    return df


# === BAIXA AS SÉRIES ===
CDI = get_bcb_series(4391).rename(columns={"valor": "CDI"})
IPCA = get_bcb_series(433).rename(columns={"valor": "IPCA"})
TR   = get_bcb_series(7811).rename(columns={"valor": "TR"})

# Resamplar para mensal
CDI_M = (CDI.resample("ME").last()) / 100
IPCA_M = (IPCA.resample("ME").last()) / 100
TR_M = (TR.resample("ME").last()) / 100

# Concat
indicadores = pd.concat([CDI_M, IPCA_M, TR_M], axis=1)
indicadores.index.name = "Data"

# === GARANTE TODAS AS COLUNAS ===
for col in ["CDI", "IPCA", "TR"]:
    if col not in indicadores.columns:
        indicadores[col] = None

# Ordem exata
indicadores = indicadores[["CDI", "IPCA", "TR"]]

# Data vira coluna
indicadores = indicadores.reset_index()

# === GRAVA NO DUCKDB ===
con = duckdb.connect(DB_PATH)

con.execute("""
CREATE TABLE IF NOT EXISTS indicadores_bcb (
    Data DATE,
    CDI DOUBLE,
    IPCA DOUBLE,
    TR DOUBLE
)
""")

# Limpa a tabela toda (sempre reescreve)
con.execute("DELETE FROM indicadores_bcb")

# Insere corretamente
con.execute("INSERT INTO indicadores_bcb SELECT * FROM indicadores")

con.close()

print("Indicadores inseridos no DuckDB com sucesso.")
