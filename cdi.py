import duckdb
import pandas as pd
import requests
from io import StringIO
from datetime import datetime

DB_PATH = "base.duckdb"

def get_bcb_series(series_id):
    """
    Baixa série completa do BCB (desde o início),
    com tratamento de erros e CSV inconsistente.
    """
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

    df = df.dropna(subset=["data"])
    df = df.set_index("data")

    return df

# ======== BAIXA SÉRIES ========
CDI = get_bcb_series(4391).rename(columns={"valor": "CDI"})
IPCA = get_bcb_series(433).rename(columns={"valor": "IPCA"})
TR = get_bcb_series(7811).rename(columns={"valor": "TR"})

# Converter para taxas corretas (mensal)
CDI_M = (CDI.resample("ME").last()) / 100
IPCA_M = (IPCA.resample("ME").last()) / 100
TR_M = (TR.resample("ME").last()) / 100

# Unir todas
indicadores = pd.concat([CDI_M, IPCA_M, TR_M], axis=1)
indicadores.index = indicadores.index.rename("Data")

# ======== INSERIR NO DUCKDB ========
con = duckdb.connect(DB_PATH)

con.execute("""
CREATE TABLE IF NOT EXISTS indicadores_bcb (
    Data DATE,
    CDI DOUBLE,
    IPCA DOUBLE,
    TR DOUBLE
)
""")

con.execute("DELETE FROM indicadores_bcb")   # substitui tudo
con.execute("INSERT INTO indicadores_bcb SELECT * FROM indicadores")

con.close()

print("Indicadores inseridos no DuckDB com sucesso.")
