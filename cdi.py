import pandas as pd
import requests
from io import StringIO

def get_bcb_series(series_id, start='2000-01-01'):
    """
    Baixa uma série histórica do Banco Central do Brasil (BCB),
    com tratamento para respostas com ou sem aspas, valores ausentes
    e erros de conexão, retornando índice como DatetimeIndex.

    Parâmetros:
        series_id (int): ID da série no SGS/BCB
        start (str): Data inicial no formato 'YYYY-MM-DD'

    Retorno:
        pd.DataFrame: Série histórica com índice de datas e coluna 'valor'
    """
    url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{series_id}/dados?formato=csv"

    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        r.encoding = "utf-8-sig"
    except requests.RequestException as e:
        print(f"[ERRO] Falha ao acessar série {series_id}: {e}")
        df = pd.DataFrame(columns=["valor"])
        df.index = pd.to_datetime(df.index, errors="coerce")
        return df

    # Validação do cabeçalho, removendo aspas para comparar
    primeira_linha = r.text.strip().split("\n", 1)[0].lower().replace('"', '')
    if not primeira_linha.startswith("data;valor"):
        print(f"[ERRO] Resposta inesperada para série {series_id}: {r.text[:200]}")
        df = pd.DataFrame(columns=["valor"])
        df.index = pd.to_datetime(df.index, errors="coerce")
        return df

    # Leitura do CSV com tratamento de erros e valores ausentes
    try:
        df = pd.read_csv(StringIO(r.text), sep=';', decimal=',', na_values=['', 'NA', '-', '"'])
    except pd.errors.ParserError as e:
        print(f"[ERRO] Falha ao processar CSV da série {series_id}: {e}")
        df = pd.DataFrame(columns=["valor"])
        df.index = pd.to_datetime(df.index, errors="coerce")
        return df

    # Conversão de datas e filtro inicial
    df['data'] = pd.to_datetime(df['data'], dayfirst=True, errors='coerce')
    df.dropna(subset=['data'], inplace=True)
    df.set_index('data', inplace=True)
    df = df[df.index >= pd.to_datetime(start)]

    # Garantir índice como datetime, mesmo se vazio
    df.index = pd.to_datetime(df.index, errors="coerce")

    return df


# CDI
CDI = get_bcb_series(4391)
CDI.rename(columns={'valor': 'CDI'}, inplace=True)
CDI.index.name = "Data"
CDI_M = CDI.resample("ME").last()
CDI_M_Correto = CDI_M / 100

# IPCA
IPCA = get_bcb_series(433)
IPCA.rename(columns={'valor': 'IPCA'}, inplace=True)
IPCA.index.name = "Data"
IPCA_M = IPCA.resample("ME").last()
IPCA_M_Correto = IPCA_M / 100

# TR
TR = get_bcb_series(7811)
TR.rename(columns={'valor': 'TR'}, inplace=True)
TR.index.name = "Data"
TR_M = TR.resample("ME").last()
TR_M_Correto = TR_M / 100
