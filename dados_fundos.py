import requests
import pandas as pd
import zipfile
import io
import os

# Lista manual de meses (202101 a 202311)
meses = [202512]

# Pasta de saída
os.makedirs("dados_parquet", exist_ok=True)

# Loop mês a mês (NÃO ARMAZENA TUDO NA MEMÓRIA)
for mes in meses:
    print(f"Baixando e processando: {mes}")

    url = f"https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_{mes}.zip"
    
    try:
        r = requests.get(url)
        r.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print(f"Erro ao baixar {mes}: {e}")
        continue

    with zipfile.ZipFile(io.BytesIO(r.content)) as z:
        nome_csv = [n for n in z.namelist() if n.lower().endswith(".csv")][0]

        # Nome final do arquivo parquet por mês
        nome_parquet = f"dados_parquet/fundos_{mes}.parquet"

        # Criar lista para armazenar chunks temporários
        lista_chunks = []

        with z.open(nome_csv) as f:
            # Ler em chunks SEM estourar memória
            for chunk in pd.read_csv(
                f,
                sep=";",
                decimal=",",
                dtype={"CNPJ_FUNDO": str},
                parse_dates=["DT_COMPTC"],
                chunksize=400_000,
                low_memory=False
            ):
                # Renomeia e limpa
                chunk = chunk.rename(columns={
                    "CNPJ_FUNDO": "CNPJ",
                    "DT_COMPTC": "DATA",
                    "VL_QUOTA": "COTA",
                    "VL_PATRIM_LIQ": "PATRIMÔNIO LÍQUIDO",
                    "CAPTC_DIA": "CAPTAÇÃO",
                    "RESG_DIA": "RESGATES",
                    "NR_COTST": "NÚMERO DE COTISTAS"
                })

                # Remove colunas não utilizadas
                drop_cols = ['TP_FUNDO', 'ID_SUBCLASSE', 'VL_TOTAL']
                chunk = chunk.drop(columns=drop_cols, errors='ignore')

                # Converte DATA
                chunk["DATA"] = pd.to_datetime(chunk["DATA"])

                chunk["COTA"] = pd.to_numeric(chunk["COTA"], errors='coerce')

                chunk["PATRIMÔNIO LÍQUIDO"] = pd.to_numeric(chunk["PATRIMÔNIO LÍQUIDO"], errors='coerce')

                chunk["CAPTAÇÃO"] = pd.to_numeric(chunk["CAPTAÇÃO"], errors='coerce')

                chunk["RESGATES"] = pd.to_numeric(chunk["RESGATES"], errors='coerce')

                chunk["NÚMERO DE COTISTAS"] = pd.to_numeric(chunk["NÚMERO DE COTISTAS"], errors='coerce')
                
                # Armazena o chunk já processado
                lista_chunks.append(chunk)

        # Concatena APENAS os chunks daquele mês
        if lista_chunks:
            df_mes = pd.concat(lista_chunks)
            # Salva um PARQUET por mês
            df_mes.to_parquet(nome_parquet, index=True)
            print(f"Salvo: {nome_parquet}")
        else:
             print(f"Nenhum dado encontrado para {mes}")

print("Processo concluído!")
