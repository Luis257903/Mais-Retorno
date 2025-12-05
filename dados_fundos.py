import requests
import pandas as pd
import zipfile
import io
import os
import datetime as dt

# ============================================================
#  CRIA PASTA DE ARMAZENAMENTO
# ============================================================
os.makedirs("dados_parquet", exist_ok=True)


# ============================================================
# 1) DETECTA ÚLTIMO ARQUIVO DISPONÍVEL
# ============================================================
def obter_ultimo_mes_existente():
    arquivos = os.listdir("dados_parquet")
    meses = []

    for f in arquivos:
        if f.startswith("fundos_") and f.endswith(".parquet"):
            try:
                num = int(f.replace("fundos_", "").replace(".parquet", ""))
                meses.append(num)
            except:
                pass

    if not meses:
        return None

    return max(meses)


# ============================================================
# 2) GERA LISTA DE MESES QUE DEVEM SER BAIXADOS
# ============================================================
def gerar_lista_meses():
    hoje = dt.date.today()
    mes_atual = hoje.year * 100 + hoje.month  # ex: 202512

    ultimo = obter_ultimo_mes_existente()

    # 1 — Nenhum arquivo ainda → baixa só o mês atual
    if ultimo is None:
        return [mes_atual]

    meses = []

    # 2 — Sempre baixar o mês atual (para substituir diariamente)
    meses.append(mes_atual)

    # 3 — Se virou o mês e existem meses faltantes (ex: 202512 → 202601)
    if mes_atual > ultimo:
        ano = ultimo // 100
        mes = ultimo % 100

        while True:
            mes += 1
            if mes == 13:
                mes = 1
                ano += 1

            novo_mes = ano * 100 + mes
            meses.append(novo_mes)

            if novo_mes == mes_atual:
                break

    # Remove duplicatas e ordena
    meses = sorted(list(set(meses)))

    return meses


# ============================================================
#  LISTA FINAL DOS MESES QUE SERÃO BAIXADOS
# ============================================================
meses = gerar_lista_meses()
print("Meses que serão processados:", meses)


# ============================================================
# 3) DOWNLOAD + CONVERSÃO PARA PARQUET
# ============================================================
for mes in meses:
    print(f"\n🔽 Baixando e processando: {mes}")

    url = f"https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_{mes}.zip"

    try:
        r = requests.get(url)
        r.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print(f"❌ Erro ao baixar {mes}: {e}")
        continue

    with zipfile.ZipFile(io.BytesIO(r.content)) as z:

        # encontra o csv dentro do ZIP
        nome_csv = [n for n in z.namelist() if n.lower().endswith(".csv")][0]

        nome_parquet = f"dados_parquet/fundos_{mes}.parquet"

        lista_chunks = []

        with z.open(nome_csv) as f:

            for chunk in pd.read_csv(
                f,
                sep=";",
                decimal=",",
                dtype={"CNPJ_FUNDO": str},
                parse_dates=["DT_COMPTC"],
                chunksize=400_000,
                low_memory=False
            ):

                # Renomear colunas
                chunk = chunk.rename(columns={
                    "CNPJ_FUNDO_CLASSE": "CNPJ",
                    "DT_COMPTC": "DATA",
                    "VL_QUOTA": "COTA",
                    "VL_PATRIM_LIQ": "PATRIMÔNIO LÍQUIDO",
                    "CAPTC_DIA": "CAPTAÇÃO",
                    "RESG_DIA": "RESGATES",
                    "NR_COTST": "NÚMERO DE COTISTAS",
                })

                # Remove colunas inúteis
                chunk = chunk.drop(columns=['TP_FUNDO_CLASSE', 'ID_SUBCLASSE', 'VL_TOTAL'], errors='ignore')

                # Converte formatos
                chunk["DATA"] = pd.to_datetime(chunk["DATA"], errors='coerce')

                chunk["COTA"] = pd.to_numeric(chunk["COTA"], errors='coerce')
                chunk["PATRIMÔNIO LÍQUIDO"] = pd.to_numeric(chunk["PATRIMÔNIO LÍQUIDO"], errors='coerce')
                chunk["CAPTAÇÃO"] = pd.to_numeric(chunk["CAPTAÇÃO"], errors='coerce')
                chunk["RESGATES"] = pd.to_numeric(chunk["RESGATES"], errors='coerce')

                # NÚMERO DE COTISTAS deve SEMPRE ser string
                chunk["NÚMERO DE COTISTAS"] = chunk["NÚMERO DE COTISTAS"].astype(str)

                # Remover totalmente qualquer __index_level_0__
                if "__index_level_0__" in chunk.columns:
                    chunk = chunk.drop(columns=["__index_level_0__"])

                lista_chunks.append(chunk)

        if lista_chunks:
            df_mes = pd.concat(lista_chunks, ignore_index=True)

            # Garante coluna DATE ordenada
            df_mes = df_mes.sort_values("DATA")

            # Remove qualquer índice estranho
            df_mes = df_mes.reset_index(drop=True)

            df_mes.to_parquet(nome_parquet, index=False)
            print(f"✅ Salvo: {nome_parquet}")
        else:
            print(f"⚠ Nenhum dado encontrado para o mês {mes}")

print("\n🎉 Processo concluído com sucesso!")
