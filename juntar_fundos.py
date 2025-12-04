import pandas as pd
import glob
import os
import pyarrow as pa
import pyarrow.parquet as pq

pasta = "dados_parquet"
arquivos = glob.glob(os.path.join(pasta, "fundos_*.parquet"))

# Ordena corretamente
arquivos = sorted(
    arquivos,
    key=lambda x: int(os.path.basename(x).split("_")[1].split(".")[0])
)

saida = "fundos_completo.parquet"
writer = None

# Define os tipos padronizados
tipos_padrao = {
    "CNPJ": "string",
    "COTA": "float64",
    "PATRIMÔNIO LÍQUIDO": "float64",
    "CAPTAÇÃO": "float64",
    "RESGATES": "float64",
    "NÚMERO DE COTISTAS": "float64",  # PADRONIZA COMO FLOAT!!!
}

for arquivo in arquivos:
    print(f"Lendo: {arquivo}")

    df = pd.read_parquet(arquivo)

    # Ajustar DATA
    if "DATA" in df.columns:
        df["DATA"] = pd.to_datetime(df["DATA"])

    # PADRONIZAR TIPOS
    for coluna, tipo in tipos_padrao.items():
        if coluna in df.columns:
            df[coluna] = df[coluna].astype(tipo)

    # Ordena
    df = df.sort_values("DATA")

    # Remove index
    table = pa.Table.from_pandas(df, preserve_index=True)

    # Cria writer ou apenda
    if writer is None:
        writer = pq.ParquetWriter(saida, table.schema)

    writer.write_table(table)
    print(f"Adicionado ao parquet final: {arquivo}")

# Fechar writer
if writer:
    writer.close()

print("\n✔ Arquivo final gerado com sucesso:")
print(saida)
