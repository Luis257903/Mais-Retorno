import pandas as pd
import pyarrow.parquet as pq

# Caminho do arquivo (ajuste se necessário)
file_path = "fundos_202512.parquet"

# --- 1. Ler o arquivo parquet ---
table = pq.read_table(file_path)
df = table.to_pandas()

# Converter DATA para datetime (garante ordenação correta)
df["DATA"] = pd.to_datetime(df["DATA"])

# --- 2. Pedir o CNPJ ---
cnpj_input = input("Digite o CNPJ do fundo: ").strip()

# --- 3. Filtrar pelo CNPJ ---
df_filtrado = df[df["CNPJ"] == cnpj_input]

# --- 4. Ordenar por DATA ---
df_filtrado = df_filtrado.sort_values("DATA")

# --- 5. Mostrar resultado ---
if df_filtrado.empty:
    print("\n⚠ Nenhum registro encontrado para esse CNPJ.\n")
else:
    print("\nPrimeiras linhas do fundo selecionado:\n")
    print(df_filtrado.head(20).to_string(index=False))  # mostra até 20 linhas formatadas
