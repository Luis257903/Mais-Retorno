import os
import pandas as pd
import pyarrow.parquet as pq

# Caminho seguro para a pasta
folder_path = os.path.join(os.path.dirname(__file__), "dados_parquet")

# Caminho completo do arquivo
file_path = os.path.join(folder_path, "fundos_202512.parquet")

# Verificação importante
if not os.path.exists(file_path):
    st.error(f"Arquivo não encontrado: {file_path}")
    raise FileNotFoundError(file_path)

# Carregar o arquivo
table = pq.read_table(file_path)
df = table.to_pandas()

# Converter a data
df["DATA"] = pd.to_datetime(df["DATA"])

# Input do CNPJ
cnpj_input = st.text_input("Digite o CNPJ do fundo:")

if cnpj_input:
    df_filtrado = df[df["CNPJ"] == cnpj_input].sort_values("DATA")

    if df_filtrado.empty:
        st.warning("Nenhum registro encontrado.")
    else:
        st.dataframe(df_filtrado.head(20))
