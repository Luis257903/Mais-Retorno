import duckdb
import os
import pandas as pd

# Caminho da pasta onde estão os PARQUETS
PASTA = "dados_parquet"

# Caminho do arquivo DuckDB final
BANCO = "base.duckdb"

# Remove banco antigo se existir (opcional)
if os.path.exists(BANCO):
    os.remove(BANCO)

# Conecta ao DuckDB
con = duckdb.connect(BANCO)

# Cria tabela vazia
con.execute("""
    CREATE TABLE fundos (
        CNPJ VARCHAR,
        DATA DATE,
        COTA DOUBLE,
        PATRIMONIO_LIQUIDO DOUBLE,
        CAPTACAO DOUBLE,
        RESGATES DOUBLE,
        NUMERO_COTISTAS DOUBLE
    );
""")

# Lê todos os arquivos .parquet
arquivos = [f for f in os.listdir(PASTA) if f.endswith(".parquet")]
arquivos.sort()

print(f"Total de arquivos encontrados: {len(arquivos)}")

for arq in arquivos:
    caminho = os.path.join(PASTA, arq)
    print(f"Importando: {caminho}")

    con.execute(f"""
        INSERT INTO fundos
        SELECT 
            CNPJ,
            DATA,
            COTA,
            "PATRIMÔNIO LÍQUIDO" AS PATRIMONIO_LIQUIDO,
            CAPTAÇÃO AS CAPTACAO,
            RESGATES,
            "NÚMERO DE COTISTAS" AS NUMERO_COTISTAS
        FROM read_parquet('{caminho}')
    """)

# Cria um índice lógico (DuckDB otimiza via zone maps)
con.execute("CREATE INDEX idx_fundos_cnpj_data ON fundos (CNPJ, DATA);")

print("✔ Banco criado com sucesso!")
