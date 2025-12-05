import duckdb
import pandas as pd
import os
from datetime import date

BANCO = "base.duckdb"
PASTA = "dados_parquet"

# Conecta (cria se não existir)
con = duckdb.connect(BANCO)

# Cria tabela caso não exista
con.execute("""
CREATE TABLE IF NOT EXISTS fundos (
    CNPJ STRING,
    DATA DATE,
    COTA DOUBLE,
    PATRIMONIO_LIQUIDO DOUBLE,
    CAPTACAO DOUBLE,
    RESGATES DOUBLE,
    NUMERO_COTISTAS DOUBLE
);
""")

# Ano/mes atual
hoje = date.today()
ano_mes = hoje.strftime("%Y%m")

arquivo = f"fundos_{ano_mes}.parquet"
caminho = os.path.join(PASTA, arquivo)

if not os.path.exists(caminho):
    raise FileNotFoundError(f"Arquivo mensal não encontrado: {caminho}")

print(f"Atualizando dados do mês {ano_mes}...")

# Remove dados apenas deste mês
con.execute(f"""
DELETE FROM fundos
WHERE strftime(DATA, '%Y%m') = '{ano_mes}';
""")

# Insere os novos dados do mês atual
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
FROM read_parquet('{caminho}');
""")

print("✔ Atualização concluída.")
print("✔ Arquivo base.duckdb criado/atualizado.")
