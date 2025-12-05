import pandas as pd
import quantstats as qs

fundos1 = pd.read_csv("dados/cad_fi_hist_denom_social.csv", encoding="latin1", sep=";")

df_filtrado = fundos1[fundos1['DT_FIM_DENOM_SOCIAL'].isna()]
fundos10 = df_filtrado[["CNPJ_FUNDO", "DENOM_SOCIAL"]]

fundos10.columns = ["CNPJ", "Nome"]

fundos7 = pd.read_csv("dados/extrato_fi.csv", encoding="latin1", sep=";")

fundos7_1 = fundos7[["CNPJ_FUNDO_CLASSE", "DENOM_SOCIAL"]]

fundos7_1.columns = ["CNPJ", "Nome"]

fundos_novos = fundos7_1[~fundos7_1["CNPJ"].isin(fundos10["CNPJ"])]
fundos2 = pd.concat([fundos10, fundos_novos], ignore_index=True)

fundos1 = pd.read_csv("dados/extrato_fi.csv", encoding="latin1", sep=";")

fundos3_1 = fundos1[["CNPJ_FUNDO_CLASSE", "CLASSE_ANBIMA"]]

fundos3_1.columns = ["CNPJ", "Classe"]

fundos3 = pd.merge(fundos3_1, fundos2[['CNPJ', 'Nome']], on='CNPJ', how='left')
