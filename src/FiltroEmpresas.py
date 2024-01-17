import os

diretorio_atual = os.getcwd()
dados_part_path = os.path.join(diretorio_atual, 'Dados_particionados')
output_path = os.path.join(diretorio_atual, 'Saída')

arq_empresas = [arquivo for arquivo in os.listdir(dados_part_path) if 'Empre' in arquivo]

nomes_colunas_empre = ['CNPJ Básico', 'Razão Social', 'Capital Social', 'Porte da Empresa']

dtype = {'CNPJ Básico': 'object',
         'Porte da Empresa': 'object'}

nomes_colunas_est = ['CNPJ Básico', 'Matriz/Filial', 'Nome Fantasia', 'Situação Cadastral', 'Data da Situação Cadastral',
                     'Cnae Principal', 'UF', 'Município', 'Correio Eletrônico', 'CNPJ Completo']

# Leitura de Dataframess
import dask.dataframe as dd
from dask.diagnostics import ProgressBar as pbar
import pandas as pd

dataframes = [dd.read_csv(os.path.join(dados_part_path, empre), sep=';', header=None,
                          names=nomes_colunas_empre, encoding='utf-8', dtype=dtype)
                          for empre in arq_empresas]

# Concatenando os Dframes
df_empre = dd.concat(dataframes, axis=0)

# Alterando o dataType da coluna chave-primária para realizar o merge
df_empre['CNPJ Básico'] = df_empre['CNPJ Básico'].astype(pd.StringDtype())

# Lendo os estabelecimentos filtrados segundo o CNAE selecionado
df_estb_filtro = pd.read_csv(os.path.join(output_path + '/estb_filtrado.csv'), sep=';', encoding='utf-8')

# Alterando o dataType da coluna chave-primária para realizar o merge
df_estb_filtro['CNPJ Básico'] = df_estb_filtro['CNPJ Básico'].astype(pd.StringDtype())

# Merge das empresas para inserir a razão social dos estabelecimentos filtrados
# Esse é o único propósito do merge 
df_merged = df_empre.merge(df_estb_filtro, on=["CNPJ Básico"], how="inner")

# Removendo colunas inutilizadas para o meu propósito, caso necessite basta remover o nome da lista
df_merged = df_merged.drop(columns=['CNPJ Básico', 'Capital Social', 'Situação Cadastral', 'Cnae Principal', 'Matriz/Filial',
                                    'Data da Situação Cadastral', 'Município'])

# Filtrando as UFs que eu quero extrair, para carregar todas as UFs remova esta linha.
df_merged = df_merged[df_merged['UF'].isin(['AC','AP','DF','TO'])]

# Classificando por UF de ordem ascendente, ex.: 'TO', 'AP', 'DF', 'AC' para >> 'AC', 'AP', 'DF', 'TO'
df_merged = df_merged.sort(by='UF')

# COMPUTING
with pbar():
    df_merged = df_merged.compute()

output_end_file_csv = os.path.join(output_path + f'\\merged.csv')
output_end_file_xlsx = os.path.join(output_path + f'\\merged.xlsx')

# CSV das empresas
df_merged.to_csv(output_end_file_csv, sep=';', index=False, header=True, encoding='utf-8')

# XLSX das empresas
with pd.ExcelWriter(output_end_file_xlsx, engine='opnepyxl') as writer:
    df_merged.to_excel(excel_writer=writer, sheet_name='Merged', index=False, header=True, encoding='utf-8')
