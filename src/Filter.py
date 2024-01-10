import os
import dask.dataframe as dd
from GetCnae import Selecionar_CNAE
from time import time
import pandas as pd

diretorio_atual = os.getcwd()
dados_part_path = os.path.join(diretorio_atual, 'Dados_particionados')

arq_empresas = [arquivo for arquivo in os.listdir(dados_part_path) if 'Empre' in arquivo]
arq_estabelecimentos = [arquivo for arquivo in os.listdir(dados_part_path) if 'Estabele' in arquivo]

num_colunas_estb = 11
nomes_colunas_est = {
    0: 'CNPJ Básico',
    1: 'Matriz/Filial',
    2: 'Nome Fantasia',
    3: 'Situação Cadastral',
    4: 'Data da Situação Cadastral',
    5: 'Cnae Principal',
    6: 'Cnae Secundário',
    7: 'UF',
    8:'Município',
    9: 'Correio Eletrônico',
    10: 'CNPJ Completo'
}

# Corrigindo o erro de alguns cnaes secundários
dtype = {'Cnae Secundário': 'object'}

municipios = pd.read_csv(diretorio_atual+'/municipios.csv',sep=';', index_col=0).to_dict()

# Leitura de Dataframess
dataframes = []
for estb in arq_estabelecimentos:
    df = dd.read_csv(os.path.join(dados_part_path, estb), sep=';', header=None,
                     names=[nomes_colunas_est[key] for key in range(num_colunas_estb)], dtype=dtype, encoding='utf-8')
    dataframes.append(df)

# Concatenando os Dframes
df_final = dd.concat(dataframes, axis=0)

# Removendo linhas sem E-mail
df_final = df_final.dropna(subset=['Correio Eletrônico'])

# Filtrando por CNAE
cnae = Selecionar_CNAE()
df_final = df_final.loc[df_final['Cnae Principal'] == cnae]

# Algumas substituições para melhor visualização do arquivo
df_final['Data da Situação Cadastral'] = dd.to_datetime(df_final['Data da Situação Cadastral'], format='%Y%m%d')
df_final['Data da Situação Cadastral'] = df_final['Data da Situação Cadastral'].dt.strftime('%d/%m/%Y')
df_final['Correio Eletrônico'] = df_final['Correio Eletrônico'].str.lower()
df_final['Matriz/Filial'] = df_final['Matriz/Filial'].map({1: 'Matriz', 2: 'Filial'}, na_action='ignore')


# Transformar o dask em pandas pode demorar relativamente, aproximadamente 80 segundos
s_t = time()
df_pandas = df_final.compute()
e_t = time()
print(f'tempo de duração: {(e_t - s_t):.2f}s')

to_folder_path = os.path.join(diretorio_atual, 'Test')
df_pandas.to_csv(to_folder_path + '/arquivo.csv', sep=';', index=True, header=True, encoding='utf-8')
