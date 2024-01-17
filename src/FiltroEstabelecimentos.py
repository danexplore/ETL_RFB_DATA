import os
from GetCnae import main as get_cnaes

diretorio_atual = os.getcwd()
dados_part_path = os.path.join(diretorio_atual, 'Dados_particionados')

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

# Leitura dos múnicipios 'Código': 'Município'
import pandas as pd
municipios = municipios = pd.read_csv('municipios.txt', sep=':', header=None, names=['ID', 'Nome'], index_col=0).squeeze().to_dict()

# Leitura de Dataframess
import dask.dataframe as dd
from dask.diagnostics import ProgressBar
dataframes = [] 
for estb in arq_estabelecimentos:
    df = dd.read_csv(os.path.join(dados_part_path, estb), sep=';', header=None,
                     names=[nomes_colunas_est[key] for key in range(num_colunas_estb)], dtype=dtype, encoding='utf-8')
    dataframes.append(df)

# Concatenando os Dframes
df_final = dd.concat(dataframes, axis=0)

# Removendo linhas sem E-mail
df_final = df_final.dropna(subset=['Correio Eletrônico'])

# Selecionando todas as empresas ativas: 2
df_final = df_final.loc[df_final['Situação Cadastral'] == 2]

# Filtrando por CNAE
cnaes = get_cnaes()
df_final = df_final[df_final['Cnae Principal'].isin(cnaes)]

# Remove e-mails duplicados de filiais e mantém somente o email da empresa matriz.
df_final = df_final.sort(by='Matriz/Filial')
df_final = df_final.drop_duplicates(subset=['Correio Eletrônico'], keep='first')

# Remove todos os e-mails contendo os seguintes complementos ou domínios que não utilizo, também e-mails invalidos e de contábeis.
lista_exclusao = ['contab', '@bol', '@uol', '@globo', '@ig', '@msn', '@terra', '@brturbo', 'xxx', '000000', ',']

# Os e-mails são removidos ao usar o metodo drop com a lista dos indicies de cada linha que contém os e-mails inválidos.
indices = [x for x in df_final['Correio Eletrônico'].index if x in lista_exclusao]
df_final = df_final['Correio Eletrônico'].drop(labels=indices, axis=0)

# Reset index após filtros
df_final = df_final.reset_index(drop=True)

with ProgressBar():
    df_final = df_final.persist()

# Transformação para data
df_final['Data da Situação Cadastral'] = dd.to_datetime(df_final['Data da Situação Cadastral'], format='%Y%m%d')

# Modelo de data utilizado 'dd/mm/yyyy'
df_final['Data da Situação Cadastral'] = df_final['Data da Situação Cadastral'].dt.strftime('%d/%m/%Y')

# E-mails para minúsculo
df_final['Correio Eletrônico'] = df_final['Correio Eletrônico'].str.lower()

# Identificação de Matrizes e Filiais
df_final['Matriz/Filial'] = df_final['Matriz/Filial'].map({1: 'Matriz', 2: 'Filial'}, na_action='ignore')

# Identificação de Munícipios pelos códigos TOM_IBGE
df_final['Município'] = df_final['Município'].map(municipios, na_action='ignore')

# Transformar o dask em pandas pode demorar relativamente, aproximadamente 80 segundos
with ProgressBar():
    df_pandas = df_final.compute()

to_folder_path = os.path.join(diretorio_atual, 'Saída')
file_name_path = '/estb_filtrado.csv'
df_pandas.to_csv(to_folder_path + file_name_path, sep=';', index=False, header=True, encoding='utf-8')

from subprocess import run
run(['python', 'src\\Filter_empresas.py'], shell=True)
