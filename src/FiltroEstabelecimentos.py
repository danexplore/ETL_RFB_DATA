import os
from GetCnae import get_cnaes_number as get_cnaes

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
print(f'Cnaes selecionados: {[cnae for cnae in cnaes]}\n')
df_final = df_final[df_final['Cnae Principal'].isin(cnaes)]

# Remove e-mails duplicados de filiais e mantém somente o email da empresa matriz.
df_final = df_final.drop_duplicates(subset=['Correio Eletrônico'], keep='first')

# Transforma todos os e-mails em minúsuculo
df_final['Correio Eletrônico'] = df_final['Correio Eletrônico'].str.lower()

# Função para verificar se a linha contém um padrão indesejado
def contem_padrao_indesejado(email):
    padroes_indesejados = ['contab', '@bol', '@uol', '@globo', '@ig', '@msn', '@terra', '@brturbo', 'contador', 'xxx',
                           '000', '***', ',']
    for padrao in padroes_indesejados:
        if padrao in email:
            return True
        if '@' not in email:
            return True
    return False

df_final = df_final[~df_final['Correio Eletrônico'].apply(contem_padrao_indesejado, meta=('x', 'bool'))]

# Reset index após filtros
df_final = df_final.reset_index(drop=True)

with ProgressBar():
    df_final = df_final.persist()

# Transformação para data
df_final['Data da Situação Cadastral'] = dd.to_datetime(df_final['Data da Situação Cadastral'], format='%Y%m%d')

# Modelo de data utilizado 'dd/mm/yyyy'
df_final['Data da Situação Cadastral'] = df_final['Data da Situação Cadastral'].dt.strftime('%d/%m/%Y')

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
run(['python', 'src\\FiltroEmpresas.py'], shell=True)
