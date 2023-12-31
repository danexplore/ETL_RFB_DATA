import os
import pandas as pd
import PySimpleGUI as sg

diretorio_atual = os.getcwd()
print(f'Diretório atual: {diretorio_atual}\n')
dados_part_path = os.path.join(diretorio_atual, 'Dados_particionados')
dados_rfb_path = os.path.join(diretorio_atual, 'Dados_RFB')

arq_empresas = []
arq_estabelecimentos = []

for i, arquivo in enumerate(os.listdir(dados_rfb_path)):
    if 'Empre' in arquivo:
        arq_empresas.append(arquivo)
    elif 'Estabele' in arquivo:
        arq_estabelecimentos.append(arquivo)
    else:
        continue

print(f'Quantidade de arquivos de empresas: {len(arq_empresas)}\n'
      + f'Quantidade de arquivos de estabelecimentos {len(arq_estabelecimentos)}')

colunas_necessarias_empresas = [0,1,4,5]
colunas_necessarias_estabelecimentos = [0,3,4,5,6,11,12,19,20,27,30]

for i, arquivo in enumerate(arq_empresas):
    empresa_path = os.path.join(dados_rfb_path, arquivo)
    empresa_part_path = os.path.join(dados_part_path, arquivo)
    
    df_emp = pd.read_csv(empresa_path, sep=';', header=None, usecols=colunas_necessarias_empresas)
    df_emp.to_csv(empresa_part_path, sep=';', index=False, header=False)

for i, arquivo in enumerate(arq_estabelecimentos):
    estab_path = os.path.join(dados_rfb_path, arquivo)
    estab_part_path = os.path.join(dados_part_path, arquivo)
    
    df_emp = pd.read_csv(estab_path, sep=';', header=None, usecols=colunas_necessarias_estabelecimentos)
    df_emp.to_csv(estab_part_path, sep=';', index=False, header=False)

# To do ✅:
# Particionar arquivos de acordo com as colunas necessárias ✅
# armazenar os arquivos particionados na pasta 'Dados_particionados' ✅
# ler todos os arquivos e criar uma gui para filtrar por cnae e UF
# Inserir os dados filtrados em uma planihla excel.
# Usufruir do código!