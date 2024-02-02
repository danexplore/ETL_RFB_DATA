import os
import pandas as pd

"""
Obs.: Todos os dados particionados estão disponíveis para download.
Total de 2.2 gb aproximadamente, faça o download deles e extraia na pasta `Dados_particionados`
"""

diretorio_atual = os.getcwd()
print(f'Diretório atual: {diretorio_atual}\n')

# Nessa pasta, insira todos os downloads da Receita Federal, fornecida no readme.md
dados_rfb_path = os.path.join(diretorio_atual, 'Dados_RFB')

# Nessa pasta será inserido todos os arquivos particionados pelo código.
dados_part_path = os.path.join(diretorio_atual, 'Dados_particionados')

# Cria a pasta caso não exista
if not os.path.exists(dados_part_path):
    os.mkdir(dados_part_path)


# Contagem
arq_empresas = [empre for empre in os.listdir(dados_rfb_path) if 'Empre' in empre]
arq_estabelecimentos = [estb for estb in os.listdir(dados_rfb_path) if 'Estabele' in estb]

print(f'Quantidade de arquivos de empresas: {len(arq_empresas)}\n'
      + f'Quantidade de arquivos de estabelecimentos {len(arq_estabelecimentos)}')

# Verifique na pasta extras, que identificam as colunas necessesárias.
colunas_necessarias_empresas = [0,1,4,5]
colunas_necessarias_estabelecimentos = [0,3,4,5,6,11,12,19,20,27,30]

# Esse particionamento remove as colunas desnecessárias reduzindo a memória do arquivo, para melhor utilização.

# Particionamento empresas.
for i, arquivo in enumerate(arq_empresas):
    empresa_path = os.path.join(dados_rfb_path, arquivo)
    empresa_part_path = os.path.join(dados_part_path, arquivo)
    
    df_emp = pd.read_csv(empresa_path, sep=';', header=None, usecols=colunas_necessarias_empresas)
    df_emp.to_csv(empresa_part_path, sep=';', index=False, header=False)

# Particionamento estabelecimentos.
for i, arquivo in enumerate(arq_estabelecimentos):
    estab_path = os.path.join(dados_rfb_path, arquivo)
    estab_part_path = os.path.join(dados_part_path, arquivo)
    
    df_emp = pd.read_csv(estab_path, sep=';', header=None, usecols=colunas_necessarias_estabelecimentos)
    df_emp.to_csv(estab_part_path, sep=';', index=False, header=False)
