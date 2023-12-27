import os

diretorio_atual = os.getcwd()
print(f'Diretório atual: {diretorio_atual}\n')

arq_cnae = os.path.join(diretorio_atual, 'Dados_RFB', 'Cnae.csv')

arq_empresas = []
arq_estabelecimentos = []

for i, arquivo in enumerate(os.listdir(diretorio_atual + '/Dados_RFB')):
    if 'Empre' in arquivo:
        arq_empresas.append(arquivo)
    elif 'Estabele' in arquivo:
        arq_estabelecimentos.append(arquivo)
    else:
        continue

print(f'Quantidade de arquivos de empresas: {len(arq_empresas)}\n'
      + f'Quantidade de arquivos de estabelecimentos {len(arq_estabelecimentos)}')

# To do:
# Particionar arquivos de acordo com as colunas necessárias
# armazenar os arquivos particionados na pasta 'Dados_particionados'
# Depois Inserir no banco de dados Postgresql (psycopg2)
# Criar relacionamentos e excluir duplicados
# Postgre continue ==>