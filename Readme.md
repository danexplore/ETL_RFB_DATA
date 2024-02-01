# Projeto ETL - Receita Federal do Brasil

Este projeto realiza a Extração, Transformação e Carga (ETL) dos dados públicos da Receita Federal do Brasil, visando proporcionar uma visualização fácil e intuitiva, simplificando o acesso aos dados.

## Funcionalidades
* **Extrair:** Extração dos dados obtidos da RFB.

* **Transformar:** Transformar os dados brutos em dados limpos e 

* **Carregar:** Apresenta os dados de maneira amigável, utilizando tabelas para facilitar a utilização.

## Requisitos do Sistema
* Python (versão utilizada 3.12)

* Bibliotecas Python específicas listadas no arquivo **requirements.txt**

## Fontes
* Fonte oficial da Receita Federal do Brasil: [SITE](https://dados.gov.br/dados/conjuntos-dados/cadastro-nacional-da-pessoa-juridica---cnpj)
* Fonte separada para download dos arquivos: [SITE](https://dadosabertos.rfb.gov.br/CNPJ/)
* Faça o download dos `Dados_particionados` no link (Senha para acesso: 1234): [Download](https://www.transfernow.net/dl/202402019GMwmeVX)

#### Arquivos baixados:
* Cnaes.zip
* Empresas{0-9}.zip
* Estabelecimentos{0-9}.zip
* Layout dos dados (para compreender e utilizar os arquivos)

## Inspiração
Este projeto foi inspirado por outro projeto mais abrangente, desenvolvido pelo Cientista de Dados Allan Batista Martins. Você pode conferir o projeto original no seguinte **[Github](https://github.com/allanbmartins/Projeto_ETL_RFB_IBGE_ANP)**

Utilizei a estrutura desse projeto como base, aproveitando a função split_csv_pandas_todos para dividir os arquivos em partes. Estou continuando este projeto de forma pessoal para adaptar os dados de acordo com as minhas necessidades.

### Resultado
Após aplicar os filtros e realizar a operação de merge, apresento o resultado final. O programa tem uma média de conclusão de 2-3 minutos devido à execução do `dask.DataFrame.compute()`, sendo que cada operação de compute leva em média 85 segundos para ser concluída.

![image](https://github.com/danexplore/ETL_RFB_DATA/assets/74932150/e545cf39-cb4c-43f3-a299-5ea22a04f6db)
