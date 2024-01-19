# Projeto ETL - Receita Federal do Brasil

Este projeto realiza a extração, transformação e carga (ETL) dos dados públicos da Receita Federal do Brasil. O objetivo é fornecer uma visualização fácil e intuitiva, e de maneira simples a utilização dos dados.

## Funcionalidades
* **Extrair:** Extração dos dados obtidos da RFB.

* **Transformar:** Transformar os dados brutos em dados necessários para utilização pessoal.

* **Carregar:** Apresentar os dados de forma amigável, utilizando tabelas, facilitando o uso dos dados.

## Requisitos do Sistema
* Python (versão utilizada 3.12)

* Bibliotecas Python específicas listadas no arquivo **requirements.txt**

## Fontes
* Fonte oficial da Receita Federal do Brasil: [SITE](https://dados.gov.br/dados/conjuntos-dados/cadastro-nacional-da-pessoa-juridica---cnpj)
* Fonte separada para download dos arquivos: [SITE](https://dadosabertos.rfb.gov.br/CNPJ/)

#### Arquivos baixados:
* Cnaes.zip
* Empresas{0-9}.zip
* Estabelecimentos{0-9}.zip
* Layout dos dados (para compreender e utilizar os arquivos)

## Inspiração
Esse projeto foi inspirado por outro projeto, porém completo feito pelo Cientista de Dados Allan Batista Martins: **[Github com o projeto original](https://github.com/allanbmartins/Projeto_ETL_RFB_IBGE_ANP)**

Eu utilizei o seu projeto para separar os arquivos em partes, usando a função split_csv_pandas_todos, pois estava indeciso de como iniciar o projeto.
estarei dando continuidade neste projeto pessoal para transformar os dados segundo a minha necessidade.

### Resultado
Após os filtros e merge, aqui está o resultado, o programa demora em média 2-3 minutos para finalizar, por conta do dask.DataFrame.compute(), cada compute demora em média 85 segundos para concluir.

![image](https://github.com/danexplore/ETL_RFB_DATA/assets/74932150/e545cf39-cb4c-43f3-a299-5ea22a04f6db)
