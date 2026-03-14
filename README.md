# Framework de Qualidade de Dados

Este repositório contém a implementação prática do Trabalho de Conclusão de Curso (TCC) do MBA em Engenharia de Software/Dados. O projeto consiste em uma abordagem genérica para avaliação e garantia de qualidade de dados (Data Quality) em arquiteturas de Big Data.

O estudo de caso utiliza dados públicos do **CNPJ (Receita Federal)** para demonstrar ingestão, tratamento e validação automatizada.

## Estrutura do Repositório

O projeto está modularizado nas seguintes etapas de engenharia de dados:

###  `data_ingestion_cnpj.py`
Responsável pela coleta dos dados brutos de Cadastro Nacional da Pessoa Jurídica [CNPJ]
- Realiza o download dos arquivos `.zip` do site da Receita Federal.
- Extrai e organiza os arquivos CSV na camada de entrada (Raw/Bronze) do Data Lake.
- Prepara o ambiente para o processamento distribuído.

###  `data_prep_cnpj.py`
Script de ETL (Extract, Transform, Load) e Modelagem.
- Lê os arquivos brutos (CSV com encoding `ISO-8859-1`).
- Aplica tratamento de tipagem e converte para `UTF-8`.
- Cria as tabelas **Delta Lake** seguindo o modelo dimensional (Star Schema):
  - **Fatos:** Empresas, Estabelecimentos, Sócios.
  - **Dimensões:** Municípios, CNAEs, Natureza Jurídica, etc.
- Implementa particionamento para otimização de leitura.

###  `data_prep_arrecadacao.py`
Script de ETL (Extract, Transform, Load) e Modelagem.
- Lê os arquivos brutos (CSV com encoding `ISO-8859-1`).
- Aplica tratamento de tipagem e converte para `UTF-8`.
- Cria as tabelas **Delta Lake** seguindo o modelo dimensional (Star Schema):
  - **Fatos:** Tabela de Arrecadação, IR e IPI

### `data_prep_hospitais.py`
Script de ETL (Extract, Transform, Load) e Modelagem.
- Lê os arquivos brutos (CSV com encoding `ISO-8859-1`).
- Aplica tratamento de tipagem e converte para `UTF-8`.
- Cria as tabelas **Delta Lake** seguindo o modelo dimensional (Star Schema):
  - **Fatos:** Leitos hospitalares


### `dataquality.py`
Módulo core de Governança e Qualidade.
- Contém a classe `MonitoramentoQualidade`.
- Aplica regras de validação automatizadas (Completude, Unicidade, Consistência).
- Persiste os logs de execução na tabela de auditoria `qualidade_dados`, permitindo o monitoramento histórico da saúde do pipeline.

## Tecnologias Utilizadas

* **Plataforma:** Databricks
* **Processamento:** Apache Spark (PySpark)
* **Armazenamento:** Delta Lake
* **Linguagem:** Python & SQL
* **Versonamento de código:** Git e GitHub.

## Como Executar

1.  Clone este repositório no seu Databricks Repos.
2.  Execute o `data_ingestion.py` para baixar a carga inicial.
3.  Execute o `data_prep.py` para criar as tabelas Fato e Dimensão.
4.  Importe a classe do `dataquality.py` em seus notebooks para rodar as validações e gerar os logs de qualidade.
