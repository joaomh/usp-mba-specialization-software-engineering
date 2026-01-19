# Databricks notebook source
# MAGIC %md
# MAGIC ## Tabelas Dimensões

# COMMAND ----------

# MAGIC %md
# MAGIC ### Classificação Nacional de Atividades Econômicas

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark.sql(f"""
CREATE OR REPLACE TABLE dim_cnae
USING DELTA
COMMENT 'Tabela Dimensão de CNAEs - Classificação Nacional de Atividades Econômicas'
AS
SELECT
    CAST(_c0 AS STRING) AS cd_cnae,
    CAST(_c1 AS STRING) AS ds_cnae
FROM read_files(
    '/Volumes/workspace/default/raw/dim_cnae/*.CNAECSV',
    format => 'csv',
    delimiter => ';',
    header => 'false',
    encoding => 'ISO-8859-1'
);
""")
# 2. Adiciona os comentários nas colunas para documentação
spark.sql("ALTER TABLE dim_cnae ALTER COLUMN cd_cnae COMMENT 'CÓDIGO DA ATIVIDADE ECONÔMICA'")
spark.sql("ALTER TABLE dim_cnae ALTER COLUMN ds_cnae COMMENT 'NOME DA ATIVIDADE ECONÔMICA'")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Situação Cadastral

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark.sql(f"""
CREATE OR REPLACE TABLE dim_motivo_situacao_cadastral
USING DELTA
COMMENT 'Tabela Dimensão de Motivos da Situação Cadastral - Dados Abertos CNPJ'
AS
SELECT
    CAST(_c0 AS STRING) AS cod_motivo_situacao,
    CAST(_c1 AS STRING) AS desc_motivo_situacao
FROM read_files(
    '/Volumes/workspace/default/raw/dim_motivo_situacao/*.MOTICSV',
    format => 'csv',
    delimiter => ';',
    header => 'false',
    encoding => 'ISO-8859-1'
);
""")
# 2. Adiciona os comentários nas colunas para documentação
spark.sql("ALTER TABLE dim_motivo_situacao_cadastral ALTER COLUMN cod_motivo_situacao COMMENT 'CÓDIGO DO MOTIVO DA SITUAÇÃO CADASTRAL'")
spark.sql("ALTER TABLE dim_motivo_situacao_cadastral ALTER COLUMN desc_motivo_situacao COMMENT 'DESCRIÇÃO DO MOTIVO DA SITUAÇÃO CADASTRAL'")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Municípios

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# 1. Cria a tabela e carrega os dados
spark.sql(f"""
CREATE OR REPLACE TABLE dim_municipio
USING DELTA
COMMENT 'Tabela Dimensão de Municípios - Dados Abertos CNPJ'
AS
SELECT
    CAST(_c0 AS STRING) AS cod_municipio,
    CAST(_c1 AS STRING) AS nome_municipio
FROM read_files(
    '/Volumes/workspace/default/raw/dim_municipios/*.MUNICCSV',
    format => 'csv',
    delimiter => ';',
    header => 'false',
    encoding => 'ISO-8859-1'
)
""")

# 2. Adiciona os comentários nas colunas para documentação
spark.sql("ALTER TABLE dim_municipio ALTER COLUMN cod_municipio COMMENT 'CÓDIGO DO MUNICÍPIO'")
spark.sql("ALTER TABLE dim_municipio ALTER COLUMN nome_municipio COMMENT 'NOME DO MUNICÍPIO'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Natureza Jurídica

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# 1. Cria a tabela e carrega os dados
spark.sql(f"""
CREATE OR REPLACE TABLE dim_natureza_juridica
USING DELTA
COMMENT 'Tabela Dimensão de Naturezas Jurídicas - Dados Abertos CNPJ'
AS
SELECT
    CAST(_c0 AS STRING) AS cod_natureza_juridica,
    CAST(_c1 AS STRING) AS nome_natureza_juridica
FROM read_files(
    '/Volumes/workspace/default/raw/dim_natureza_juridica/*.NATJUCSV',
    format => 'csv',
    delimiter => ';',
    header => 'false',
    encoding => 'ISO-8859-1'
)
""")

# 2. Adiciona os comentários nas colunas (Documentação)
spark.sql("ALTER TABLE dim_natureza_juridica ALTER COLUMN cod_natureza_juridica COMMENT 'CÓDIGO DA NATUREZA JURÍDICA'")
spark.sql("ALTER TABLE dim_natureza_juridica ALTER COLUMN nome_natureza_juridica COMMENT 'NOME DA NATUREZA JURÍDICA'")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Países

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# 1. Cria a tabela e carrega os dados
spark.sql(f"""
CREATE OR REPLACE TABLE dim_pais
USING DELTA
COMMENT 'Tabela Dimensão de Países - Dados Abertos CNPJ'
AS
SELECT
    CAST(_c0 AS STRING) AS cod_pais,
    CAST(_c1 AS STRING) AS nome_pais
FROM read_files(
    '/Volumes/workspace/default/raw/dim_pais/*.PAISCSV',
    format => 'csv',
    delimiter => ';',
    header => 'false',
    encoding => 'ISO-8859-1'
)
""")

# 2. Adiciona os comentários nas colunas conforme a imagem
spark.sql("ALTER TABLE dim_pais ALTER COLUMN cod_pais COMMENT 'CÓDIGO DO PAÍS'")
spark.sql("ALTER TABLE dim_pais ALTER COLUMN nome_pais COMMENT 'NOME DO PAÍS'")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Qualificação Sócios

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# 1. Cria a tabela e carrega os dados
spark.sql(f"""
CREATE OR REPLACE TABLE dim_qualificacao_socio
USING DELTA
COMMENT 'Tabela Dimensão de Qualificação de Sócios - Dados Abertos CNPJ'
AS
SELECT
    CAST(_c0 AS STRING) AS cod_qualificacao_socio,
    CAST(_c1 AS STRING) AS nome_qualificacao_socio
FROM read_files(
    '/Volumes/workspace/default/raw/dim_qualificacao_socio/*.QUALSCSV',
    format => 'csv',
    delimiter => ';',
    header => 'false',
    encoding => 'ISO-8859-1'
)
""")

# 2. Adiciona os comentários nas colunas conforme a imagem
spark.sql("ALTER TABLE dim_qualificacao_socio ALTER COLUMN cod_qualificacao_socio COMMENT 'CÓDIGO DA QUALIFICAÇÃO DO SÓCIO'")
spark.sql("ALTER TABLE dim_qualificacao_socio ALTER COLUMN nome_qualificacao_socio COMMENT 'NOME DA QUALIFICAÇÃO DO SÓCIO'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tabelas Fatos
# MAGIC ### Fato Empresa

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# 1. Criação da Tabela Fato (CTAS)
spark.sql(f"""
CREATE OR REPLACE TABLE fato_empresas
USING DELTA
PARTITIONED BY (anomes)
COMMENT 'Tabela Fato de Empresas - Dados Abertos CNPJ'
AS
SELECT
    CAST(_c0 AS STRING) AS cd_cnpj_basico,
    CAST(_c1 AS STRING) AS nm_razao_social,
    CAST(_c2 AS INT) AS cd_natureza_juridica,
    CAST(CAST(REPLACE(_c3, ',', '.') AS DECIMAL(10,2)) AS INT) AS cd_qualificacao_responsavel,
    CAST(REPLACE(_c4, ',', '.') AS DECIMAL(18,2)) AS vl_capital_social,
    CAST(_c5 AS STRING) AS cd_porte_empresa,
    CAST(_c6 AS STRING) AS ds_ente_federativo,
    
    -- Coluna de particionamento (exemplo estático)
    '202512' AS anomes

FROM read_files(
    '/Volumes/workspace/default/raw/fact_empresas/202512/*.EMPRECSV',
    format => 'csv',
    delimiter => ';',
    header => 'false',
    encoding => 'ISO-8859-1'
)
""")

# 2. Documentação: Adiciona os comentários nas colunas conforme o layout oficial
spark.sql("ALTER TABLE fato_empresas ALTER COLUMN cd_cnpj_basico COMMENT 'NÚMERO BASE DE INSCRIÇÃO NO CNPJ (OITO PRIMEIROS DÍGITOS DO CNPJ)'")
spark.sql("ALTER TABLE fato_empresas ALTER COLUMN nm_razao_social COMMENT 'NOME EMPRESARIAL DA PESSOA JURÍDICA'")
spark.sql("ALTER TABLE fato_empresas ALTER COLUMN cd_natureza_juridica COMMENT 'CÓDIGO DA NATUREZA JURÍDICA'")
spark.sql("ALTER TABLE fato_empresas ALTER COLUMN cd_qualificacao_responsavel COMMENT 'QUALIFICAÇÃO DA PESSOA FÍSICA RESPONSÁVEL PELA EMPRESA'")
spark.sql("ALTER TABLE fato_empresas ALTER COLUMN vl_capital_social COMMENT 'CAPITAL SOCIAL DA EMPRESA'")
spark.sql("ALTER TABLE fato_empresas ALTER COLUMN cd_porte_empresa COMMENT 'CÓDIGO DO PORTE DA EMPRESA: 00 – NÃO INFORMADO, 01 – MICRO EMPRESA, 03 - EMPRESA DE PEQUENO PORTE, 05 - DEMAIS'")
spark.sql("ALTER TABLE fato_empresas ALTER COLUMN ds_ente_federativo COMMENT 'ENTE FEDERATIVO RESPONSÁVEL (PREENCHIDO APENAS PARA NATUREZA JURÍDICA 1XXX)'")
spark.sql("ALTER TABLE fato_empresas ALTER COLUMN anomes COMMENT 'ANO E MÊS DE REFERÊNCIA DA CARGA'")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fato Empresas - Nova Partição

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark.sql(f"""
INSERT INTO fato_empresas
SELECT
    CAST(_c0 AS STRING) AS cd_cnpj_basico,
    CAST(_c1 AS STRING) AS nm_razao_social,
    CAST(_c2 AS INT) AS cd_natureza_juridica,
    CAST(CAST(REPLACE(_c3, ',', '.') AS DECIMAL(10,2)) AS INT) AS cd_qualificacao_responsavel,
    CAST(REPLACE(_c4, ',', '.') AS DECIMAL(18,2)) AS vl_capital_social,
    CAST(_c5 AS STRING) AS cd_porte_empresa,
    CAST(_c6 AS STRING) AS ds_ente_federativo,
    
    -- Valor da NOVA partição
    '202511' AS anomes

FROM read_files(
    '/Volumes/workspace/default/raw/fact_empresas/202511/*.EMPRECSV', -- Caminho da nova pasta
    format => 'csv',
    delimiter => ';',
    header => 'false',
    encoding => 'ISO-8859-1'
)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fato Estabelecimento

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# 1. Criação da Tabela Fato (CTAS)
spark.sql(f"""
CREATE OR REPLACE TABLE fato_estabelecimentos
USING DELTA
PARTITIONED BY (anomes)
COMMENT 'Tabela Fato de Estabelecimentos - Dados Abertos CNPJ'
AS
SELECT
    CAST(_c0 AS STRING) AS cd_cnpj_basico,
    CAST(_c1 AS STRING) AS cd_cnpj_ordem,
    CAST(_c2 AS STRING) AS cd_cnpj_dv,
    CAST(_c3 AS STRING) AS cd_matriz_filial,
    CAST(_c4 AS STRING) AS nm_fantasia,
    CAST(_c5 AS STRING) AS cd_situacao_cadastral,
    CAST(_c6 AS STRING) AS dt_situacao_cadastral,
    CAST(_c7 AS STRING) AS cd_motivo_situacao_cadastral,
    CAST(_c8 AS STRING) AS nm_cidade_exterior,
    CAST(_c9 AS STRING) AS cd_pais,
    CAST(_c10 AS STRING) AS dt_inicio_atividade,
    CAST(_c11 AS STRING) AS cd_cnae_principal,
    CAST(_c12 AS STRING) AS cd_cnae_secundaria,
    CAST(_c13 AS STRING) AS ds_tipo_logradouro,
    CAST(_c14 AS STRING) AS nm_logradouro,
    CAST(_c15 AS STRING) AS nr_logradouro,
    CAST(_c16 AS STRING) AS ds_complemento,
    CAST(_c17 AS STRING) AS nm_bairro,
    CAST(_c18 AS STRING) AS nr_cep,
    CAST(_c19 AS STRING) AS sg_uf,
    CAST(_c20 AS STRING) AS cd_municipio,
    CAST(_c21 AS STRING) AS nr_ddd1,
    CAST(_c22 AS STRING) AS nr_telefone1,
    CAST(_c23 AS STRING) AS nr_ddd2,
    CAST(_c24 AS STRING) AS nr_telefone2,
    CAST(_c25 AS STRING) AS nr_ddd_fax,
    CAST(_c26 AS STRING) AS nr_fax,
    CAST(LOWER(_c27) AS STRING) AS ds_email,
    CAST(_c28 AS STRING) AS ds_situacao_especial,
    CAST(_c29 AS STRING) AS dt_situacao_especial,

    -- Particionamento
    '202512' AS anomes

FROM read_files(
    '/Volumes/workspace/default/raw/fact_estabelecimentos/202512/*.ESTABELE',
    format => 'csv',
    delimiter => ';',
    header => 'false',
    encoding => 'ISO-8859-1'
)
""")

# 2. Documentação: Adiciona os comentários em todas as colunas
spark.sql("ALTER TABLE fato_estabelecimentos ALTER COLUMN cd_cnpj_basico COMMENT 'NÚMERO BASE DE INSCRIÇÃO NO CNPJ (OITO PRIMEIROS DÍGITOS)'")
spark.sql("ALTER TABLE fato_estabelecimentos ALTER COLUMN cd_cnpj_ordem COMMENT 'NÚMERO DO ESTABELECIMENTO DE INSCRIÇÃO NO CNPJ (DO NONO ATÉ O DÉCIMO SEGUNDO DÍGITO)'")
spark.sql("ALTER TABLE fato_estabelecimentos ALTER COLUMN cd_cnpj_dv COMMENT 'DÍGITO VERIFICADOR DO NÚMERO DE INSCRIÇÃO NO CNPJ (DOIS ÚLTIMOS DÍGITOS)'")
spark.sql("ALTER TABLE fato_estabelecimentos ALTER COLUMN cd_matriz_filial COMMENT 'CÓDIGO DO IDENTIFICADOR MATRIZ/FILIAL (1 – MATRIZ, 2 – FILIAL)'")
spark.sql("ALTER TABLE fato_estabelecimentos ALTER COLUMN nm_fantasia COMMENT 'CORRESPONDE AO NOME FANTASIA'")
spark.sql("ALTER TABLE fato_estabelecimentos ALTER COLUMN cd_situacao_cadastral COMMENT 'CÓDIGO DA SITUAÇÃO CADASTRAL (01-NULA, 02-ATIVA, 03-SUSPENSA, 04-INAPTA, 08-BAIXADA)'")
spark.sql("ALTER TABLE fato_estabelecimentos ALTER COLUMN dt_situacao_cadastral COMMENT 'DATA DO EVENTO DA SITUAÇÃO CADASTRAL'")
spark.sql("ALTER TABLE fato_estabelecimentos ALTER COLUMN cd_motivo_situacao_cadastral COMMENT 'CÓDIGO DO MOTIVO DA SITUAÇÃO CADASTRAL'")
spark.sql("ALTER TABLE fato_estabelecimentos ALTER COLUMN nm_cidade_exterior COMMENT 'NOME DA CIDADE NO EXTERIOR'")
spark.sql("ALTER TABLE fato_estabelecimentos ALTER COLUMN cd_pais COMMENT 'CÓDIGO DO PAÍS'")
spark.sql("ALTER TABLE fato_estabelecimentos ALTER COLUMN dt_inicio_atividade COMMENT 'DATA DE INÍCIO DA ATIVIDADE'")
spark.sql("ALTER TABLE fato_estabelecimentos ALTER COLUMN cd_cnae_principal COMMENT 'CÓDIGO DA ATIVIDADE ECONÔMICA PRINCIPAL DO ESTABELECIMENTO'")
spark.sql("ALTER TABLE fato_estabelecimentos ALTER COLUMN cd_cnae_secundaria COMMENT 'CÓDIGO DA(S) ATIVIDADE(S) ECONÔMICA(S) SECUNDÁRIA(S) DO ESTABELECIMENTO'")
spark.sql("ALTER TABLE fato_estabelecimentos ALTER COLUMN ds_tipo_logradouro COMMENT 'DESCRIÇÃO DO TIPO DE LOGRADOURO'")
spark.sql("ALTER TABLE fato_estabelecimentos ALTER COLUMN nm_logradouro COMMENT 'NOME DO LOGRADOURO ONDE SE LOCALIZA O ESTABELECIMENTO'")
spark.sql("ALTER TABLE fato_estabelecimentos ALTER COLUMN nr_logradouro COMMENT 'NÚMERO ONDE SE LOCALIZA O ESTABELECIMENTO'")
spark.sql("ALTER TABLE fato_estabelecimentos ALTER COLUMN ds_complemento COMMENT 'COMPLEMENTO PARA O ENDEREÇO DE LOCALIZAÇÃO DO ESTABELECIMENTO'")
spark.sql("ALTER TABLE fato_estabelecimentos ALTER COLUMN nm_bairro COMMENT 'BAIRRO ONDE SE LOCALIZA O ESTABELECIMENTO'")
spark.sql("ALTER TABLE fato_estabelecimentos ALTER COLUMN nr_cep COMMENT 'CÓDIGO DE ENDEREÇAMENTO POSTAL'")
spark.sql("ALTER TABLE fato_estabelecimentos ALTER COLUMN sg_uf COMMENT 'SIGLA DA UNIDADE DA FEDERAÇÃO EM QUE SE ENCONTRA O ESTABELECIMENTO'")
spark.sql("ALTER TABLE fato_estabelecimentos ALTER COLUMN cd_municipio COMMENT 'CÓDIGO DO MUNICÍPIO DE JURISDIÇÃO ONDE SE ENCONTRA O ESTABELECIMENTO'")
spark.sql("ALTER TABLE fato_estabelecimentos ALTER COLUMN nr_ddd1 COMMENT 'CONTÉM O DDD 1'")
spark.sql("ALTER TABLE fato_estabelecimentos ALTER COLUMN nr_telefone1 COMMENT 'CONTÉM O NÚMERO DO TELEFONE 1'")
spark.sql("ALTER TABLE fato_estabelecimentos ALTER COLUMN nr_ddd2 COMMENT 'CONTÉM O DDD 2'")
spark.sql("ALTER TABLE fato_estabelecimentos ALTER COLUMN nr_telefone2 COMMENT 'CONTÉM O NÚMERO DO TELEFONE 2'")
spark.sql("ALTER TABLE fato_estabelecimentos ALTER COLUMN nr_ddd_fax COMMENT 'CONTÉM O DDD DO FAX'")
spark.sql("ALTER TABLE fato_estabelecimentos ALTER COLUMN nr_fax COMMENT 'CONTÉM O NÚMERO DO FAX'")
spark.sql("ALTER TABLE fato_estabelecimentos ALTER COLUMN ds_email COMMENT 'CONTÉM O E-MAIL DO CONTRIBUINTE'")
spark.sql("ALTER TABLE fato_estabelecimentos ALTER COLUMN ds_situacao_especial COMMENT 'SITUAÇÃO ESPECIAL DA EMPRESA'")
spark.sql("ALTER TABLE fato_estabelecimentos ALTER COLUMN dt_situacao_especial COMMENT 'DATA EM QUE A EMPRESA ENTROU EM SITUAÇÃO ESPECIAL'")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fato Estabelecimento - Nova Partição

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark.sql(f"""
INSERT INTO fato_estabelecimentos
SELECT
    CAST(_c0 AS STRING) AS cd_cnpj_basico,
    CAST(_c1 AS STRING) AS cd_cnpj_ordem,
    CAST(_c2 AS STRING) AS cd_cnpj_dv,
    CAST(_c3 AS STRING) AS cd_matriz_filial,
    CAST(_c4 AS STRING) AS nm_fantasia,
    CAST(_c5 AS STRING) AS cd_situacao_cadastral,
    CAST(_c6 AS STRING) AS dt_situacao_cadastral,
    CAST(_c7 AS STRING) AS cd_motivo_situacao_cadastral,
    CAST(_c8 AS STRING) AS nm_cidade_exterior,
    CAST(_c9 AS STRING) AS cd_pais,
    CAST(_c10 AS STRING) AS dt_inicio_atividade,
    CAST(_c11 AS STRING) AS cd_cnae_principal,
    CAST(_c12 AS STRING) AS cd_cnae_secundaria,
    CAST(_c13 AS STRING) AS ds_tipo_logradouro,
    CAST(_c14 AS STRING) AS nm_logradouro,
    CAST(_c15 AS STRING) AS nr_logradouro,
    CAST(_c16 AS STRING) AS ds_complemento,
    CAST(_c17 AS STRING) AS nm_bairro,
    CAST(_c18 AS STRING) AS nr_cep,
    CAST(_c19 AS STRING) AS sg_uf,
    CAST(_c20 AS STRING) AS cd_municipio,
    CAST(_c21 AS STRING) AS nr_ddd1,
    CAST(_c22 AS STRING) AS nr_telefone1,
    CAST(_c23 AS STRING) AS nr_ddd2,
    CAST(_c24 AS STRING) AS nr_telefone2,
    CAST(_c25 AS STRING) AS nr_ddd_fax,
    CAST(_c26 AS STRING) AS nr_fax,
    CAST(LOWER(_c27) AS STRING) AS ds_email,
    CAST(_c28 AS STRING) AS ds_situacao_especial,
    CAST(_c29 AS STRING) AS dt_situacao_especial,

    -- Define a NOVA partição explicitamente
    '202511' AS anomes

FROM read_files(
    '/Volumes/workspace/default/raw/fact_estabelecimentos/202511/*.ESTABELE', -- Caminho da nova carga
    format => 'csv',
    delimiter => ';',
    header => 'false',
    encoding => 'ISO-8859-1'
)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fato Sócioes

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# 1. Criação da Tabela Fato via SQL Direto
spark.sql(f"""
CREATE OR REPLACE TABLE fato_socios
USING DELTA
PARTITIONED BY (anomes)
COMMENT 'Tabela Fato de Sócios - Dados Abertos CNPJ'
AS
SELECT
    CAST(_c0 AS STRING) AS cd_cnpj_basico,
    CAST(_c1 AS INT) AS cd_identificador_socio,
    CAST(_c2 AS STRING) AS nm_socio_razao_social,
    CAST(_c3 AS STRING) AS nr_cpf_cnpj_socio,
    CAST(_c4 AS INT) AS cd_qualificacao_socio,
    CAST(_c5 AS STRING) AS dt_entrada_sociedade,
    CAST(_c6 AS STRING) AS cd_pais_socio_estrangeiro,
    CAST(_c7 AS STRING) AS nr_cpf_representante_legal,
    CAST(_c8 AS STRING) AS nm_representante_legal,
    CAST(_c9 AS INT) AS cd_qualificacao_representante_legal,
    CAST(_c10 AS INT) AS cd_faixa_etaria,

    -- Coluna de Partição
    '202512' AS anomes

FROM read_files(
    '/Volumes/workspace/default/raw/fact_socios/202512/*.SOCIOCSV',
    format => 'csv',
    delimiter => ';',
    header => 'false',
    encoding => 'ISO-8859-1'
)
""")

# 2. Documentação: Adiciona os comentários nas colunas
spark.sql("ALTER TABLE fato_socios ALTER COLUMN cd_cnpj_basico COMMENT 'NÚMERO BASE DE INSCRIÇÃO NO CNPJ (CADASTRO NACIONAL DA PESSOA JURÍDICA)'")
spark.sql("ALTER TABLE fato_socios ALTER COLUMN cd_identificador_socio COMMENT 'CÓDIGO DO IDENTIFICADOR DE SÓCIO (1 – PESSOA JURÍDICA, 2 – PESSOA FÍSICA, 3 – ESTRANGEIRO)'")
spark.sql("ALTER TABLE fato_socios ALTER COLUMN nm_socio_razao_social COMMENT 'NOME DO SÓCIO PESSOA FÍSICA OU A RAZÃO SOCIAL E/OU NOME EMPRESARIAL DA PESSOA JURÍDICA E/OU NOME DO SÓCIO/RAZÃO SOCIAL DO SÓCIO ESTRANGEIRO'")
spark.sql("ALTER TABLE fato_socios ALTER COLUMN nr_cpf_cnpj_socio COMMENT 'CPF OU CNPJ DO SÓCIO (SÓCIO ESTRANGEIRO NÃO TEM ESTA INFORMAÇÃO)'")
spark.sql("ALTER TABLE fato_socios ALTER COLUMN cd_qualificacao_socio COMMENT 'CÓDIGO DA QUALIFICAÇÃO DO SÓCIO'")
spark.sql("ALTER TABLE fato_socios ALTER COLUMN dt_entrada_sociedade COMMENT 'DATA DE ENTRADA NA SOCIEDADE'")
spark.sql("ALTER TABLE fato_socios ALTER COLUMN cd_pais_socio_estrangeiro COMMENT 'CÓDIGO PAÍS DO SÓCIO ESTRANGEIRO'")
spark.sql("ALTER TABLE fato_socios ALTER COLUMN nr_cpf_representante_legal COMMENT 'NÚMERO DO CPF DO REPRESENTANTE LEGAL'")
spark.sql("ALTER TABLE fato_socios ALTER COLUMN nm_representante_legal COMMENT 'NOME DO REPRESENTANTE LEGAL'")
spark.sql("ALTER TABLE fato_socios ALTER COLUMN cd_qualificacao_representante_legal COMMENT 'CÓDIGO DA QUALIFICAÇÃO DO REPRESENTANTE LEGAL'")
spark.sql("ALTER TABLE fato_socios ALTER COLUMN cd_faixa_etaria COMMENT 'CÓDIGO CORRESPONDENTE À FAIXA ETÁRIA DO SÓCIO'")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fató Sócioes - Nova Partição

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark.sql(f"""
INSERT INTO fato_socios
SELECT
    CAST(_c0 AS STRING) AS cd_cnpj_basico,
    CAST(_c1 AS INT) AS cd_identificador_socio,
    CAST(_c2 AS STRING) AS nm_socio_razao_social,
    CAST(_c3 AS STRING) AS nr_cpf_cnpj_socio,
    CAST(_c4 AS INT) AS cd_qualificacao_socio,
    CAST(_c5 AS STRING) AS dt_entrada_sociedade,
    CAST(_c6 AS STRING) AS cd_pais_socio_estrangeiro,
    CAST(_c7 AS STRING) AS nr_cpf_representante_legal,
    CAST(_c8 AS STRING) AS nm_representante_legal,
    CAST(_c9 AS INT) AS cd_qualificacao_representante_legal,
    CAST(_c10 AS INT) AS cd_faixa_etaria,

    -- Define a NOVA partição explicitamente
    '202511' AS anomes

FROM read_files(
    '/Volumes/workspace/default/raw/fact_socios/202511/*.SOCIOCSV', -- Caminho da nova carga
    format => 'csv',
    delimiter => ';',
    header => 'false',
    encoding => 'ISO-8859-1'
)
""")