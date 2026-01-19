# Databricks notebook source
from pyspark.sql import SparkSession

# Criando a tabela vazia para receber os logs de qualidade
spark.sql("""
CREATE TABLE IF NOT EXISTS qualidade_dados (
    particao_tabela STRING COMMENT 'Valor da partição dos dados verificados (ex: 202512)',
    nm_tabela STRING COMMENT 'Nome da tabela verificada',
    timestamp TIMESTAMP COMMENT 'Data e hora exata da verificação',
    regra STRING COMMENT 'Nome descritivo ou tipo da regra aplicada',
    valor_regra STRING COMMENT 'Resultado numérico ou texto da métrica calculada',
    passou BOOLEAN COMMENT 'Flag indicando sucesso (true) ou falha (false)',
    
    -- Coluna de Partição da tabela de qualidade (Data da Execução)
    anomesdia STRING COMMENT 'Data da execução no formato YYYYMMDD'
)
USING DELTA
PARTITIONED BY (anomesdia)
COMMENT 'Logs de execução de regras de Data Quality'
""")

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from datetime import datetime

class MonitoramentoQualidade:
    def __init__(self, spark: SparkSession, nm_tabela: str, particao_tabela: str):
        self.spark = spark
        self.nm_tabela = nm_tabela
        self.particao_tabela = particao_tabela
        # Define o timestamp e a partição de execução (anomesdia) uma única vez
        self.timestamp_execucao = datetime.now()
        self.anomesdia = self.timestamp_execucao.strftime("%Y%m%d")

    def registrar_resultado(self, regra: str, valor_regra: str, passou: bool, msg: str = ""):
        """
        Salva o resultado de um teste na tabela 'qualidade_dados'.
        Pode ser chamado manualmente para regras ad-hoc.
        """
        # Esquema deve bater com o CREATE TABLE feito anteriormente
        schema = StructType([
            StructField("particao_tabela", StringType(), True),
            StructField("nm_tabela", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("regra", StringType(), True),
            StructField("valor_regra", StringType(), True),
            StructField("passou", BooleanType(), True),
            StructField("anomesdia", StringType(), True) # Coluna de partição da tabela de log
        ])
        
        # Cria o DataFrame com uma única linha (o resultado atual)
        dados = [(
            self.particao_tabela,
            self.nm_tabela,
            self.timestamp_execucao,
            regra,
            str(valor_regra),
            passou,
            self.anomesdia
        )]
        
        df_log = self.spark.createDataFrame(dados, schema=schema)
        
        # --- O PULO DO GATO: APPEND NA TABELA DELTA ---
        # O modo 'append' adiciona sem apagar o histórico
        df_log.write.format("delta").mode("append").saveAsTable("qualidade_dados")
        
        status = "✅ APROVADO" if passou else "❌ FALHOU"
        print(f"[{status}] Regra: {regra} | Valor: {valor_regra}")

    # --- Regras Prontas que usam o registrar_resultado ---

    def check_count(self, df, min_esperado: int):
        qtd = df.count()
        passou = qtd >= min_esperado
        self.registrar_resultado(
            regra=f"Volume de Dados (Min {min_esperado})",
            valor_regra=str(qtd),
            passou=passou
        )

    def check_unique(self, df, col_name: str):
        # Verifica se há duplicatas na chave
        duplicatas = df.groupBy(col_name).count().filter("count > 1").count()
        passou = duplicatas == 0
        self.registrar_resultado(
            regra=f"Unicidade ({col_name})",
            valor_regra=f"{duplicatas} duplicados",
            passou=passou
        )

    def check_custom_sql(self, query: str, check_lambda):
        """
        Executa SQL e valida o retorno com uma função lambda.
        Ex: check_lambda = lambda x: x == 0
        """
        try:
            resultado = self.spark.sql(query).collect()[0][0]
            passou = check_lambda(resultado)
            self.registrar_resultado(
                regra="Regra Customizada SQL",
                valor_regra=str(resultado),
                passou=passou
            )
        except Exception as e:
            self.registrar_resultado("Erro Execução SQL", str(e), False)

# COMMAND ----------

# 1. Inicializa o monitoramento para a carga atual
monitor = MonitoramentoQualidade(spark, nm_tabela="fato_empresas", anomesdia="202512")

# 2. Carrega os dados que serão testados
df_estabelecimentos = spark.read.table("fato_empresas").filter("anomes = '202512'")

# 3. Aplica as regras (Elas salvam automaticamente na tabela de qualidade)
monitor.check_count(df_estabelecimentos, min_esperado=1000000)
monitor.check_unique(df_estabelecimentos, col_name="cd_cnpj_basico")

# 4. Exemplo de regra manual (caso você calcule algo complexo fora da classe)
media_capital = df_estabelecimentos.agg({"vl_capital_social": "avg"}).collect()[0][0]
monitor.registrar_resultado(
    regra="Média Capital Social > 0", 
    valor_regra=str(media_capital), 
    passou=(media_capital > 0)
)