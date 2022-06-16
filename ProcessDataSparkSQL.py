# Databricks notebook source
# MAGIC %md # Processamento de dados usando o Databricks - PySpark e Spark SQL
# MAGIC   * Este notebook tem como objetivo processar um dataset do Kaggle (https://www.kaggle.com/datasets/lfarhat/brasil-students-scholarship-prouni-20052019) aplicando algumas funções de PySpark e Spark SQL
# MAGIC   * Ao longo do laboratório algumas transformações serão feitas neste dataset e também algumas perguntas serão respondidas.

# COMMAND ----------

# Importando as bibliotecas que serão usadas no laboratório.

from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md # Importando o arquivo csv para processamento

# COMMAND ----------

# Tipo do arquivos
file_type = "csv"

# Path dos arquivos
path = '/FileStore/tables/prouni_2005_2019.csv'

arq_prouni = spark \
             .read.format(file_type) \
             .option("inferSchema","True") \
             .option("header","True") \
             .csv(path)

# COMMAND ----------

# MAGIC %fs ls /FileStore/tables/prouni_2005_2019.csv

# COMMAND ----------

#Quantidade total de linhas

arq_prouni.count()

# COMMAND ----------

# Verificando o schema das colunas.

arq_prouni.printSchema()

# COMMAND ----------

#Visualizando algumas linhas do dataframe

arq_prouni.show(10)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md #Gerando cache dos DataFrames
# MAGIC O objetivo é facilitar o processamento uma vez que estaremos realizando algumas operações neles.

# COMMAND ----------

arq_prouni.cache();

# COMMAND ----------

# MAGIC %md #Criando uma tabela temporia SQL

# COMMAND ----------

arq_prouni.createOrReplaceTempView("TempProuni")

# COMMAND ----------

#Tipando a coluna 'idade' e 'data_nascimento'. 
#Criando um novo DataFrame apartir de uma query SQL

df_sql_full = spark.sql("""
            select
            
             ANO_CONCESSAO_BOLSA,
             CODIGO_EMEC_IES_BOLSA,
             NOME_IES_BOLSA,
             TIPO_BOLSA,
                 CASE WHEN TIPO_BOLSA == 'BOLSA INTEGRAL' THEN '1'
                     WHEN TIPO_BOLSA == 'BOLSA PARCIAL 25%' THEN '2'
                         WHEN TIPO_BOLSA == 'BOLSA PARCIAL 50%' THEN '3'
                             END AS NR_TIPO_BOLSA,
             MODALIDADE_ENSINO_BOLSA,
             NOME_CURSO_BOLSA,
             NOME_TURNO_CURSO_BOLSA,
             CPF_BENEFICIARIO_BOLSA,
             SEXO_BENEFICIARIO_BOLSA,
             RACA_BENEFICIARIO_BOLSA,
             
                 CAST(DT_NASCIMENTO_BENEFICIARIO as DATE) as DATA_NASCIMENTO,
                 
             BENEFICIARIO_DEFICIENTE_FISICO,
             REGIAO_BENEFICIARIO_BOLSA,
             SIGLA_UF_BENEFICIARIO_BOLSA,
             MUNICIPIO_BENEFICIARIO_BOLSA,
             
                 CAST(idade AS INT) AS IDADE
 
             
             from TempProuni
""")

#Verificando a nova tipagem das colunas
df_sql_full.printSchema()


# COMMAND ----------

# MAGIC %md #Respondendo algumas perguntas sobre os dados

# COMMAND ----------

# MAGIC %md * Escrever uma query que mostre a quantidade de inscrições por região.

# COMMAND ----------

# MAGIC %sql
# MAGIC  
# MAGIC SELECT REGIAO_BENEFICIARIO_BOLSA, count(CPF_BENEFICIARIO_BOLSA)
# MAGIC       FROM TempProuni
# MAGIC WHERE 
# MAGIC REGIAO_BENEFICIARIO_BOLSA <> 'null'
# MAGIC  
# MAGIC GROUP BY
# MAGIC REGIAO_BENEFICIARIO_BOLSA
# MAGIC  
# MAGIC ORDER BY
# MAGIC count(CPF_BENEFICIARIO_BOLSA) DESC
# MAGIC  

# COMMAND ----------

# MAGIC %md * Escrever uma  query que mostre as inscrições por turno, tipo da bolsa e região ?

# COMMAND ----------

# MAGIC %sql
# MAGIC  
# MAGIC SELECT REGIAO_BENEFICIARIO_BOLSA, NOME_TURNO_CURSO_BOLSA, TIPO_BOLSA , count(CPF_BENEFICIARIO_BOLSA) AS TOTAL_INSCRICAO
# MAGIC       FROM TempProuni
# MAGIC WHERE 
# MAGIC REGIAO_BENEFICIARIO_BOLSA <> 'null'
# MAGIC 
# MAGIC GROUP BY
# MAGIC NOME_TURNO_CURSO_BOLSA,
# MAGIC REGIAO_BENEFICIARIO_BOLSA,
# MAGIC TIPO_BOLSA
# MAGIC 
# MAGIC ORDER BY
# MAGIC count(CPF_BENEFICIARIO_BOLSA) DESC

# COMMAND ----------

# MAGIC %md * Escreva uma query que mostre qual é a quantidade total de inscrições por curso.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT  NOME_CURSO_BOLSA , count(CPF_BENEFICIARIO_BOLSA) AS TOTAL_INSCRITOS
# MAGIC   FROM TempProuni
# MAGIC GROUP BY
# MAGIC NOME_CURSO_BOLSA
# MAGIC 
# MAGIC ORDER BY
# MAGIC count(CPF_BENEFICIARIO_BOLSA) DESC

# COMMAND ----------

# MAGIC %md * Qual o total de beneficiários com deficiência física ?

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT  BENEFICIARIO_DEFICIENTE_FISICO,  count(CPF_BENEFICIARIO_BOLSA) AS TOTAL_INSCRITOS
# MAGIC   FROM TempProuni
# MAGIC   WHERE BENEFICIARIO_DEFICIENTE_FISICO in('sim','Sim')
# MAGIC 
# MAGIC GROUP BY
# MAGIC BENEFICIARIO_DEFICIENTE_FISICO
# MAGIC 
# MAGIC ORDER BY
# MAGIC count(CPF_BENEFICIARIO_BOLSA) DESC
