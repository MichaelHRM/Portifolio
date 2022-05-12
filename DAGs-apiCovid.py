
##### ------------------------------------------------------------- #####
##### Objetivo :                                                    #####
#####  Simples Pipeline que consome dados de uma API e verifica     #####
#####  se existe dados para serem gravados no Banco de Dados.       #####
##### ------------------------------------------------------------- #####

#Importando as bibliotecas

import pandas as pd
from datetime import datetime
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.models.xcom import XCom
import requests
import json
#GCP storage
import os
from google.cloud import storage

# PythonOperator - Executa programas escritos em python.
# BashOperator -   Executar programas escritos em Shell.

def captura_dados_api_covid():
    url = "https://covid19-brazil-api.now.sh/api/report/v1"
    response = requests.get(url)
    df = pd.DataFrame(response.json()['data'])
    df['datetime'] = pd.to_datetime(df['datetime']).dt.strftime("%Y-%m-%d")
    qtd = len(df.index)
    return qtd
    
def e_valida(**context):
    qtd = context['ti'].xcom_pull(task_ids = 'captura_dados_api_covid')
    if (qtd > 0):
        return 'validacao'
    return 'nvalidacao'

# Definindo alguns argumentos básicos

default_args = {
   'owner': 'michaelm_moreira',
   'start_date': days_ago(1),
   'render_template_as_native_obj':True
   }
   
# Nomeando a DAG e definindo quando ela vai ser executada
# (você pode usar argumentos em Crontab também caso queira que a DAG execute por
# exemplo todos os dias as 8 da manhã)

with DAG(
        'Primeira_dag',
        #Executa todo os dias as 22:00 horas.
         schedule_interval = '0 22 * * *',
         catchup= False,
         default_args=default_args) as dag:
         
    #Executando a função criada para obter os dados da API   
    captura_dados_api_covid = PythonOperator(
         task_id = 'captura_dados_api_covid',
         #callable chama uma função que foi criada.
         python_callable = captura_dados_api_covid,
    )
    
    e_valida = BranchPythonOperator(
        task_id = 'e_valida',
        provide_context=True,
        python_callable = e_valida, 
    )
    
    validacao = BashOperator(
        task_id = 'validacao',
        bash_command = "echo ' --> Temos dados para serem carregados no Banco <--'"  
    )
    
    nvalidacao = BashOperator(
        task_id = 'nvalidacao',
        bash_command = "echo 'ALERTA --> Não temos dados para serem carregados no Banco <--'"  
    )
    
# Seqência de execução das tarefas;

captura_dados_api_covid >> e_valida >> [validacao,nvalidacao]