from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator, BranchPythonOperator ## operador da task, se fosse um bash seria BashOperator
from airflow.operators.bash import BashOperator
import pandas as pd
import requests
import json

def captura_conta_dados():
    url = "https://data.cityofnewyork.us/resource/rc75-m7u3.json"
    response = requests.get(url)
    df = pd.DataFrame(json.loads(response.content))
    qtd = len(df.index)
    print("Quantidade:" + str(qtd))
    return qtd

def e_valida(ti):
    qtd = ti.xcom_pull(task_ids='captura_conta_dados')
    if (qtd > 100):
        return 'valido'
    return 'naovalido'


## o dag sempre sera executado a partir da start_date + schedule_interval, ex:
    ## se o start date é 2021-01-01 00:00:00, e o schedule interval é de 30 min
    ## será executado 2021-01-01 00:30
with DAG(
        'new_york_data',  ## nome unico
         start_date=datetime(2022,4,20), # inicio da execucao da dag
         schedule_interval= '30 * * * *', # executa em 30 em 30 min - tipo crontab
         catchup=False ## vai criar a dag para essa ultima execucao, boa pratica 
         ) as dag:
    
    captura_conta_dados = PythonOperator(
        task_id = 'captura_conta_dados',
        python_callable= captura_conta_dados
    )
    
    e_valido = BranchPythonOperator(
        task_id="e_valida",
        python_callable= e_valida
    )
    
    valido = BashOperator(
        task_id='valido',
        bash_command="echo 'qtd OK'"
    )

    naovalido = BashOperator(
        task_id='naovalido',
        bash_command="echo 'qtd NAO OK'"
    )
    
    captura_conta_dados >> e_valido >> [valido, naovalido]

    