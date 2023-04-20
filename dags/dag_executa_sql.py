from airflow import DAG
from datetime import datetime
from airflow.providers.postgres.operators.postgres import PostgresOperator

with DAG(
        'dag_executa_sql',  ## nome unico
         start_date=datetime(2022,4,20), # inicio da execucao da dag
         schedule_interval= '30 * * * *', # executa em 30 em 30 min - tipo crontab
         catchup=False, ## vai criar a dag para essa ultima execucao, boa pratica 
         template_searchpath='/opt/airflow/sql' ## para scripts sql é bom deixa-los em pasta separada
         ) as dag:
    
    criar_tabela_db =PostgresOperator(
        task_id='criar_tabela_db',
        postgres_conn_id = 'postgres-airflow',
        sql = 'criar_tabela_db.sql'
    )
    
    insere_dados_tabela_db = PostgresOperator(
        task_id='insere_dados_tabela_db',
        postgres_conn_id = 'postgres-airflow',
        sql = 'insere_dados_tb.sql'
    )
    
    criar_tabela_db >> insere_dados_tabela_db
