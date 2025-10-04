from datetime import datetime
from lib.ETL.el_meteo import loader
from airflow.models import DAG
from airflow.decorators import task_group
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

with DAG(
    dag_id = 'ETL_METEO',
    tags=['etl'],
    start_date=datetime(year=2025,
                        month=10,
                        day=4),
    schedule_interval=None,
    ) as dag:
    

    @task_group(group_id = 'Extract-Load')
    def EL():
        raw_fact = loader(dir='dags/data/meteo-jsonl/') # loader for raw_hourly_meteo (FCT)
        raw_dim = loader(dir='dags/data')               # loader for the dimensional tables (DIM)

        el_meteo = PythonOperator(
            task_id = 'RAW_METEO_LOAD',
            python_callable=raw_fact.load,
            op_kwargs={
                'file':'hourly'
            }
        )

        el_province = PythonOperator(
            task_id = 'RAW_PROVINCE_LOAD',
            python_callable=raw_dim.load,
            op_kwargs={
                'file':'province'
            }
        )    

        el_cities = PythonOperator(
            task_id = 'RAW_CITIES_LOAD',
            python_callable=raw_dim.load,
            op_kwargs={
                'file':'city'
            }
        )    

        [el_province,el_cities,el_meteo] 
    
    @task_group(group_id= 'Transform')
    def T():

        work = PostgresOperator(
            task_id = 'NULLS_and_DUPLICATES',
            sql='lib/SQL_SCRIPTS/work_meteo.sql',
            postgres_conn_id = 'meteo_db'
        )
        fct = PostgresOperator(
                    task_id = 'TARGET_TABLE',
                    sql='lib/SQL_SCRIPTS/fct_meteo.sql',
                    postgres_conn_id = 'meteo_db'
                )

        work >> fct

    EL() >> T()