from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
import sqlalchemy

@dag(schedule_interval='0 7 * * *', 
     start_date=datetime(2024, 9, 1), 
     catchup=False)
def etl_dag():
    
    start_task = EmptyOperator(task_id="start_task")
    end_task = EmptyOperator(task_id="end_task", trigger_rule=TriggerRule.ALL_DONE)

    @task
    def extract_data():
        mysql_hook = MySqlHook(mysql_conn_id='mysql_dibimbing')
        sql = """
            SELECT id, product_name, quantity, total_amount, transaction_date
            FROM dibimbing.sales_transactions
            WHERE DATE(transaction_date) = CURDATE() - INTERVAL 1 DAY;
        """
        df = mysql_hook.get_pandas_df(sql)
        parquet_file = '/tmp/staging_data.parquet'
        df.to_parquet(parquet_file)
        return parquet_file

    @task
    def load_data(parquet_file: str):
        df = pd.read_parquet(parquet_file)
        df['transaction_date'] = pd.to_datetime(df['transaction_date'])
        
        # Load data into PostgreSQL
        postgres_hook = PostgresHook(postgres_conn_id='postgres_dibimbing')
        engine = postgres_hook.get_sqlalchemy_engine()
        
        # Load into sales_transactions
        df.to_sql('sales_transactions', engine, if_exists='append', index=False, method='multi', dtype={
            'transaction_date': sqlalchemy.Date(),
            'product_name': sqlalchemy.String(),
            'quantity': sqlalchemy.Integer(),
            'total_amount': sqlalchemy.Numeric(10, 2)
        })

    # Trigger the aggregation DAG after loading data
    trigger_aggregat_dag = TriggerDagRunOperator(
        task_id='trigger_aggregat_dag',
        trigger_dag_id='aggregation_dag'
    )

    # Task dependencies
    extracted_file = extract_data()
    load_data_task = load_data(extracted_file)

    start_task >> extracted_file >> load_data_task >> trigger_aggregat_dag >> end_task

etl_dag()
