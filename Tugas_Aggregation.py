from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import logging
import pandas as pd
import sqlalchemy

@dag(schedule_interval=None,
     start_date=datetime(2024, 9, 1),
     catchup=False)
def aggregation_dag():

    start_task = EmptyOperator(task_id="start_task")
    end_task = EmptyOperator(task_id="end_task", trigger_rule=TriggerRule.ALL_DONE)

    @task
    def create_aggregated_table():
        postgres_hook = PostgresHook(postgres_conn_id='postgres_dibimbing')
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS sales_aggregated (
            transaction_date DATE NOT NULL,
            product_name VARCHAR(50) NOT NULL,
            total_quantity INT DEFAULT 0,
            total_sales NUMERIC(10, 2) DEFAULT 0.00,
            PRIMARY KEY (transaction_date, product_name)
        );
        """
        postgres_hook.run(create_table_sql)
        logging.info("Table sales_aggregated has been created or already exists.")

    @task
    def aggregate_sales_data():
        parquet_file = '/tmp/staging_data.parquet'
        df = pd.read_parquet(parquet_file)
        
        postgres_hook = PostgresHook(postgres_conn_id='postgres_dibimbing')
        engine = postgres_hook.get_sqlalchemy_engine()
        
        aggregated_df = df.groupby(['transaction_date', 'product_name']).agg(
            total_quantity=('quantity', 'sum'),
            total_sales=('total_amount', 'sum')
        ).reset_index()

        aggregated_df.to_sql('sales_aggregated', engine, if_exists='append', index=False, method='multi', dtype={
            'transaction_date': sqlalchemy.Date(),
            'product_name': sqlalchemy.String(),
            'total_quantity': sqlalchemy.Integer(),
            'total_sales': sqlalchemy.Numeric(10, 2)
        })
        logging.info("Data aggregated and saved to sales_aggregated.")

    start_task >> create_aggregated_table() >> aggregate_sales_data() >> end_task

aggregation_dag()