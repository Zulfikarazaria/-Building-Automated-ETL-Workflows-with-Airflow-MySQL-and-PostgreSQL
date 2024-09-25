from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.email import EmailOperator
from datetime import datetime
import pandas as pd
import os

@dag(schedule_interval='0 9 * * *',
     start_date=datetime(2024, 9, 1),
     catchup=False,
     default_args={'email_on_failure': False})
def send_analysis_notification_dag():
    
    start_task = EmptyOperator(task_id="start_task")
    end_task = EmptyOperator(task_id="end_task", trigger_rule=TriggerRule.ALL_DONE)

    @task
    def extract_and_save_to_excel():
        postgres_hook = PostgresHook(postgres_conn_id='postgres_dibimbing')
        sql = "SELECT * FROM sales_aggregated;"
        df = postgres_hook.get_pandas_df(sql)
        excel_file = '/tmp/sales_report.xlsx'
        df.to_excel(excel_file, index=False)
        return excel_file

    @task
    def send_notification_email(excel_file):
        email_task = EmailOperator(
            task_id='send_notification_email',
            to='zulfikarazaria@gmail.com',
            subject='Daily Sales Report',
            html_content="""<p>The daily sales report has been generated. Please find the file attached.</p>""",
            files=[excel_file]
        )
        email_task.execute(context={})

    @task
    def clear_data_and_files(excel_file: str):
        parquet_file = '/tmp/staging_data.parquet'
        if os.path.exists(parquet_file):
            os.remove(parquet_file)
        if os.path.exists(excel_file):
            os.remove(excel_file)
        postgres_hook = PostgresHook(postgres_conn_id='postgres_dibimbing')
        truncate_sql = "TRUNCATE TABLE sales_aggregated;"
        postgres_hook.run(truncate_sql)

    @task(trigger_rule=TriggerRule.ONE_FAILED)
    def failure_notification():
        email_task = EmailOperator(
            task_id='failure_notification',
            to='zulfikarazaria@gmail.com',
            subject='Sales Report DAG Failed',
            html_content="""<p>The sales report DAG has failed. Please check the logs for more details.</p>"""
        )
        email_task.execute(context={})

    excel_file = extract_and_save_to_excel()
    start_task >> excel_file >> send_notification_email(excel_file) >> clear_data_and_files(excel_file) >> end_task
    send_notification_email(excel_file) >> failure_notification()

send_analysis_notification_dag()
