from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Импорт функций из твоих модулей
from log_parser import parse_logs
from aggregator import aggregar, build_hourly_calendar
from db import create_tables, upload_to_postgres_orm, upload_user_activity_calendar

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Основной DAG
with DAG(
    dag_id='log_parser_hourly',
    default_args=default_args,
    description='Парсинг логов → агрегация → загрузка в PostgreSQL',
    schedule_interval='@hourly',    # каждый час
    #schedule_interval='*/5 * * * *',   # каждые 5 мин
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["log_parser"],
) as dag:

    # Шаг 1: Создание таблиц (однократно)
    init_db = PythonOperator(
        task_id='init_db',
        python_callable=create_tables
    )

    # Шаг 2: Парсинг логов
    parse = PythonOperator(
        task_id='parse_logs',
        python_callable=parse_logs
    )

    # Шаг 3: Агрегация и загрузка в PostgreSQL
    def aggregate_and_upload():
        df = aggregar()
        upload_to_postgres_orm(df)

        calendar_df = build_hourly_calendar(df)
        upload_user_activity_calendar(calendar_df)

    agg_upload = PythonOperator(
        task_id='aggregate_and_upload',
        python_callable=aggregate_and_upload
    )

    # Зависимости
    init_db >> parse >> agg_upload
