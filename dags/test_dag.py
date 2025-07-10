from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd

# Функция-задание с использованием pandas
def pandas_task():
    # Создаём простой DataFrame
    df = pd.DataFrame({
        'name': ['Alice', 'Bob', 'Charlie'],
        'age': [25, 30, 35]
    })

    # Фильтруем пользователей старше 28
    adults = df[df['age'] > 28]

    print("Исходный DataFrame:")
    print(df)
    print("Фильтрованные пользователи (age > 28):")
    print(adults)

# Определяем DAG
with DAG(
    dag_id="pandas_teta",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",  # запуск 1 раз в день
    catchup=False,
    tags=["pandas", "test"],
) as dag:

    task = PythonOperator(
        task_id="pandas_print_task",
        python_callable=pandas_task,
    )
