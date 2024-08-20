from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.dates import timedelta
from datetime import datetime

def generate_pascals_triangle(n):
    triangle = []

    for i in range(n):
        row = [1] * (i + 1)
        for j in range(1, len(row) - 1):
            row[j] = triangle[i - 1][j - 1] + triangle[i - 1][j]
        triangle.append(row)

    return triangle

def print_pascals_triangle(triangle):
    max_width = len(" ".join(map(str, triangle[-1])))
    for row in triangle:
        row_str = " ".join(map(str, row))
        print(row_str.center(max_width))

def generate_and_print_pascals_triangle():
    levels = 10
    triangle = generate_pascals_triangle(levels)
    print_pascals_triangle(triangle)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 20),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'pascal',
    default_args=default_args,
    description='A DAG to generate and print Pascal\'s Triangle',
    schedule_interval='45 10 * * *',  # Запуск ежедневно в 12:45 по москве
    catchup=False,
)

generate_and_print_task = PythonOperator(
    task_id='generate_and_print_pascals_triangle',
    python_callable=generate_and_print_pascals_triangle,
    dag=dag,
)

dummy_task = EmptyOperator(
task_id='dummy_task',
retries=3,
dag=dag,
)




