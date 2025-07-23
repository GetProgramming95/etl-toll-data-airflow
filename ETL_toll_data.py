# ETL_toll_data.py

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# DAG-Argumente definieren
default_args = {
    'owner': 'airflow_user',
    'start_date': datetime.now(),
    'email': ['airflow@example.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definieren
with DAG(
    dag_id='ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Abschlussaufgabe',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    unzip_data = BashOperator(
        task_id='unzip_data',
        bash_command='tar -xzf /home/project/airflow/dags/finalassignment/tolldata.tgz -C /home/project/airflow/dags/finalassignment/staging'
    )

    extract_data_from_csv = BashOperator(
        task_id='extract_data_from_csv',
        bash_command='cut -d "," -f 1-4 /home/project/airflow/dags/finalassignment/staging/vehicle-data.csv > /home/project/airflow/dags/finalassignment/staging/csv_data.csv'
    )

    extract_data_from_tsv = BashOperator(
        task_id='extract_data_from_tsv',
        bash_command='cut -f5-7 /home/project/airflow/dags/finalassignment/staging/tollplaza-data.tsv > /home/project/airflow/dags/finalassignment/staging/tsv_data.csv'
    )

    extract_data_from_fixed_width = BashOperator(
        task_id='extract_data_from_fixed_width',
        bash_command='cut -c 59-67 /home/project/airflow/dags/finalassignment/staging/payment-data.txt > /home/project/airflow/dags/finalassignment/staging/fixed_width_data.csv'
    )

    consolidate_data = BashOperator(
        task_id='consolidate_data',
        bash_command='paste -d "," /home/project/airflow/dags/finalassignment/staging/csv_data.csv '
                     '/home/project/airflow/dags/finalassignment/staging/tsv_data.csv '
                     '/home/project/airflow/dags/finalassignment/staging/fixed_width_data.csv '
                     '> /home/project/airflow/dags/finalassignment/staging/extracted_data.csv'
    )

    transform_data = BashOperator(
        task_id='transform_data',
        bash_command='cut -d "," -f 1-3 /home/project/airflow/dags/finalassignment/staging/extracted_data.csv > /home/project/airflow/dags/finalassignment/staging/tmp.csv && '
                     'cut -d "," -f 4 /home/project/airflow/dags/finalassignment/staging/extracted_data.csv | tr "a-z" "A-Z" > /home/project/airflow/dags/finalassignment/staging/tmp2.csv && '
                     'cut -d "," -f 5-9 /home/project/airflow/dags/finalassignment/staging/extracted_data.csv > /home/project/airflow/dags/finalassignment/staging/tmp3.csv && '
                     'paste -d "," /home/project/airflow/dags/finalassignment/staging/tmp.csv '
                     '/home/project/airflow/dags/finalassignment/staging/tmp2.csv '
                     '/home/project/airflow/dags/finalassignment/staging/tmp3.csv '
                     '> /home/project/airflow/dags/finalassignment/staging/transformed_data.csv'
    )

    # Aufgabenpipeline definieren
    unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data
