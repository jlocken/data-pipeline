import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.hive_operator import HiveOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'PowerGen',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(2),
    'email': ['jlocken@powergen-re.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'start_date': datetime(2019, 11,10 ),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG(
    'steamaco_etl_tasks', 
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False
    
    )


stage_agents_data_from_api = BashOperator(
    task_id='stage_agents_data_from_api',
    bash_command='python3 ~/dags/stage_agents_data_task.py',
    retries=0,
    dag=dag)


move_agents_data_to_dimension = BashOperator(
    task_id='move_agents_data_to_dimension',
    bash_command='python3 ~/dags/move_agents_to_dimension_table.py',
    retries=0,
    dag=dag)


move_agents_data_to_dimension.set_upstream(stage_agents_data_from_api)


stage_sites_data_from_api = BashOperator(
    task_id='stage_sites_data_from_api',
    bash_command='python3 ~/dags/stage_sites_data_task.py',
    retries=0,
    dag=dag)


move_sites_data_to_dimension = BashOperator(
    task_id='move_sites_data_to_dimension',
    bash_command='python3 ~/dags/move_sites_to_dimension_table.py',
    retries=0,
    dag=dag)

move_sites_data_to_dimension.set_upstream(stage_sites_data_from_api)



stage_meters_data_from_api = BashOperator(
    task_id='stage_meters_data_from_api',
    bash_command='python3 ~/dags/stage_meters_data_task.py',
    retries=0,
    dag=dag)


move_meters_data_to_dimension = BashOperator(
    task_id='move_meters_data_to_dimension',
    bash_command='python3 ~/dags/move_meters_data_to_dimension_table.py',
    retries=0,
    dag=dag)

move_meters_data_to_dimension.set_upstream(stage_meters_data_from_api)

stage_customers_data_from_api = BashOperator(
    task_id='stage_customers_data_from_api',
    bash_command='python3 ~/dags/stage_customers_data_task.py',
    retries=0,
    dag=dag)


move_customers_data_to_dimension = BashOperator(
    task_id='move_customers_data_to_dimension',
    bash_command='python3 ~/dags/move_customers_to_dimension_table.py',
    retries=0,
    dag=dag)

move_customers_data_to_dimension.set_upstream(stage_customers_data_from_api)


stage_harvesters_data_from_api = BashOperator(
    task_id='stage_harvesters_data_from_api',
    bash_command='python3 ~/dags/stage_bitharvesters_data_task.py',
    retries=0,
    dag=dag)


move_harversters_data_to_dimension = BashOperator(
    task_id='move_harvesters_data_to_dimension',
    bash_command='python3 ~/dags/move_harvesters_to_dimension_table.py',
    retries=0,
    dag=dag)

move_harversters_data_to_dimension.set_upstream(stage_harvesters_data_from_api)



stage_power_usage_data_from_api = BashOperator(
    task_id='stage_power_usage_data_from_api',
    bash_command='python3 ~/dags/stage_power_usage_data_task.py',
    retries=0,
    dag=dag)


move_power_usage_data_to_fact = BashOperator(
    task_id='move_power_usage_data_to_fact',
    bash_command='python3 ~/dags/move_usage_data_to_fact_table.py',
    retries=0,
    dag=dag)




move_agents_data_to_dimension>>stage_power_usage_data_from_api
move_sites_data_to_dimension>>stage_power_usage_data_from_api


move_customers_data_to_dimension>>stage_power_usage_data_from_api
move_meters_data_to_dimension>>stage_power_usage_data_from_api

move_harversters_data_to_dimension>>stage_power_usage_data_from_api

move_power_usage_data_to_fact.set_upstream(stage_power_usage_data_from_api)


