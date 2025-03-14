from airflow import DAG, task
from groups.group_downloads import downloads_tasks 
from groups.group_transforms import transforms_tasks 
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup


from datetime import datetime
 
with DAG('group_dag', start_date=datetime(2022, 1, 1), 
    schedule_interval='@daily', catchup=False) as dag:


 
    
    downloads = downloads_tasks()
    
    check_files = BashOperator(
        task_id='check_files',
        bash_command='sleep 10'
    )
 
   
    transform = transforms_tasks()

    downloads >> check_files >> transform