from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup


def downloads_tasks():

    with TaskGroup("downloads", tooltip="downloads") as group:
        
        downlad_a= BashOperator(
            task_id='download_a',
            bash_command='sleep 10'
        )
        
        downlad_b= BashOperator(
            task_id='download_b',
            bash_command='sleep 10'
        )
        
        downlad_c= BashOperator(
            task_id='download_c',
            bash_command='sleep 10'
        )

        return group