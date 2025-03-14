from airflow import DAG, Dataset
from airflow.decorators import task


from datetime import datetime


my_file = Dataset("/tmp/file_text.txt")
my_file2 = Dataset("/tmp/file_text.txt")

with DAG(

    dag_id='consumer',
    schedule=[my_file,my_file2],
    start_date=datetime(2025,3,1),
    catchup=False

) as dag:


    @task
    def read_dataset():
        with open(my_file.uri,"r") as f:
            for row in f:
                print(row)
    @task
    def read_dataset2():
        with open(my_file2.uri,"r") as f:
            for row in f:
                print(row)


    read_dataset() >> read_dataset2()
            