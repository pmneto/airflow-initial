from airflow import DAG,Dataset
from airflow.decorators import task


from datetime import datetime 

my_file = Dataset("/tmp/file_text.txt")
my_file2 = Dataset("/tmp/file_text.txt")


with DAG(
    dag_id="producer",
    schedule="@daily",
    start_date = datetime(2025,3,1),
    catchup=False

) as dag:


    @task(outlets=[my_file])
    def update_dataset():
        with open(my_file.uri,"a+") as f:
            f.write("producer update")

    @task(outlets=[my_file])
    def update_dataset2():
        with open(my_file2.uri,"a+") as f:
            f.write("producer update")

    update_dataset() >> update_dataset2()