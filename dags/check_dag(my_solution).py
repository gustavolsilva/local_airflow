from airflow.sdk import dag, task
from pendulum import datetime
import subprocess

@dag(
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    description="DAG to check data and belongs to the data_engineering team",
    tags=["data_engineering"],
    max_consecutive_failed_dag_runs=3
)
def check_dag():
    @task
    def create_file():
        # Execute bash command to create the file
        subprocess.run(['bash', '-c', 'echo "Hi, there!" > /tmp/dummy'], check=True)
    
    @task
    def check_file():
        # Execute bash command to check if the file exists
        result = subprocess.run(['bash', '-c', 'test -f /tmp/dummy'], check=False)
        if result.returncode == 0:
            print("File exists.")
        else:
            raise FileNotFoundError("File does not exist.")

    @task
    def view_file():
        print(open('/tmp/dummy', 'rb').read())

    create_file() >> check_file() >> view_file()