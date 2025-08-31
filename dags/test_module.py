from airflow.decorators import dag, task
import pendulum
from my_packages.module_a.module_a import TestClass

@dag(schedule=None, start_date=pendulum.datetime(2023, 1, 1, tz="UTC"), catchup=False)
def test_module():

    @task
    def test_task():
        print(TestClass().my_time())
    
    test_task()

dag = test_module()