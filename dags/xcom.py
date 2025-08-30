from airflow.sdk import dag, task, Context

@dag
def xcom_dag():

    # @task
    # # def task_a(**context: Context):
    # def task_a():
    #     val = 42
    #     #context['ti'].xcom_push(key='my_key', value=val)
    #     return val # xcom_push(key='return_value', value=42)
    # @task
    # # def task_b(**context: Context):
    # def task_b(val: int):
    #     # val = context['ti'].xcom_pull(task_ids='task_a', key='my_key')
    #     print(val)

    # val = task_a() 
    # task_b(val)

    # other possibility is used ti (Mark's used)
    @task
    def task_a(ti):
        val = {
            "val_1": 42,
            "val_2": 43
        }
        ti.xcom_push(key='my_key', value=val)

    # @task
    # def task_c(ti):
    #     val = 43
    #     ti.xcom_push(key='my_key', value=val)

    @task
    def task_b(ti):
        vals = ti.xcom_pull(task_ids=['task_a', 'task_c'], key=['my_key'])

    task_a() >>task_b()

xcom_dag()