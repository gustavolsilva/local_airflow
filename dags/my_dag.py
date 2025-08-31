from airflow.sdk import dag, task, DAG, chain
from pendulum import datetime


# 1st Way
@dag(
        schedule="@daily",
        start_date=datetime(2025, 1, 1),
        description="This dags does...",
        tags=["team_a", "source_a"],
        max_consecutive_failed_dag_runs=3
)
def my_dag():
    @task
    def task_a():
        print("Hello from task A!")

    @task
    def task_b():
        print("Hello from task B!")


    @task
    def task_c():
        print("Hello from task C!")

    @task
    def task_d():
        print("Hello from task D!")

    @task
    def task_e():
        print("Hello from task E!")

    # sequence execution crescent from left to right >> operator 99% times
    task_a() >> task_b() >> task_c() >> task_d()

    # # when change direction, started in task d to task a (from de right to left)
    # task_a() << task_b() << task_c() << task_d()

    # when need paralels execution, enough tasks allocate in list
    # task_a() >> [task_b(), task_d()] >> [task_c(), task_e()]

    # when need the task_c depends on the task_b in paralels the task_e depends on the task_d and started on task_a
    # task_a() >> [task_b(), task_c()] # expected
    # task_a() >> [task_d(), task_e()] # not expected
    # # this create task_a_1 in diagram airflow

    # one solution is variable
    a = task_a()

    # a >> [task_b(), task_c()] # wrong, because its runing indempendecy tasks
    # a >> [task_d(), task_e()] # wrong, because its runing indempendecy tasks
    # a >> task_b() >> task_c()
    # a >> task_d() >> task_e()

    # using chain, get a single line
    chain(task_a(), [task_b(), task_d()],[task_c(), task_e()])


my_dag()


# #2nd Way using DAG and tasks
# with DAG(

#     schedule="@daily",
#     start_date=datetime(2025, 1, 1),
#     description="This dags does...",
#     tags=["team_a", "source_a"],
#     max_consecutive_failed_dag_runs=3
# ):
#     @task(dag=d)
#     def task_a():
#         print("Hello from tas A!")

#     task_a()