import pendulum

from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.vertica.hooks.vertica import VerticaHook
from airflow.providers.vertica.operators.vertica import VerticaOperator


doc_md = """
### Final project in de course
#### Build datamart global metric
#### Source
- Vertica
"""

VERTICA_CONN_ID = VerticaHook('vertica_conn')

def table_action(table_name, action):
    return VerticaOperator(
                            task_id=f'{action}_{table_name}',
                            vertica_conn_id='vertica_conn',
                            sql=f'sql/datamarts/{action}_{table_name}.sql')
    

@dag(description='Provide default dag for final project',
     schedule_interval="30 8 * * *",
     start_date=pendulum.parse('2022-10-01'),
     end_date=pendulum.parse('2022-11-01'),
     catchup=True,
     tags=['final', 'project'],
     max_active_runs=1
     )

def global_metrics_dag():

    start = DummyOperator(task_id='start')

    create_global_metric = table_action('global_metrics', 'create')
    insert_global_metric = table_action('global_metrics', 'insert')

    end = DummyOperator(task_id='end')

    (
    start
    >> create_global_metric
    >> insert_global_metric
    >> end
    )
    
_ = global_metrics_dag()