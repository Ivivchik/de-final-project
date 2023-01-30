import re
import pendulum

from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.vertica.hooks.vertica import VerticaHook
from airflow.providers.vertica.operators.vertica import VerticaOperator


doc_md = """
### Final project in de course
#### Create data vault for finance
#### Source
- s3
"""

S3_CONN = S3Hook.get_connection('s3_yandex_storage')
AWS_ACCESS_KEY_ID = S3_CONN.extra_dejson.get('aws_access_key_id')
AWS_SECRET_ACCESS_KEY = S3_CONN.extra_dejson.get('aws_secret_access_key')
AWS_ENDPOINT = S3_CONN.extra_dejson.get('aws_endpoint')

VERTICA_CONN_ID = VerticaHook('vertica_conn')
VERTICA_AWS_AUTH = f'{AWS_ACCESS_KEY_ID}:{AWS_SECRET_ACCESS_KEY}'

@task
def upload_table_from_s3(bucket_name, table_name, delimiter):
    s3_url = f's3://{bucket_name}/{table_name}.csv'
    regex = r"transactions_batch_\d+"
    if re.match(regex, table_name):
        table_name = 'transactions'
    query = f'COPY ivivchikyandexru__staging.{table_name} FROM \'{s3_url}\' DELIMITER \'{delimiter}\''
    with VERTICA_CONN_ID.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f'ALTER SESSION SET AWSAuth = \'{VERTICA_AWS_AUTH}\'')
            cur.execute(f'ALTER SESSION SET AWSENDPOINT = \'{AWS_ENDPOINT}\'')
            cur.execute(query)

def table_action(table_name, prefix, action):
    return VerticaOperator(
                            task_id=f'{action}_{table_name}',
                            vertica_conn_id='vertica_conn',
                            sql=f'sql/{action}_{prefix}/{table_name}.sql')
    

@dag(description='Provide default dag for final project',
     schedule="0 8 * * *",
     start_date=pendulum.parse('2022-10-01'),
     catchup=True,
     tags=['final', 'project'],
     max_active_runs=1
     )

def finance_fill_db_dag():

    start = DummyOperator(task_id='start')

    with TaskGroup(group_id='create_stg_tables') as create_stg_tables:
        create_currencies = table_action('currencies', 'stg', 'create')
        create_transactions = table_action('transactions', 'stg', 'create')

        create_currencies >> create_transactions
   
    with TaskGroup(group_id='create_dwh_tables') as create_dwh_tables:
        create_h_account_number = table_action('h_account_number', 'dwh', 'create')
        create_h_currency = table_action('h_currency', 'dwh', 'create')
        create_h_transaction = table_action('h_transaction', 'dwh', 'create')
        create_l_currency_exchange = table_action('l_currency_exchange', 'dwh', 'create')
        create_l_transaction_account = table_action('l_transaction_account', 'dwh', 'create')
        create_l_transaction_currency = table_action('l_transaction_currency', 'dwh', 'create')
        create_s_currency_exchange = table_action('s_currency_exchange', 'dwh', 'create')
        create_s_transaction_amount = table_action('s_transaction_amount', 'dwh', 'create')
        create_s_transaction_country = table_action('s_transaction_country', 'dwh', 'create')
        create_s_transaction_status = table_action('s_transaction_status', 'dwh', 'create')
        create_s_transaction_type = table_action('s_transaction_type', 'dwh', 'create')

        inter1 = DummyOperator(task_id='inter1')

        ([create_h_account_number, create_h_currency, create_h_transaction]
         >> inter1
         >> [create_l_currency_exchange, create_l_transaction_account,
             create_l_transaction_currency, create_s_currency_exchange,
             create_s_transaction_amount, create_s_transaction_country,
             create_s_transaction_status, create_s_transaction_type]
        )

    inter_create_upload = DummyOperator(task_id='inter_create_upload')

    with TaskGroup(group_id='upload_stg_tables') as upload_stg_tables:

        upload_transactions = [upload_table_from_s3('final-project', f'transactions_batch_{i}', ',') for i in range(1, 11)]
        upload_currencies = upload_table_from_s3('final-project', 'currencies_history', ',')

        [upload_transactions, upload_currencies]

    inter_upload_drop_duplicate = DummyOperator(task_id='inter_upload_drop_duplicate')

    with TaskGroup(group_id='drop_duplicate_stg') as drop_duplicate_stg:

        drop_currencies = table_action('currencies', 'stg', 'drop_duplicate')
        drop_transactions = table_action('transactions', 'stg', 'drop_duplicate')

        [drop_currencies, drop_transactions]

    inter_drop_duplicate_insert = DummyOperator(task_id='inter_drop_duplicate_insert')

    with TaskGroup(group_id='insert_dwh_tables') as insert_dwh_tables:
        insert_h_account_number = table_action('h_account_number', 'dwh', 'insert')
        insert_h_currency = table_action('h_currency', 'dwh', 'insert')
        insert_h_transaction = table_action('h_transaction', 'dwh', 'insert')
        insert_l_currency_exchange = table_action('l_currency_exchange', 'dwh', 'insert')
        insert_l_transaction_account = table_action('l_transaction_account', 'dwh', 'insert')
        insert_l_transaction_currency = table_action('l_transaction_currency', 'dwh', 'insert')
        insert_s_currency_exchange = table_action('s_currency_exchange', 'dwh', 'insert')
        insert_s_transaction_amount = table_action('s_transaction_amount', 'dwh', 'insert')
        insert_s_transaction_country = table_action('s_transaction_country', 'dwh', 'insert')
        insert_s_transaction_status = table_action('s_transaction_status', 'dwh', 'insert')
        insert_s_transaction_type = table_action('s_transaction_type', 'dwh', 'insert')

        inter2 = DummyOperator(task_id='inter2')

        ([insert_h_account_number, insert_h_currency, insert_h_transaction]
         >> inter2
         >> [insert_l_currency_exchange, insert_l_transaction_account,
             insert_l_transaction_currency, insert_s_currency_exchange,
             insert_s_transaction_amount, insert_s_transaction_country,
             insert_s_transaction_status, insert_s_transaction_type]
        )

    end = DummyOperator(task_id='end')

    (
    start
    >> [create_stg_tables, create_dwh_tables]
    >> inter_create_upload
    >> upload_stg_tables
    >> inter_upload_drop_duplicate
    >> drop_duplicate_stg
    >> inter_drop_duplicate_insert
    >> insert_dwh_tables
    >> end
    )
    
_ = finance_fill_db_dag()