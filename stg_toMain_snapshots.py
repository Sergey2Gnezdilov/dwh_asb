from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator

# Define the DAG parameters
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'max_active_runs': 3  # Limit the number of active DAG runs to 5
}

# Define the tables to transfer data from STG_ to main tables
stg_to_main_tables = [
    ('stg_region', 'region'),
    ('stg_country', 'country'),
    ('stg_state', 'state'),
    ('stg_city', 'city'),
    ('stg_person', 'person'),
    ('stg_segments', 'segments'),
    ('stg_categories', 'categories'),
    ('stg_sub_categories', 'sub_categories'),
    ('stg_products', 'products'),
    ('stg_customers', 'customers'),
    ('stg_shipment_types', 'shipment_types')

    ('stg_shipment_types', 'ord_head')
    ('stg_ord_detail', 'ord_detail')
    ('stg_shipments', 'shipments')
    ('stg_returns', 'returns')
]


# Define the DAG
with DAG('postgres_data_transfer_stg_to_main', default_args=default_args, schedule_interval=timedelta(days=1)) as dag:
    # Define task for each table transfer
    transfer_tasks = []
    for source_table, dest_table in stg_to_main_tables:
        # Define the SQL query to transfer data from staging table to main table
        sql_query = f"""
            INSERT INTO {dest_table}
            SELECT * FROM {source_table};
        """
        # Define the operator to execute the SQL query
        transfer_task = PostgresOperator(
            task_id=f'transfer_{source_table}_to_{dest_table}',
            sql=sql_query,
            postgres_conn_id='destination_postgres_conn'  # Use connection to destination database
        )
        transfer_tasks.append(transfer_task)

    # Set task dependencies
    for i in range(len(transfer_tasks) - 1):
        transfer_tasks[i].set_downstream(transfer_tasks[i + 1])