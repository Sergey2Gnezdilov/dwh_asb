from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

def extract_and_load_customer_data():
    # Connect to source and destination databases
    source_hook = PostgresHook(postgres_conn_id='source_postgres_conn')
    destination_hook = PostgresHook(postgres_conn_id='destination_postgres_conn')
    
    # Get cursor for source database
    source_conn = source_hook.get_conn()
    source_cursor = source_conn.cursor()
    
    # Get cursor for destination database
    destination_conn = destination_hook.get_conn()
    destination_cursor = destination_conn.cursor()
    
    # Extract data from source table
    source_cursor.execute("SELECT customer_id, customer_name, segment_id FROM customers;")
    rows = source_cursor.fetchall()
    
    # Load data into destination table
    destination_cursor.execute("TRUNCATE TABLE stg_customers")
    for row in rows:
        destination_cursor.execute("INSERT INTO stg_customers (customer_id, customer_name, segment_id) VALUES (%s, %s, %s);", row)
    
    # Commit and close connections
    destination_conn.commit()
    destination_conn.close()
    source_conn.close()

dag_params = {
    'dag_id': 'postgres_data_migration',
    'start_date': datetime(2020, 4, 20),
    'schedule_interval': timedelta(seconds=60)
}

with DAG(**dag_params) as dag:
    extract_and_load_task = PythonOperator(
        task_id='extract_and_load_customer_data',
        python_callable=extract_and_load_customer_data,
    )

extract_and_load_customer_data