from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import psycopg2

# Define the connection parameters for the source and destination databases
source_db_conn = {
    'host': 'cornelius.db.elephantsql.com',
    'dbname': 'hrhuqjni',
    'user': 'hrhuqjni',
    'password': 'VohkQF7h6K9rUMPvC_Rn2mV1swCe4lfX'
}

dest_db_conn = {
    'host': 'cornelius.db.elephantsql.com',
    'dbname': 'fhogqepp',
    'user': 'fhogqepp',
    'password': 'BniXKgAiREjFk-fWFTw0q_Rv8Pcw3s89'
}

# Function to transfer data from source_table to dest_table
def transfer_data(source_table, dest_table, **kwargs):
    source_conn = psycopg2.connect(**source_db_conn)
    source_cursor = source_conn.cursor()

    dest_conn = psycopg2.connect(**dest_db_conn)
    dest_cursor = dest_conn.cursor()

    # Get the column names from the source table
    source_cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{source_table}'")
    source_columns = [row[0] for row in source_cursor.fetchall()]

    # Retrieve data from the source table
    source_cursor.execute(f"SELECT * FROM {source_table}")
    rows = source_cursor.fetchall()

    # Get the column names from the destination table
    dest_cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{dest_table}'")
    dest_columns = [row[0] for row in dest_cursor.fetchall()]

    # Filter out any extra columns from the source data
    rows_filtered = [tuple(row[source_columns.index(col)] for col in dest_columns) for row in rows]

    # Truncate the destination table
    dest_cursor.execute(f"TRUNCATE TABLE {dest_table}")

    # Insert the filtered data into the destination table
    for row in rows_filtered:
        dest_cursor.execute(f"INSERT INTO {dest_table} VALUES ({', '.join(['%s'] * len(row))})", row)

    # Commit changes and close connections
    dest_conn.commit()
    source_conn.close()
    dest_conn.close()

# Define DAG parameters
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'max_active_runs': 3  # Limit the number of active DAG runs to 5
}

# Define the DAG
with DAG('postgres_data_transfer', default_args=default_args, schedule_interval=timedelta(days=1)) as dag:
    # Define tasks for transferring data for each table
    transfer_region = PythonOperator(
        task_id='transfer_region',
        python_callable=transfer_data,
        op_kwargs={'source_table': 'region', 'dest_table': 'stg_region'},
    )

    transfer_country = PythonOperator(
        task_id='transfer_country',
        python_callable=transfer_data,
        op_kwargs={'source_table': 'country', 'dest_table': 'stg_country'},
    )

    transfer_state = PythonOperator(
        task_id='transfer_state',
        python_callable=transfer_data,
        op_kwargs={'source_table': 'state', 'dest_table': 'stg_state'},
    )

    transfer_city = PythonOperator(
        task_id='transfer_city',
        python_callable=transfer_data,
        op_kwargs={'source_table': 'city', 'dest_table': 'stg_city'},
    )

    transfer_person = PythonOperator(
        task_id='transfer_person',
        python_callable=transfer_data,
        op_kwargs={'source_table': 'person', 'dest_table': 'stg_person'},
    )

    transfer_segments = PythonOperator(
        task_id='transfer_segments',
        python_callable=transfer_data,
        op_kwargs={'source_table': 'segments', 'dest_table': 'stg_segments'},
    )

    transfer_categories = PythonOperator(
        task_id='transfer_categories',
        python_callable=transfer_data,
        op_kwargs={'source_table': 'categories', 'dest_table': 'stg_categories'},
    )

    transfer_sub_categories = PythonOperator(
        task_id='transfer_sub_categories',
        python_callable=transfer_data,
        op_kwargs={'source_table': 'sub_categories', 'dest_table': 'stg_sub_categories'},
    )

    transfer_products = PythonOperator(
        task_id='transfer_products',
        python_callable=transfer_data,
        op_kwargs={'source_table': 'products', 'dest_table': 'stg_products'},
    )

    transfer_customers = PythonOperator(
        task_id='transfer_customers',
        python_callable=transfer_data,
        op_kwargs={'source_table': 'customers', 'dest_table': 'stg_customers'},
    )

    transfer_shipment_types = PythonOperator(
        task_id='transfer_shipment_types',
        python_callable=transfer_data,
        op_kwargs={'source_table': 'shipment_types', 'dest_table': 'stg_shipment_types'},
    )
    
# Define task dependencies
transfer_region >> transfer_country >> \
transfer_state >> transfer_city >> \
transfer_segments >> transfer_person >> \
transfer_categories >> transfer_sub_categories >> \
transfer_products >> transfer_customers >> transfer_shipment_types