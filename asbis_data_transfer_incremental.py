from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

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
def transfer_data(source_table, param_name, dest_table, last_timeload_field):
    source_hook = PostgresHook(postgres_conn_id='source_postgres_conn')
    destination_hook = PostgresHook(postgres_conn_id='destination_postgres_conn')
    
    source_conn = source_hook.get_conn()
    source_cursor = source_conn.cursor()
    
    destination_conn = destination_hook.get_conn()
    destination_cursor = destination_conn.cursor()
    
    dest_cursor.execute(f"SELECT {param_name} FROM etl_controls SET {param_name} = current_timestamp" )
    rows = source_cursor.fetchall()
    cut_date = row[0]

    source_cursor.execute(f"SELECT * FROM {source_table} WHERE {last_timeload_field} > %s", cut_date)
    rows = source_cursor.fetchall()

    destination_cursor.execute(f"TRUNCATE TABLE {dest_table}")
    
    for row in rows:
        destination_cursor.execute(f"INSERT INTO {dest_table} VALUES ({', '.join(['%s'] * len(row))})", row)
    source_conn.commit()
    destination_conn.commit()
    source_conn.close()
    destination_conn.close()

    # Update etl_controls table with the latest last_timeload date
    dest_cursor = destination_conn.cursor()
    dest_cursor.execute(f"UPDATE etl_controls SET {source_table}_last_timeload = current_timestamp" )
    destination_conn.commit()

# ORD DETAIL 
    if source_table == "ord_head":
        dest_cursor.execute("SELECT ORDER_ID FROM STG_ORD_HEAD")
        orders_id_data = dest_cursor.fetchall()
        for row_ord_head in orders_id_data:
            source_cursor.execute("SELECT * FROM ORD_DETAIL WHERE ORDER_ID= %s", row_ord_head[0])
            orders_id_data = source_cursor.fetchall()
            for row_det in orders_id_data:
                dest_cursor.execute("INSERT INTO STG_ORD_DETAIL VALUES (%s,%s,%s,%s,%s,%s,%s,)", \
                    (row_det[0], row_det[1], row_det[2],row_det[3], row_det[4], row_det[5], row_det[6], row_det[7]))

    destination_conn.commit()
    destination_conn.close()
    source_conn.close()

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
with DAG('postgres_data_transfer_incremental', default_args=default_args, schedule_interval=timedelta(days=1)) as dag:
    # Define tasks for transferring data for each table
    tables_to_transfer = [
        ('ord_head', 'orders_last_timeload', 'stg_ord_head', 'order_date'),
        ('shipments', 'returns_last_timeload', 'stg_shipments', 'shipment_date'),
        ('returns', 'shipments_last_timeload', 'stg_returns', 'return_date')
    ]
    
    for source_table, param_name, dest_table, last_timeload_field in tables_to_transfer:
        transfer_task = PythonOperator(
            task_id=f'transfer_{source_table}',
            python_callable=transfer_data,
            op_kwargs={'source_table': source_table, 'param_name': param_name, 'dest_table': dest_table, 'last_timeload_field': last_timeload_field},
            pool='postgres_pool',  # Assign the task to a connection pool
        )

# Set task dependencies
for i in range(len(tables_to_transfer) - 1):
    dag.set_dependency(f'transfer_{tables_to_transfer[i][0]}', f'transfer_{tables_to_transfer[i+1][0]}')
