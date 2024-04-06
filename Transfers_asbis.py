from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
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

# Function to transfer data from source to destination database
def transfer_data():
    try:
        # Connect to source database
        source_conn = psycopg2.connect(**source_db_conn)
        source_cursor = source_conn.cursor()
        
        # Connect to destination database
        dest_conn = psycopg2.connect(**dest_db_conn)
        dest_cursor = dest_conn.cursor()

        # Transfer Customers table
        source_cursor.execute("SELECT * FROM Customers")
        customers_data = source_cursor.fetchall()
        for row in customers_data:
            dest_cursor.execute("INSERT INTO STG_Customers VALUES (%s, %s)", row)

        # Transfer Segments table
        source_cursor.execute("SELECT * FROM Segments")
        segments_data = source_cursor.fetchall()
        for row in segments_data:
            dest_cursor.execute("INSERT INTO STG_Segments VALUES (%s, %s)", row)

        # Transfer Categories table
        source_cursor.execute("SELECT * FROM Categories")
        categories_data = source_cursor.fetchall()
        for row in categories_data:
            dest_cursor.execute("INSERT INTO STG_Categories VALUES (%s, %s)", row)

        # Transfer Sub_Categories table
        source_cursor.execute("SELECT * FROM Sub_Categories")
        sub_categories_data = source_cursor.fetchall()
        for row in sub_categories_data:
            dest_cursor.execute("INSERT INTO STG_Sub_Categories VALUES (%s, %s, %s)", row)

        # Commit and close connections
        dest_conn.commit()
        source_conn.close()
        dest_conn.close()
    except Exception as e:
        print("Error transferring data:", str(e))

# Define the DAG
dag = DAG(
    'data_transfer_dag',
    start_date=datetime(2024, 3, 11),
    schedule_interval='@once'  # This DAG will run only once
)

transfer_data_task = PythonOperator(
    task_id='transfer_data',
    python_callable=transfer_data,
    dag=dag
)


transfer_data_task
