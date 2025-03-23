from airflow import DAG # type: ignore
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator # type: ignore
from airflow.utils.dates import days_ago # type: ignore

# Define default arguments
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1
}

# Define the DAG
dag = DAG(
    'etl_to_snowflake',
    default_args=default_args,
    schedule_interval="30 2 * * *",  # Runs daily at 2:30 AM
    catchup=False
)

# SQL queries to load data into Snowflake
load_user_session_channel = """
COPY INTO raw.user_session_channel
FROM @dev.raw.BLOB_STAGE/user_session_channel.csv
FILE_FORMAT = (TYPE = CSV, SKIP_HEADER = 1);
"""

load_session_timestamp = """
COPY INTO raw.session_timestamp
FROM @dev.raw.BLOB_STAGE/session_timestamp.csv
FILE_FORMAT = (TYPE = CSV, SKIP_HEADER = 1);
"""

# Define Snowflake Operators
load_user_session_task = SnowflakeOperator(
    task_id='load_user_session_channel',
    sql=load_user_session_channel,
    snowflake_conn_id='snowflake_conn',
    dag=dag
)

load_session_timestamp_task = SnowflakeOperator(
    task_id='load_session_timestamp',
    sql=load_session_timestamp,
    snowflake_conn_id='snowflake_conn',
    dag=dag
)

load_user_session_task >> load_session_timestamp_task
