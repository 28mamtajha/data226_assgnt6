
from airflow.decorators import task
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import get_current_context
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime
from datetime import timedelta
import logging
import snowflake.connector

"""
This pipeline assumes that there are two other tables in your snowflake DB
 - user_session_channel
 - session_timestamp

With regard to how to set up these two tables, please refer to this README file:
 - https://github.com/keeyong/sjsu-data226-SP25/blob/main/week8/How-to-setup-ETL-tables-for-ELT.md
"""

def return_snowflake_conn():

    # Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')

    # Execute the query and fetch results
    conn = hook.get_conn()
    return conn.cursor()


@task
def run_ctas(database, schema, table, select_sql, primary_key=None, duplicate_check_column=None):

    logging.info(table)
    logging.info(select_sql)

    cur = return_snowflake_conn()

    try:
        sql = f"CREATE TABLE {database}.{schema}.temp_{table} AS {select_sql}"
        logging.info(sql)
        cur.execute(sql)

        # primary key uniqueness check
        if primary_key is not None:
            sql = f"""
              SELECT {primary_key}, COUNT(1) AS cnt 
              FROM {database}.{schema}.temp_{table}
              GROUP BY {primary_key}
              HAVING COUNT(1) > 1
              ORDER BY cnt DESC
              LIMIT 1"""
            cur.execute(sql)
            result = cur.fetchone()
            if result:
                raise Exception(f"Primary key uniqueness failed: {result}")

        # duplicate check 
        if duplicate_check_column is not None:
            sql = f"""
              SELECT {duplicate_check_column}, COUNT(1) AS cnt 
              FROM {database}.{schema}.temp_{table}
              GROUP BY {duplicate_check_column}
              HAVING COUNT(1) > 1
              ORDER BY cnt DESC
              LIMIT 1"""
            cur.execute(sql)
            result = cur.fetchone()
            
            if result:
                 print(f"Duplicate records found based on {duplicate_check_column}: {result}")

            #if result:
             #   raise Exception(f"Duplicate records found based on {duplicate_check_column}: {result}")

        main_table_creation_if_not_exists_sql = f"""
            CREATE TABLE IF NOT EXISTS {database}.{schema}.{table} AS
            SELECT * FROM {database}.{schema}.temp_{table} WHERE 1=0;"""
        cur.execute(main_table_creation_if_not_exists_sql)

        swap_sql = f"""ALTER TABLE {database}.{schema}.{table} SWAP WITH {database}.{schema}.temp_{table};"""
        cur.execute(swap_sql)
    except Exception as e:
        raise

with DAG(
    dag_id='BuildELT_CTAS',
    start_date=datetime(2024, 10, 2),
    catchup=False,
    tags=['ELT'],
    schedule='45 2 * * *'
) as dag:

    database = "USER_DB_CAMEL"
    schema = "analytics"
    table = "session_summary"
    select_sql = """SELECT u.*, s.ts
    FROM USER_DB_CAMEL.raw.user_session_channel u
    JOIN USER_DB_CAMEL.raw.session_timestamp s ON u.sessionId = s.sessionId
    """

    run_ctas(database, schema, table, select_sql, primary_key='sessionId', duplicate_check_column='userId')
