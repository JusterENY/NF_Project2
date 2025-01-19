from airflow import DAG

from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python_operator import PythonOperator
from airflow.configuration import conf
from airflow.models import Variable

import pandas
from datetime import datetime

PATH = Variable.get("path_rd")
conf.set("core","template_searchpath",PATH)

def insert_data(table_name):
    df = pandas.read_csv(PATH + f"{table_name}.csv",sep=None,encoding="cp1251")
    postgres_hook = PostgresHook("dwh-db")
    engine = postgres_hook.get_sqlalchemy_engine()
    df.to_sql(table_name,engine,schema="stage",if_exists="replace",index=False)
default_args = {
    "owner" : "postgres",
    "start_date" : datetime(2025, 1, 19),
     "retries" : 2
}

with DAG(
    "insert_rd",
    default_args=default_args,
    description="Загрузка данных в stage",
    catchup=False,
    template_searchpath = [PATH],
    schedule="0 0 * * *"
) as dag:
    
    start = DummyOperator(
        task_id = "start"
    )

    create_schema = SQLExecuteQueryOperator(
        task_id="create_schema",
        conn_id="dwh-db",
        sql="CREATE SCHEMA IF NOT EXISTS stage;"
    )   

    deal_info = PythonOperator(
        task_id="deal_info",
        python_callable=insert_data,
        op_kwargs={"table_name":"deal_info"}
    )

    product_info = PythonOperator(
        task_id="product_info",
        python_callable=insert_data,
        op_kwargs={"table_name":"product_info"}
    )

    end = DummyOperator(
        task_id = "end"
    )

    (
        start 
        >> create_schema
        >> [deal_info,product_info]
        >> end
    )