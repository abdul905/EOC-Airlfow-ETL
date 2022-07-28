
from airflow import DAG
import pandas as pd
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import requests
from clickhouse_driver import Client
import logging

default_args = {'owner': 'airflow',
                'start_date': datetime(2021, 1, 1)
                }

logger = logging.getLogger(__name__)
# Create handlers
c_handler = logging.StreamHandler()
f_handler = logging.FileHandler('logger-file_name')
c_handler.setLevel(logging.WARNING)
c_handler.setLevel(logging.INFO)
# f_handler.setLevel(logging.ERROR)
# Create formatters and add it to handlers
c_format = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
f_format = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
c_handler.setFormatter(c_format)
f_handler.setFormatter(f_format)
# Add handlers to the logger
logger.addHandler(c_handler)
logger.addHandler(f_handler)

#-------------------------------------------------------------------------------------------------------------------------------#
#-------------------------------------------------- DAG-TEMPLATE GENERAL  ------------------------------------------------------#
#------------------------------------------------ Author: BABAR ALI SHAH -------------------------------------------------------#
#-------------------------------------------------------------------------------------------------------------------------------#

def t1():
    logger.info('Function \' t1 \' Started Off')
    client = Client(host='161.97.136.95',
                    user='default',
                    password='pakistan',
                    port='9000', settings={"use_numpy": True})

    url = 'URL-HERE'
    logger.info('Requested Data From Api URL:  \' URL-HERE \' ')

    r = requests.get(url)
    data = r.json()
    logger.info('Received Data  From Api URL:  \' URL-HERE \' ')

    rowsData = data["data"]["data"]
    apiDataFrame = pd.DataFrame(rowsData)
    sql = "query-create-api_table"
    client.execute(sql)

    df2 = client.query_dataframe(
        "query-get-api_table")
    if df2.empty:
        client.insert_dataframe(
            'query-insert-into_api_table', apiDataFrame)


def t2():
    logger.info(' Function  \' t2 \' has been Initiated')
    client = Client(host='161.97.136.95',
                    user='default',
                    password='pakistan',
                    port='9000', settings={"use_numpy": True})
    sql = "query-create-xbi_table"
    client.execute(sql)
    cols = "xbi_columns-names"
    sql = "SELECT " + cols + " query-joined_table "

    data = client.execute(sql)
    apiDataFrame = pd.DataFrame(data)
    all_columns = list(apiDataFrame)  # Creates list of all column headers
    cols = apiDataFrame.iloc[0]
    apiDataFrame[all_columns] = apiDataFrame[all_columns].astype(str)
    d = all-column-names
    dff = pd.DataFrame(columns=d)
    for index, item in enumerate(d):
        dff[item] = apiDataFrame[index].values
    df2 = client.query_dataframe(
        "query-get-xbi_table")
    if df2.empty:
        client.insert_dataframe(
            'query-insert-xbi_table', dff)
        logger.info(
            ' Data has been inserted into Table\' query-insert-xbi_table \' ')

        sql = "query-drop-api_table"
        client.execute(sql)

    else:
        df = pd.concat([dff, df2])
        df = df.astype('str')
        df = df.drop_duplicates(subset='primary-key',
                                keep="first", inplace=False)
        sql = "query-drop-xbi_table;"
        client.execute(sql)
        sql = "query-create-xbi_table"
        client.execute(sql)
        client.insert_dataframe(
            'query-insert-xbi_table', df)

        sql = "query-drop-api_table"
        client.execute(sql)


dag = DAG(
    'dag_id',
    schedule_interval='*/59 * * * *',  # will run every 10 min.
    default_args=default_args,
    catchup=False)

with dag:
    t1 = PythonOperator(
        task_id='t1',
        python_callable=t1,
    )
    t2 = PythonOperator(
        task_id='t2',
        python_callable=t2,
    )
t1 >> t2
