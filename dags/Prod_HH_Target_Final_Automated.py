
# from asyncio.windows_events import NULL
# from cmath import nan

from airflow import DAG
import pandas as pd
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import requests
import numpy as np
import sshtunnel as sshtunnel
from clickhouse_driver import connect
from clickhouse_driver import Client
import logging
import sshtunnel as sshtunnel

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
#-------------------------------------------------- GENERATE DAG FILES  --------------------------------------------------------#
#------------------------------------------------ Author: AB MALIK -------------------------------------------------------------#
#-------------------------------------------------------------------------------------------------------------------------------#

def GetAndInsertApiDataHHTargetFinalUC():
    logger.info('Function \' GetAndInsertApiDataHHTargetFinalUC\' Started Off')
    with sshtunnel.SSHTunnelForwarder(
        ('172.16.3.68', 22),
        ssh_username="root",
        ssh_password="COV!D@19#",
        remote_bind_address=('localhost', 9000)) as server:

        local_port = server.local_bind_port
        print(local_port)

        conn = connect(f'clickhouse://default:mm@1234@localhost:{local_port}/test')
        #conn = connect(host='172.16.3.68', database='test', user='default', password='mm@1234')
        
        cursor = conn.cursor()
        # cursor.execute('SHOW TABLES')
        # print(cursor.fetchall())

        client = Client(host='localhost',port=local_port, database='test',
                                user='default',
                                password='mm@1234',
                                settings={"use_numpy": True})

        get_q = "select * FROM (select * from test.xbi_campaign xc  order by xc.campaign_ID desc limit 1) ORDER BY campaign_ID ASC "
        df2 = client.query_dataframe(get_q)
        #df2 = client.execute(get_q)
        print(df2)
        df = pd.DataFrame(df2)

        campID_list = int(df2.campaign_ID)
        campName_list = str(df2.campaign_ActivityName)#str(df2.campaign_ActivityName)
        campIDold_list = int(df2.campaign_ActivityID_old)
        campYr_list = int(df2.campaign_Yr)

        print(campID_list)
        print(campName_list)
        print(campIDold_list)
        print(campYr_list) 

        
        df = pd.DataFrame()
        url_c = "http://idims.eoc.gov.pk/api_who/proposed/hh/Al68XE2L3N6CzR7qRR1/"
        r = requests.get(url_c)
        data_c = r.json()
        #print(data_c)
        if data_c['data'] == "No data found":
            print("Not Data found for campaign: "+url_c)
        else:
            if data_c['data']['data'] == "No data found":
                print("Not Data found for campaign: "+url_c)    
            else:
                print("Data found for campaign: "+url_c)
                url_c = "http://idims.eoc.gov.pk/api_who/proposed/hh/Al68XE2L3N6CzR7qRR1/"
                r2 = requests.get(url_c)
                data_c = r2.json()
                rowsData = data_c["data"]["data"]["data"]
                print("Data found for campaign: "+url_c)
                apiDataFrame = pd.DataFrame(rowsData)
                apiDataFrame['ActivityID_fk'] = pd.Series([campID_list for x in range(len(apiDataFrame.index))])
                apiDataFrame['ActivityID_old'] = pd.Series([campIDold_list for x in range(len(apiDataFrame.index))])
                apiDataFrame['Activity_year'] = pd.Series([campYr_list for x in range(len(apiDataFrame.index))])
                print("DF Page Size",len(apiDataFrame))
                df = df.append(apiDataFrame, ignore_index=True)
                print("DF API Size",len(df))
            # merged_df = merged_df.append(df, ignore_index=True)
            # print("Merged DF Size",len(merged_df),'\n')     
            
            #merged_df.to_excel("Downloads/dataHHTarget.xlsx",sheet_name='Sheet_1', index=False)

            sql = " Create Table if not exists test.get_hhtarget_final(Tcode Int32, Ucode Int32, IDcampCat Int32, Proposed_HH_Targets Int32, ActivityID_fk Int32, ActivityID_old Int32, Activity_year Int32)ENGINE = MergeTree PRIMARY KEY Ucode ORDER BY Ucode"
            client.execute(sql)

            df2 = client.query_dataframe(
                "SELECT * FROM test.get_hhtarget_final")
            if df2.empty:
                client.insert_dataframe(
                    'INSERT INTO test.get_hhtarget_final VALUES', apiDataFrame)       


def CreateJoinTableOfHHTargetUC():
    logger.info(' Function  \' CreateJoinTableOfHHTargetUC\' has been Initiated')
    with sshtunnel.SSHTunnelForwarder(
        ('172.16.3.68', 22),
        ssh_username="root",
        ssh_password="COV!D@19#",
        remote_bind_address=('localhost', 9000)) as server:

        local_port = server.local_bind_port
        print(local_port)

        conn = connect(f'clickhouse://default:mm@1234@localhost:{local_port}/test')
        #conn = connect(host='172.16.3.68', database='test', user='default', password='mm@1234')
        
        cursor = conn.cursor()
        # cursor.execute('SHOW TABLES')
        # print(cursor.fetchall())

        client = Client(host='localhost',port=local_port, database='test',
                                user='default',
                                password='mm@1234',
                                settings={"use_numpy": True})
        sql = "CREATE TABLE if not exists test.xbi_hhtarget_final (Tcode Int32, Ucode Int32, IDcampCat Int32, Proposed_HH_Targets Int32, ActivityID_fk Int32, ActivityID_old Int32, Activity_year Int32)ENGINE = MergeTree PRIMARY KEY Ucode ORDER BY Ucode"
        client.execute(sql)
        cols = "test.get_hhtarget_final.Tcode, test.get_hhtarget_final.Ucode, test.get_hhtarget_final.IDcampCat, test.get_hhtarget_final.Proposed_HH_Targets, test.get_hhtarget_final.ActivityID_fk, test.get_hhtarget_final.ActivityID_old, test.get_hhtarget_final.Activity_year"
        sql = "SELECT " + cols + "  FROM test.get_hhtarget_final"

        data = client.execute(sql)
        apiDataFrame = pd.DataFrame(data)
        all_columns = list(apiDataFrame)  # Creates list of all column headers
        cols = apiDataFrame.iloc[0]
        apiDataFrame[all_columns] = apiDataFrame[all_columns].astype(str)
        d = 'Tcode', 'Ucode', 'IDcampCat', 'Proposed_HH_Targets','ActivityID_fk', 'ActivityID_old', 'Activity_year'
        dff = pd.DataFrame(columns=d)
        for index, item in enumerate(d):
            dff[item] = apiDataFrame[index].values
        df2 = client.query_dataframe(
            "SELECT * FROM test.xbi_hhtarget_final")
        if df2.empty:
            client.insert_dataframe(
                'INSERT INTO test.xbi_hhtarget_final  VALUES', dff)

            sql = "DROP table if exists test.get_hhtarget_final"
            client.execute(sql)

        else:
            sql = "TRUNCATE TABLE if exists test.xbi_hhtarget_final"
            client.execute(sql)
            
            client.insert_dataframe(
                'INSERT INTO test.xbi_hhtarget_final  VALUES', dff)

            sql = "DROP table if exists test.get_hhtarget_final"
            client.execute(sql)


dag = DAG(
    'Prod_HH_Target_Final_UC',
    #schedule_interval='0 0 * * 0', #Run once a week at midnight on Sunday morning
    schedule_interval=None,
    #schedule_interval='0 0 * * *',  # once a day at midnight
    #schedule_interval='*/59 * * * *',  # will run every 10 min.
    default_args=default_args,
    catchup=False)

with dag:
    GetAndInsertApiDataHHTargetFinalUC= PythonOperator(
        task_id='GetAndInsertApiDataHHTargetFinalUC',
        python_callable=GetAndInsertApiDataHHTargetFinalUC,
    )
    CreateJoinTableOfHHTargetUC= PythonOperator(
        task_id='CreateJoinTableOfHHTargetUC',
        python_callable=CreateJoinTableOfHHTargetUC,
    )

GetAndInsertApiDataHHTargetFinalUC >> CreateJoinTableOfHHTargetUC     