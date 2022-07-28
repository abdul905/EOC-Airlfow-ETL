
from airflow import DAG
import pandas as pd
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import requests
from clickhouse_driver import Client
import logging
import numpy as np

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
#---------------------------------------- INSERT LATEST CAMPAIGN DATA ICM_Cluster_HH Data --------------------------------------#
#------------------------------------------------ Author: Abdul Bari Malik -----------------------------------------------------#
#-------------------------------------------------------------------------------------------------------------------------------#
def GetAndInsertApiDataICMClusterHH():
    #li = [222,223,224,225,226,227,228,229,230,231,232,233,234,235,236,237,238,239,240,241,242,243,244,245,247,248,249,250,251,252,253,254,255,256]
    li = [243]
    df_all = pd.DataFrame()
    FIELDS = ['houses']
    merged_df = pd.DataFrame()
    for cid in li:
        logger.info('Function \' GetAndInsertApiDataICMClusterHH \' Started Off')
        client = Client(host='161.97.136.95',
                        user='default',
                        password='pakistan',
                        port='9000', settings={"use_numpy": True})
                                        
        url = "http://idims.eoc.gov.pk/api_who/api/get_icm_cluster/Al68XE2L3N6CzR7qRR1/"+ str(cid)+"/"
        print('\n\n\n Campaign-ID\t\t--------\t', cid)
        logger.info('Requested Data From Api URL:'+url)
        
        r = requests.get(url)
        data = r.json()
        
        if data['data'] == "No data found":
            print("Not Data found for campaign: "+str(cid))
            logger.info('Not Data found for campaign: '+str(cid))
        else:
            print("Data found for campaign: "+str(cid))
            
            print("Campaign_Total_Pages",data['total_page'])
            i = 1
            while int(i) <= data['total_page']:
                url_c = "http://idims.eoc.gov.pk/api_who/api/get_icm_cluster/Al68XE2L3N6CzR7qRR1/"+str(cid)+'/'+str(i)
                r2 = requests.get(url_c)
                data_c = r2.json()
                if data_c['data'] == "No data found":
                    print("Not Data found for campaign: "+url_c)
                    break
                else:
                    rowsData = data_c["data"]["data"]#[0]['houses']
                    print("Data found for campaign: "+url_c)
                    print("Data found for Page: "+str(i))
                    apiDataFrame = pd.DataFrame(rowsData)
                    df = apiDataFrame[FIELDS]
                    df = df.explode('houses')
                    df_final  = (pd.DataFrame(df['houses'].apply(pd.Series)))
                    df_final = df_final.reset_index(drop=True)
                    print("DF Page Size",len(df_final))
                    df_all = df_all.append(df_final, ignore_index=True)
                    print("DF API Size",len(df_all))
                    i+=1
                    
                    logger.info('Received Data  From Api URL: '+url_c)
            merged_df = merged_df.append(df_all, ignore_index=True)
            print("Merged DF Size",len(merged_df))            
            sql = "CREATE TABLE if not exists test.get_icm_cluster_hh (pk_icm_hh_house_21_id String, fk_form_id String, fk_user_id String, date_created String, fk_parentform_id String, fk_parent_id String, hh_no String, child_0_11 String, child_0_11_vac String , child_0_11_recal String, child_12_59 String, child_12_59_vac String, child_12_59_recal String, team_miss_house String, team_miss_child String, child_away String, refusal String, refusal_reason String, guest String, guest_vac String, total_seen String, total_finger_mark String, hh_name String, doormarked String, rutine_epi String, comments_missed_child String, no_vacc_monitor String, old_info1 String, notv_fm String, final_remarks_h String, fake_fm_source String, date_updated String )ENGINE = MergeTree PRIMARY KEY pk_icm_hh_house_21_id ORDER BY pk_icm_hh_house_21_id"
            client.execute(sql)
            df2 = client.query_dataframe("SELECT * FROM test.get_icm_cluster_hh")
            if df2.empty:
                df_all = df_all.replace(r'^\s*$', np.nan, regex=True)
                client.insert_dataframe(
                    'INSERT INTO test.get_icm_cluster_hh VALUES', df_all)
                logger.info(
                        ' Data has been inserted into Table get_icm_cluster for campaign'+str(cid))    
                        
dag = DAG(
    'ICM_Cluster_HH_Automated',
    #schedule_interval='*/10 * * * *',# will run every 10 min.
    #schedule_interval='0 0 * * *',  # will run every mid-night
    schedule_interval=None,
    default_args=default_args,
    catchup=False)

with dag:
    GetAndInsertApiDataICMClusterHH = PythonOperator(
        task_id='GetAndInsertApiDataICMClusterHH',
        python_callable=GetAndInsertApiDataICMClusterHH,
    )
GetAndInsertApiDataICMClusterHH
