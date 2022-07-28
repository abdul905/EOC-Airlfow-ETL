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
#----------------------------------------------- INSERT HH Target Data ---------------------------------------------------------#
#-------------------------------------------- Author: Abdul Bari Malik ---------------------------------------------------------#
#-------------------------------------------------------------------------------------------------------------------------------#

def GetAndInsertApiDataHHTarget():
    
    # li1 = [28, 29, 31, 30, 1, 106, 34, 9, 37, 40, 40, 43, 10, 2, 46, 11, 3, 107, 1, 106, 2, 9, 8, 10, 3, 107, 28, 31, 4, 9, 1, 28, 31, 2]
    # li2 = [2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2022, 2022, 2022, 2022, 2022]

    li1 = [2]
    li2 = [2022]
    
    merged_df = pd.DataFrame()

    for f, b in zip(li1, li2):
        df = pd.DataFrame()
        logger.info('Function \' GetAndInsertApiDataHHTarget \' Started Off')
        client = Client(host='161.97.136.95',
                        user='default',
                        password='pakistan',
                        port='9000', settings={"use_numpy": True})

        url = "https://idims.eoc.gov.pk/api_who/index.php/api/hhtargets/5468xe2ln6CZR7QRG041/"+ str(f)+"/"+ str(b)
        
        logger.info('Requested Data From Api URL: '+url)

        r = requests.get(url)
        data = r.json()
        if data['data'] == "No data found":
            print("Not Data found for campaign: "+url)
        else:
            if data['data']['data'] == "No data found":
                print("Not Data found for campaign: "+url)    
            else:
                print("Data found for campaign: "+url)
                total_page = data["data"]["total_page"]
                print("Total Pages for an API Data:"+str(total_page) )
                i=1
                while int(i) <= total_page:
                    url_c = "https://idims.eoc.gov.pk/api_who/index.php/api/hhtargets/5468xe2ln6CZR7QRG041/"+ str(f)+"/"+ str(b)+"/"+str(i)
                    r2 = requests.get(url_c)
                    data_c = r2.json()
              
                    rowsData = data_c["data"]["data"]["data"]
                    print("Data found for campaign: "+url_c)
                    print("Data found for Page: "+str(i))
                    apiDataFrame = pd.DataFrame(rowsData)
                    print("DF Page Size",len(apiDataFrame))
                    df = df.append(apiDataFrame, ignore_index=True)
                    print("DF API Size",len(df))
                    i+=1
                merged_df = merged_df.append(df, ignore_index=True)
                print("Merged DF Size",len(merged_df),'\n')     
                print('\nCampaign \t\t--------\t',f,'--',b,'\t-------- Data Inserted \n')

                sql = "CREATE TABLE if not exists test.get_hhtarget  (ID Int32,TimeStamp DateTime('Asia/Karachi'),Yr Int32,UserID Int32,ActivityID_fk Int32,CategoryID Int32,ProvID Int32,DivID Int32,DistID Int32,TehsilID Int32,UCID Int32,Ucode Int32,Proposed_HH_Targets Int32,Final_HH_Targets Int32,Seasonal String,Nomads String,Agriculture String,Beggers String,IDPs String,Family String,Others String,Status String,Trash Int32,Remarks String,PRIMARY KEY(ID ))ENGINE = MergeTree"
                client.execute(sql)
    
                df2 = client.query_dataframe("SELECT * FROM test.get_hhtarget")            
    
                if df2.empty:
                    #df = df.replace(r'^\s*$', np.nan, regex=True)
                    df['TimeStamp'] = pd.to_datetime(df['TimeStamp'], errors='coerce')
                    client.insert_dataframe(
                        'INSERT INTO test.get_hhtarget VALUES', df)
                


def CreateJoinTableOfHHTarget():
    c = 2
    y = 2022
    
    logger.info('Function \' CreateJoinTableOfHHTarget \' Started Off')
    
    client = Client(host='161.97.136.95',
            user='default',
            password='pakistan',
            port='9000', settings={"use_numpy": True})

    sql = "CREATE TABLE if not exists test.xbi_hhtarget (ID Int32,TimeStamp DateTime('Asia/Karachi'),Yr Int32,UserID Int32,ActivityID_fk Int32,CategoryID Int32,ProvID Int32,DivID Int32,DistID Int32,TehsilID Int32,UCID Int32,Ucode Int32,Proposed_HH_Targets Int32,Final_HH_Targets Int32,Seasonal String,Nomads String,Agriculture String,Beggers String,IDPs String,Family String,Others String,Status String,Trash Int32,Remarks String, campaign_ID Int32, campaign_ActivityName String, campaign_ActivityID_old Int32, campaign_Yr Int32, campaign_SubActivityName String, geoLocation_name String, geoLocation_type Int32, geoLocation_code Int32, geoLocation_census_pop Int32, geoLocation_target Int32, geoLocation_status Int32, geoLocation_pname String, geoLocation_dname String, geoLocation_namedistrict String, geoLocation_codedistrict String, geoLocation_tname String, geoLocation_provincecode Int32, geoLocation_districtcode Int32, geoLocation_tehsilcode Int32, geoLocation_priority Int32, geoLocation_commnet Int32, geoLocation_hr Int32, geoLocation_fcm Int32, geoLocation_tier Int32, geoLocation_block String, geoLocation_division String, geoLocation_cordinates String, geoLocation_latitude String, geoLocation_longitude String, geoLocation_x String, geoLocation_y String, geoLocation_imagepath String, geoLocation_isccpv Int32, geoLocation_rank Int32, geoLocation_rank_score String, geoLocation_ishealthcamp Int32, geoLocation_isdsc Int32, geoLocation_ucorg String, geoLocation_organization String, geoLocation_tierfromaug161 Int32, geoLocation_tierfromsep171 Int32, geoLocation_tierfromdec181 Int32, geoLocation_mtap Int32, geoLocation_rspuc Int32, geoLocation_issmt Int32, geoLocation_updateddatetime String, geoLocation_x_code Int32, geoLocation_draining_uc Int32, geoLocation_upap_districts Int32, geoLocation_shruc Int32, geoLocation_khidist_id String)ENGINE = MergeTree PRIMARY KEY ID ORDER BY ID"
    client.execute(sql)
                
    cols = "test.get_hhtarget.ID,test.get_hhtarget.TimeStamp,test.get_hhtarget.Yr,test.get_hhtarget.UserID,test.get_hhtarget.ActivityID_fk,test.get_hhtarget.CategoryID,test.get_hhtarget.ProvID,test.get_hhtarget.DivID,test.get_hhtarget.DistID,test.get_hhtarget.TehsilID,test.get_hhtarget.UCID,test.get_hhtarget.Ucode,test.get_hhtarget.Proposed_HH_Targets,test.get_hhtarget.Final_HH_Targets,test.get_hhtarget.Seasonal,test.get_hhtarget.Nomads,test.get_hhtarget.Agriculture,	test.get_hhtarget.Beggers,test.get_hhtarget.IDPs,test.get_hhtarget.Family,test.get_hhtarget.Others,test.get_hhtarget.Status,test.get_hhtarget.Trash,test.get_hhtarget.Remarks, test.xbi_campaign.campaign_ID, test.xbi_campaign.campaign_ActivityName, test.xbi_campaign.campaign_ActivityID_old, test.xbi_campaign.campaign_Yr, test.xbi_campaign.campaign_SubActivityName,  test.eoc_geolocation_t.name, test.eoc_geolocation_t.type, test.eoc_geolocation_t.code, test.eoc_geolocation_t.census_pop, test.eoc_geolocation_t.target, test.eoc_geolocation_t.status, test.eoc_geolocation_t.pname, test.eoc_geolocation_t.dname, test.eoc_geolocation_t.namedistrict, test.eoc_geolocation_t.codedistrict, test.eoc_geolocation_t.tname, test.eoc_geolocation_t.provincecode, test.eoc_geolocation_t.districtcode, test.eoc_geolocation_t.tehsilcode, test.eoc_geolocation_t.priority, test.eoc_geolocation_t.commnet, test.eoc_geolocation_t.hr, test.eoc_geolocation_t.fcm, test.eoc_geolocation_t.tier, test.eoc_geolocation_t.block, test.eoc_geolocation_t.division, test.eoc_geolocation_t.cordinates, test.eoc_geolocation_t.latitude, test.eoc_geolocation_t.longitude, test.eoc_geolocation_t.x, test.eoc_geolocation_t.y, test.eoc_geolocation_t.imagepath, test.eoc_geolocation_t.isccpv, test.eoc_geolocation_t.rank, test.eoc_geolocation_t.rank_score, test.eoc_geolocation_t.ishealthcamp, test.eoc_geolocation_t.isdsc, test.eoc_geolocation_t.ucorg, test.eoc_geolocation_t.organization, test.eoc_geolocation_t.tierfromaug161, test.eoc_geolocation_t.tierfromsep171, test.eoc_geolocation_t.tierfromdec181, test.eoc_geolocation_t.mtap, test.eoc_geolocation_t.rspuc, test.eoc_geolocation_t.issmt, test.eoc_geolocation_t.updateddatetime, test.eoc_geolocation_t.x_code, test.eoc_geolocation_t.draining_uc, test.eoc_geolocation_t.upap_districts, test.eoc_geolocation_t.shruc, test.eoc_geolocation_t.khidist_id"
                
    sql = "SELECT " + cols + "  FROM test.get_hhtarget eoc_1 left JOIN test.eoc_geolocation_t eoc_2 ON eoc_1.Ucode  = eoc_2.code left JOIN test.xbi_campaign eoc_3 ON (eoc_1.ActivityID_fk  = eoc_3.campaign_ActivityID_old And eoc_1.Yr = eoc_3.campaign_Yr) "
    data = client.execute(sql)
                
    apiDataFrame = pd.DataFrame(data)
    all_columns = list(apiDataFrame)  # Creates list of all column headers
    cols = apiDataFrame.iloc[0]

    apiDataFrame[all_columns] = apiDataFrame[all_columns].astype(str)
    d ='ID','TimeStamp','Yr','UserID','ActivityID_fk','CategoryID','ProvID','DivID','DistID','TehsilID','UCID','Ucode','Proposed_HH_Targets','Final_HH_Targets','Seasonal','Nomads','Agriculture','Beggers','IDPs','Family','Others','Status','Trash','Remarks', 'campaign_ID', 'campaign_ActivityName', 'campaign_ActivityID_old', 'campaign_Yr', 'campaign_SubActivityName','geoLocation_name', 'geoLocation_type', 'geoLocation_code', 'geoLocation_census_pop', 'geoLocation_target', 'geoLocation_status', 'geoLocation_pname', 'geoLocation_dname', 'geoLocation_namedistrict', 'geoLocation_codedistrict', 'geoLocation_tname', 'geoLocation_provincecode', 'geoLocation_districtcode', 'geoLocation_tehsilcode', 'geoLocation_priority', 'geoLocation_commnet', 'geoLocation_hr', 'geoLocation_fcm', 'geoLocation_tier', 'geoLocation_block', 'geoLocation_division', 'geoLocation_cordinates', 'geoLocation_latitude', 'geoLocation_longitude', 'geoLocation_x', 'geoLocation_y', 'geoLocation_imagepath', 'geoLocation_isccpv', 'geoLocation_rank', 'geoLocation_rank_score', 'geoLocation_ishealthcamp', 'geoLocation_isdsc', 'geoLocation_ucorg', 'geoLocation_organization', 'geoLocation_tierfromaug161', 'geoLocation_tierfromsep171', 'geoLocation_tierfromdec181', 'geoLocation_mtap', 'geoLocation_rspuc', 'geoLocation_issmt', 'geoLocation_updateddatetime', 'geoLocation_x_code', 'geoLocation_draining_uc', 'geoLocation_upap_districts', 'geoLocation_shruc', 'geoLocation_khidist_id' 
                
    dff = pd.DataFrame(columns=d)
    
    for index, item in enumerate(d):
        dff[item] = apiDataFrame[index].values
    
    df2 = client.query_dataframe(
        "SELECT * FROM test.xbi_hhtarget WHERE Yr = 2022 and ActivityID_fk = 2")
    
    if df2.empty:
        dff[["TimeStamp"]] = dff[["TimeStamp"]].apply(pd.to_datetime)
        client.insert_dataframe(
            'INSERT INTO test.xbi_hhtarget  VALUES', dff)
        
        logger.info(
                ' Data has been inserted into Table\' INSERT INTO test.xbi_hhtarget  VALUES \' ')

        sql = "DROP table if exists test.get_hhtarget"
        client.execute(sql)
        print('\n\n\n C\t\t--------\t', c)
        print('\n\n\n Y\t\t--------\t', y)

    else:
        sql = "ALTER TABLE test.xbi_hhtarget DELETE WHERE Yr = 2022 and ActivityID_fk = 2"
        client.execute(sql)

        dff[["TimeStamp"]] = dff[["TimeStamp"]].apply(pd.to_datetime)
        client.insert_dataframe(
            'INSERT INTO test.xbi_hhtarget  VALUES', dff)
        print('\n\n\n C\t\t--------\t', c)
        print('\n\n\n Y\t\t--------\t', y)
        sql = "DROP table if exists test.get_hhtarget"
        client.execute(sql)

dag = DAG(
    'HHTarget_Automated',
    #schedule_interval=None,
    schedule_interval='0 0 * * *',  # once a day at midnight
    default_args=default_args,
    catchup=False)
    
with dag:
    GetAndInsertApiDataHHTarget = PythonOperator(
        task_id='GetAndInsertApiDataHHTarget',
        python_callable=GetAndInsertApiDataHHTarget,
    )
    CreateJoinTableOfHHTarget = PythonOperator(
        task_id='CreateJoinTableOfHHTarget',
        python_callable=CreateJoinTableOfHHTarget,
    )

GetAndInsertApiDataHHTarget >> CreateJoinTableOfHHTarget

#-------------------------------------------------------------------------------------------------------------------------------#
#----------------------------------------------- INSERT HH Target Data ---------------------------------------------------------#
#-------------------------------------------- Author: Abdul Bari Malik ---------------------------------------------------------#
#-------------------------------------------------------------------------------------------------------------------------------#

# def GetAndInsertApiDataHHTarget():
    
#     li1 = [28, 29, 31, 30, 1, 106, 34, 9, 37, 40, 40, 43, 10, 2, 46, 11, 3, 107, 1, 106, 2, 9, 8, 10, 3, 107, 28, 31, 4, 9, 1, 28, 31, 2]
#     li2 = [2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2022, 2022, 2022, 2022, 2022]

#     merged_df = pd.DataFrame()

#     for f, b in zip(li1, li2):
#         df = pd.DataFrame()
#         logger.info('Function \' GetAndInsertApiDataHHTarget \' Started Off')
#         client = Client(host='161.97.136.95',
#                         user='default',
#                         password='pakistan',
#                         port='9000', settings={"use_numpy": True})

#         url = "https://idims.eoc.gov.pk/api_who/index.php/api/hhtargets/5468xe2ln6CZR7QRG041/"+ str(f)+"/"+ str(b)
        
#         logger.info('Requested Data From Api URL: '+url)

#         r = requests.get(url)
#         data = r.json()
#         if data['data'] == "No data found":
#             print("Not Data found for campaign: "+url)
#         else:
#             if data['data']['data'] == "No data found":
#                 print("Not Data found for campaign: "+url)    
#             else:
#                 print("Data found for campaign: "+url)
#                 total_page = data["data"]["total_page"]
#                 print("Total Pages for an API Data:"+str(total_page) )
#                 i=1
#                 while int(i) <= total_page:
#                     url_c = "https://idims.eoc.gov.pk/api_who/index.php/api/hhtargets/5468xe2ln6CZR7QRG041/"+ str(f)+"/"+ str(b)+"/"+str(i)
#                     r2 = requests.get(url_c)
#                     data_c = r2.json()
              
#                     rowsData = data_c["data"]["data"]["data"]
#                     print("Data found for campaign: "+url_c)
#                     print("Data found for Page: "+str(i))
#                     apiDataFrame = pd.DataFrame(rowsData)
#                     print("DF Page Size",len(apiDataFrame))
#                     df = df.append(apiDataFrame, ignore_index=True)
#                     print("DF API Size",len(df))
#                     i+=1
#                 merged_df = merged_df.append(df, ignore_index=True)
#                 print("Merged DF Size",len(merged_df),'\n')     
#                 print('\nCampaign \t\t--------\t',f,'--',b,'\t-------- Data Inserted \n')

#                 sql = "CREATE TABLE if not exists test.get_hhtarget  (ID Int32,TimeStamp DateTime('Asia/Karachi'),Yr Int32,UserID Int32,ActivityID_fk Int32,CategoryID Int32,ProvID Int32,DivID Int32,DistID Int32,TehsilID Int32,UCID Int32,Ucode Int32,Proposed_HH_Targets Int32,Final_HH_Targets Int32,Seasonal String,Nomads String,Agriculture String,Beggers String,IDPs String,Family String,Others String,Status String,Trash Int32,Remarks String,PRIMARY KEY(ID ))ENGINE = MergeTree"
#                 client.execute(sql)
#                 df2 = client.query_dataframe("SELECT * FROM test.get_hhtarget")            
#                 if df2.empty:
#                     #df = df.replace(r'^\s*$', np.nan, regex=True)
#                     df['TimeStamp'] = pd.to_datetime(df['TimeStamp'], errors='coerce')
#                     client.insert_dataframe(
#                         'INSERT INTO test.get_hhtarget VALUES', df)
                
#                 sql = "CREATE TABLE if not exists test.xbi_hhtarget (ID Int32,TimeStamp DateTime('Asia/Karachi'),Yr Int32,UserID Int32,ActivityID_fk Int32,CategoryID Int32,ProvID Int32,DivID Int32,DistID Int32,TehsilID Int32,UCID Int32,Ucode Int32,Proposed_HH_Targets Int32,Final_HH_Targets Int32,Seasonal String,Nomads String,Agriculture String,Beggers String,IDPs String,Family String,Others String,Status String,Trash Int32,Remarks String, campaign_ID Int32, campaign_ActivityName String, campaign_ActivityID_old Int32, campaign_Yr Int32, campaign_SubActivityName String, geoLocation_name String, geoLocation_type Int32, geoLocation_code Int32, geoLocation_census_pop Int32, geoLocation_target Int32, geoLocation_status Int32, geoLocation_pname String, geoLocation_dname String, geoLocation_namedistrict String, geoLocation_codedistrict String, geoLocation_tname String, geoLocation_provincecode Int32, geoLocation_districtcode Int32, geoLocation_tehsilcode Int32, geoLocation_priority Int32, geoLocation_commnet Int32, geoLocation_hr Int32, geoLocation_fcm Int32, geoLocation_tier Int32, geoLocation_block String, geoLocation_division String, geoLocation_cordinates String, geoLocation_latitude String, geoLocation_longitude String, geoLocation_x String, geoLocation_y String, geoLocation_imagepath String, geoLocation_isccpv Int32, geoLocation_rank Int32, geoLocation_rank_score String, geoLocation_ishealthcamp Int32, geoLocation_isdsc Int32, geoLocation_ucorg String, geoLocation_organization String, geoLocation_tierfromaug161 Int32, geoLocation_tierfromsep171 Int32, geoLocation_tierfromdec181 Int32, geoLocation_mtap Int32, geoLocation_rspuc Int32, geoLocation_issmt Int32, geoLocation_updateddatetime String, geoLocation_x_code Int32, geoLocation_draining_uc Int32, geoLocation_upap_districts Int32, geoLocation_shruc Int32, geoLocation_khidist_id String)ENGINE = MergeTree PRIMARY KEY ID ORDER BY ID"
#                 client.execute(sql)
                
#                 cols = "test.get_hhtarget.ID,test.get_hhtarget.TimeStamp,test.get_hhtarget.Yr,test.get_hhtarget.UserID,test.get_hhtarget.ActivityID_fk,test.get_hhtarget.CategoryID,test.get_hhtarget.ProvID,test.get_hhtarget.DivID,test.get_hhtarget.DistID,test.get_hhtarget.TehsilID,test.get_hhtarget.UCID,test.get_hhtarget.Ucode,test.get_hhtarget.Proposed_HH_Targets,test.get_hhtarget.Final_HH_Targets,test.get_hhtarget.Seasonal,test.get_hhtarget.Nomads,test.get_hhtarget.Agriculture,	test.get_hhtarget.Beggers,test.get_hhtarget.IDPs,test.get_hhtarget.Family,test.get_hhtarget.Others,test.get_hhtarget.Status,test.get_hhtarget.Trash,test.get_hhtarget.Remarks, test.xbi_campaign.campaign_ID, test.xbi_campaign.campaign_ActivityName, test.xbi_campaign.campaign_ActivityID_old, test.xbi_campaign.campaign_Yr, test.xbi_campaign.campaign_SubActivityName,  test.eoc_geolocation_t.name, test.eoc_geolocation_t.type, test.eoc_geolocation_t.code, test.eoc_geolocation_t.census_pop, test.eoc_geolocation_t.target, test.eoc_geolocation_t.status, test.eoc_geolocation_t.pname, test.eoc_geolocation_t.dname, test.eoc_geolocation_t.namedistrict, test.eoc_geolocation_t.codedistrict, test.eoc_geolocation_t.tname, test.eoc_geolocation_t.provincecode, test.eoc_geolocation_t.districtcode, test.eoc_geolocation_t.tehsilcode, test.eoc_geolocation_t.priority, test.eoc_geolocation_t.commnet, test.eoc_geolocation_t.hr, test.eoc_geolocation_t.fcm, test.eoc_geolocation_t.tier, test.eoc_geolocation_t.block, test.eoc_geolocation_t.division, test.eoc_geolocation_t.cordinates, test.eoc_geolocation_t.latitude, test.eoc_geolocation_t.longitude, test.eoc_geolocation_t.x, test.eoc_geolocation_t.y, test.eoc_geolocation_t.imagepath, test.eoc_geolocation_t.isccpv, test.eoc_geolocation_t.rank, test.eoc_geolocation_t.rank_score, test.eoc_geolocation_t.ishealthcamp, test.eoc_geolocation_t.isdsc, test.eoc_geolocation_t.ucorg, test.eoc_geolocation_t.organization, test.eoc_geolocation_t.tierfromaug161, test.eoc_geolocation_t.tierfromsep171, test.eoc_geolocation_t.tierfromdec181, test.eoc_geolocation_t.mtap, test.eoc_geolocation_t.rspuc, test.eoc_geolocation_t.issmt, test.eoc_geolocation_t.updateddatetime, test.eoc_geolocation_t.x_code, test.eoc_geolocation_t.draining_uc, test.eoc_geolocation_t.upap_districts, test.eoc_geolocation_t.shruc, test.eoc_geolocation_t.khidist_id"
                
#                 sql = "SELECT " + cols + "  FROM test.get_hhtarget eoc_1 left JOIN test.eoc_geolocation_t eoc_2 ON eoc_1.Ucode  = eoc_2.code left JOIN test.xbi_campaign eoc_3 ON (eoc_1.ActivityID_fk  = eoc_3.campaign_ActivityID_old And eoc_1.Yr = eoc_3.campaign_Yr) "
#                 data = client.execute(sql)
                
#                 apiDataFrame = pd.DataFrame(data)
#                 all_columns = list(apiDataFrame)  # Creates list of all column headers
#                 cols = apiDataFrame.iloc[0]

#                 apiDataFrame[all_columns] = apiDataFrame[all_columns].astype(str)
#                 d ='ID','TimeStamp','Yr','UserID','ActivityID_fk','CategoryID','ProvID','DivID','DistID','TehsilID','UCID','Ucode','Proposed_HH_Targets','Final_HH_Targets','Seasonal','Nomads','Agriculture','Beggers','IDPs','Family','Others','Status','Trash','Remarks', 'campaign_ID', 'campaign_ActivityName', 'campaign_ActivityID_old', 'campaign_Yr', 'campaign_SubActivityName','geoLocation_name', 'geoLocation_type', 'geoLocation_code', 'geoLocation_census_pop', 'geoLocation_target', 'geoLocation_status', 'geoLocation_pname', 'geoLocation_dname', 'geoLocation_namedistrict', 'geoLocation_codedistrict', 'geoLocation_tname', 'geoLocation_provincecode', 'geoLocation_districtcode', 'geoLocation_tehsilcode', 'geoLocation_priority', 'geoLocation_commnet', 'geoLocation_hr', 'geoLocation_fcm', 'geoLocation_tier', 'geoLocation_block', 'geoLocation_division', 'geoLocation_cordinates', 'geoLocation_latitude', 'geoLocation_longitude', 'geoLocation_x', 'geoLocation_y', 'geoLocation_imagepath', 'geoLocation_isccpv', 'geoLocation_rank', 'geoLocation_rank_score', 'geoLocation_ishealthcamp', 'geoLocation_isdsc', 'geoLocation_ucorg', 'geoLocation_organization', 'geoLocation_tierfromaug161', 'geoLocation_tierfromsep171', 'geoLocation_tierfromdec181', 'geoLocation_mtap', 'geoLocation_rspuc', 'geoLocation_issmt', 'geoLocation_updateddatetime', 'geoLocation_x_code', 'geoLocation_draining_uc', 'geoLocation_upap_districts', 'geoLocation_shruc', 'geoLocation_khidist_id' 
                
#                 dff = pd.DataFrame(columns=d)
#                 for index, item in enumerate(d):
#                     dff[item] = apiDataFrame[index].values
#                 df2 = client.query_dataframe(
#                     "SELECT * FROM test.xbi_hhtarget")
#                 if df2.empty:
#                     dff[["TimeStamp"]] = dff[["TimeStamp"]].apply(pd.to_datetime)
#                     client.insert_dataframe(
#                         'INSERT INTO test.xbi_hhtarget  VALUES', dff)
#                     logger.info(
#                         ' Data has been inserted into Table\' INSERT INTO test.xbi_hhtarget  VALUES \' ')

#                     sql = "DROP table if exists test.get_hhtarget"
#                     client.execute(sql)
#                     print('\n\n\n F\t\t--------\t', f)
#                     print('\n\n\n B\t\t--------\t', b)

#                 else:
#                     dff[["TimeStamp"]] = dff[["TimeStamp"]].apply(pd.to_datetime)
#                     client.insert_dataframe(
#                         'INSERT INTO test.xbi_hhtarget  VALUES', dff)
#                     print('\n\n\n F\t\t--------\t', f)
#                     print('\n\n\n B\t\t--------\t', b)
#                     sql = "DROP table if exists test.get_hhtarget"
#                     client.execute(sql)

# dag = DAG(
#     'HHTarget_Automated',
#     schedule_interval=None,
#     #schedule_interval='0 0 * * *',  # once a day at midnight
#     default_args=default_args,
#     catchup=False)
    
# with dag:
#     GetAndInsertApiDataHHTarget = PythonOperator(
#         task_id='GetAndInsertApiDataHHTarget',
#         python_callable=GetAndInsertApiDataHHTarget,
#     )
# GetAndInsertApiDataHHTarget
