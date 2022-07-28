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
#------------------------------------------- INSERT LATEST CAMPAIGN DATA After_CatchUp -----------------------------------------#
#-------------------------------------------- Author: Abdul Bari Malik ---------------------------------------------------------#
#-------------------------------------------------------------------------------------------------------------------------------#

def GetAndInsertApiDataAfterCatchUp():
    logger.info('Function \' GetAndInsertApiDataAfterCatchUp \' Started Off')
    client = Client(host='161.97.136.95',
                    user='default',
                    password='pakistan',
                    port='9000', settings={"use_numpy": True})

    c = 2
    y = 2022
    url = "http://idims.eoc.gov.pk/api_who/api/get_allaftercatchup/5468XE2LN6CzR7qRG041/"+ str(c)+"/"+ str(y)
    #url = 'http://idims.eoc.gov.pk/api_who/api/get_allaftercatchup/5468XE2LN6CzR7qRG041/2/2022'
    logger.info('Requested Data From Api URL:  \' http://idims.eoc.gov.pk/api_who/api/get_allcatchup/5468XE2LN6CzR7qRG041/"+ str(c)+"/"+ str(y) \' ')

    r = requests.get(url)
    data = r.json()
    
    logger.info('Requested Data From Api URL:  \' http://idims.eoc.gov.pk/api_who/api/get_allcatchup/5468XE2LN6CzR7qRG041/"+ str(c)+"/"+ str(y) \' ')

    rowsData = data["data"]["data"]
    apiDataFrame = pd.DataFrame(rowsData)

    count_row = apiDataFrame.shape[0]
    count_col = apiDataFrame.shape[1]
    print("DF Rows",count_row)
    print("DF Cols",count_col)
    print('\n\n\n CID\t\t--------\t', c)
    print('\n\n\n Yr\t\t--------\t', y) 
    
    sql = " Create Table if not exists test.get_after_catch_up (ID Int32, UserName String, IDcampCat Int32, ActivityID Int32, TimeStamp String, Yr Int32, TehsilID Int32, UCID Int32, DistID Int32, DivID Int32, ProvID Int32, cday Int32,VaccinationDate String,ACCovNA011 Int32, ACCovNA1259 Int32, ACCovRef011 Int32, ACCovRef1259 Int32, ACOutofH011 Int32, ACOutofH1259 Int32, ACGuests Int32, ACCovMob Int32, ACNewBorn Int32, PersistentlyMC Int32, PersistentlyMCHRMP Int32, ACAlreadyVaccinated Int32, UnRecCov Int32, OPVGiven Int32, OPVUsed Int32, OPVReturned Int32, status Int32, trash Int32, isSync Int32, fm_issued Int32, fm_retrieved Int32, Remarks String, location_code Int32 NULL)ENGINE = MergeTree PRIMARY KEY ID ORDER BY ID"
    client.execute(sql)

    df2 = client.query_dataframe(
        "SELECT * FROM test.get_after_catch_up")
    if df2.empty:
        apiDataFrame = apiDataFrame.replace(r'^\s*$', np.nan, regex=True)
        client.insert_dataframe(
            'INSERT INTO test.get_after_catch_up VALUES', apiDataFrame)

def CreateJoinTableOfAfterCatchUp():
    logger.info(' Function  \' CreateJoinTableOfAfterCatchUp \' has been Initiated')
    client = Client(host='161.97.136.95',
                    user='default',
                    password='pakistan',
                    port='9000', settings={"use_numpy": True})
    
    sql = "CREATE TABLE if not exists test.xbi_after_catchUp (ID Int32, UserName String, IDcampCat Int32, ActivityID Int32, TimeStamp String, Yr Int32, TehsilID Int32, UCID Int32, DistID Int32, DivID Int32, ProvID Int32, cday Int32, VaccinationDate String, ACCovNA011 Int32, ACCovNA1259 Int32, ACCovRef011 Int32, ACCovRef1259 Int32, ACOutofH011 Int32, ACOutofH1259 Int32, ACGuests Int32, ACCovMob Int32, ACNewBorn Int32, PersistentlyMC Int32, PersistentlyMCHRMP Int32, ACAlreadyVaccinated Int32,  UnRecCov Int32, OPVGiven Int32, OPVUsed Int32, OPVReturned Int32, status Int32, trash Int32, isSync Int32,fm_issued Int32,fm_retrieved Int32, Remarks String, location_code Int32 NULL, campaign_ID Int32, campaign_ActivityName String, campaign_ActivityID_old Int32, campaign_Yr Int32, campaign_SubActivityName String, geoLocation_name String, geoLocation_type Int32, geoLocation_code Int32, geoLocation_census_pop Int32, geoLocation_target Int32, geoLocation_status Int32, geoLocation_pname String, geoLocation_dname String, geoLocation_namedistrict String, geoLocation_codedistrict String, geoLocation_tname String, geoLocation_provincecode Int32, geoLocation_districtcode Int32, geoLocation_tehsilcode Int32, geoLocation_priority Int32, geoLocation_commnet Int32, geoLocation_hr Int32, geoLocation_fcm Int32, geoLocation_tier Int32, geoLocation_block String, geoLocation_division String, geoLocation_cordinates String, geoLocation_latitude String, geoLocation_longitude String, geoLocation_x String, geoLocation_y String, geoLocation_imagepath String, geoLocation_isccpv Int32, geoLocation_rank Int32, geoLocation_rank_score String, geoLocation_ishealthcamp Int32, geoLocation_isdsc Int32, geoLocation_ucorg String, geoLocation_organization String, geoLocation_tierfromaug161 Int32, geoLocation_tierfromsep171 Int32, geoLocation_tierfromdec181 Int32, geoLocation_mtap Int32, geoLocation_rspuc Int32, geoLocation_issmt Int32, geoLocation_updateddatetime String, geoLocation_x_code Int32, geoLocation_draining_uc Int32, geoLocation_upap_districts Int32, geoLocation_shruc Int32, geoLocation_khidist_id String )ENGINE = MergeTree PRIMARY KEY ID ORDER BY ID"
    client.execute(sql)

    cols = "test.get_after_catch_up.ID, test.get_after_catch_up.UserName, test.get_after_catch_up.IDcampCat, test.get_after_catch_up.ActivityID, test.get_after_catch_up.TimeStamp, test.get_after_catch_up.Yr, test.get_after_catch_up.TehsilID, test.get_after_catch_up.UCID, test.get_after_catch_up.DistID, test.get_after_catch_up.DivID, test.get_after_catch_up.ProvID, test.get_after_catch_up.cday, test.get_after_catch_up.VaccinationDate,test.get_after_catch_up.ACCovNA011, test.get_after_catch_up.ACCovNA1259, test.get_after_catch_up.ACCovRef011, test.get_after_catch_up.ACCovRef1259, test.get_after_catch_up.ACOutofH011, test.get_after_catch_up.ACOutofH1259, test.get_after_catch_up.ACGuests, test.get_after_catch_up.ACCovMob, test.get_after_catch_up.ACNewBorn, test.get_after_catch_up.PersistentlyMC, test.get_after_catch_up.PersistentlyMCHRMP, test.get_after_catch_up.ACAlreadyVaccinated, test.get_after_catch_up.UnRecCov, test.get_after_catch_up.OPVGiven, test.get_after_catch_up.OPVUsed, test.get_after_catch_up.OPVReturned,test.get_after_catch_up.status, test.get_after_catch_up.trash, test.get_after_catch_up.isSync,test.get_after_catch_up.fm_issued, test.get_after_catch_up.fm_retrieved,test.get_after_catch_up.Remarks, test.get_after_catch_up.location_code, test.xbi_campaign.campaign_ID, test.xbi_campaign.campaign_ActivityName, test.xbi_campaign.campaign_ActivityID_old, test.xbi_campaign.campaign_Yr, test.xbi_campaign.campaign_SubActivityName, test.eoc_geolocation_t.name, test.eoc_geolocation_t.type, test.eoc_geolocation_t.code, test.eoc_geolocation_t.census_pop, test.eoc_geolocation_t.target, test.eoc_geolocation_t.status, test.eoc_geolocation_t.pname, test.eoc_geolocation_t.dname, test.eoc_geolocation_t.namedistrict, test.eoc_geolocation_t.codedistrict, test.eoc_geolocation_t.tname, test.eoc_geolocation_t.provincecode, test.eoc_geolocation_t.districtcode, test.eoc_geolocation_t.tehsilcode, test.eoc_geolocation_t.priority, test.eoc_geolocation_t.commnet, test.eoc_geolocation_t.hr, test.eoc_geolocation_t.fcm, test.eoc_geolocation_t.tier, test.eoc_geolocation_t.block, test.eoc_geolocation_t.division, test.eoc_geolocation_t.cordinates, test.eoc_geolocation_t.latitude, test.eoc_geolocation_t.longitude, test.eoc_geolocation_t.x, test.eoc_geolocation_t.y, test.eoc_geolocation_t.imagepath, test.eoc_geolocation_t.isccpv, test.eoc_geolocation_t.rank, test.eoc_geolocation_t.rank_score, test.eoc_geolocation_t.ishealthcamp, test.eoc_geolocation_t.isdsc, test.eoc_geolocation_t.ucorg, test.eoc_geolocation_t.organization, test.eoc_geolocation_t.tierfromaug161, test.eoc_geolocation_t.tierfromsep171, test.eoc_geolocation_t.tierfromdec181, test.eoc_geolocation_t.mtap, test.eoc_geolocation_t.rspuc, test.eoc_geolocation_t.issmt, test.eoc_geolocation_t.updateddatetime, test.eoc_geolocation_t.x_code, test.eoc_geolocation_t.draining_uc, test.eoc_geolocation_t.upap_districts, test.eoc_geolocation_t.shruc, test.eoc_geolocation_t.khidist_id"
            
    sql = "SELECT " + cols + "  FROM test.get_after_catch_up eoc_1 left JOIN test.eoc_geolocation_t eoc_2 ON eoc_1.location_code  = eoc_2.code JOIN test.xbi_campaign eoc_3 ON (eoc_1.ActivityID  = eoc_3.campaign_ActivityID_old And eoc_1.Yr = eoc_3.campaign_Yr) "

    data = client.execute(sql)
    apiDataFrame = pd.DataFrame(data)
    apiDataFrame = apiDataFrame.replace(r'^\s*$', np.nan, regex=True)
    all_columns = list(apiDataFrame)  # Creates list of all column headers
    cols = apiDataFrame.iloc[0]
    apiDataFrame[all_columns] = apiDataFrame[all_columns].astype(str)
    
    d = 'ID', 'UserName', 'IDcampCat', 'ActivityID', 'TimeStamp', 'Yr', 'TehsilID', 'UCID', 'DistID', 'DivID', 'ProvID', 'cday', 'VaccinationDate', 'ACCovNA011', 'ACCovNA1259', 'ACCovRef011', 'ACCovRef1259', 'ACOutofH011', 'ACOutofH1259', 'ACGuests', 'ACCovMob', 'ACNewBorn', 'PersistentlyMC', 'PersistentlyMCHRMP', 'ACAlreadyVaccinated',  'UnRecCov', 'OPVGiven', 'OPVUsed', 'OPVReturned', 'status', 'trash', 'isSync','fm_issued','fm_retrieved', 'Remarks', 'location_code', 'campaign_ID', 'campaign_ActivityName', 'campaign_ActivityID_old', 'campaign_Yr', 'campaign_SubActivityName','geoLocation_name', 'geoLocation_type', 'geoLocation_code', 'geoLocation_census_pop', 'geoLocation_target', 'geoLocation_status', 'geoLocation_pname', 'geoLocation_dname', 'geoLocation_namedistrict', 'geoLocation_codedistrict', 'geoLocation_tname', 'geoLocation_provincecode', 'geoLocation_districtcode', 'geoLocation_tehsilcode', 'geoLocation_priority', 'geoLocation_commnet', 'geoLocation_hr', 'geoLocation_fcm', 'geoLocation_tier', 'geoLocation_block', 'geoLocation_division', 'geoLocation_cordinates', 'geoLocation_latitude', 'geoLocation_longitude', 'geoLocation_x', 'geoLocation_y', 'geoLocation_imagepath', 'geoLocation_isccpv', 'geoLocation_rank', 'geoLocation_rank_score', 'geoLocation_ishealthcamp', 'geoLocation_isdsc', 'geoLocation_ucorg', 'geoLocation_organization', 'geoLocation_tierfromaug161', 'geoLocation_tierfromsep171', 'geoLocation_tierfromdec181', 'geoLocation_mtap', 'geoLocation_rspuc', 'geoLocation_issmt', 'geoLocation_updateddatetime', 'geoLocation_x_code', 'geoLocation_draining_uc', 'geoLocation_upap_districts', 'geoLocation_shruc', 'geoLocation_khidist_id'  
            
    dff = pd.DataFrame(columns=d)
    for index, item in enumerate(d):
        dff[item] = apiDataFrame[index].values
    df2 = client.query_dataframe(
        "SELECT * FROM test.xbi_after_catchUp WHERE Yr = 2022 and ActivityID = 2")
    if df2.empty:
        client.insert_dataframe(
            'INSERT INTO test.xbi_after_catchUp  VALUES', dff)
        logger.info(
            ' Data has been inserted into Table\' INSERT INTO test.xbi_after_catchUp  VALUES \' ')

        sql = "DROP table if exists test.get_after_catch_up"
        client.execute(sql)
        print('\n\n\n CID\t\t--------\t', c)
        print('\n\n\n Year\t\t--------\t', y)

    else:
        sql = "ALTER TABLE test.xbi_after_catchUp DELETE WHERE Yr = 2022 and ActivityID = 2"
        client.execute(sql)

        dff = dff.replace(r'^\s*$', np.nan, regex=True)
        dff['location_code'] = pd.to_numeric(dff['location_code'], errors='coerce').fillna(0)
        client.insert_dataframe(
            'INSERT INTO test.xbi_after_catchUp  VALUES', dff)

        sql = "DROP table if exists test.get_after_catch_up"
        client.execute(sql)


dag = DAG(
    'AfterCatchUp_Automated',
    schedule_interval='0 0 * * *',  # once a day at midnight.
    #schedule_interval='0 * * * *', # Run once an hour at the beginning of the hour 
    default_args=default_args,
    catchup=False)

with dag:
    GetAndInsertApiDataAfterCatchUp = PythonOperator(
        task_id='GetAndInsertApiDataAfterCatchUp',
        python_callable=GetAndInsertApiDataAfterCatchUp,
    )
    CreateJoinTableOfAfterCatchUp = PythonOperator(
        task_id='CreateJoinTableOfAfterCatchUp',
        python_callable=CreateJoinTableOfAfterCatchUp,
    )

GetAndInsertApiDataAfterCatchUp >> CreateJoinTableOfAfterCatchUp


#-------------------------------------------------------------------------------------------------------------------------------#
#---------------------------------------------DATA INSERTION: After_CatchUp-----------------------------------------------------#
#-------------------------------------------- Author: Abdul Bari Malik ---------------------------------------------------------#
#-------------------------------------------------------------------------------------------------------------------------------#

# def GetAndInsertApiDataAfterCatchUp():
#     li1 = [28, 29, 31, 30, 1, 106, 34, 9, 37, 40, 40, 43, 10, 2, 46, 11, 3, 107, 1, 106, 2, 9, 8, 10, 3, 107, 28, 31, 4, 9, 1, 28, 31, 2]
#     li2 = [2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2022, 2022, 2022, 2022, 2022]

#     for c, y in zip(li1, li2):
#         logger.info('Function \' GetAndInsertApiDataAfterCatchUp \' Started Off')
#         client = Client(host='161.97.136.95',
#                         user='default',
#                         password='pakistan',
#                         port='9000', settings={"use_numpy": True})

#         url = "http://idims.eoc.gov.pk/api_who/api/get_allaftercatchup/5468XE2LN6CzR7qRG041/"+ str(c)+"/"+ str(y)
#         #url = 'http://idims.eoc.gov.pk/api_who/api/get_allaftercatchup/5468XE2LN6CzR7qRG041/2/2022'
#         print('\n\n\n CID\t\t--------\t', c)
#         print('\n\n\n Year\t\t--------\t', y)
        
#         logger.info('Requested Data From Api URL:  \' http://idims.eoc.gov.pk/api_who/api/get_allaftercatchup/5468XE2LN6CzR7qRG041/"+ str(c)+"/"+ str(y) \' ')

#         r = requests.get(url)
#         data = r.json()
     
#         if data['data'] != "No data found":
#             logger.info('Received Data  From Api URL:  \' http://idims.eoc.gov.pk/api_who/api/get_allaftercatchup/5468XE2LN6CzR7qRG041/"+ str(c)+"/"+ str(y) \' ')

#             rowsData = data["data"]["data"]
#             apiDataFrame = pd.DataFrame(rowsData)
            
#             sql = " Create Table if not exists test.get_after_catch_up (ID Int32, UserName String, IDcampCat Int32, ActivityID Int32, TimeStamp String, Yr Int32, TehsilID Int32, UCID Int32, DistID Int32, DivID Int32, ProvID Int32, cday Int32,VaccinationDate String,ACCovNA011 Int32, ACCovNA1259 Int32, ACCovRef011 Int32, ACCovRef1259 Int32, ACOutofH011 Int32, ACOutofH1259 Int32, ACGuests Int32, ACCovMob Int32, ACNewBorn Int32, PersistentlyMC Int32, PersistentlyMCHRMP Int32, ACAlreadyVaccinated Int32, UnRecCov Int32, OPVGiven Int32, OPVUsed Int32, OPVReturned Int32, status Int32, trash Int32, isSync Int32, fm_issued Int32, fm_retrieved Int32, Remarks String, location_code Int32 NULL)ENGINE = MergeTree PRIMARY KEY ID ORDER BY ID"
#             client.execute(sql)

#             df2 = client.query_dataframe(
#                 "SELECT * FROM test.get_after_catch_up")
#             if df2.empty:
#                 apiDataFrame = apiDataFrame.replace(r'^\s*$', np.nan, regex=True)
#                 client.insert_dataframe(
#                     'INSERT INTO test.get_after_catch_up VALUES', apiDataFrame)
            
#             sql = "CREATE TABLE if not exists test.xbi_after_catchUp (ID Int32, UserName String, IDcampCat Int32, ActivityID Int32, TimeStamp String, Yr Int32, TehsilID Int32, UCID Int32, DistID Int32, DivID Int32, ProvID Int32, cday Int32, VaccinationDate String, ACCovNA011 Int32, ACCovNA1259 Int32, ACCovRef011 Int32, ACCovRef1259 Int32, ACOutofH011 Int32, ACOutofH1259 Int32, ACGuests Int32, ACCovMob Int32, ACNewBorn Int32, PersistentlyMC Int32, PersistentlyMCHRMP Int32, ACAlreadyVaccinated Int32,  UnRecCov Int32, OPVGiven Int32, OPVUsed Int32, OPVReturned Int32, status Int32, trash Int32, isSync Int32,fm_issued Int32,fm_retrieved Int32, Remarks String, location_code Int32 NULL, campaign_ID Int32, campaign_ActivityName String, campaign_ActivityID_old Int32, campaign_Yr Int32, campaign_SubActivityName String, geoLocation_name String, geoLocation_type Int32, geoLocation_code Int32, geoLocation_census_pop Int32, geoLocation_target Int32, geoLocation_status Int32, geoLocation_pname String, geoLocation_dname String, geoLocation_namedistrict String, geoLocation_codedistrict String, geoLocation_tname String, geoLocation_provincecode Int32, geoLocation_districtcode Int32, geoLocation_tehsilcode Int32, geoLocation_priority Int32, geoLocation_commnet Int32, geoLocation_hr Int32, geoLocation_fcm Int32, geoLocation_tier Int32, geoLocation_block String, geoLocation_division String, geoLocation_cordinates String, geoLocation_latitude String, geoLocation_longitude String, geoLocation_x String, geoLocation_y String, geoLocation_imagepath String, geoLocation_isccpv Int32, geoLocation_rank Int32, geoLocation_rank_score String, geoLocation_ishealthcamp Int32, geoLocation_isdsc Int32, geoLocation_ucorg String, geoLocation_organization String, geoLocation_tierfromaug161 Int32, geoLocation_tierfromsep171 Int32, geoLocation_tierfromdec181 Int32, geoLocation_mtap Int32, geoLocation_rspuc Int32, geoLocation_issmt Int32, geoLocation_updateddatetime String, geoLocation_x_code Int32, geoLocation_draining_uc Int32, geoLocation_upap_districts Int32, geoLocation_shruc Int32, geoLocation_khidist_id String )ENGINE = MergeTree PRIMARY KEY ID ORDER BY ID"
#             client.execute(sql)

#             cols = "test.get_after_catch_up.ID, test.get_after_catch_up.UserName, test.get_after_catch_up.IDcampCat, test.get_after_catch_up.ActivityID, test.get_after_catch_up.TimeStamp, test.get_after_catch_up.Yr, test.get_after_catch_up.TehsilID, test.get_after_catch_up.UCID, test.get_after_catch_up.DistID, test.get_after_catch_up.DivID, test.get_after_catch_up.ProvID, test.get_after_catch_up.cday, test.get_after_catch_up.VaccinationDate,test.get_after_catch_up.ACCovNA011, test.get_after_catch_up.ACCovNA1259, test.get_after_catch_up.ACCovRef011, test.get_after_catch_up.ACCovRef1259, test.get_after_catch_up.ACOutofH011, test.get_after_catch_up.ACOutofH1259, test.get_after_catch_up.ACGuests, test.get_after_catch_up.ACCovMob, test.get_after_catch_up.ACNewBorn, test.get_after_catch_up.PersistentlyMC, test.get_after_catch_up.PersistentlyMCHRMP, test.get_after_catch_up.ACAlreadyVaccinated, test.get_after_catch_up.UnRecCov, test.get_after_catch_up.OPVGiven, test.get_after_catch_up.OPVUsed, test.get_after_catch_up.OPVReturned,test.get_after_catch_up.status, test.get_after_catch_up.trash, test.get_after_catch_up.isSync,test.get_after_catch_up.fm_issued, test.get_after_catch_up.fm_retrieved,test.get_after_catch_up.Remarks, test.get_after_catch_up.location_code, test.xbi_campaign.campaign_ID, test.xbi_campaign.campaign_ActivityName, test.xbi_campaign.campaign_ActivityID_old, test.xbi_campaign.campaign_Yr, test.xbi_campaign.campaign_SubActivityName, test.eoc_geolocation_t.name, test.eoc_geolocation_t.type, test.eoc_geolocation_t.code, test.eoc_geolocation_t.census_pop, test.eoc_geolocation_t.target, test.eoc_geolocation_t.status, test.eoc_geolocation_t.pname, test.eoc_geolocation_t.dname, test.eoc_geolocation_t.namedistrict, test.eoc_geolocation_t.codedistrict, test.eoc_geolocation_t.tname, test.eoc_geolocation_t.provincecode, test.eoc_geolocation_t.districtcode, test.eoc_geolocation_t.tehsilcode, test.eoc_geolocation_t.priority, test.eoc_geolocation_t.commnet, test.eoc_geolocation_t.hr, test.eoc_geolocation_t.fcm, test.eoc_geolocation_t.tier, test.eoc_geolocation_t.block, test.eoc_geolocation_t.division, test.eoc_geolocation_t.cordinates, test.eoc_geolocation_t.latitude, test.eoc_geolocation_t.longitude, test.eoc_geolocation_t.x, test.eoc_geolocation_t.y, test.eoc_geolocation_t.imagepath, test.eoc_geolocation_t.isccpv, test.eoc_geolocation_t.rank, test.eoc_geolocation_t.rank_score, test.eoc_geolocation_t.ishealthcamp, test.eoc_geolocation_t.isdsc, test.eoc_geolocation_t.ucorg, test.eoc_geolocation_t.organization, test.eoc_geolocation_t.tierfromaug161, test.eoc_geolocation_t.tierfromsep171, test.eoc_geolocation_t.tierfromdec181, test.eoc_geolocation_t.mtap, test.eoc_geolocation_t.rspuc, test.eoc_geolocation_t.issmt, test.eoc_geolocation_t.updateddatetime, test.eoc_geolocation_t.x_code, test.eoc_geolocation_t.draining_uc, test.eoc_geolocation_t.upap_districts, test.eoc_geolocation_t.shruc, test.eoc_geolocation_t.khidist_id"
            
#             sql = "SELECT " + cols + "  FROM test.get_after_catch_up eoc_1 left JOIN test.eoc_geolocation_t eoc_2 ON eoc_1.location_code  = eoc_2.code JOIN test.xbi_campaign eoc_3 ON (eoc_1.ActivityID  = eoc_3.campaign_ActivityID_old And eoc_1.Yr = eoc_3.campaign_Yr) "

#             data = client.execute(sql)
#             apiDataFrame = pd.DataFrame(data)
#             apiDataFrame = apiDataFrame.replace(r'^\s*$', np.nan, regex=True)
#             #apiDataFrame['location_code'] = pd.to_numeric(apiDataFrame['location_code'], errors='coerce').fillna(0)
#             all_columns = list(apiDataFrame)  # Creates list of all column headers
#             cols = apiDataFrame.iloc[0]
#             apiDataFrame[all_columns] = apiDataFrame[all_columns].astype(str)
#             d = 'ID', 'UserName', 'IDcampCat', 'ActivityID', 'TimeStamp', 'Yr', 'TehsilID', 'UCID', 'DistID', 'DivID', 'ProvID', 'cday', 'VaccinationDate', 'ACCovNA011', 'ACCovNA1259', 'ACCovRef011', 'ACCovRef1259', 'ACOutofH011', 'ACOutofH1259', 'ACGuests', 'ACCovMob', 'ACNewBorn', 'PersistentlyMC', 'PersistentlyMCHRMP', 'ACAlreadyVaccinated',  'UnRecCov', 'OPVGiven', 'OPVUsed', 'OPVReturned', 'status', 'trash', 'isSync','fm_issued','fm_retrieved', 'Remarks', 'location_code', 'campaign_ID', 'campaign_ActivityName', 'campaign_ActivityID_old', 'campaign_Yr', 'campaign_SubActivityName','geoLocation_name', 'geoLocation_type', 'geoLocation_code', 'geoLocation_census_pop', 'geoLocation_target', 'geoLocation_status', 'geoLocation_pname', 'geoLocation_dname', 'geoLocation_namedistrict', 'geoLocation_codedistrict', 'geoLocation_tname', 'geoLocation_provincecode', 'geoLocation_districtcode', 'geoLocation_tehsilcode', 'geoLocation_priority', 'geoLocation_commnet', 'geoLocation_hr', 'geoLocation_fcm', 'geoLocation_tier', 'geoLocation_block', 'geoLocation_division', 'geoLocation_cordinates', 'geoLocation_latitude', 'geoLocation_longitude', 'geoLocation_x', 'geoLocation_y', 'geoLocation_imagepath', 'geoLocation_isccpv', 'geoLocation_rank', 'geoLocation_rank_score', 'geoLocation_ishealthcamp', 'geoLocation_isdsc', 'geoLocation_ucorg', 'geoLocation_organization', 'geoLocation_tierfromaug161', 'geoLocation_tierfromsep171', 'geoLocation_tierfromdec181', 'geoLocation_mtap', 'geoLocation_rspuc', 'geoLocation_issmt', 'geoLocation_updateddatetime', 'geoLocation_x_code', 'geoLocation_draining_uc', 'geoLocation_upap_districts', 'geoLocation_shruc', 'geoLocation_khidist_id'  
#             dff = pd.DataFrame(columns=d)
#             for index, item in enumerate(d):
#                 dff[item] = apiDataFrame[index].values
#             df2 = client.query_dataframe(
#                 "SELECT * FROM test.xbi_after_catchUp")
#             if df2.empty:
#                 client.insert_dataframe(
#                     'INSERT INTO test.xbi_after_catchUp  VALUES', dff)
#                 logger.info(
#                     'Data has been inserted into Table\' INSERT INTO test.xbi_after_catchUp  VALUES \' ')

#                 sql = "DROP table if exists test.get_after_catch_up"
#                 client.execute(sql)
#                 print('\n\n\n CID\t\t--------\t', c)
#                 print('\n\n\n Year\t\t--------\t', y)
#             else:
#                 # df = pd.concat([dff, df2])
#                 # df = df.astype('str')
#                 # df = df.drop_duplicates(subset='ID',
#                 #                         keep="first", inplace=False)
#                 # sql = "DROP TABLE test.xbi_after_catchUp ;"
#                 # client.execute(sql)
#                 print('\n\n\n CID\t\t--------\t', c)
#                 print('\n\n\n Year\t\t--------\t', y)
#                 dff = dff.replace(r'^\s*$', np.nan, regex=True)
#                 dff['location_code'] = pd.to_numeric(dff['location_code'], errors='coerce').fillna(0)
#                 client.insert_dataframe(
#                     'INSERT INTO test.xbi_after_catchUp  VALUES', dff)

#                 sql = "DROP table if exists test.get_after_catch_up"
#                 client.execute(sql)

#             # apiDataFrame = apiDataFrame.drop_duplicates(subset='ID', keep="first", inplace=False)
           



# dag = DAG(
#     'AfterCatchUp_Automated',
#     schedule_interval='0 0 * * *',  # once a day at midnight
#     #schedule_interval='*/59 * * * *',  # will run after an hour[Every 60 minute].
#     default_args=default_args,
#     catchup=False)

# with dag:
#     GetAndInsertApiDataAfterCatchUp = PythonOperator(
#         task_id='GetAndInsertApiDataAfterCatchUp',
#         python_callable=GetAndInsertApiDataAfterCatchUp,
#     )

# GetAndInsertApiDataAfterCatchUp 

