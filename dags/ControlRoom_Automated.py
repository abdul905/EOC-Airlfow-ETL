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
#---------------------------------------- INSERT CONTROL ROOM COVERAGE DATA   --------------------------------------------------#
#-------------------------------------------- Author: Abdul Bari Malik ---------------------------------------------------------#
#-------------------------------------------------------------------------------------------------------------------------------#
def GetAndInsertApiDataControlRoom():
    logger.info('Function \' GetAndInsertApiDataControlRoom \' Started Off')
    client = Client(host='161.97.136.95',
                    user='default',
                    password='pakistan',
                    port='9000', settings={"use_numpy": True})
    cid = 2
    y = 2022
    url = "http://idims.eoc.gov.pk/api_who/api/get_allcontrollroom_combined/5468XE2LN6CzR7qRG041/"+ str(cid)+"/"+ str(y)
    #url = 'http://idims.eoc.gov.pk/api_who/api/get_allcontrollroom_combined/5468XE2LN6CzR7qRG041/2/2022'
    logger.info('Requested Data From Api URL: '+url)
    #logger.info('Requested Data From Api URL:  \' http://idims.eoc.gov.pk/api_who/api/get_allcontrollroom_combined/5468XE2LN6CzR7qRG041/2/2022 \' ')

    r = requests.get(url)
    data = r.json()
    logger.info('Received Data  From Api URL: '+url)
    
    rowsData = data["data"]["data"]
    apiDataFrame = pd.DataFrame(rowsData)
    count_row = apiDataFrame.shape[0]
    count_col = apiDataFrame.shape[1]
    print("DF Rows",count_row)
 
    sql = "CREATE TABLE if not exists test.get_allcontrollroom_combined  (ID Int32, IDcampCat Int32, ActivityID Int32, Yr Int32, TimeStamp varchar(10), Cday Int32, TehsilID Int32, UCID Int32, ccpvTargets Int32, VaccinationDate varchar(50), TeamsRpt Int32, HH011_MP Int32, HH1259_MP Int32, HH011_TS Int32, HH1259_TS Int32, HH011 Int32, HH1259 Int32, OutofH011 Int32, OutofH1259 Int32, RecNA011 Int32, RecNA1259 Int32, RecRef011 Int32, RecRef1259 Int32, CovNA011 Int32, CovNA1259 Int32, CovRef011 Int32, CovRef1259 Int32, Guests Int32, VaccinatedSchool Int32, NewBorn Int32, AlreadyVaccinated Int32, FixSite011 Int32, FixSite1259 Int32, Transit011 Int32, Transit1259 Int32, CovMob Int32, HHvisit Int32, HHPlan Int32, MultipleFamily String, Weakness Int32, ZeroRt Int32, OPVGiven Int32, OPVUsed Int32, OPVReturned Int32, Stage34 Int32, Inaccessible Int32, Remarks String, V_Age611 Int32, v_Age1259 Int32, V_capGiven Int32, V_capUsed Int32, V_capRet Int32, status Int32, trash Int32, isSync Int32, trigers String, Covid19 Int32, location_code Int32, campcat_name String, PRIMARY KEY(ID ))ENGINE = MergeTree"
    client.execute(sql)

    df2 = client.query_dataframe(
        "SELECT * FROM test.get_allcontrollroom_combined")
    if df2.empty:
        apiDataFrame = apiDataFrame.replace(r'^\s*$', np.nan, regex=True)
        client.insert_dataframe(
            'INSERT INTO test.get_allcontrollroom_combined VALUES', apiDataFrame)


def CreateJoinTableOfControlRoom():
    logger.info(' Function  \' CreateJoinTableOfControlRoom \' has been Initiated')
    client = Client(host='161.97.136.95',
                    user='default',
                    password='pakistan',
                    port='9000', settings={"use_numpy": True})
    sql = "CREATE TABLE if not exists test.xbi_controlroom (ID Int32, IDcampCat Int32,ActivityID Int32,Yr Int32,TimeStamp String,Cday Int32,TehsilID Int32,UCID Int32,ccpvTargets Int32,VaccinationDate String,TeamsRpt Int32,HH011_MP Int32,HH1259_MP Int32,HH011_TS Int32,HH1259_TS Int32,HH011 Int32,HH1259 Int32,OutofH011 Int32,OutofH1259 Int32,RecNA011 Int32,RecNA1259 Int32,RecRef011 Int32,RecRef1259 Int32,CovNA011 Int32,CovNA1259 Int32,CovRef011 Int32,CovRef1259 Int32,Guests Int32,VaccinatedSchool Int32,NewBorn Int32,AlreadyVaccinated Int32,FixSite011 Int32,FixSite1259 Int32,Transit011 Int32,Transit1259 Int32,CovMob Int32,HHvisit Int32,HHPlan Int32,MultipleFamily String,Weakness Int32,ZeroRt Int32,OPVGiven Int32,OPVUsed Int32,OPVReturned Int32,Stage34 Int32,Inaccessible Int32,Remarks String,V_Age611 Int32,v_Age1259 Int32,V_capGiven Int32,V_capUsed Int32,V_capRet Int32,status Int32,trash Int32,isSync Int32,trigers String,Covid19 Int32,location_code Int32,campcat_name String, campaign_ID Int32, campaign_ActivityName String, campaign_ActivityID_old Int32, campaign_Yr Int32, campaign_SubActivityName String, geoLocation_name String,geoLocation_type Int32,geoLocation_code Int32,geoLocation_census_pop Int32,geoLocation_target Int32,geoLocation_status Int32,geoLocation_pname String,geoLocation_dname String,geoLocation_namedistrict String,geoLocation_codedistrict String,geoLocation_tname String,geoLocation_provincecode Int32,geoLocation_districtcode Int32,geoLocation_tehsilcode Int32,geoLocation_priority Int32,geoLocation_commnet Int32,geoLocation_hr Int32,geoLocation_fcm Int8,geoLocation_tier Int32,geoLocation_block String,geoLocation_division String,geoLocation_cordinates String,geoLocation_latitude String,geoLocation_longitude String,geoLocation_x String,geoLocation_y String,geoLocation_imagepath String,geoLocation_isccpv Int32,geoLocation_rank Int32,geoLocation_rank_score Float64,geoLocation_ishealthcamp Int32,geoLocation_isdsc Int32,geoLocation_ucorg String,geoLocation_organization String,geoLocation_tierfromaug161 Int32,geoLocation_tierfromsep171 Int32,geoLocation_tierfromdec181 Int32,geoLocation_mtap Int32,geoLocation_rspuc Int32,geoLocation_issmt Int32,geoLocation_updateddatetime String,geoLocation_x_code Int32,geoLocation_draining_uc Int32,geoLocation_upap_districts Int32,geoLocation_shruc Int32,geoLocation_khidist_id String)ENGINE = MergeTree PRIMARY KEY ID ORDER BY ID"
    client.execute(sql)
    cols = "test.get_allcontrollroom_combined.ID,test.get_allcontrollroom_combined.IDcampCat,test.get_allcontrollroom_combined.ActivityID,test.get_allcontrollroom_combined.Yr,test.get_allcontrollroom_combined.TimeStamp,test.get_allcontrollroom_combined.Cday,test.get_allcontrollroom_combined.TehsilID,test.get_allcontrollroom_combined.UCID,test.get_allcontrollroom_combined.ccpvTargets,test.get_allcontrollroom_combined.VaccinationDate,test.get_allcontrollroom_combined.TeamsRpt,test.get_allcontrollroom_combined.HH011_MP,test.get_allcontrollroom_combined.HH1259_MP,test.get_allcontrollroom_combined.HH011_TS,test.get_allcontrollroom_combined.HH1259_TS,test.get_allcontrollroom_combined.HH011,test.get_allcontrollroom_combined.HH1259,test.get_allcontrollroom_combined.OutofH011,test.get_allcontrollroom_combined.OutofH1259,test.get_allcontrollroom_combined.RecNA011,test.get_allcontrollroom_combined.RecNA1259,test.get_allcontrollroom_combined.RecRef011,test.get_allcontrollroom_combined.RecRef1259,test.get_allcontrollroom_combined.CovNA011,test.get_allcontrollroom_combined.CovNA1259,test.get_allcontrollroom_combined.CovRef011,test.get_allcontrollroom_combined.CovRef1259,test.get_allcontrollroom_combined.Guests,test.get_allcontrollroom_combined.VaccinatedSchool,test.get_allcontrollroom_combined.NewBorn,test.get_allcontrollroom_combined.AlreadyVaccinated,test.get_allcontrollroom_combined.FixSite011,test.get_allcontrollroom_combined.FixSite1259,test.get_allcontrollroom_combined.Transit011,test.get_allcontrollroom_combined.Transit1259,test.get_allcontrollroom_combined.CovMob,test.get_allcontrollroom_combined.HHvisit,test.get_allcontrollroom_combined.HHPlan,test.get_allcontrollroom_combined.MultipleFamily,test.get_allcontrollroom_combined.Weakness,test.get_allcontrollroom_combined.ZeroRt,test.get_allcontrollroom_combined.OPVGiven,test.get_allcontrollroom_combined.OPVUsed,test.get_allcontrollroom_combined.OPVReturned,test.get_allcontrollroom_combined.Stage34,test.get_allcontrollroom_combined.Inaccessible,test.get_allcontrollroom_combined.Remarks,test.get_allcontrollroom_combined.V_Age611,test.get_allcontrollroom_combined.v_Age1259,test.get_allcontrollroom_combined.V_capGiven,test.get_allcontrollroom_combined.V_capUsed,test.get_allcontrollroom_combined.V_capRet,test.get_allcontrollroom_combined.status,test.get_allcontrollroom_combined.trash,test.get_allcontrollroom_combined.isSync,test.get_allcontrollroom_combined.trigers,test.get_allcontrollroom_combined.Covid19,test.get_allcontrollroom_combined.location_code,test.get_allcontrollroom_combined.campcat_name, test.xbi_campaign.campaign_ID, test.xbi_campaign.campaign_ActivityName, test.xbi_campaign.campaign_ActivityID_old, test.xbi_campaign.campaign_Yr, test.xbi_campaign.campaign_SubActivityName,  test.eoc_geolocation_t.name, test.eoc_geolocation_t.type, test.eoc_geolocation_t.code, test.eoc_geolocation_t.census_pop, test.eoc_geolocation_t.target, test.eoc_geolocation_t.status, test.eoc_geolocation_t.pname, test.eoc_geolocation_t.dname, test.eoc_geolocation_t.namedistrict, test.eoc_geolocation_t.codedistrict, test.eoc_geolocation_t.tname, test.eoc_geolocation_t.provincecode, test.eoc_geolocation_t.districtcode, test.eoc_geolocation_t.tehsilcode, test.eoc_geolocation_t.priority, test.eoc_geolocation_t.commnet, test.eoc_geolocation_t.hr, test.eoc_geolocation_t.fcm, test.eoc_geolocation_t.tier, test.eoc_geolocation_t.block, test.eoc_geolocation_t.division, test.eoc_geolocation_t.cordinates, test.eoc_geolocation_t.latitude, test.eoc_geolocation_t.longitude, test.eoc_geolocation_t.x, test.eoc_geolocation_t.y, test.eoc_geolocation_t.imagepath, test.eoc_geolocation_t.isccpv, test.eoc_geolocation_t.rank, test.eoc_geolocation_t.rank_score, test.eoc_geolocation_t.ishealthcamp, test.eoc_geolocation_t.isdsc, test.eoc_geolocation_t.ucorg, test.eoc_geolocation_t.organization, test.eoc_geolocation_t.tierfromaug161, test.eoc_geolocation_t.tierfromsep171, test.eoc_geolocation_t.tierfromdec181, test.eoc_geolocation_t.mtap, test.eoc_geolocation_t.rspuc, test.eoc_geolocation_t.issmt, test.eoc_geolocation_t.updateddatetime, test.eoc_geolocation_t.x_code, test.eoc_geolocation_t.draining_uc, test.eoc_geolocation_t.upap_districts, test.eoc_geolocation_t.shruc, test.eoc_geolocation_t.khidist_id"
    sql = "SELECT " + cols + "  FROM test.get_allcontrollroom_combined eoc_1 left JOIN test.eoc_geolocation_t eoc_2 ON eoc_1.location_code  = eoc_2.code left JOIN test.xbi_campaign eoc_3 ON (eoc_1.ActivityID  = eoc_3.campaign_ActivityID_old And eoc_1.Yr = eoc_3.campaign_Yr) "

    data = client.execute(sql)
    apiDataFrame = pd.DataFrame(data)
    all_columns = list(apiDataFrame)  # Creates list of all column headers
    cols = apiDataFrame.iloc[0]
    apiDataFrame[all_columns] = apiDataFrame[all_columns].astype(str)
    d = 'ID', 'IDcampCat', 'ActivityID', 'Yr', 'TimeStamp', 'Cday', 'TehsilID', 'UCID', 'ccpvTargets', 'VaccinationDate', 'TeamsRpt', 'HH011_MP', 'HH1259_MP', 'HH011_TS', 'HH1259_TS', 'HH011', 'HH1259', 'OutofH011', 'OutofH1259', 'RecNA011', 'RecNA1259', 'RecRef011', 'RecRef1259', 'CovNA011', 'CovNA1259', 'CovRef011', 'CovRef1259', 'Guests', 'VaccinatedSchool', 'NewBorn', 'AlreadyVaccinated', 'FixSite011', 'FixSite1259', 'Transit011', 'Transit1259', 'CovMob', 'HHvisit', 'HHPlan', 'MultipleFamily', 'Weakness', 'ZeroRt', 'OPVGiven', 'OPVUsed', 'OPVReturned', 'Stage34', 'Inaccessible', 'Remarks', 'V_Age611', 'v_Age1259', 'V_capGiven', 'V_capUsed', 'V_capRet', 'status', 'trash', 'isSync', 'trigers', 'Covid19', 'location_code', 'campcat_name', 'campaign_ID', 'campaign_ActivityName', 'campaign_ActivityID_old', 'campaign_Yr', 'campaign_SubActivityName','geoLocation_name', 'geoLocation_type', 'geoLocation_code', 'geoLocation_census_pop', 'geoLocation_target', 'geoLocation_status', 'geoLocation_pname', 'geoLocation_dname', 'geoLocation_namedistrict', 'geoLocation_codedistrict', 'geoLocation_tname', 'geoLocation_provincecode', 'geoLocation_districtcode', 'geoLocation_tehsilcode', 'geoLocation_priority', 'geoLocation_commnet', 'geoLocation_hr', 'geoLocation_fcm', 'geoLocation_tier', 'geoLocation_block', 'geoLocation_division', 'geoLocation_cordinates', 'geoLocation_latitude', 'geoLocation_longitude', 'geoLocation_x', 'geoLocation_y', 'geoLocation_imagepath', 'geoLocation_isccpv', 'geoLocation_rank', 'geoLocation_rank_score', 'geoLocation_ishealthcamp', 'geoLocation_isdsc', 'geoLocation_ucorg', 'geoLocation_organization', 'geoLocation_tierfromaug161', 'geoLocation_tierfromsep171', 'geoLocation_tierfromdec181', 'geoLocation_mtap', 'geoLocation_rspuc', 'geoLocation_issmt', 'geoLocation_updateddatetime', 'geoLocation_x_code', 'geoLocation_draining_uc', 'geoLocation_upap_districts', 'geoLocation_shruc', 'geoLocation_khidist_id' 
    dff = pd.DataFrame(columns=d)
    for index, item in enumerate(d):
        dff[item] = apiDataFrame[index].values
        
    df2 = client.query_dataframe(
        "SELECT * FROM test.xbi_controlroom WHERE Yr = 2022 and ActivityID = 2")
    if df2.empty:
        client.insert_dataframe(
            'INSERT INTO test.xbi_controlroom  VALUES', dff)
        logger.info(
            ' Data of Campaign has been inserted into Table\' INSERT INTO test.xbi_controlroom  VALUES \' ')

        sql = "DROP table if exists test.get_allcontrollroom_combined"
        client.execute(sql)
        print('\n\n\n Data Row Count\t\t--------\t', df2.shape[0])
        

    else:
        sql = "ALTER TABLE test.xbi_controlroom DELETE WHERE Yr = 2022 and ActivityID = 2"
        client.execute(sql)
        # df = pd.concat([dff, df2])
        # print('number of rows after concat: ', df.shape[0])
        # df = df.astype('str')
        # df = df.drop_duplicates(subset='ID',
        #                         keep="first", inplace=False)
        # print('number of rows after drop duplicates: ', df.shape[0])
        client.insert_dataframe(
            'INSERT INTO test.xbi_controlroom  VALUES', dff)
        print('\n\n\n Data Row Count\t\t--------\t', dff.shape[0])
        

        sql = "DROP table if exists test.get_allcontrollroom_combined"
        client.execute(sql)


dag = DAG(
    'ControlRoom_Automated',
    schedule_interval='0 0 * * *',  # once a day at midnight
    #schedule_interval='0 * * * *', # Run once an hour at the beginning of the hour 
    default_args=default_args,
    catchup=False)

with dag:
    GetAndInsertApiDataControlRoom = PythonOperator(
        task_id='GetAndInsertApiDataControlRoom',
        python_callable=GetAndInsertApiDataControlRoom,
    )
    CreateJoinTableOfControlRoom = PythonOperator(
        task_id='CreateJoinTableOfControlRoom',
        python_callable=CreateJoinTableOfControlRoom,
    )
GetAndInsertApiDataControlRoom >> CreateJoinTableOfControlRoom

#------------------------------------ Data Insertion------------------------------------------------------------------------------#
#-------------------------------------------- Author: Abdul Bari Malik ---------------------------------------------------------#

# def GetAndInsertApiDataControlRoom():
#     li1 = [28, 29, 31, 30, 1, 106, 34, 9, 37, 40, 40, 43, 10, 2, 46, 11, 3, 107, 1, 106, 2, 9, 8, 10, 3, 107, 28, 31, 4, 9, 1, 28, 31, 2]
#     li2 = [2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2022, 2022, 2022, 2022, 2022]

#     for f, b in zip(li1, li2):
#         logger.info('Function \' GetAndInsertApiDataControlRoom \' Started Off')
#         client = Client(host='161.97.136.95',
#                         user='default',
#                         password='pakistan',
#                         port='9000', settings={"use_numpy": True})

#         url = "http://idims.eoc.gov.pk/api_who/api/get_allcontrollroom_combined/5468XE2LN6CzR7qRG041/"+ str(f)+"/"+ str(b)
        
#         logger.info('Requested Data From Api URL: '+url)

#         r = requests.get(url)
#         data = r.json()
#         if data['data'] != "No data found":
#             logger.info('Received Data  From Api URL: '+url)

#             rowsData = data["data"]["data"]
#             apiDataFrame = pd.DataFrame(rowsData)
#             sql = "CREATE TABLE if not exists test.get_allcontrollroom_combined  (ID Int32, IDcampCat Int32, ActivityID Int32, Yr Int32, TimeStamp varchar(10), Cday Int32, TehsilID Int32, UCID Int32, ccpvTargets Int32, VaccinationDate varchar(50), TeamsRpt Int32, HH011_MP Int32, HH1259_MP Int32, HH011_TS Int32, HH1259_TS Int32, HH011 Int32, HH1259 Int32, OutofH011 Int32, OutofH1259 Int32, RecNA011 Int32, RecNA1259 Int32, RecRef011 Int32, RecRef1259 Int32, CovNA011 Int32, CovNA1259 Int32, CovRef011 Int32, CovRef1259 Int32, Guests Int32, VaccinatedSchool Int32, NewBorn Int32, AlreadyVaccinated Int32, FixSite011 Int32, FixSite1259 Int32, Transit011 Int32, Transit1259 Int32, CovMob Int32, HHvisit Int32, HHPlan Int32, MultipleFamily String, Weakness Int32, ZeroRt Int32, OPVGiven Int32, OPVUsed Int32, OPVReturned Int32, Stage34 Int32, Inaccessible Int32, Remarks String, V_Age611 Int32, v_Age1259 Int32, V_capGiven Int32, V_capUsed Int32, V_capRet Int32, status Int32, trash Int32, isSync Int32, trigers String, Covid19 Int32, location_code Int32, campcat_name String, PRIMARY KEY(ID ))ENGINE = MergeTree"
#             client.execute(sql)
#             df2 = client.query_dataframe("SELECT * FROM test.get_allcontrollroom_combined")            
#             # if df2.empty:
#             if df2.empty:
#                 import numpy as np
#                 apiDataFrame = apiDataFrame.replace(r'^\s*$', np.nan, regex=True)
#                 client.insert_dataframe(
#                     'INSERT INTO test.get_allcontrollroom_combined VALUES', apiDataFrame)
            
#             sql = "CREATE TABLE if not exists test.xbi_controlroom (ID Int32, IDcampCat Int32,ActivityID Int32,Yr Int32,TimeStamp String,Cday Int32,TehsilID Int32,UCID Int32,ccpvTargets Int32,VaccinationDate String,TeamsRpt Int32,HH011_MP Int32,HH1259_MP Int32,HH011_TS Int32,HH1259_TS Int32,HH011 Int32,HH1259 Int32,OutofH011 Int32,OutofH1259 Int32,RecNA011 Int32,RecNA1259 Int32,RecRef011 Int32,RecRef1259 Int32,CovNA011 Int32,CovNA1259 Int32,CovRef011 Int32,CovRef1259 Int32,Guests Int32,VaccinatedSchool Int32,NewBorn Int32,AlreadyVaccinated Int32,FixSite011 Int32,FixSite1259 Int32,Transit011 Int32,Transit1259 Int32,CovMob Int32,HHvisit Int32,HHPlan Int32,MultipleFamily String,Weakness Int32,ZeroRt Int32,OPVGiven Int32,OPVUsed Int32,OPVReturned Int32,Stage34 Int32,Inaccessible Int32,Remarks String,V_Age611 Int32,v_Age1259 Int32,V_capGiven Int32,V_capUsed Int32,V_capRet Int32,status Int32,trash Int32,isSync Int32,trigers String,Covid19 Int32,location_code Int32,campcat_name String, campaign_ID Int32, campaign_ActivityName String, campaign_ActivityID_old Int32, campaign_Yr Int32, campaign_SubActivityName String, geoLocation_name String, geoLocation_type Int32, geoLocation_code Int32, geoLocation_census_pop Int32, geoLocation_target Int32, geoLocation_status Int32, geoLocation_pname String, geoLocation_dname String, geoLocation_namedistrict String, geoLocation_codedistrict String, geoLocation_tname String, geoLocation_provincecode Int32, geoLocation_districtcode Int32, geoLocation_tehsilcode Int32, geoLocation_priority Int32, geoLocation_commnet Int32, geoLocation_hr Int32, geoLocation_fcm Int32, geoLocation_tier Int32, geoLocation_block String, geoLocation_division String, geoLocation_cordinates String, geoLocation_latitude String, geoLocation_longitude String, geoLocation_x String, geoLocation_y String, geoLocation_imagepath String, geoLocation_isccpv Int32, geoLocation_rank Int32, geoLocation_rank_score String, geoLocation_ishealthcamp Int32, geoLocation_isdsc Int32, geoLocation_ucorg String, geoLocation_organization String, geoLocation_tierfromaug161 Int32, geoLocation_tierfromsep171 Int32, geoLocation_tierfromdec181 Int32, geoLocation_mtap Int32, geoLocation_rspuc Int32, geoLocation_issmt Int32, geoLocation_updateddatetime String, geoLocation_x_code Int32, geoLocation_draining_uc Int32, geoLocation_upap_districts Int32, geoLocation_shruc Int32, geoLocation_khidist_id String)ENGINE = MergeTree PRIMARY KEY ID ORDER BY ID"
#             client.execute(sql)
#             cols = "test.get_allcontrollroom_combined.ID,test.get_allcontrollroom_combined.IDcampCat,test.get_allcontrollroom_combined.ActivityID,test.get_allcontrollroom_combined.Yr,test.get_allcontrollroom_combined.TimeStamp,test.get_allcontrollroom_combined.Cday,test.get_allcontrollroom_combined.TehsilID,test.get_allcontrollroom_combined.UCID,test.get_allcontrollroom_combined.ccpvTargets,test.get_allcontrollroom_combined.VaccinationDate,test.get_allcontrollroom_combined.TeamsRpt,test.get_allcontrollroom_combined.HH011_MP,test.get_allcontrollroom_combined.HH1259_MP,test.get_allcontrollroom_combined.HH011_TS,test.get_allcontrollroom_combined.HH1259_TS,test.get_allcontrollroom_combined.HH011,test.get_allcontrollroom_combined.HH1259,test.get_allcontrollroom_combined.OutofH011,test.get_allcontrollroom_combined.OutofH1259,test.get_allcontrollroom_combined.RecNA011,test.get_allcontrollroom_combined.RecNA1259,test.get_allcontrollroom_combined.RecRef011,test.get_allcontrollroom_combined.RecRef1259,test.get_allcontrollroom_combined.CovNA011,test.get_allcontrollroom_combined.CovNA1259,test.get_allcontrollroom_combined.CovRef011,test.get_allcontrollroom_combined.CovRef1259,test.get_allcontrollroom_combined.Guests,test.get_allcontrollroom_combined.VaccinatedSchool,test.get_allcontrollroom_combined.NewBorn,test.get_allcontrollroom_combined.AlreadyVaccinated,test.get_allcontrollroom_combined.FixSite011,test.get_allcontrollroom_combined.FixSite1259,test.get_allcontrollroom_combined.Transit011,test.get_allcontrollroom_combined.Transit1259,test.get_allcontrollroom_combined.CovMob,test.get_allcontrollroom_combined.HHvisit,test.get_allcontrollroom_combined.HHPlan,test.get_allcontrollroom_combined.MultipleFamily,test.get_allcontrollroom_combined.Weakness,test.get_allcontrollroom_combined.ZeroRt,test.get_allcontrollroom_combined.OPVGiven,test.get_allcontrollroom_combined.OPVUsed,test.get_allcontrollroom_combined.OPVReturned,test.get_allcontrollroom_combined.Stage34,test.get_allcontrollroom_combined.Inaccessible,test.get_allcontrollroom_combined.Remarks,test.get_allcontrollroom_combined.V_Age611,test.get_allcontrollroom_combined.v_Age1259,test.get_allcontrollroom_combined.V_capGiven,test.get_allcontrollroom_combined.V_capUsed,test.get_allcontrollroom_combined.V_capRet,test.get_allcontrollroom_combined.status,test.get_allcontrollroom_combined.trash,test.get_allcontrollroom_combined.isSync,test.get_allcontrollroom_combined.trigers,test.get_allcontrollroom_combined.Covid19,test.get_allcontrollroom_combined.location_code,test.get_allcontrollroom_combined.campcat_name, test.xbi_campaign.campaign_ID, test.xbi_campaign.campaign_ActivityName, test.xbi_campaign.campaign_ActivityID_old, test.xbi_campaign.campaign_Yr, test.xbi_campaign.campaign_SubActivityName,  test.eoc_geolocation_t.name, test.eoc_geolocation_t.type, test.eoc_geolocation_t.code, test.eoc_geolocation_t.census_pop, test.eoc_geolocation_t.target, test.eoc_geolocation_t.status, test.eoc_geolocation_t.pname, test.eoc_geolocation_t.dname, test.eoc_geolocation_t.namedistrict, test.eoc_geolocation_t.codedistrict, test.eoc_geolocation_t.tname, test.eoc_geolocation_t.provincecode, test.eoc_geolocation_t.districtcode, test.eoc_geolocation_t.tehsilcode, test.eoc_geolocation_t.priority, test.eoc_geolocation_t.commnet, test.eoc_geolocation_t.hr, test.eoc_geolocation_t.fcm, test.eoc_geolocation_t.tier, test.eoc_geolocation_t.block, test.eoc_geolocation_t.division, test.eoc_geolocation_t.cordinates, test.eoc_geolocation_t.latitude, test.eoc_geolocation_t.longitude, test.eoc_geolocation_t.x, test.eoc_geolocation_t.y, test.eoc_geolocation_t.imagepath, test.eoc_geolocation_t.isccpv, test.eoc_geolocation_t.rank, test.eoc_geolocation_t.rank_score, test.eoc_geolocation_t.ishealthcamp, test.eoc_geolocation_t.isdsc, test.eoc_geolocation_t.ucorg, test.eoc_geolocation_t.organization, test.eoc_geolocation_t.tierfromaug161, test.eoc_geolocation_t.tierfromsep171, test.eoc_geolocation_t.tierfromdec181, test.eoc_geolocation_t.mtap, test.eoc_geolocation_t.rspuc, test.eoc_geolocation_t.issmt, test.eoc_geolocation_t.updateddatetime, test.eoc_geolocation_t.x_code, test.eoc_geolocation_t.draining_uc, test.eoc_geolocation_t.upap_districts, test.eoc_geolocation_t.shruc, test.eoc_geolocation_t.khidist_id"
#             sql = "SELECT " + cols + "  FROM test.get_allcontrollroom_combined eoc_1 left JOIN test.eoc_geolocation_t eoc_2 ON eoc_1.location_code  = eoc_2.code left JOIN test.xbi_campaign eoc_3 ON (eoc_1.ActivityID  = eoc_3.campaign_ActivityID_old And eoc_1.Yr = eoc_3.campaign_Yr) "

#             data = client.execute(sql)
#             apiDataFrame = pd.DataFrame(data)
#             all_columns = list(apiDataFrame)  # Creates list of all column headers
#             cols = apiDataFrame.iloc[0]
#             apiDataFrame[all_columns] = apiDataFrame[all_columns].astype(str)
#             d = 'ID', 'IDcampCat', 'ActivityID', 'Yr', 'TimeStamp', 'Cday', 'TehsilID', 'UCID', 'ccpvTargets', 'VaccinationDate', 'TeamsRpt', 'HH011_MP', 'HH1259_MP', 'HH011_TS', 'HH1259_TS', 'HH011', 'HH1259', 'OutofH011', 'OutofH1259', 'RecNA011', 'RecNA1259', 'RecRef011', 'RecRef1259', 'CovNA011', 'CovNA1259', 'CovRef011', 'CovRef1259', 'Guests', 'VaccinatedSchool', 'NewBorn', 'AlreadyVaccinated', 'FixSite011', 'FixSite1259', 'Transit011', 'Transit1259', 'CovMob', 'HHvisit', 'HHPlan', 'MultipleFamily', 'Weakness', 'ZeroRt', 'OPVGiven', 'OPVUsed', 'OPVReturned', 'Stage34', 'Inaccessible', 'Remarks', 'V_Age611', 'v_Age1259', 'V_capGiven', 'V_capUsed', 'V_capRet', 'status', 'trash', 'isSync', 'trigers', 'Covid19', 'location_code', 'campcat_name', 'campaign_ID', 'campaign_ActivityName', 'campaign_ActivityID_old', 'campaign_Yr', 'campaign_SubActivityName','geoLocation_name', 'geoLocation_type', 'geoLocation_code', 'geoLocation_census_pop', 'geoLocation_target', 'geoLocation_status', 'geoLocation_pname', 'geoLocation_dname', 'geoLocation_namedistrict', 'geoLocation_codedistrict', 'geoLocation_tname', 'geoLocation_provincecode', 'geoLocation_districtcode', 'geoLocation_tehsilcode', 'geoLocation_priority', 'geoLocation_commnet', 'geoLocation_hr', 'geoLocation_fcm', 'geoLocation_tier', 'geoLocation_block', 'geoLocation_division', 'geoLocation_cordinates', 'geoLocation_latitude', 'geoLocation_longitude', 'geoLocation_x', 'geoLocation_y', 'geoLocation_imagepath', 'geoLocation_isccpv', 'geoLocation_rank', 'geoLocation_rank_score', 'geoLocation_ishealthcamp', 'geoLocation_isdsc', 'geoLocation_ucorg', 'geoLocation_organization', 'geoLocation_tierfromaug161', 'geoLocation_tierfromsep171', 'geoLocation_tierfromdec181', 'geoLocation_mtap', 'geoLocation_rspuc', 'geoLocation_issmt', 'geoLocation_updateddatetime', 'geoLocation_x_code', 'geoLocation_draining_uc', 'geoLocation_upap_districts', 'geoLocation_shruc', 'geoLocation_khidist_id' 
#             dff = pd.DataFrame(columns=d)
#             for index, item in enumerate(d):
#                 dff[item] = apiDataFrame[index].values
#             df2 = client.query_dataframe(
#                 "SELECT * FROM test.xbi_controlroom")
#             if df2.empty:
#                 client.insert_dataframe(
#                     'INSERT INTO test.xbi_controlroom  VALUES', dff)
#                 logger.info(
#                     ' Data has been inserted into Table\' INSERT INTO test.xbi_controlroom  VALUES \' ')

#                 sql = "DROP table if exists test.get_allcontrollroom_combined"
#                 client.execute(sql)
#                 print('\n\n\n F\t\t--------\t', f)
#                 print('\n\n\n B\t\t--------\t', b)

#             else:
#                 # df = pd.concat([dff, df2])
#                 # df = df.astype('str')
#                 # df = df.drop_duplicates(subset='ID',
#                 #                         keep="first", inplace=False)
#                 # sql = "DROP TABLE if exists test.xbi_controlroom;"
#                 # client.execute(sql)
#                 # sql = "CREATE TABLE if not exists test.xbi_controlroom (ID Int32, IDcampCat Int32,ActivityID Int32,Yr Int32,TimeStamp String,Cday Int32,TehsilID Int32,UCID Int32,ccpvTargets Int32,VaccinationDate String,TeamsRpt Int32,HH011_MP Int32,HH1259_MP Int32,HH011_TS Int32,HH1259_TS Int32,HH011 Int32,HH1259 Int32,OutofH011 Int32,OutofH1259 Int32,RecNA011 Int32,RecNA1259 Int32,RecRef011 Int32,RecRef1259 Int32,CovNA011 Int32,CovNA1259 Int32,CovRef011 Int32,CovRef1259 Int32,Guests Int32,VaccinatedSchool Int32,NewBorn Int32,AlreadyVaccinated Int32,FixSite011 Int32,FixSite1259 Int32,Transit011 Int32,Transit1259 Int32,CovMob Int32,HHvisit Int32,HHPlan Int32,MultipleFamily String,Weakness Int32,ZeroRt Int32,OPVGiven Int32,OPVUsed Int32,OPVReturned Int32,Stage34 Int32,Inaccessible Int32,Remarks String,V_Age611 Int32,v_Age1259 Int32,V_capGiven Int32,V_capUsed Int32,V_capRet Int32,status Int32,trash Int32,isSync Int32,trigers String,Covid19 Int32,location_code Int32,campcat_name String, campaign_ID Int32, campaign_ActivityName String, campaign_ActivityID_old Int32, campaign_Yr Int32, campaign_SubActivityName String, location_ID Int32, geo_location_code Int32, location_name String, location_type String, location_target Int32, location_status Int32, location_priority String, hr_status String)ENGINE = MergeTree PRIMARY KEY ID ORDER BY ID"
#                 # client.execute(sql)
#                 client.insert_dataframe(
#                     'INSERT INTO test.xbi_controlroom  VALUES', dff)
#                 print('\n\n\n F\t\t--------\t', f)
#                 print('\n\n\n B\t\t--------\t', b)
#                 sql = "DROP table if exists test.get_allcontrollroom_combined"
#                 client.execute(sql)
# dag = DAG(
#     'ControlRoom_Automated',
#     schedule_interval='0 0 * * *',  # once a day at midnight
#     default_args=default_args,
#     catchup=False)

# with dag:
#     GetAndInsertApiDataControlRoom = PythonOperator(
#         task_id='GetAndInsertApiDataControlRoom',
#         python_callable=GetAndInsertApiDataControlRoom,
#     )

# GetAndInsertApiDataControlRoom