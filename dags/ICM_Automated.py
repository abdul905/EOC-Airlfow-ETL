
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

def GetAndInsertApiDataICM():
    logger.info('Function \' GetAndInsertApiDataICM \' Started Off')
    client = Client(host='161.97.136.95',
                    user='default',
                    password='pakistan',
                    port='9000', settings={"use_numpy": True})
    cid = 2
    y = 2022
    url = "http://idims.eoc.gov.pk/api_who/api/get_allcontrollroom_combined/5468XE2LN6CzR7qRG041/"+ str(cid)+"/"+ str(y)
    #url = 'http://idims.eoc.gov.pk/api_who/api/get_controllroom/5468XE2LN6CzR7qRG041/2/2022'
    logger.info('Requested Data From Api URL:  \' http://idims.eoc.gov.pk/api_who/api/get_allcontrollroom_combined/5468XE2LN6CzR7qRG041 /"+ str(cid)+"/"+ str(y) \' ')
    

    r = requests.get(url)
    data = r.json()
    logger.info('Received Data  From Api URL:  \' http://idims.eoc.gov.pk/api_who/api/get_allcontrollroom_combined/5468XE2LN6CzR7qRG041 /"+ str(cid)+"/"+ str(y) \' ')
  

    rowsData = data["data"]["data"]
    apiDataFrame = pd.DataFrame(rowsData)
    count_row = apiDataFrame.shape[0]
    count_col = apiDataFrame.shape[1]
    print("DF Rows",count_row)
 
    sql = "CREATE TABLE if not exists test.get_controllroom  (ID Int32, IDcampCat Int32, ActivityID Int32, Yr Int32, TimeStamp varchar(10), Cday Int32, TehsilID Int32, UCID Int32, ccpvTargets Int32, VaccinationDate varchar(50), TeamsRpt Int32, HH011_MP Int32, HH1259_MP Int32, HH011_TS Int32, HH1259_TS Int32, HH011 Int32, HH1259 Int32, OutofH011 Int32, OutofH1259 Int32, RecNA011 Int32, RecNA1259 Int32, RecRef011 Int32, RecRef1259 Int32, CovNA011 Int32, CovNA1259 Int32, CovRef011 Int32, CovRef1259 Int32, Guests Int32, VaccinatedSchool Int32, NewBorn Int32, AlreadyVaccinated Int32, FixSite011 Int32, FixSite1259 Int32, Transit011 Int32, Transit1259 Int32, CovMob Int32, HHvisit Int32, HHPlan Int32, MultipleFamily String, Weakness Int32, ZeroRt Int32, OPVGiven Int32, OPVUsed Int32, OPVReturned Int32, Stage34 Int32, Inaccessible Int32, Remarks String, V_Age611 Int32, v_Age1259 Int32, V_capGiven Int32, V_capUsed Int32, V_capRet Int32, status Int32, trash Int32, isSync Int32, trigers String, Covid19 Int32, location_code Int32, campcat_name String, PRIMARY KEY(ID ))ENGINE = MergeTree"
    client.execute(sql)

    df2 = client.query_dataframe(
        "SELECT * FROM test.get_controllroom")
    if df2.empty:
        apiDataFrame = apiDataFrame.replace(r'^\s*$', np.nan, regex=True)
        client.insert_dataframe(
            'INSERT INTO test.get_controllroom VALUES', apiDataFrame)


def CreateJoinTableOfICM():
    logger.info(' Function  \' CreateJoinTableOfICM \' has been Initiated')
    client = Client(host='161.97.136.95',
                    user='default',
                    password='pakistan',
                    port='9000', settings={"use_numpy": True})
    sql = "CREATE TABLE if not exists test.xbi_icm (ID Int32, IDcampCat Int32,ActivityID Int32,Yr Int32,TimeStamp String,Cday Int32,TehsilID Int32,UCID Int32,ccpvTargets Int32,VaccinationDate String,TeamsRpt Int32,HH011_MP Int32,HH1259_MP Int32,HH011_TS Int32,HH1259_TS Int32,HH011 Int32,HH1259 Int32,OutofH011 Int32,OutofH1259 Int32,RecNA011 Int32,RecNA1259 Int32,RecRef011 Int32,RecRef1259 Int32,CovNA011 Int32,CovNA1259 Int32,CovRef011 Int32,CovRef1259 Int32,Guests Int32,VaccinatedSchool Int32,NewBorn Int32,AlreadyVaccinated Int32,FixSite011 Int32,FixSite1259 Int32,Transit011 Int32,Transit1259 Int32,CovMob Int32,HHvisit Int32,HHPlan Int32,MultipleFamily String,Weakness Int32,ZeroRt Int32,OPVGiven Int32,OPVUsed Int32,OPVReturned Int32,Stage34 Int32,Inaccessible Int32,Remarks String,V_Age611 Int32,v_Age1259 Int32,V_capGiven Int32,V_capUsed Int32,V_capRet Int32,status Int32,trash Int32,isSync Int32,trigers String,Covid19 Int32,location_code Int32,campcat_name String, campaign_ID Int32, campaign_ActivityName String, campaign_ActivityID_old Int32, campaign_Yr Int32, campaign_SubActivityName String, geoLocation_name String,geoLocation_type Int32,geoLocation_code Int32,geoLocation_census_pop Int32,geoLocation_target Int32,geoLocation_status Int32,geoLocation_pname String,geoLocation_dname String,geoLocation_namedistrict String,geoLocation_codedistrict String,geoLocation_tname String,geoLocation_provincecode Int32,geoLocation_districtcode Int32,geoLocation_tehsilcode Int32,geoLocation_priority Int32,geoLocation_commnet Int32,geoLocation_hr Int32,geoLocation_fcm Int8,geoLocation_tier Int32,geoLocation_block String,geoLocation_division String,geoLocation_cordinates String,geoLocation_latitude String,geoLocation_longitude String,geoLocation_x String,geoLocation_y String,geoLocation_imagepath String,geoLocation_isccpv Int32,geoLocation_rank Int32,geoLocation_rank_score Float64,geoLocation_ishealthcamp Int32,geoLocation_isdsc Int32,geoLocation_ucorg String,geoLocation_organization String,geoLocation_tierfromaug161 Int32,geoLocation_tierfromsep171 Int32,geoLocation_tierfromdec181 Int32,geoLocation_mtap Int32,geoLocation_rspuc Int32,geoLocation_issmt Int32,geoLocation_updateddatetime String,geoLocation_x_code Int32,geoLocation_draining_uc Int32,geoLocation_upap_districts Int32,geoLocation_shruc Int32,geoLocation_khidist_id String)ENGINE = MergeTree PRIMARY KEY ID ORDER BY ID"
    client.execute(sql)
    cols = "test.get_controllroom.ID,test.get_controllroom.IDcampCat,test.get_controllroom.ActivityID,test.get_controllroom.Yr,test.get_controllroom.TimeStamp,test.get_controllroom.Cday,test.get_controllroom.TehsilID,test.get_controllroom.UCID,test.get_controllroom.ccpvTargets,test.get_controllroom.VaccinationDate,test.get_controllroom.TeamsRpt,test.get_controllroom.HH011_MP,test.get_controllroom.HH1259_MP,test.get_controllroom.HH011_TS,test.get_controllroom.HH1259_TS,test.get_controllroom.HH011,test.get_controllroom.HH1259,test.get_controllroom.OutofH011,test.get_controllroom.OutofH1259,test.get_controllroom.RecNA011,test.get_controllroom.RecNA1259,test.get_controllroom.RecRef011,test.get_controllroom.RecRef1259,test.get_controllroom.CovNA011,test.get_controllroom.CovNA1259,test.get_controllroom.CovRef011,test.get_controllroom.CovRef1259,test.get_controllroom.Guests,test.get_controllroom.VaccinatedSchool,test.get_controllroom.NewBorn,test.get_controllroom.AlreadyVaccinated,test.get_controllroom.FixSite011,test.get_controllroom.FixSite1259,test.get_controllroom.Transit011,test.get_controllroom.Transit1259,test.get_controllroom.CovMob,test.get_controllroom.HHvisit,test.get_controllroom.HHPlan,test.get_controllroom.MultipleFamily,test.get_controllroom.Weakness,test.get_controllroom.ZeroRt,test.get_controllroom.OPVGiven,test.get_controllroom.OPVUsed,test.get_controllroom.OPVReturned,test.get_controllroom.Stage34,test.get_controllroom.Inaccessible,test.get_controllroom.Remarks,test.get_controllroom.V_Age611,test.get_controllroom.v_Age1259,test.get_controllroom.V_capGiven,test.get_controllroom.V_capUsed,test.get_controllroom.V_capRet,test.get_controllroom.status,test.get_controllroom.trash,test.get_controllroom.isSync,test.get_controllroom.trigers,test.get_controllroom.Covid19,test.get_controllroom.location_code,test.get_controllroom.campcat_name, test.xbi_campaign.campaign_ID, test.xbi_campaign.campaign_ActivityName, test.xbi_campaign.campaign_ActivityID_old, test.xbi_campaign.campaign_Yr, test.xbi_campaign.campaign_SubActivityName,  test.eoc_geolocation_t.name, test.eoc_geolocation_t.type, test.eoc_geolocation_t.code, test.eoc_geolocation_t.census_pop, test.eoc_geolocation_t.target, test.eoc_geolocation_t.status, test.eoc_geolocation_t.pname, test.eoc_geolocation_t.dname, test.eoc_geolocation_t.namedistrict, test.eoc_geolocation_t.codedistrict, test.eoc_geolocation_t.tname, test.eoc_geolocation_t.provincecode, test.eoc_geolocation_t.districtcode, test.eoc_geolocation_t.tehsilcode, test.eoc_geolocation_t.priority, test.eoc_geolocation_t.commnet, test.eoc_geolocation_t.hr, test.eoc_geolocation_t.fcm, test.eoc_geolocation_t.tier, test.eoc_geolocation_t.block, test.eoc_geolocation_t.division, test.eoc_geolocation_t.cordinates, test.eoc_geolocation_t.latitude, test.eoc_geolocation_t.longitude, test.eoc_geolocation_t.x, test.eoc_geolocation_t.y, test.eoc_geolocation_t.imagepath, test.eoc_geolocation_t.isccpv, test.eoc_geolocation_t.rank, test.eoc_geolocation_t.rank_score, test.eoc_geolocation_t.ishealthcamp, test.eoc_geolocation_t.isdsc, test.eoc_geolocation_t.ucorg, test.eoc_geolocation_t.organization, test.eoc_geolocation_t.tierfromaug161, test.eoc_geolocation_t.tierfromsep171, test.eoc_geolocation_t.tierfromdec181, test.eoc_geolocation_t.mtap, test.eoc_geolocation_t.rspuc, test.eoc_geolocation_t.issmt, test.eoc_geolocation_t.updateddatetime, test.eoc_geolocation_t.x_code, test.eoc_geolocation_t.draining_uc, test.eoc_geolocation_t.upap_districts, test.eoc_geolocation_t.shruc, test.eoc_geolocation_t.khidist_id"
    sql = "SELECT " + cols + "  FROM test.get_controllroom eoc_1 left JOIN test.eoc_geolocation_t eoc_2 ON eoc_1.location_code  = eoc_2.code left JOIN test.xbi_campaign eoc_3 ON (eoc_1.ActivityID  = eoc_3.campaign_ActivityID_old And eoc_1.Yr = eoc_3.campaign_Yr) "

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
        "SELECT * FROM test.xbi_icm WHERE Yr = 2022 and ActivityID = 2")
    if df2.empty:
        client.insert_dataframe(
            'INSERT INTO test.xbi_icm  VALUES', dff)
        logger.info(
            ' Data of Campaign has been inserted into Table\' INSERT INTO test.xbi_icm  VALUES \' ')

        sql = "DROP table if exists test.get_controllroom"
        client.execute(sql)
        print('\n\n\n Data Row Count\t\t--------\t', df2.shape[0])
        

    else:
        sql = "ALTER TABLE test.xbi_icm DELETE WHERE Yr = 2022 and ActivityID = 2"
        client.execute(sql)
        # df = pd.concat([dff, df2])
        # print('number of rows after concat: ', df.shape[0])
        # df = df.astype('str')
        # df = df.drop_duplicates(subset='ID',
        #                         keep="first", inplace=False)
        # print('number of rows after drop duplicates: ', df.shape[0])
        # sql = "DROP TABLE if exists test.tester;"

        # sql = "CREATE TABLE if not exists test.tester (ID Int32, IDcampCat Int32,ActivityID Int32,Yr Int32,TimeStamp String,Cday Int32,TehsilID Int32,UCID Int32,ccpvTargets Int32,VaccinationDate String,TeamsRpt Int32,HH011_MP Int32,HH1259_MP Int32,HH011_TS Int32,HH1259_TS Int32,HH011 Int32,HH1259 Int32,OutofH011 Int32,OutofH1259 Int32,RecNA011 Int32,RecNA1259 Int32,RecRef011 Int32,RecRef1259 Int32,CovNA011 Int32,CovNA1259 Int32,CovRef011 Int32,CovRef1259 Int32,Guests Int32,VaccinatedSchool Int32,NewBorn Int32,AlreadyVaccinated Int32,FixSite011 Int32,FixSite1259 Int32,Transit011 Int32,Transit1259 Int32,CovMob Int32,HHvisit Int32,HHPlan Int32,MultipleFamily String,Weakness Int32,ZeroRt Int32,OPVGiven Int32,OPVUsed Int32,OPVReturned Int32,Stage34 Int32,Inaccessible Int32,Remarks String,V_Age611 Int32,v_Age1259 Int32,V_capGiven Int32,V_capUsed Int32,V_capRet Int32,status Int32,trash Int32,isSync Int32,trigers String,Covid19 Int32,location_code Int32,campcat_name String, campaign_ID Int32, campaign_ActivityName String, campaign_ActivityID_old Int32, campaign_Yr Int32, campaign_SubActivityName String, location_ID Int32, geo_location_code Int32, location_name String, location_type String, location_target Int32, location_status Int32, location_priority String, hr_status String)ENGINE = MergeTree PRIMARY KEY ID ORDER BY ID"
        # client.execute(sql)
        # print('Insertion of finally dff dataframe into table',dff.shape[0])
        client.insert_dataframe(
            'INSERT INTO test.xbi_icm  VALUES', dff)
        print('\n\n\n Data Row Count\t\t--------\t', dff.shape[0])
        
        # sql = "DROP table if exists test.get_allcontrolRoomApiTestData"
        # client.execute(sql)
        # df = pd.concat([dff, df2])
        # df = df.astype('str')
        # df = df.drop_duplicates(subset='ID',
        #                         keep="first", inplace=False)
        # sql = "DROP TABLE if exists test.xbi_icm;"
        # client.execute(sql)
        # sql = "CREATE TABLE if not exists test.xbi_icm (ID Int32, IDcampCat Int32,ActivityID Int32,Yr Int32,TimeStamp String,Cday Int32,TehsilID Int32,UCID Int32,ccpvTargets Int32,VaccinationDate String,TeamsRpt Int32,HH011_MP Int32,HH1259_MP Int32,HH011_TS Int32,HH1259_TS Int32,HH011 Int32,HH1259 Int32,OutofH011 Int32,OutofH1259 Int32,RecNA011 Int32,RecNA1259 Int32,RecRef011 Int32,RecRef1259 Int32,CovNA011 Int32,CovNA1259 Int32,CovRef011 Int32,CovRef1259 Int32,Guests Int32,VaccinatedSchool Int32,NewBorn Int32,AlreadyVaccinated Int32,FixSite011 Int32,FixSite1259 Int32,Transit011 Int32,Transit1259 Int32,CovMob Int32,HHvisit Int32,HHPlan Int32,MultipleFamily String,Weakness Int32,ZeroRt Int32,OPVGiven Int32,OPVUsed Int32,OPVReturned Int32,Stage34 Int32,Inaccessible Int32,Remarks String,V_Age611 Int32,v_Age1259 Int32,V_capGiven Int32,V_capUsed Int32,V_capRet Int32,status Int32,trash Int32,isSync Int32,trigers String,Covid19 Int32,location_code Int32,campcat_name String, campaign_ID Int32, campaign_ActivityName String, campaign_ActivityID_old Int32, campaign_Yr Int32, campaign_SubActivityName String, location_ID Int32, geo_location_code Int32, location_name String, location_type String, location_target Int32, location_status Int32, location_priority String, hr_status String)ENGINE = MergeTree PRIMARY KEY ID ORDER BY ID"
        # client.execute(sql)
        # client.insert_dataframe(
        #     'INSERT INTO test.xbi_icm  VALUES', df)

        sql = "DROP table if exists test.get_controllroom"
        client.execute(sql)


dag = DAG(
    'ICM_Automated',
    schedule_interval='0 0 * * *',  # once a day at midnight
    #schedule_interval='0 * * * *', # Run once an hour at the beginning of the hour 
    #schedule_interval=None,
    default_args=default_args,
    catchup=False)

with dag:
    GetAndInsertApiDataICM = PythonOperator(
        task_id='GetAndInsertApiDataICM',
        python_callable=GetAndInsertApiDataICM,
    )
    CreateJoinTableOfICM = PythonOperator(
        task_id='CreateJoinTableOfICM',
        python_callable=CreateJoinTableOfICM,
    )
GetAndInsertApiDataICM >> CreateJoinTableOfICM

#------------------------------------------------------#
# def GetAndInsertApiDataICM():
#     li1 = [28, 29, 31, 30, 1, 106, 34, 9, 37, 40, 40, 43, 10, 2, 46, 11, 3, 107, 1, 106, 2, 9, 8, 10, 3, 107, 28, 31, 4, 9, 1, 28, 31, 2]
#     li2 = [2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2022, 2022, 2022, 2022, 2022]

#     for f, b in zip(li1, li2):
#         logger.info('Function \' GetAndInsertApiDataICM \' Started Off')
#         client = Client(host='161.97.136.95',
#                         user='default',
#                         password='pakistan',
#                         port='9000', settings={"use_numpy": True})

#         url = "http://idims.eoc.gov.pk/api_who/api/get_controllroom/5468XE2LN6CzR7qRG041/"+ str(f)+"/"+ str(b)
        
#         logger.info('Requested Data From Api URL:  \' http://idims.eoc.gov.pk/api_who/api/get_controllroom/5468XE2LN6CzR7qRG041 /"+ str(f)+"/"+ str(b) \' ')

#         r = requests.get(url)
#         data = r.json()
#         if data['data'] != "No data found":
#             logger.info('Received Data  From Api URL:  \' http://idims.eoc.gov.pk/api_who/api/get_controllroom/5468XE2LN6CzR7qRG041 /"+ str(f)+"/"+ str(b) \' ')

#             rowsData = data["data"]["data"]
#             apiDataFrame = pd.DataFrame(rowsData)
#             sql = "CREATE TABLE if not exists test.get_controllroom  (ID Int32, IDcampCat Int32, ActivityID Int32, Yr Int32, TimeStamp varchar(10), Cday Int32, TehsilID Int32, UCID Int32, ccpvTargets Int32, VaccinationDate varchar(50), TeamsRpt Int32, HH011_MP Int32, HH1259_MP Int32, HH011_TS Int32, HH1259_TS Int32, HH011 Int32, HH1259 Int32, OutofH011 Int32, OutofH1259 Int32, RecNA011 Int32, RecNA1259 Int32, RecRef011 Int32, RecRef1259 Int32, CovNA011 Int32, CovNA1259 Int32, CovRef011 Int32, CovRef1259 Int32, Guests Int32, VaccinatedSchool Int32, NewBorn Int32, AlreadyVaccinated Int32, FixSite011 Int32, FixSite1259 Int32, Transit011 Int32, Transit1259 Int32, CovMob Int32, HHvisit Int32, HHPlan Int32, MultipleFamily String, Weakness Int32, ZeroRt Int32, OPVGiven Int32, OPVUsed Int32, OPVReturned Int32, Stage34 Int32, Inaccessible Int32, Remarks String, V_Age611 Int32, v_Age1259 Int32, V_capGiven Int32, V_capUsed Int32, V_capRet Int32, status Int32, trash Int32, isSync Int32, trigers String, Covid19 Int32, location_code Int32, campcat_name String, PRIMARY KEY(ID ))ENGINE = MergeTree"
#             client.execute(sql)
#             df2 = client.query_dataframe("SELECT * FROM test.get_controllroom")            
#             # if df2.empty:
#             if df2.empty:
#                 import numpy as np
#                 apiDataFrame = apiDataFrame.replace(r'^\s*$', np.nan, regex=True)
#                 client.insert_dataframe(
#                     'INSERT INTO test.get_controllroom VALUES', apiDataFrame)
            
#             sql = "CREATE TABLE if not exists test.xbi_icm (ID Int32, IDcampCat Int32,ActivityID Int32,Yr Int32,TimeStamp String,Cday Int32,TehsilID Int32,UCID Int32,ccpvTargets Int32,VaccinationDate String,TeamsRpt Int32,HH011_MP Int32,HH1259_MP Int32,HH011_TS Int32,HH1259_TS Int32,HH011 Int32,HH1259 Int32,OutofH011 Int32,OutofH1259 Int32,RecNA011 Int32,RecNA1259 Int32,RecRef011 Int32,RecRef1259 Int32,CovNA011 Int32,CovNA1259 Int32,CovRef011 Int32,CovRef1259 Int32,Guests Int32,VaccinatedSchool Int32,NewBorn Int32,AlreadyVaccinated Int32,FixSite011 Int32,FixSite1259 Int32,Transit011 Int32,Transit1259 Int32,CovMob Int32,HHvisit Int32,HHPlan Int32,MultipleFamily String,Weakness Int32,ZeroRt Int32,OPVGiven Int32,OPVUsed Int32,OPVReturned Int32,Stage34 Int32,Inaccessible Int32,Remarks String,V_Age611 Int32,v_Age1259 Int32,V_capGiven Int32,V_capUsed Int32,V_capRet Int32,status Int32,trash Int32,isSync Int32,trigers String,Covid19 Int32,location_code Int32,campcat_name String, campaign_ID Int32, campaign_ActivityName String, campaign_ActivityID_old Int32, campaign_Yr Int32, campaign_SubActivityName String, geoLocation_name String, geoLocation_type Int32, geoLocation_code Int32, geoLocation_census_pop Int32, geoLocation_target Int32, geoLocation_status Int32, geoLocation_pname String, geoLocation_dname String, geoLocation_namedistrict String, geoLocation_codedistrict String, geoLocation_tname String, geoLocation_provincecode Int32, geoLocation_districtcode Int32, geoLocation_tehsilcode Int32, geoLocation_priority Int32, geoLocation_commnet Int32, geoLocation_hr Int32, geoLocation_fcm Int32, geoLocation_tier Int32, geoLocation_block String, geoLocation_division String, geoLocation_cordinates String, geoLocation_latitude String, geoLocation_longitude String, geoLocation_x String, geoLocation_y String, geoLocation_imagepath String, geoLocation_isccpv Int32, geoLocation_rank Int32, geoLocation_rank_score String, geoLocation_ishealthcamp Int32, geoLocation_isdsc Int32, geoLocation_ucorg String, geoLocation_organization String, geoLocation_tierfromaug161 Int32, geoLocation_tierfromsep171 Int32, geoLocation_tierfromdec181 Int32, geoLocation_mtap Int32, geoLocation_rspuc Int32, geoLocation_issmt Int32, geoLocation_updateddatetime String, geoLocation_x_code Int32, geoLocation_draining_uc Int32, geoLocation_upap_districts Int32, geoLocation_shruc Int32, geoLocation_khidist_id String)ENGINE = MergeTree PRIMARY KEY ID ORDER BY ID"
#             client.execute(sql)
#             cols = "test.get_controllroom.ID,test.get_controllroom.IDcampCat,test.get_controllroom.ActivityID,test.get_controllroom.Yr,test.get_controllroom.TimeStamp,test.get_controllroom.Cday,test.get_controllroom.TehsilID,test.get_controllroom.UCID,test.get_controllroom.ccpvTargets,test.get_controllroom.VaccinationDate,test.get_controllroom.TeamsRpt,test.get_controllroom.HH011_MP,test.get_controllroom.HH1259_MP,test.get_controllroom.HH011_TS,test.get_controllroom.HH1259_TS,test.get_controllroom.HH011,test.get_controllroom.HH1259,test.get_controllroom.OutofH011,test.get_controllroom.OutofH1259,test.get_controllroom.RecNA011,test.get_controllroom.RecNA1259,test.get_controllroom.RecRef011,test.get_controllroom.RecRef1259,test.get_controllroom.CovNA011,test.get_controllroom.CovNA1259,test.get_controllroom.CovRef011,test.get_controllroom.CovRef1259,test.get_controllroom.Guests,test.get_controllroom.VaccinatedSchool,test.get_controllroom.NewBorn,test.get_controllroom.AlreadyVaccinated,test.get_controllroom.FixSite011,test.get_controllroom.FixSite1259,test.get_controllroom.Transit011,test.get_controllroom.Transit1259,test.get_controllroom.CovMob,test.get_controllroom.HHvisit,test.get_controllroom.HHPlan,test.get_controllroom.MultipleFamily,test.get_controllroom.Weakness,test.get_controllroom.ZeroRt,test.get_controllroom.OPVGiven,test.get_controllroom.OPVUsed,test.get_controllroom.OPVReturned,test.get_controllroom.Stage34,test.get_controllroom.Inaccessible,test.get_controllroom.Remarks,test.get_controllroom.V_Age611,test.get_controllroom.v_Age1259,test.get_controllroom.V_capGiven,test.get_controllroom.V_capUsed,test.get_controllroom.V_capRet,test.get_controllroom.status,test.get_controllroom.trash,test.get_controllroom.isSync,test.get_controllroom.trigers,test.get_controllroom.Covid19,test.get_controllroom.location_code,test.get_controllroom.campcat_name, test.xbi_campaign.campaign_ID, test.xbi_campaign.campaign_ActivityName, test.xbi_campaign.campaign_ActivityID_old, test.xbi_campaign.campaign_Yr, test.xbi_campaign.campaign_SubActivityName,  test.eoc_geolocation_t.name, test.eoc_geolocation_t.type, test.eoc_geolocation_t.code, test.eoc_geolocation_t.census_pop, test.eoc_geolocation_t.target, test.eoc_geolocation_t.status, test.eoc_geolocation_t.pname, test.eoc_geolocation_t.dname, test.eoc_geolocation_t.namedistrict, test.eoc_geolocation_t.codedistrict, test.eoc_geolocation_t.tname, test.eoc_geolocation_t.provincecode, test.eoc_geolocation_t.districtcode, test.eoc_geolocation_t.tehsilcode, test.eoc_geolocation_t.priority, test.eoc_geolocation_t.commnet, test.eoc_geolocation_t.hr, test.eoc_geolocation_t.fcm, test.eoc_geolocation_t.tier, test.eoc_geolocation_t.block, test.eoc_geolocation_t.division, test.eoc_geolocation_t.cordinates, test.eoc_geolocation_t.latitude, test.eoc_geolocation_t.longitude, test.eoc_geolocation_t.x, test.eoc_geolocation_t.y, test.eoc_geolocation_t.imagepath, test.eoc_geolocation_t.isccpv, test.eoc_geolocation_t.rank, test.eoc_geolocation_t.rank_score, test.eoc_geolocation_t.ishealthcamp, test.eoc_geolocation_t.isdsc, test.eoc_geolocation_t.ucorg, test.eoc_geolocation_t.organization, test.eoc_geolocation_t.tierfromaug161, test.eoc_geolocation_t.tierfromsep171, test.eoc_geolocation_t.tierfromdec181, test.eoc_geolocation_t.mtap, test.eoc_geolocation_t.rspuc, test.eoc_geolocation_t.issmt, test.eoc_geolocation_t.updateddatetime, test.eoc_geolocation_t.x_code, test.eoc_geolocation_t.draining_uc, test.eoc_geolocation_t.upap_districts, test.eoc_geolocation_t.shruc, test.eoc_geolocation_t.khidist_id"
#             sql = "SELECT " + cols + "  FROM test.get_controllroom eoc_1 left JOIN test.eoc_geolocation_t eoc_2 ON eoc_1.location_code  = eoc_2.code left JOIN test.xbi_campaign eoc_3 ON (eoc_1.ActivityID  = eoc_3.campaign_ActivityID_old And eoc_1.Yr = eoc_3.campaign_Yr) "

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
#                 "SELECT * FROM test.xbi_icm")
#             if df2.empty:
#                 client.insert_dataframe(
#                     'INSERT INTO test.xbi_icm  VALUES', dff)
#                 logger.info(
#                     ' Data has been inserted into Table\' INSERT INTO test.xbi_icm  VALUES \' ')

#                 sql = "DROP table if exists test.get_controllroom"
#                 client.execute(sql)
#                 print('\n\n\n F\t\t--------\t', f)
#                 print('\n\n\n B\t\t--------\t', b)

#             else:
#                 # df = pd.concat([dff, df2])
#                 # df = df.astype('str')
#                 # df = df.drop_duplicates(subset='ID',
#                 #                         keep="first", inplace=False)
#                 # sql = "DROP TABLE if exists test.xbi_icm;"
#                 # client.execute(sql)
#                 # sql = "CREATE TABLE if not exists test.xbi_icm (ID Int32, IDcampCat Int32,ActivityID Int32,Yr Int32,TimeStamp String,Cday Int32,TehsilID Int32,UCID Int32,ccpvTargets Int32,VaccinationDate String,TeamsRpt Int32,HH011_MP Int32,HH1259_MP Int32,HH011_TS Int32,HH1259_TS Int32,HH011 Int32,HH1259 Int32,OutofH011 Int32,OutofH1259 Int32,RecNA011 Int32,RecNA1259 Int32,RecRef011 Int32,RecRef1259 Int32,CovNA011 Int32,CovNA1259 Int32,CovRef011 Int32,CovRef1259 Int32,Guests Int32,VaccinatedSchool Int32,NewBorn Int32,AlreadyVaccinated Int32,FixSite011 Int32,FixSite1259 Int32,Transit011 Int32,Transit1259 Int32,CovMob Int32,HHvisit Int32,HHPlan Int32,MultipleFamily String,Weakness Int32,ZeroRt Int32,OPVGiven Int32,OPVUsed Int32,OPVReturned Int32,Stage34 Int32,Inaccessible Int32,Remarks String,V_Age611 Int32,v_Age1259 Int32,V_capGiven Int32,V_capUsed Int32,V_capRet Int32,status Int32,trash Int32,isSync Int32,trigers String,Covid19 Int32,location_code Int32,campcat_name String, campaign_ID Int32, campaign_ActivityName String, campaign_ActivityID_old Int32, campaign_Yr Int32, campaign_SubActivityName String, location_ID Int32, geo_location_code Int32, location_name String, location_type String, location_target Int32, location_status Int32, location_priority String, hr_status String)ENGINE = MergeTree PRIMARY KEY ID ORDER BY ID"
#                 # client.execute(sql)
#                 client.insert_dataframe(
#                     'INSERT INTO test.xbi_icm  VALUES', dff)
#                 print('\n\n\n F\t\t--------\t', f)
#                 print('\n\n\n B\t\t--------\t', b)
#                 sql = "DROP table if exists test.get_controllroom"
#                 client.execute(sql)
# dag = DAG(
#     'ICM_Automated',
#     schedule_interval='0 0 * * *',  # once a day at midnight
#     default_args=default_args,
#     catchup=False)

# with dag:
#     GetAndInsertApiDataICM = PythonOperator(
#         task_id='GetAndInsertApiDataICM',
#         python_callable=GetAndInsertApiDataICM,
#     )

# GetAndInsertApiDataICM
#-------------------------------------------------------------------------------------------------------------------------------#
#---------------------------------------------------DATA INSERTION: ICM_Automated-----------------------------------------------#
#-------------------------------------------------------------------------------------------------------------------------------#
# def GetAndInsertApiDataICM():
#     logger.info('Function \' GetAndInsertApiDataICM \' Started Off')
#     client = Client(host='161.97.136.95',
#                     user='default',
#                     password='pakistan',
#                     port='9000', settings={"use_numpy": True})

#     url = 'https://idims.eoc.gov.pk/api_who/api/get_icm/5468XE2LN6CzR7qRG041/team/247'
#     logger.info('Requested Data From Api URL:  \' https://idims.eoc.gov.pk/api_who/api/get_icm/5468XE2LN6CzR7qRG041/team/247 \' ')

#     r = requests.get(url)
#     data = r.json()
#     logger.info('Received Data  From Api URL:  \' https://idims.eoc.gov.pk/api_who/api/get_icm/5468XE2LN6CzR7qRG041/team/247 \' ')

#     rowsData = data["data"]["data"]
#     apiDataFrame = pd.DataFrame(rowsData)
#     sql = "CREATE TABLE if not exists test.get_icm ( pk_icm_team_21_id Int32, fk_form_id Int32, fk_user_id Int32, date_created String, campaign_id Int32, surveyor_name String, designation String, surveyor_affliate String, fk_prov_id Int32, fk_dist_id Int32, fk_tehsil_id Int32, fk_uc_id Int32, g1_cbv_noncbv String, g1_aic_name String, g1_team_no String, g1_day String, g1_team_composition_government String, g1_team_composition_local String, g1_team_composition_female String, g1_team_composition_adult String, g1_NamesMP_match String, g1_valid_marker String, g1_T_memTrained String, g1_Tdate String, g1_opv_dry_valid_vvm String, g1_opv_dry_cool String, g1_opv_dry_dry String, g1_t_sheetcorrect String, g1_vitamin_a String, g1_T_recordedMissed String, g1_T_mapAvailable String, g1_Tworkload String, g1_Comment String, g2_obsorve String, g2_T_chiledAwayHome Int32, g2_T_rmAFP Int32, g2_T_markerUsed Int32, g2_T_CfMarked Int32, g2_T_childVaccinatedToday Int32, g2_Comments String, covid_indi String, covid_brief_guidline Int32, covid_symptoms Int32, covid_wear_mask Int32, covid_use_santzr Int32, covid_age_member Int32, covid_hold_child Int32, covid_chllenges String, covid_chllenges_dscrptn String, unique_id String, dev_remarks String, app_version String, cold_chain String, g1_opv_dry_intact String, g1_opv_dry_re_capped String, g1_matched_admin_team String)ENGINE = MergeTree PRIMARY KEY pk_icm_team_21_id ORDER BY pk_icm_team_21_id"
#     client.execute(sql)

#     df2 = client.query_dataframe(
#         "SELECT * FROM test.get_icm")
#     if df2.empty:
#         client.insert_dataframe(
#             'INSERT INTO test.get_icm VALUES', apiDataFrame)


# def CreateJoinTableOfICM():
#     logger.info(' Function  \' CreateJoinTableOfICM \' has been Initiated')
#     client = Client(host='161.97.136.95',
#                     user='default',
#                     password='pakistan',
#                     port='9000', settings={"use_numpy": True})
#     sql = "CREATE TABLE if not exists test.xbi_icm (icm_pk_icm_team_21_id Int32, icm_fk_form_id Int32, icm_fk_user_id Int32, icm_date_created String, icm_campaign_id Int32, icm_surveyor_name String, icm_designation String, icm_surveyor_affliate String, icm_fk_prov_id Int32, icm_fk_dist_id Int32, icm_fk_tehsil_id Int32, icm_fk_uc_id Int32, icm_g1_cbv_noncbv Int32, icm_g1_aic_name String, icm_g1_team_no String, icm_g1_day String, icm_g1_team_composition_government Int32, icm_g1_team_composition_local Int32, icm_g1_team_composition_female Int32, icm_g1_team_composition_adult Int32, icm_g1_NamesMP_match Int32, icm_g1_valid_marker Int32, icm_g1_T_memTrained Int32, icm_g1_Tdate String, icm_g1_opv_dry_valid_vvm Int32, icm_g1_opv_dry_cool String, icm_g1_opv_dry_dry Int32, icm_g1_t_sheetcorrect Int32, icm_g1_vitamin_a String, icm_g1_T_recordedMissed Int32, icm_g1_T_mapAvailable Int32, icm_g1_Tworkload Int32, icm_g1_Comment String, icm_g2_obsorve String, icm_g2_T_chiledAwayHome Int32, icm_g2_T_rmAFP Int32, icm_g2_T_markerUsed Int32, icm_g2_T_CfMarked Int32, icm_g2_T_childVaccinatedToday Int32, icm_g2_Comments String, icm_covid_indi String, icm_covid_brief_guidline Int32, icm_covid_symptoms Int32, icm_covid_wear_mask Int32, icm_covid_use_santzr Int32, icm_covid_age_member Int32, icm_covid_hold_child Int32, icm_covid_chllenges String, icm_covid_chllenges_dscrptn String, icm_unique_id String, icm_dev_remarks String, icm_app_version String, icm_cold_chain String, icm_g1_opv_dry_intact String, icm_g1_opv_dry_re_capped String, icm_g1_matched_admin_team String, compaign_id Int32, compaign_compaign_name String, compaign_type Int32, compaign_month_date Int32, compaign_comp_year Int32, compaign_start_date String, compaign_end_date String, compaign_code String, compaign_created_by Int32, compaign_create_date String, compaign_status Int32, compaign_is_deleted Int32, compaign_sortorder Int32, compaign_iscomnet Int32, compaign_api_id Int32, compaign_exports Int32, compaign_isipv Int32, geoLocation_name String, geoLocation_type Int32, geoLocation_code Int32, geoLocation_census_pop Int32, geoLocation_target Int32, geoLocation_status Int32, geoLocation_pname String, geoLocation_dname String, geoLocation_namedistrict String, geoLocation_codedistrict String, geoLocation_tname String, geoLocation_provincecode Int32, geoLocation_districtcode Int32, geoLocation_tehsilcode Int32, geoLocation_priority Int32, geoLocation_commnet Int32, geoLocation_hr Int32, geoLocation_fcm Int32, geoLocation_tier Int32, geoLocation_block String, geoLocation_division String, geoLocation_cordinates String, geoLocation_latitude String, geoLocation_longitude String, geoLocation_x String, geoLocation_y String, geoLocation_imagepath String, geoLocation_isccpv Int32, geoLocation_rank Int32, geoLocation_rank_score String, geoLocation_ishealthcamp Int32, geoLocation_isdsc Int32, geoLocation_ucorg String, geoLocation_organization String, geoLocation_tierfromaug161 Int32, geoLocation_tierfromsep171 Int32, geoLocation_tierfromdec181 Int32, geoLocation_mtap Int32, geoLocation_rspuc Int32, geoLocation_issmt Int32, geoLocation_updateddatetime String, geoLocation_x_code Int32, geoLocation_draining_uc Int32, geoLocation_upap_districts Int32, geoLocation_shruc Int32, geoLocation_khidist_id String)ENGINE = MergeTree PRIMARY KEY icm_pk_icm_team_21_id ORDER BY icm_pk_icm_team_21_id"
#     client.execute(sql)
#     cols = "test.get_icm.pk_icm_team_21_id, test.get_icm.fk_form_id, test.get_icm.fk_user_id, test.get_icm.date_created, test.get_icm.campaign_id, test.get_icm.surveyor_name, test.get_icm.designation, test.get_icm.surveyor_affliate, test.get_icm.fk_prov_id, test.get_icm.fk_dist_id, test.get_icm.fk_tehsil_id, test.get_icm.fk_uc_id, test.get_icm.g1_cbv_noncbv, test.get_icm.g1_aic_name, test.get_icm.g1_team_no, test.get_icm.g1_day, test.get_icm.g1_team_composition_government, test.get_icm.g1_team_composition_local, test.get_icm.g1_team_composition_female, test.get_icm.g1_team_composition_adult, test.get_icm.g1_NamesMP_match, test.get_icm.g1_valid_marker, test.get_icm.g1_T_memTrained, test.get_icm.g1_Tdate, test.get_icm.g1_opv_dry_valid_vvm, test.get_icm.g1_opv_dry_cool, test.get_icm.g1_opv_dry_dry, test.get_icm.g1_t_sheetcorrect, test.get_icm.g1_vitamin_a, test.get_icm.g1_T_recordedMissed, test.get_icm.g1_T_mapAvailable, test.get_icm.g1_Tworkload, test.get_icm.g1_Comment, test.get_icm.g2_obsorve, test.get_icm.g2_T_chiledAwayHome, test.get_icm.g2_T_rmAFP, test.get_icm.g2_T_markerUsed, test.get_icm.g2_T_CfMarked, test.get_icm.g2_T_childVaccinatedToday, test.get_icm.g2_Comments, test.get_icm.covid_indi, test.get_icm.covid_brief_guidline, test.get_icm.covid_symptoms, test.get_icm.covid_wear_mask, test.get_icm.covid_use_santzr, test.get_icm.covid_age_member, test.get_icm.covid_hold_child, test.get_icm.covid_chllenges, test.get_icm.covid_chllenges_dscrptn, test.get_icm.unique_id, test.get_icm.dev_remarks, test.get_icm.app_version, test.get_icm.cold_chain, test.get_icm.g1_opv_dry_intact, test.get_icm.g1_opv_dry_re_capped, test.get_icm.g1_matched_admin_team,test.eoc_compaign_tbl.id, test.eoc_compaign_tbl.compaign_name, test.eoc_compaign_tbl.type, test.eoc_compaign_tbl.month_date, test.eoc_compaign_tbl.comp_year, test.eoc_compaign_tbl.start_date, test.eoc_compaign_tbl.end_date, test.eoc_compaign_tbl.code, test.eoc_compaign_tbl.created_by, test.eoc_compaign_tbl.create_date, test.eoc_compaign_tbl.status, test.eoc_compaign_tbl.is_deleted, test.eoc_compaign_tbl.sortorder, test.eoc_compaign_tbl.iscomnet, test.eoc_compaign_tbl.api_id, test.eoc_compaign_tbl.exports, test.eoc_compaign_tbl.isipv, test.eoc_geolocation_t.name, test.eoc_geolocation_t.type, test.eoc_geolocation_t.code, test.eoc_geolocation_t.census_pop, test.eoc_geolocation_t.target, test.eoc_geolocation_t.status, test.eoc_geolocation_t.pname, test.eoc_geolocation_t.dname, test.eoc_geolocation_t.namedistrict, test.eoc_geolocation_t.codedistrict, test.eoc_geolocation_t.tname, test.eoc_geolocation_t.provincecode, test.eoc_geolocation_t.districtcode, test.eoc_geolocation_t.tehsilcode, test.eoc_geolocation_t.priority, test.eoc_geolocation_t.commnet, test.eoc_geolocation_t.hr, test.eoc_geolocation_t.fcm, test.eoc_geolocation_t.tier, test.eoc_geolocation_t.block, test.eoc_geolocation_t.division, test.eoc_geolocation_t.cordinates, test.eoc_geolocation_t.latitude, test.eoc_geolocation_t.longitude, test.eoc_geolocation_t.x, test.eoc_geolocation_t.y, test.eoc_geolocation_t.imagepath, test.eoc_geolocation_t.isccpv, test.eoc_geolocation_t.rank, test.eoc_geolocation_t.rank_score, test.eoc_geolocation_t.ishealthcamp, test.eoc_geolocation_t.isdsc, test.eoc_geolocation_t.ucorg, test.eoc_geolocation_t.organization, test.eoc_geolocation_t.tierfromaug161, test.eoc_geolocation_t.tierfromsep171, test.eoc_geolocation_t.tierfromdec181, test.eoc_geolocation_t.mtap, test.eoc_geolocation_t.rspuc, test.eoc_geolocation_t.issmt, test.eoc_geolocation_t.updateddatetime, test.eoc_geolocation_t.x_code, test.eoc_geolocation_t.draining_uc, test.eoc_geolocation_t.upap_districts, test.eoc_geolocation_t.shruc, test.eoc_geolocation_t.khidist_id"
#     sql = "SELECT " + cols + "  FROM test.get_icm eoc_1 left JOIN test.eoc_geolocation_t eoc_2 ON eoc_1.fk_uc_id  = eoc_2.code left JOIN test.eoc_compaign_tbl eoc_3 ON eoc_1.campaign_id  = eoc_3.id "

#     data = client.execute(sql)
#     apiDataFrame = pd.DataFrame(data)
#     all_columns = list(apiDataFrame)  # Creates list of all column headers
#     cols = apiDataFrame.iloc[0]
#     apiDataFrame[all_columns] = apiDataFrame[all_columns].astype(str)
#     d = 'icm_pk_icm_team_21_id', 'icm_fk_form_id', 'icm_fk_user_id', 'icm_date_created', 'icm_campaign_id', 'icm_surveyor_name', 'icm_designation', 'icm_surveyor_affliate', 'icm_fk_prov_id', 'icm_fk_dist_id', 'icm_fk_tehsil_id', 'icm_fk_uc_id', 'icm_g1_cbv_noncbv', 'icm_g1_aic_name', 'icm_g1_team_no', 'icm_g1_day', 'icm_g1_team_composition_government', 'icm_g1_team_composition_local', 'icm_g1_team_composition_female', 'icm_g1_team_composition_adult', 'icm_g1_NamesMP_match', 'icm_g1_valid_marker', 'icm_g1_T_memTrained', 'icm_g1_Tdate', 'icm_g1_opv_dry_valid_vvm', 'icm_g1_opv_dry_cool', 'icm_g1_opv_dry_dry', 'icm_g1_t_sheetcorrect', 'icm_g1_vitamin_a', 'icm_g1_T_recordedMissed', 'icm_g1_T_mapAvailable', 'icm_g1_Tworkload', 'icm_g1_Comment', 'icm_g2_obsorve', 'icm_g2_T_chiledAwayHome', 'icm_g2_T_rmAFP', 'icm_g2_T_markerUsed', 'icm_g2_T_CfMarked', 'icm_g2_T_childVaccinatedToday', 'icm_g2_Comments', 'icm_covid_indi', 'icm_covid_brief_guidline', 'icm_covid_symptoms', 'icm_covid_wear_mask', 'icm_covid_use_santzr', 'icm_covid_age_member', 'icm_covid_hold_child', 'icm_covid_chllenges', 'icm_covid_chllenges_dscrptn', 'icm_unique_id', 'icm_dev_remarks', 'icm_app_version', 'icm_cold_chain', 'icm_g1_opv_dry_intact', 'icm_g1_opv_dry_re_capped', 'icm_g1_matched_admin_team', 'compaign_id', 'compaign_compaign_name', 'compaign_type', 'compaign_month_date', 'compaign_comp_year', 'compaign_start_date', 'compaign_end_date', 'compaign_code', 'compaign_created_by', 'compaign_create_date', 'compaign_status', 'compaign_is_deleted', 'compaign_sortorder', 'compaign_iscomnet', 'compaign_api_id', 'compaign_exports', 'compaign_isipv', 'geoLocation_name', 'geoLocation_type', 'geoLocation_code', 'geoLocation_census_pop', 'geoLocation_target', 'geoLocation_status', 'geoLocation_pname', 'geoLocation_dname', 'geoLocation_namedistrict', 'geoLocation_codedistrict', 'geoLocation_tname', 'geoLocation_provincecode', 'geoLocation_districtcode', 'geoLocation_tehsilcode', 'geoLocation_priority', 'geoLocation_commnet', 'geoLocation_hr', 'geoLocation_fcm', 'geoLocation_tier', 'geoLocation_block', 'geoLocation_division', 'geoLocation_cordinates', 'geoLocation_latitude', 'geoLocation_longitude', 'geoLocation_x', 'geoLocation_y', 'geoLocation_imagepath', 'geoLocation_isccpv', 'geoLocation_rank', 'geoLocation_rank_score', 'geoLocation_ishealthcamp', 'geoLocation_isdsc', 'geoLocation_ucorg', 'geoLocation_organization', 'geoLocation_tierfromaug161', 'geoLocation_tierfromsep171', 'geoLocation_tierfromdec181', 'geoLocation_mtap', 'geoLocation_rspuc', 'geoLocation_issmt', 'geoLocation_updateddatetime', 'geoLocation_x_code', 'geoLocation_draining_uc', 'geoLocation_upap_districts', 'geoLocation_shruc', 'geoLocation_khidist_id'
#     dff = pd.DataFrame(columns=d)
#     for index, item in enumerate(d):
#         dff[item] = apiDataFrame[index].values
#     df2 = client.query_dataframe(
#         "SELECT * FROM test.xbi_icm")
#     if df2.empty:
#         client.insert_dataframe(
#             'INSERT INTO test.xbi_icm  VALUES', dff)
#         logger.info(
#             ' Data has been inserted into Table\' INSERT INTO test.xbi_icm  VALUES \' ')

#         sql = "DROP table if exists test.get_icm"
#         client.execute(sql)

#     else:
#         df = pd.concat([dff, df2])
#         df = df.astype('str')
#         df = df.drop_duplicates(subset='icm_pk_icm_team_21_id',
#                                 keep="first", inplace=False)
#         sql = "DROP TABLE if exists  test.xbi_icm;"
#         client.execute(sql)
#         sql = "CREATE TABLE if not exists test.xbi_icm (icm_pk_icm_team_21_id Int32, icm_fk_form_id Int32, icm_fk_user_id Int32, icm_date_created String, icm_campaign_id Int32, icm_surveyor_name String, icm_designation String, icm_surveyor_affliate String, icm_fk_prov_id Int32, icm_fk_dist_id Int32, icm_fk_tehsil_id Int32, icm_fk_uc_id Int32, icm_g1_cbv_noncbv Int32, icm_g1_aic_name String, icm_g1_team_no String, icm_g1_day String, icm_g1_team_composition_government Int32, icm_g1_team_composition_local Int32, icm_g1_team_composition_female Int32, icm_g1_team_composition_adult Int32, icm_g1_NamesMP_match Int32, icm_g1_valid_marker Int32, icm_g1_T_memTrained Int32, icm_g1_Tdate String, icm_g1_opv_dry_valid_vvm Int32, icm_g1_opv_dry_cool String, icm_g1_opv_dry_dry Int32, icm_g1_t_sheetcorrect Int32, icm_g1_vitamin_a String, icm_g1_T_recordedMissed Int32, icm_g1_T_mapAvailable Int32, icm_g1_Tworkload Int32, icm_g1_Comment String, icm_g2_obsorve String, icm_g2_T_chiledAwayHome Int32, icm_g2_T_rmAFP Int32, icm_g2_T_markerUsed Int32, icm_g2_T_CfMarked Int32, icm_g2_T_childVaccinatedToday Int32, icm_g2_Comments String, icm_covid_indi String, icm_covid_brief_guidline Int32, icm_covid_symptoms Int32, icm_covid_wear_mask Int32, icm_covid_use_santzr Int32, icm_covid_age_member Int32, icm_covid_hold_child Int32, icm_covid_chllenges String, icm_covid_chllenges_dscrptn String, icm_unique_id String, icm_dev_remarks String, icm_app_version String, icm_cold_chain String, icm_g1_opv_dry_intact String, icm_g1_opv_dry_re_capped String, icm_g1_matched_admin_team String, compaign_id Int32, compaign_compaign_name String, compaign_type Int32, compaign_month_date Int32, compaign_comp_year Int32, compaign_start_date String, compaign_end_date String, compaign_code String, compaign_created_by Int32, compaign_create_date String, compaign_status Int32, compaign_is_deleted Int32, compaign_sortorder Int32, compaign_iscomnet Int32, compaign_api_id Int32, compaign_exports Int32, compaign_isipv Int32, geoLocation_name String, geoLocation_type Int32, geoLocation_code Int32, geoLocation_census_pop Int32, geoLocation_target Int32, geoLocation_status Int32, geoLocation_pname String, geoLocation_dname String, geoLocation_namedistrict String, geoLocation_codedistrict String, geoLocation_tname String, geoLocation_provincecode Int32, geoLocation_districtcode Int32, geoLocation_tehsilcode Int32, geoLocation_priority Int32, geoLocation_commnet Int32, geoLocation_hr Int32, geoLocation_fcm Int32, geoLocation_tier Int32, geoLocation_block String, geoLocation_division String, geoLocation_cordinates String, geoLocation_latitude String, geoLocation_longitude String, geoLocation_x String, geoLocation_y String, geoLocation_imagepath String, geoLocation_isccpv Int32, geoLocation_rank Int32, geoLocation_rank_score String, geoLocation_ishealthcamp Int32, geoLocation_isdsc Int32, geoLocation_ucorg String, geoLocation_organization String, geoLocation_tierfromaug161 Int32, geoLocation_tierfromsep171 Int32, geoLocation_tierfromdec181 Int32, geoLocation_mtap Int32, geoLocation_rspuc Int32, geoLocation_issmt Int32, geoLocation_updateddatetime String, geoLocation_x_code Int32, geoLocation_draining_uc Int32, geoLocation_upap_districts Int32, geoLocation_shruc Int32, geoLocation_khidist_id String)ENGINE = MergeTree PRIMARY KEY icm_pk_icm_team_21_id ORDER BY icm_pk_icm_team_21_id"
#         client.execute(sql)
#         client.insert_dataframe(
#             'INSERT INTO test.xbi_icm  VALUES', df)

#         sql = "DROP table if exists test.get_icm"
#         client.execute(sql)


# dag = DAG(
#     'ICM_Automated',
#     schedule_interval='*/59 * * * *',  # will run every 10 min.
#     default_args=default_args,
#     catchup=False)

# with dag:
#     GetAndInsertApiDataICM = PythonOperator(
#         task_id='GetAndInsertApiDataICM',
#         python_callable=GetAndInsertApiDataICM,
#     )
#     CreateJoinTableOfICM = PythonOperator(
#         task_id='CreateJoinTableOfICM',
#         python_callable=CreateJoinTableOfICM,
#     )
# GetAndInsertApiDataICM >> CreateJoinTableOfICM
