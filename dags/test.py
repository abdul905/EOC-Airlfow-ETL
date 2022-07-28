from airflow import DAG
import pandas as pd
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import requests
from clickhouse_driver import Client
import logging
from datetime import timedelta

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
#---------------------------------------------------DATA INSERTION: TEST_ControlRoom--------------------------------------------#
#-------------------------------------------------------------------------------------------------------------------------------#
def GetAndInsertApiDataTest():
    li1 = [28, 29, 31, 30, 1, 106, 34, 9, 37, 40, 40, 43, 10, 2, 46, 11, 3, 107, 1, 106, 2, 9, 8, 10, 3, 107, 28, 31, 4, 9, 1, 28, 31, 2]
    li2 = [2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2022, 2022, 2022, 2022, 2022]

    for c, y in zip(li1, li2):
        logger.info('Function \' GetAndInsertApiDataTest \' Started Off')
        client = Client(host='161.97.136.95',
                        user='default',
                        password='pakistan',
                        port='9000', settings={"use_numpy": True})

        url = "http://idims.eoc.gov.pk/api_who/api/get_allcontrollroom_combined/5468XE2LN6CzR7qRG041/"+ str(c)+"/"+ str(y)
        print('\n\n\n F\t\t--------\t', c)
        print('\n\n\n B\t\t--------\t', y)
        
        logger.info('Requested Data From Api URL:  \' http://idims.eoc.gov.pk/api_who/api/get_allcontrollroom_combined/5468XE2LN6CzR7qRG041 /"+ str(c)+"/"+ str(y) \' ')
        
        r = requests.get(url)
        data = r.json()
        print('\n\n\n F\t\t--------\t', c)
        print('\n\n\n B\t\t--------\t', y)
        if data['data'] != "No data found":
            logger.info('Received Data  From Api URL:  \' http://idims.eoc.gov.pk/api_who/api/get_allcontrollroom_combined/5468XE2LN6CzR7qRG041 /"+ str(c)+"/"+ str(y) \' ')

            rowsData = data["data"]["data"]
            apiDataFrame = pd.DataFrame(rowsData)
            sql = "CREATE TABLE if not exists test.tester  (ID Int32, IDcampCat Int32, ActivityID Int32, Yr Int32, TimeStamp varchar(10), Cday Int32, TehsilID Int32, UCID Int32, ccpvTargets Int32, VaccinationDate varchar(50), TeamsRpt Int32, HH011_MP Int32, HH1259_MP Int32, HH011_TS Int32, HH1259_TS Int32, HH011 Int32, HH1259 Int32, OutofH011 Int32, OutofH1259 Int32, RecNA011 Int32, RecNA1259 Int32, RecRef011 Int32, RecRef1259 Int32, CovNA011 Int32, CovNA1259 Int32, CovRef011 Int32, CovRef1259 Int32, Guests Int32, VaccinatedSchool Int32, NewBorn Int32, AlreadyVaccinated Int32, FixSite011 Int32, FixSite1259 Int32, Transit011 Int32, Transit1259 Int32, CovMob Int32, HHvisit Int32, HHPlan Int32, MultipleFamily String, Weakness Int32, ZeroRt Int32, OPVGiven Int32, OPVUsed Int32, OPVReturned Int32, Stage34 Int32, Inaccessible Int32, Remarks String, V_Age611 Int32, v_Age1259 Int32, V_capGiven Int32, V_capUsed Int32, V_capRet Int32, status Int32, trash Int32, isSync Int32, trigers String, Covid19 Int32, location_code Int32, campcat_name String, PRIMARY KEY(ID ))ENGINE = MergeTree"
            client.execute(sql)
            # if df2.empty:
            import numpy as np
            apiDataFrame = apiDataFrame.replace(r'^\s*$', np.nan, regex=True)
            # df2 = client.query_dataframe("SELECT * FROM test.tester")
            # apiDataFrame = apiDataFrame.drop_duplicates(subset='ID', keep="first", inplace=False)
            client.insert_dataframe(
            'INSERT INTO test.tester VALUES', apiDataFrame)
            
dag = DAG(
    'test_dag',
    schedule_interval=None,
    #schedule_interval='0 0 * * *',  # once a day at midnight
    default_args=default_args,
    catchup=False)

with dag:
    GetAndInsertApiDataTest = PythonOperator(
        task_id='GetAndInsertApiDataTest',
        python_callable=GetAndInsertApiDataTest,
    )

GetAndInsertApiDataTest


#---------------------------------------------Data_Insetion: Latest Campaign CR-----------------------------------------------------------#



# def GetAndInsertApiDataTest():
#     logger.info('Function \' GetAndInsertApiDataTest \' Started Off')
#     client = Client(host='161.97.136.95',
#                     user='default',
#                     password='pakistan',
#                     port='9000', settings={"use_numpy": True})

#     cid = 2
#     y = 2022
#     url = "http://idims.eoc.gov.pk/api_who/api/get_allcontrollroom_combined/5468XE2LN6CzR7qRG041/"+ str(cid)+"/"+ str(y)
#     logger.info('Requested Data From Api URL:  \' http://idims.eoc.gov.pk/api_who/api/get_allcontrollroom_combined/5468XE2LN6CzR7qRG041 /"+ str(cid)+"/"+ str(y) \' ')
#     # url = 'http://idims.eoc.gov.pk/api_who/api/get_allcontrollroom_combined/5468XE2LN6CzR7qRG041/2/2022'
#     # logger.info('Requested Data From Api URL:  \' http://idims.eoc.gov.pk/api_who/api/get_allcontrollroom_combined/5468XE2LN6CzR7qRG041/2/2022 \' ')

#     r = requests.get(url)
#     data = r.json()

#     if data['data'] != "No data found":
#         print('data\t\t--------\t', data)
#         logger.info('Received Data  From Api URL:  \' http://idims.eoc.gov.pk/api_who/api/get_allcontrollroom_combined/5468XE2LN6CzR7qRG041 /"+ str(cid)+"/"+ str(y) \' ')

#         rowsData = data["data"]["data"]
#         apiDataFrame = pd.DataFrame(rowsData)
#         sql = "CREATE TABLE if not exists test.get_allcontrolRoomApiTestData  (ID Int32, IDcampCat Int32, ActivityID Int32, Yr Int32, TimeStamp varchar(10), Cday Int32, TehsilID Int32, UCID Int32, ccpvTargets Int32, VaccinationDate varchar(50), TeamsRpt Int32, HH011_MP Int32, HH1259_MP Int32, HH011_TS Int32, HH1259_TS Int32, HH011 Int32, HH1259 Int32, OutofH011 Int32, OutofH1259 Int32, RecNA011 Int32, RecNA1259 Int32, RecRef011 Int32, RecRef1259 Int32, CovNA011 Int32, CovNA1259 Int32, CovRef011 Int32, CovRef1259 Int32, Guests Int32, VaccinatedSchool Int32, NewBorn Int32, AlreadyVaccinated Int32, FixSite011 Int32, FixSite1259 Int32, Transit011 Int32, Transit1259 Int32, CovMob Int32, HHvisit Int32, HHPlan Int32, MultipleFamily String, Weakness Int32, ZeroRt Int32, OPVGiven Int32, OPVUsed Int32, OPVReturned Int32, Stage34 Int32, Inaccessible Int32, Remarks String, V_Age611 Int32, v_Age1259 Int32, V_capGiven Int32, V_capUsed Int32, V_capRet Int32, status Int32, trash Int32, isSync Int32, trigers String, Covid19 Int32, location_code Int32, campcat_name String, PRIMARY KEY(ID ))ENGINE = MergeTree"
#         client.execute(sql)

#         df2 = client.query_dataframe(
#             "SELECT * FROM test.get_allcontrolRoomApiTestData")
#         if df2.empty:
#             import numpy as np
#             apiDataFrame = apiDataFrame.replace(r'^\s*$', np.nan, regex=True)
#             client.insert_dataframe(
#                 'INSERT INTO test.get_allcontrolRoomApiTestData VALUES', apiDataFrame)
#     else:
#         print('No Data Found')


# def CreateJoinTableOfControlRoom():
#     logger.info(' Function  \' CreateJoinTableOfControlRoom \' has been Initiated')
#     client = Client(host='161.97.136.95',
#                     user='default',
#                     password='pakistan',
#                     port='9000', settings={"use_numpy": True})
#     sql = "CREATE TABLE if not exists test.tester (ID Int32, IDcampCat Int32,ActivityID Int32,Yr Int32,TimeStamp String,Cday Int32,TehsilID Int32,UCID Int32,ccpvTargets Int32,VaccinationDate String,TeamsRpt Int32,HH011_MP Int32,HH1259_MP Int32,HH011_TS Int32,HH1259_TS Int32,HH011 Int32,HH1259 Int32,OutofH011 Int32,OutofH1259 Int32,RecNA011 Int32,RecNA1259 Int32,RecRef011 Int32,RecRef1259 Int32,CovNA011 Int32,CovNA1259 Int32,CovRef011 Int32,CovRef1259 Int32,Guests Int32,VaccinatedSchool Int32,NewBorn Int32,AlreadyVaccinated Int32,FixSite011 Int32,FixSite1259 Int32,Transit011 Int32,Transit1259 Int32,CovMob Int32,HHvisit Int32,HHPlan Int32,MultipleFamily String,Weakness Int32,ZeroRt Int32,OPVGiven Int32,OPVUsed Int32,OPVReturned Int32,Stage34 Int32,Inaccessible Int32,Remarks String,V_Age611 Int32,v_Age1259 Int32,V_capGiven Int32,V_capUsed Int32,V_capRet Int32,status Int32,trash Int32,isSync Int32,trigers String,Covid19 Int32,location_code Int32,campcat_name String, campaign_ID Int32, campaign_ActivityName String, campaign_ActivityID_old Int32, campaign_Yr Int32, campaign_SubActivityName String, location_ID Int32, geo_location_code Int32, location_name String, location_type String, location_target Int32, location_status Int32, location_priority String, hr_status String)ENGINE = MergeTree PRIMARY KEY ID ORDER BY ID"
#     client.execute(sql)
#     cols = "test.get_allcontrolRoomApiTestData.ID,test.get_allcontrolRoomApiTestData.IDcampCat,test.get_allcontrolRoomApiTestData.ActivityID,test.get_allcontrolRoomApiTestData.Yr,test.get_allcontrolRoomApiTestData.TimeStamp,test.get_allcontrolRoomApiTestData.Cday,test.get_allcontrolRoomApiTestData.TehsilID,test.get_allcontrolRoomApiTestData.UCID,test.get_allcontrolRoomApiTestData.ccpvTargets,test.get_allcontrolRoomApiTestData.VaccinationDate,test.get_allcontrolRoomApiTestData.TeamsRpt,test.get_allcontrolRoomApiTestData.HH011_MP,test.get_allcontrolRoomApiTestData.HH1259_MP,test.get_allcontrolRoomApiTestData.HH011_TS,test.get_allcontrolRoomApiTestData.HH1259_TS,test.get_allcontrolRoomApiTestData.HH011,test.get_allcontrolRoomApiTestData.HH1259,test.get_allcontrolRoomApiTestData.OutofH011,test.get_allcontrolRoomApiTestData.OutofH1259,test.get_allcontrolRoomApiTestData.RecNA011,test.get_allcontrolRoomApiTestData.RecNA1259,test.get_allcontrolRoomApiTestData.RecRef011,test.get_allcontrolRoomApiTestData.RecRef1259,test.get_allcontrolRoomApiTestData.CovNA011,test.get_allcontrolRoomApiTestData.CovNA1259,test.get_allcontrolRoomApiTestData.CovRef011,test.get_allcontrolRoomApiTestData.CovRef1259,test.get_allcontrolRoomApiTestData.Guests,test.get_allcontrolRoomApiTestData.VaccinatedSchool,test.get_allcontrolRoomApiTestData.NewBorn,test.get_allcontrolRoomApiTestData.AlreadyVaccinated,test.get_allcontrolRoomApiTestData.FixSite011,test.get_allcontrolRoomApiTestData.FixSite1259,test.get_allcontrolRoomApiTestData.Transit011,test.get_allcontrolRoomApiTestData.Transit1259,test.get_allcontrolRoomApiTestData.CovMob,test.get_allcontrolRoomApiTestData.HHvisit,test.get_allcontrolRoomApiTestData.HHPlan,test.get_allcontrolRoomApiTestData.MultipleFamily,test.get_allcontrolRoomApiTestData.Weakness,test.get_allcontrolRoomApiTestData.ZeroRt,test.get_allcontrolRoomApiTestData.OPVGiven,test.get_allcontrolRoomApiTestData.OPVUsed,test.get_allcontrolRoomApiTestData.OPVReturned,test.get_allcontrolRoomApiTestData.Stage34,test.get_allcontrolRoomApiTestData.Inaccessible,test.get_allcontrolRoomApiTestData.Remarks,test.get_allcontrolRoomApiTestData.V_Age611,test.get_allcontrolRoomApiTestData.v_Age1259,test.get_allcontrolRoomApiTestData.V_capGiven,test.get_allcontrolRoomApiTestData.V_capUsed,test.get_allcontrolRoomApiTestData.V_capRet,test.get_allcontrolRoomApiTestData.status,test.get_allcontrolRoomApiTestData.trash,test.get_allcontrolRoomApiTestData.isSync,test.get_allcontrolRoomApiTestData.trigers,test.get_allcontrolRoomApiTestData.Covid19,test.get_allcontrolRoomApiTestData.location_code,test.get_allcontrolRoomApiTestData.campcat_name, test.xbi_campaign.campaign_ID, test.xbi_campaign.campaign_ActivityName, test.xbi_campaign.campaign_ActivityID_old, test.xbi_campaign.campaign_Yr, test.xbi_campaign.campaign_SubActivityName,  test.xbi_geolocation.ID, test.xbi_geolocation.code, test.xbi_geolocation.name, test.xbi_geolocation.type,  test.xbi_geolocation.location_target, test.xbi_geolocation.location_status, test.xbi_geolocation.location_priority, test.xbi_geolocation.hr_status"
#     sql = "SELECT " + cols + "  FROM test.get_allcontrolRoomApiTestData eoc_1 left JOIN test.xbi_geolocation eoc_2 ON eoc_1.location_code  = eoc_2.code left JOIN test.xbi_campaign eoc_3 ON (eoc_1.ActivityID  = eoc_3.campaign_ActivityID_old And eoc_1.Yr = eoc_3.campaign_Yr) "

#     data = client.execute(sql)
#     apiDataFrame = pd.DataFrame(data)
#     all_columns = list(apiDataFrame)  # Creates list of all column headers
#     cols = apiDataFrame.iloc[0]
#     apiDataFrame[all_columns] = apiDataFrame[all_columns].astype(str)
#     d = 'ID', 'IDcampCat', 'ActivityID', 'Yr', 'TimeStamp', 'Cday', 'TehsilID', 'UCID', 'ccpvTargets', 'VaccinationDate', 'TeamsRpt', 'HH011_MP', 'HH1259_MP', 'HH011_TS', 'HH1259_TS', 'HH011', 'HH1259', 'OutofH011', 'OutofH1259', 'RecNA011', 'RecNA1259', 'RecRef011', 'RecRef1259', 'CovNA011', 'CovNA1259', 'CovRef011', 'CovRef1259', 'Guests', 'VaccinatedSchool', 'NewBorn', 'AlreadyVaccinated', 'FixSite011', 'FixSite1259', 'Transit011', 'Transit1259', 'CovMob', 'HHvisit', 'HHPlan', 'MultipleFamily', 'Weakness', 'ZeroRt', 'OPVGiven', 'OPVUsed', 'OPVReturned', 'Stage34', 'Inaccessible', 'Remarks', 'V_Age611', 'v_Age1259', 'V_capGiven', 'V_capUsed', 'V_capRet', 'status', 'trash', 'isSync', 'trigers', 'Covid19', 'location_code', 'campcat_name', 'campaign_ID', 'campaign_ActivityName', 'campaign_ActivityID_old', 'campaign_Yr', 'campaign_SubActivityName','location_ID', 'geo_location_code', 'location_name', 'location_type', 'location_target', 'location_status', 'location_priority', 'hr_status' 
#     dff = pd.DataFrame(columns=d)
#     for index, item in enumerate(d):
#         dff[item] = apiDataFrame[index].values
#     df2 = client.query_dataframe(
#         "SELECT * FROM test.tester")
#     if df2.empty:
#         client.insert_dataframe(
#             'INSERT INTO test.tester  VALUES', dff)
#         logger.info(
#             ' Data has been inserted into Table\' INSERT INTO test.tester  VALUES \' ')

#         sql = "DROP table if exists test.get_allcontrolRoomApiTestData"
#         # client.execute(sql)

#     else:
#         # dff = current joined tables and api data
#         # df2 = current Db table
#         # df['Yr'].isin(['2022'])
#         # found = df[df['Yr'].str.contains(2022)]
#         # print(found.count())
        
#         df2 = client.query_dataframe(
#         "SELECT * FROM test.tester")
#         count_row = df2.shape[0]  # gives number of row count
#         count_col = df2.shape[1]  # gives number of col count
#         print('number of rows in table: ', count_row)
#         print('number of cols in table: ', count_col)
#         print('number of rows in api: ', dff.shape[0])
#         print('number of cols in api: ', dff.shape[1])
#         df_new = df2.loc[(df2['Yr'] == 2022) & (df2['ActivityID'] == 2)]
#         if (df_new.empty):
#             print("Data of May 2022 campaign Not Exists")

#         else:
#             sql = "ALTER TABLE test.tester DELETE WHERE Yr = 2022 and ActivityID = 2"
#             client.execute(sql)
#         df = pd.concat([dff, df2])
#         print('number of rows after concat: ', df.shape[0])
#         df = df.astype('str')
#         df = df.drop_duplicates(subset='ID',
#                                 keep="first", inplace=False)
#         print('number of rows after drop duplicates: ', df.shape[0])
#         # sql = "DROP TABLE if exists test.tester;"

#         # sql = "CREATE TABLE if not exists test.tester (ID Int32, IDcampCat Int32,ActivityID Int32,Yr Int32,TimeStamp String,Cday Int32,TehsilID Int32,UCID Int32,ccpvTargets Int32,VaccinationDate String,TeamsRpt Int32,HH011_MP Int32,HH1259_MP Int32,HH011_TS Int32,HH1259_TS Int32,HH011 Int32,HH1259 Int32,OutofH011 Int32,OutofH1259 Int32,RecNA011 Int32,RecNA1259 Int32,RecRef011 Int32,RecRef1259 Int32,CovNA011 Int32,CovNA1259 Int32,CovRef011 Int32,CovRef1259 Int32,Guests Int32,VaccinatedSchool Int32,NewBorn Int32,AlreadyVaccinated Int32,FixSite011 Int32,FixSite1259 Int32,Transit011 Int32,Transit1259 Int32,CovMob Int32,HHvisit Int32,HHPlan Int32,MultipleFamily String,Weakness Int32,ZeroRt Int32,OPVGiven Int32,OPVUsed Int32,OPVReturned Int32,Stage34 Int32,Inaccessible Int32,Remarks String,V_Age611 Int32,v_Age1259 Int32,V_capGiven Int32,V_capUsed Int32,V_capRet Int32,status Int32,trash Int32,isSync Int32,trigers String,Covid19 Int32,location_code Int32,campcat_name String, campaign_ID Int32, campaign_ActivityName String, campaign_ActivityID_old Int32, campaign_Yr Int32, campaign_SubActivityName String, location_ID Int32, geo_location_code Int32, location_name String, location_type String, location_target Int32, location_status Int32, location_priority String, hr_status String)ENGINE = MergeTree PRIMARY KEY ID ORDER BY ID"
#         # client.execute(sql)
#         print('Insertion of finally dff dataframe into table',dff.shape[0])
#         client.insert_dataframe(
#             'INSERT INTO test.tester  VALUES', dff)

#         sql = "DROP table if exists test.get_allcontrolRoomApiTestData"
#         client.execute(sql)


# dag = DAG(
#     'test_dag',
#     #schedule_interval=timedelta(minutes=15),
#     #schedule_interval='0 0 * * *', # once a day at midnight
#     schedule_interval='0 * * * *', # Run once an hour at the beginning of the hour 
#     default_args=default_args,
#     catchup=False)

# with dag:

    # GetAndInsertApiDataTest = PythonOperator(
    #     task_id='GetAndInsertApiDataTest',
    #     python_callable=GetAndInsertApiDataTest,
    # )
    # CreateJoinTableOfControlRoom = PythonOperator(
    #     task_id='CreateJoinTableOfControlRoom',
    #     python_callable=CreateJoinTableOfControlRoom,
    # )
# GetAndInsertApiDataTest >> CreateJoinTableOfControlRoom
