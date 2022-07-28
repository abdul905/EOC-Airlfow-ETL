
from airflow import DAG
import pandas as pd
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import requests
from clickhouse_driver import connect
from clickhouse_driver import Client
import logging
import numpy as np
import db_connection as dbConn
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
#------------------------------------------------- INSERT LATEST CAMPAIGN DATA CatchUp -----------------------------------------#
#-------------------------------------------- Author: Abdul Bari Malik ---------------------------------------------------------#
#-------------------------------------------------------------------------------------------------------------------------------#
def GetAndInsertApiDataCatchUp():
    logger.info('Function \' GetAndInsertApiDataCatchUp \' Started Off')

    # cid = 2
    # y = 2022
    
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

        get_q = "select * FROM (select * from test.xbi_campaign xc  order by xc.campaign_ID desc limit 4) ORDER BY campaign_ID ASC "
        df2 = client.query_dataframe(get_q)
        #df2 = client.execute(get_q)
        print(df2)
        df = pd.DataFrame(df2)

        campID_list = df2.campaign_ID.tolist()
        campName_list = df2.campaign_ActivityName.tolist()
        campIDold_list = df2.campaign_ActivityID_old.tolist()
        campYr_list = df2.campaign_Yr.tolist()

        print(campID_list)
        print(campName_list)
        print(campIDold_list)
        print(campYr_list)        

        for cid, y in zip(campIDold_list, campYr_list):
            url = "http://idims.eoc.gov.pk/api_who/api/get_allcatchup/5468XE2LN6CzR7qRG041/"+ str(cid)+"/"+ str(y)
            print("Requested Data from URL: ",url)
            r = requests.get(url)
            data = r.json()

            if data['data'] == "No data found":
                print("No Data found for campaign: "+str(cid))
                logger.info('No Data found for campaign: '+str(cid))
            else:
                logger.info('Received Data  From Api URL: '+url)
                rowsData = data["data"]["data"]
                apiDataFrame = pd.DataFrame(rowsData)
                
                count_row = apiDataFrame.shape[0]
                count_col = apiDataFrame.shape[1]
                print("DF Rows",count_row)
                print("DF Cols",count_col)
                print('\n\n\n CID\t\t--------\t', cid)
                print('\n\n\n Yr\t\t--------\t', y) 

                sql = " Create Table if not exists test.get_catch_up (ID Int32, UserName String, IDcampCat Int32, ActivityID Int32, TimeStamp String, Yr Int32, TehsilID Int32, UCID Int32, DistID Int32, DivID Int32, ProvID Int32, Cday Int32, CCovNA011 Int32, CCovNA1259 Int32, CCovRef011 Int32, CCovRef1259 Int32, COutofH011 Int32, COutofH1259 Int32, CGuests Int32, CCovMob Int32, CNewBorn Int32, PersistentlyMC Int32, PersistentlyMCHRMP Int32, CAlreadyVaccinated Int32, CFixSite011 Int32, CFixSite1259 Int32, CTransit011 Int32, CTransit1259 Int32, UnRecorded_Cov Int32, OPVGiven Int32, OPVUsed Int32, OPVReturned Int32, AFP_CaseRpt Int32, ZeroDos1 Int32, V_Age611_CP Int32, v_Age1259_CP Int32,fm_issued Int32,fm_retrieved Int32,missing_vacc_carr Int32,missing_vacc_carr_reason String, status Int32, trash Int32, isSync Int32, Remarks String, location_code Int32 NULL)ENGINE = MergeTree PRIMARY KEY ID ORDER BY ID"
                client.execute(sql)

                df2 = client.query_dataframe(
                    "SELECT * FROM test.get_catch_up")
                if df2.empty:
                    apiDataFrame = apiDataFrame.replace(r'^\s*$', np.nan, regex=True)
                    client.insert_dataframe(
                        'INSERT INTO test.get_catch_up VALUES', apiDataFrame)
                else:
                    apiDataFrame = apiDataFrame.replace(r'^\s*$', np.nan, regex=True)
                    client.insert_dataframe(
                        'INSERT INTO test.get_catch_up VALUES', apiDataFrame)

def CreateJoinTableOfCatchUp():
    logger.info(' Function  \' CreateJoinTableOfCatchUp \' has been Initiated')

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

        get_q = "select * FROM (select * from test.xbi_campaign xc  order by xc.campaign_ID desc limit 4) ORDER BY campaign_ID ASC "
        df2 = client.query_dataframe(get_q)
        #df2 = client.execute(get_q)
        print(df2)
        df = pd.DataFrame(df2)

        campID_list = df2.campaign_ID.tolist()
        campName_list = df2.campaign_ActivityName.tolist()
        campIDold_list = df2.campaign_ActivityID_old.tolist()
        campYr_list = df2.campaign_Yr.tolist()

        print(campID_list)
        print(campName_list)
        print(campIDold_list)
        print(campYr_list)

        for f, b in zip(campIDold_list, campYr_list):
            sql = "CREATE TABLE if not exists test.xbi_catchUp (ID Int32, UserName String, IDcampCat Int32, ActivityID Int32, TimeStamp String, Yr Int32, TehsilID Int32, UCID Int32, DistID Int32, DivID Int32, ProvID Int32, Cday Int32, CCovNA011 Int32, CCovNA1259 Int32, CCovRef011 Int32, CCovRef1259 Int32, COutofH011 Int32, COutofH1259 Int32, CGuests Int32, CCovMob Int32, CNewBorn Int32, PersistentlyMC Int32, PersistentlyMCHRMP Int32, CAlreadyVaccinated Int32, CFixSite011 Int32, CFixSite1259 Int32, CTransit011 Int32, CTransit1259 Int32, UnRecorded_Cov Int32, OPVGiven Int32, OPVUsed Int32, OPVReturned String, AFP_CaseRpt Int32, ZeroDos1 Int32, V_Age611_CP Int32, v_Age1259_CP Int32,fm_issued Int32,fm_retrieved Int32,missing_vacc_carr Int32,missing_vacc_carr_reason String, status Int32, trash Int32, isSync Int32, Remarks String, location_code Int32 NULL, campaign_ID Int32, campaign_ActivityName String, campaign_ActivityID_old Int32, campaign_Yr Int32, campaign_SubActivityName String, geoLocation_name String, geoLocation_type Int32, geoLocation_code Int32, geoLocation_census_pop Int32, geoLocation_target Int32, geoLocation_status Int32, geoLocation_pname String, geoLocation_dname String, geoLocation_namedistrict String, geoLocation_codedistrict String, geoLocation_tname String, geoLocation_provincecode Int32, geoLocation_districtcode Int32, geoLocation_tehsilcode Int32, geoLocation_priority Int32, geoLocation_commnet Int32, geoLocation_hr Int32, geoLocation_fcm Int8, geoLocation_tier Int32, geoLocation_block String, geoLocation_division String, geoLocation_cordinates String, geoLocation_latitude String, geoLocation_longitude String, geoLocation_x String, geoLocation_y String, geoLocation_imagepath String, geoLocation_isccpv Int32, geoLocation_rank Int32, geoLocation_rank_score Int32, geoLocation_ishealthcamp Int32, geoLocation_isdsc Int32, geoLocation_ucorg String, geoLocation_organization String, geoLocation_tierfromaug161 Int32, geoLocation_tierfromsep171 Int32, geoLocation_tierfromdec181 Int32, geoLocation_mtap Int32, geoLocation_rspuc Int32, geoLocation_issmt Int32, geoLocation_updateddatetime String, geoLocation_x_code Int32, geoLocation_draining_uc Int32, geoLocation_upap_districts Int32, geoLocation_shruc Int32, geoLocation_khidist_id String)ENGINE = MergeTree PRIMARY KEY ID ORDER BY ID"
            client.execute(sql)
            cols = "test.get_catch_up.ID, test.get_catch_up.UserName, test.get_catch_up.IDcampCat, test.get_catch_up.ActivityID, test.get_catch_up.TimeStamp, test.get_catch_up.Yr, test.get_catch_up.TehsilID, test.get_catch_up.UCID, test.get_catch_up.DistID, test.get_catch_up.DivID, test.get_catch_up.ProvID, test.get_catch_up.Cday, test.get_catch_up.CCovNA011, test.get_catch_up.CCovNA1259, test.get_catch_up.CCovRef011, test.get_catch_up.CCovRef1259, test.get_catch_up.COutofH011, test.get_catch_up.COutofH1259, test.get_catch_up.CGuests, test.get_catch_up.CCovMob, test.get_catch_up.CNewBorn, test.get_catch_up.PersistentlyMC, test.get_catch_up.PersistentlyMCHRMP, test.get_catch_up.CAlreadyVaccinated, test.get_catch_up.CFixSite011, test.get_catch_up.CFixSite1259, test.get_catch_up.CTransit011, test.get_catch_up.CTransit1259, test.get_catch_up.UnRecorded_Cov, test.get_catch_up.OPVGiven, test.get_catch_up.OPVUsed, test.get_catch_up.OPVReturned, test.get_catch_up.AFP_CaseRpt, test.get_catch_up.ZeroDos1, test.get_catch_up.V_Age611_CP, test.get_catch_up.v_Age1259_CP,test.get_catch_up.fm_issued,test.get_catch_up.fm_retrieved,test.get_catch_up.missing_vacc_carr,test.get_catch_up.missing_vacc_carr_reason, test.get_catch_up.status, test.get_catch_up.trash, test.get_catch_up.isSync, test.get_catch_up.Remarks, test.get_catch_up.location_code, test.xbi_campaign.campaign_ID, test.xbi_campaign.campaign_ActivityName, test.xbi_campaign.campaign_ActivityID_old, test.xbi_campaign.campaign_Yr, test.xbi_campaign.campaign_SubActivityName, test.eoc_geolocation_tbl.name, test.eoc_geolocation_tbl.type, test.eoc_geolocation_tbl.code, test.eoc_geolocation_tbl.census_pop, test.eoc_geolocation_tbl.target, test.eoc_geolocation_tbl.status, test.eoc_geolocation_tbl.pname, test.eoc_geolocation_tbl.dname, test.eoc_geolocation_tbl.namedistrict, test.eoc_geolocation_tbl.codedistrict, test.eoc_geolocation_tbl.tname, test.eoc_geolocation_tbl.provincecode, test.eoc_geolocation_tbl.districtcode, test.eoc_geolocation_tbl.tehsilcode, test.eoc_geolocation_tbl.priority, test.eoc_geolocation_tbl.commnet, test.eoc_geolocation_tbl.hr, test.eoc_geolocation_tbl.fcm, test.eoc_geolocation_tbl.tier, test.eoc_geolocation_tbl.block, test.eoc_geolocation_tbl.division, test.eoc_geolocation_tbl.cordinates, test.eoc_geolocation_tbl.latitude, test.eoc_geolocation_tbl.longitude, test.eoc_geolocation_tbl.x, test.eoc_geolocation_tbl.y, test.eoc_geolocation_tbl.imagepath, test.eoc_geolocation_tbl.isccpv, test.eoc_geolocation_tbl.rank, test.eoc_geolocation_tbl.rank_score, test.eoc_geolocation_tbl.ishealthcamp, test.eoc_geolocation_tbl.isdsc, test.eoc_geolocation_tbl.ucorg, test.eoc_geolocation_tbl.organization, test.eoc_geolocation_tbl.tierfromaug161, test.eoc_geolocation_tbl.tierfromsep171, test.eoc_geolocation_tbl.tierfromdec181, test.eoc_geolocation_tbl.mtap, test.eoc_geolocation_tbl.rspuc, test.eoc_geolocation_tbl.issmt, test.eoc_geolocation_tbl.updateddatetime, test.eoc_geolocation_tbl.x_code, test.eoc_geolocation_tbl.draining_uc, test.eoc_geolocation_tbl.upap_districts, test.eoc_geolocation_tbl.shruc, test.eoc_geolocation_tbl.khidist_id"
            sql = "SELECT " + cols + "  FROM test.get_catch_up eoc_1 left JOIN test.eoc_geolocation_tbl eoc_2 ON eoc_1.location_code  = eoc_2.code JOIN test.xbi_campaign eoc_3 ON (eoc_1.ActivityID  = eoc_3.campaign_ActivityID_old And eoc_1.Yr = eoc_3.campaign_Yr) WHERE eoc_1.ActivityID="+str(f)+" AND eoc_1.Yr="+str(b)

            data = client.execute(sql)
            apiDataFrame = pd.DataFrame(data)

            if apiDataFrame.empty:
                print("No Data found for campaign: "+str(f))
                print("API DF Size: ",len(apiDataFrame))
            else:
                all_columns = list(apiDataFrame)  # Creates list of all column headers
                cols = apiDataFrame.iloc[0]
                apiDataFrame[all_columns] = apiDataFrame[all_columns].astype(str)
                d = 'ID', 'UserName', 'IDcampCat', 'ActivityID', 'TimeStamp', 'Yr', 'TehsilID', 'UCID', 'DistID', 'DivID', 'ProvID', 'Cday', 'CCovNA011', 'CCovNA1259', 'CCovRef011', 'CCovRef1259', 'COutofH011', 'COutofH1259', 'CGuests', 'CCovMob', 'CNewBorn', 'PersistentlyMC', 'PersistentlyMCHRMP', 'CAlreadyVaccinated', 'CFixSite011', 'CFixSite1259', 'CTransit011', 'CTransit1259', 'UnRecorded_Cov', 'OPVGiven', 'OPVUsed', 'OPVReturned', 'AFP_CaseRpt', 'ZeroDos1', 'V_Age611_CP', 'v_Age1259_CP','fm_issued','fm_retrieved','missing_vacc_carr','missing_vacc_carr_reason', 'status', 'trash', 'isSync', 'Remarks', 'location_code', 'campaign_ID', 'campaign_ActivityName', 'campaign_ActivityID_old', 'campaign_Yr', 'campaign_SubActivityName','geoLocation_name', 'geoLocation_type', 'geoLocation_code', 'geoLocation_census_pop', 'geoLocation_target', 'geoLocation_status', 'geoLocation_pname', 'geoLocation_dname', 'geoLocation_namedistrict', 'geoLocation_codedistrict', 'geoLocation_tname', 'geoLocation_provincecode', 'geoLocation_districtcode', 'geoLocation_tehsilcode', 'geoLocation_priority', 'geoLocation_commnet', 'geoLocation_hr', 'geoLocation_fcm', 'geoLocation_tier', 'geoLocation_block', 'geoLocation_division', 'geoLocation_cordinates', 'geoLocation_latitude', 'geoLocation_longitude', 'geoLocation_x', 'geoLocation_y', 'geoLocation_imagepath', 'geoLocation_isccpv', 'geoLocation_rank', 'geoLocation_rank_score', 'geoLocation_ishealthcamp', 'geoLocation_isdsc', 'geoLocation_ucorg', 'geoLocation_organization', 'geoLocation_tierfromaug161', 'geoLocation_tierfromsep171', 'geoLocation_tierfromdec181', 'geoLocation_mtap', 'geoLocation_rspuc', 'geoLocation_issmt', 'geoLocation_updateddatetime', 'geoLocation_x_code', 'geoLocation_draining_uc', 'geoLocation_upap_districts', 'geoLocation_shruc', 'geoLocation_khidist_id'  
                dff = pd.DataFrame(columns=d)
                for index, item in enumerate(d):
                    dff[item] = apiDataFrame[index].values
                
                data_q = "SELECT * FROM test.xbi_catchUp WHERE Yr = "+str(b)+" AND ActivityID = "+str(f)
                df2 = client.query_dataframe(data_q)  
                #df2 = client.query_dataframe("SELECT * FROM test.xbi_catchUp WHERE Yr = 2022 and ActivityID = 2")
                if df2.empty:
                    client.insert_dataframe(
                        'INSERT INTO test.xbi_catchUp  VALUES', dff)
                    logger.info(
                        ' Data has been inserted into Table\' INSERT INTO test.xbi_catchUp  VALUES \' ')

                    # sql = "DROP table if exists test.get_catch_up"
                    # client.execute(sql)
                    print('\n\n\n Data Row Count\t\t--------\t', dff.shape[0])

                else:
                    sql = "ALTER TABLE test.xbi_catchUp DELETE WHERE Yr = "+ str(b) + " AND ActivityID = "+str(f)
                    client.execute(sql)

                    dff['location_code'] = pd.to_numeric(dff['location_code'], errors='coerce').fillna(0)
                    
                    client.insert_dataframe(
                        'INSERT INTO test.xbi_catchUp  VALUES', dff)
                    
                    print('\n\n\n DF Campaign/Year\t\t--------\t', dff.ActivityID.unique(),dff.Yr.unique())
                    print('\n\n\n Data Row Count\t\t--------\t', dff.shape[0])    
                    print('\n\n\n Data Col Count\t\t--------\t', dff.shape[1]) 
        sql = "DROP table if exists test.get_catch_up"
        client.execute(sql)


dag = DAG(
    'Prod_CatchUp_Automated',
    schedule_interval='0 0 * * *',  # once a day at midnight
    #schedule_interval='0 * * * *', # Run once an hour at the beginning of the hour 
    #schedule_interval='*/59 * * * *',  # will run every 60 min.
    default_args=default_args,
    catchup=False)

with dag:
    GetAndInsertApiDataCatchUp = PythonOperator(
        task_id='GetAndInsertApiDataCatchUp',
        python_callable=GetAndInsertApiDataCatchUp,
    )
    CreateJoinTableOfCatchUp = PythonOperator(
        task_id='CreateJoinTableOfCatchUp',
        python_callable=CreateJoinTableOfCatchUp,
    )
GetAndInsertApiDataCatchUp >> CreateJoinTableOfCatchUp

#-------------------------------------------------------------------------------------------------------------------------------#
#-------------------------------------------------- DATA INSERTION: CatchUp ----------------------------------------------------#
#-------------------------------------------- Author: Abdul Bari Malik ---------------------------------------------------------#
#-------------------------------------------------------------------------------------------------------------------------------#
# def GetAndInsertApiDataCatchUp():
#     li1 = [28, 29, 31, 30, 1, 106, 34, 9, 37, 40, 40, 43, 10, 2, 46, 11, 3, 107, 1, 106, 2, 9, 8, 10, 3, 107, 28, 31, 4, 9, 1, 28, 31, 2, 257, 258, 10 ]
#     li2 = [2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2022, 2022, 2022, 2022, 2022, 2022, 2022, 2022]
    
#     with sshtunnel.SSHTunnelForwarder(
#         ('172.16.3.68', 22),
#         ssh_username="root",
#         ssh_password="COV!D@19#",
#         remote_bind_address=('localhost', 9000)) as server:

#         local_port = server.local_bind_port
#         print(local_port)

#         conn = connect(f'clickhouse://default:mm@1234@localhost:{local_port}/test')
#         #conn = connect(host='172.16.3.68', database='test', user='default', password='mm@1234')
            
#         cursor = conn.cursor()
#         # cursor.execute('SHOW TABLES')
#         # print(cursor.fetchall())

#         client = Client(host='localhost',port=local_port, database='test',
#                                 user='default',
#                                 password='mm@1234',
#                                 settings={"use_numpy": True})     

#         for c, y in zip(li1, li2):
#             logger.info('Function \' GetAndInsertApiDataCatchUp \' Started Off')

#             url = "http://idims.eoc.gov.pk/api_who/api/get_allcatchup/5468XE2LN6CzR7qRG041/"+ str(c)+"/"+ str(y)
            
#             print('\n\n\n CID\t\t--------\t', c)
#             print('\n\n\n Year\t\t--------\t', y)
            
#             logger.info('Requested Data From Api URL: '+url)

#             r = requests.get(url)
#             data = r.json()
    
#             if data['data'] != "No data found":
#                 logger.info('Received Data  From Api URL: '+url)

#                 rowsData = data["data"]["data"]
#                 apiDataFrame = pd.DataFrame(rowsData)
#                 sql = " Create Table if not exists test.get_catch_up (ID Int32, UserName String, IDcampCat Int32, ActivityID Int32, TimeStamp String, Yr Int32, TehsilID Int32, UCID Int32, DistID Int32, DivID Int32, ProvID Int32, Cday Int32, CCovNA011 Int32, CCovNA1259 Int32, CCovRef011 Int32, CCovRef1259 Int32, COutofH011 Int32, COutofH1259 Int32, CGuests Int32, CCovMob Int32, CNewBorn Int32, PersistentlyMC Int32, PersistentlyMCHRMP Int32, CAlreadyVaccinated Int32, CFixSite011 Int32, CFixSite1259 Int32, CTransit011 Int32, CTransit1259 Int32, UnRecorded_Cov Int32, OPVGiven Int32, OPVUsed Int32, OPVReturned Int32, AFP_CaseRpt Int32, ZeroDos1 Int32, V_Age611_CP Int32, v_Age1259_CP Int32,fm_issued Int32,fm_retrieved Int32,missing_vacc_carr Int32,missing_vacc_carr_reason String, status Int32, trash Int32, isSync Int32, Remarks String, location_code Int32 NULL)ENGINE = MergeTree PRIMARY KEY ID ORDER BY ID"
#                 client.execute(sql)
#                 df2 = client.query_dataframe(
#                     "SELECT * FROM test.get_catch_up")
#                 if df2.empty:
#                     apiDataFrame = apiDataFrame.replace(r'^\s*$', np.nan, regex=True)
#                     client.insert_dataframe(
#                         'INSERT INTO test.get_catch_up VALUES', apiDataFrame)
#                 sql = "CREATE TABLE if not exists test.xbi_catchUp (ID Int32, UserName String, IDcampCat Int32, ActivityID Int32, TimeStamp String, Yr Int32, TehsilID Int32, UCID Int32, DistID Int32, DivID Int32, ProvID Int32, Cday Int32, CCovNA011 Int32, CCovNA1259 Int32, CCovRef011 Int32, CCovRef1259 Int32, COutofH011 Int32, COutofH1259 Int32, CGuests Int32, CCovMob Int32, CNewBorn Int32, PersistentlyMC Int32, PersistentlyMCHRMP Int32, CAlreadyVaccinated Int32, CFixSite011 Int32, CFixSite1259 Int32, CTransit011 Int32, CTransit1259 Int32, UnRecorded_Cov Int32, OPVGiven Int32, OPVUsed Int32, OPVReturned String, AFP_CaseRpt Int32, ZeroDos1 Int32, V_Age611_CP Int32, v_Age1259_CP Int32,fm_issued Int32,fm_retrieved Int32,missing_vacc_carr Int32,missing_vacc_carr_reason String, status Int32, trash Int32, isSync Int32, Remarks String, location_code Int32 NULL, campaign_ID Int32, campaign_ActivityName String, campaign_ActivityID_old Int32, campaign_Yr Int32, campaign_SubActivityName String, geoLocation_name String, geoLocation_type Int32, geoLocation_code Int32, geoLocation_census_pop Int32, geoLocation_target Int32, geoLocation_status Int32, geoLocation_pname String, geoLocation_dname String, geoLocation_namedistrict String, geoLocation_codedistrict String, geoLocation_tname String, geoLocation_provincecode Int32, geoLocation_districtcode Int32, geoLocation_tehsilcode Int32, geoLocation_priority Int32, geoLocation_commnet Int32, geoLocation_hr Int32, geoLocation_fcm Int8, geoLocation_tier Int32, geoLocation_block String, geoLocation_division String, geoLocation_cordinates String, geoLocation_latitude String, geoLocation_longitude String, geoLocation_x String, geoLocation_y String, geoLocation_imagepath String, geoLocation_isccpv Int32, geoLocation_rank Int32, geoLocation_rank_score Int32, geoLocation_ishealthcamp Int32, geoLocation_isdsc Int32, geoLocation_ucorg String, geoLocation_organization String, geoLocation_tierfromaug161 Int32, geoLocation_tierfromsep171 Int32, geoLocation_tierfromdec181 Int32, geoLocation_mtap Int32, geoLocation_rspuc Int32, geoLocation_issmt Int32, geoLocation_updateddatetime String, geoLocation_x_code Int32, geoLocation_draining_uc Int32, geoLocation_upap_districts Int32, geoLocation_shruc Int32, geoLocation_khidist_id String)ENGINE = MergeTree PRIMARY KEY ID ORDER BY ID"
#                 client.execute(sql)
#                 cols = "test.get_catch_up.ID, test.get_catch_up.UserName, test.get_catch_up.IDcampCat, test.get_catch_up.ActivityID, test.get_catch_up.TimeStamp, test.get_catch_up.Yr, test.get_catch_up.TehsilID, test.get_catch_up.UCID, test.get_catch_up.DistID, test.get_catch_up.DivID, test.get_catch_up.ProvID, test.get_catch_up.Cday, test.get_catch_up.CCovNA011, test.get_catch_up.CCovNA1259, test.get_catch_up.CCovRef011, test.get_catch_up.CCovRef1259, test.get_catch_up.COutofH011, test.get_catch_up.COutofH1259, test.get_catch_up.CGuests, test.get_catch_up.CCovMob, test.get_catch_up.CNewBorn, test.get_catch_up.PersistentlyMC, test.get_catch_up.PersistentlyMCHRMP, test.get_catch_up.CAlreadyVaccinated, test.get_catch_up.CFixSite011, test.get_catch_up.CFixSite1259, test.get_catch_up.CTransit011, test.get_catch_up.CTransit1259, test.get_catch_up.UnRecorded_Cov, test.get_catch_up.OPVGiven, test.get_catch_up.OPVUsed, test.get_catch_up.OPVReturned, test.get_catch_up.AFP_CaseRpt, test.get_catch_up.ZeroDos1, test.get_catch_up.V_Age611_CP, test.get_catch_up.v_Age1259_CP,test.get_catch_up.fm_issued,test.get_catch_up.fm_retrieved,test.get_catch_up.missing_vacc_carr,test.get_catch_up.missing_vacc_carr_reason, test.get_catch_up.status, test.get_catch_up.trash, test.get_catch_up.isSync, test.get_catch_up.Remarks, test.get_catch_up.location_code, test.xbi_campaign.campaign_ID, test.xbi_campaign.campaign_ActivityName, test.xbi_campaign.campaign_ActivityID_old, test.xbi_campaign.campaign_Yr, test.xbi_campaign.campaign_SubActivityName, test.eoc_geolocation_tbl.name, test.eoc_geolocation_tbl.type, test.eoc_geolocation_tbl.code, test.eoc_geolocation_tbl.census_pop, test.eoc_geolocation_tbl.target, test.eoc_geolocation_tbl.status, test.eoc_geolocation_tbl.pname, test.eoc_geolocation_tbl.dname, test.eoc_geolocation_tbl.namedistrict, test.eoc_geolocation_tbl.codedistrict, test.eoc_geolocation_tbl.tname, test.eoc_geolocation_tbl.provincecode, test.eoc_geolocation_tbl.districtcode, test.eoc_geolocation_tbl.tehsilcode, test.eoc_geolocation_tbl.priority, test.eoc_geolocation_tbl.commnet, test.eoc_geolocation_tbl.hr, test.eoc_geolocation_tbl.fcm, test.eoc_geolocation_tbl.tier, test.eoc_geolocation_tbl.block, test.eoc_geolocation_tbl.division, test.eoc_geolocation_tbl.cordinates, test.eoc_geolocation_tbl.latitude, test.eoc_geolocation_tbl.longitude, test.eoc_geolocation_tbl.x, test.eoc_geolocation_tbl.y, test.eoc_geolocation_tbl.imagepath, test.eoc_geolocation_tbl.isccpv, test.eoc_geolocation_tbl.rank, test.eoc_geolocation_tbl.rank_score, test.eoc_geolocation_tbl.ishealthcamp, test.eoc_geolocation_tbl.isdsc, test.eoc_geolocation_tbl.ucorg, test.eoc_geolocation_tbl.organization, test.eoc_geolocation_tbl.tierfromaug161, test.eoc_geolocation_tbl.tierfromsep171, test.eoc_geolocation_tbl.tierfromdec181, test.eoc_geolocation_tbl.mtap, test.eoc_geolocation_tbl.rspuc, test.eoc_geolocation_tbl.issmt, test.eoc_geolocation_tbl.updateddatetime, test.eoc_geolocation_tbl.x_code, test.eoc_geolocation_tbl.draining_uc, test.eoc_geolocation_tbl.upap_districts, test.eoc_geolocation_tbl.shruc, test.eoc_geolocation_tbl.khidist_id"
#                 sql = "SELECT " + cols + "  FROM test.get_catch_up eoc_1 left JOIN test.eoc_geolocation_tbl eoc_2 ON eoc_1.location_code  = eoc_2.code JOIN test.xbi_campaign eoc_3 ON (eoc_1.ActivityID  = eoc_3.campaign_ActivityID_old And eoc_1.Yr = eoc_3.campaign_Yr) "

#                 data = client.execute(sql)
#                 apiDataFrame = pd.DataFrame(data)
#                 apiDataFrame = apiDataFrame.replace(r'^\s*$', np.nan, regex=True)
#                 #apiDataFrame['location_code'] = pd.to_numeric(apiDataFrame['location_code'], errors='coerce').fillna(0)
#                 all_columns = list(apiDataFrame)  # Create list of all column headers
#                 cols = apiDataFrame.iloc[0]
#                 apiDataFrame[all_columns] = apiDataFrame[all_columns].astype(str)
#                 d = 'ID', 'UserName', 'IDcampCat', 'ActivityID', 'TimeStamp', 'Yr', 'TehsilID', 'UCID', 'DistID', 'DivID', 'ProvID', 'Cday', 'CCovNA011', 'CCovNA1259', 'CCovRef011', 'CCovRef1259', 'COutofH011', 'COutofH1259', 'CGuests', 'CCovMob', 'CNewBorn', 'PersistentlyMC', 'PersistentlyMCHRMP', 'CAlreadyVaccinated', 'CFixSite011', 'CFixSite1259', 'CTransit011', 'CTransit1259', 'UnRecorded_Cov', 'OPVGiven', 'OPVUsed', 'OPVReturned', 'AFP_CaseRpt', 'ZeroDos1', 'V_Age611_CP', 'v_Age1259_CP','fm_issued','fm_retrieved','missing_vacc_carr','missing_vacc_carr_reason','status', 'trash', 'isSync', 'Remarks', 'location_code', 'campaign_ID', 'campaign_ActivityName', 'campaign_ActivityID_old', 'campaign_Yr', 'campaign_SubActivityName','geoLocation_name', 'geoLocation_type', 'geoLocation_code', 'geoLocation_census_pop', 'geoLocation_target', 'geoLocation_status', 'geoLocation_pname', 'geoLocation_dname', 'geoLocation_namedistrict', 'geoLocation_codedistrict', 'geoLocation_tname', 'geoLocation_provincecode', 'geoLocation_districtcode', 'geoLocation_tehsilcode', 'geoLocation_priority', 'geoLocation_commnet', 'geoLocation_hr', 'geoLocation_fcm', 'geoLocation_tier', 'geoLocation_block', 'geoLocation_division', 'geoLocation_cordinates', 'geoLocation_latitude', 'geoLocation_longitude', 'geoLocation_x', 'geoLocation_y', 'geoLocation_imagepath', 'geoLocation_isccpv', 'geoLocation_rank', 'geoLocation_rank_score', 'geoLocation_ishealthcamp', 'geoLocation_isdsc', 'geoLocation_ucorg', 'geoLocation_organization', 'geoLocation_tierfromaug161', 'geoLocation_tierfromsep171', 'geoLocation_tierfromdec181', 'geoLocation_mtap', 'geoLocation_rspuc', 'geoLocation_issmt', 'geoLocation_updateddatetime', 'geoLocation_x_code', 'geoLocation_draining_uc', 'geoLocation_upap_districts', 'geoLocation_shruc', 'geoLocation_khidist_id'  
#                 dff = pd.DataFrame(columns=d)
#                 for index, item in enumerate(d):
#                     dff[item] = apiDataFrame[index].values
#                 df2 = client.query_dataframe(
#                     "SELECT * FROM test.xbi_catchUp")
#                 if df2.empty:
#                     client.insert_dataframe(
#                         'INSERT INTO test.xbi_catchUp  VALUES', dff)
#                     logger.info(
#                         'Data has been inserted into Table\' INSERT INTO test.xbi_catchUp  VALUES \' ')

#                     sql = "DROP table if exists test.get_catch_up"
#                     client.execute(sql)
#                     print('\n\n\n CID\t\t--------\t', c)
#                     print('\n\n\n Year\t\t--------\t', y)
#                 else:
#                     dff = dff.replace(r'^\s*$', np.nan, regex=True)
#                     dff['location_code'] = pd.to_numeric(dff['location_code'], errors='coerce').fillna(0)
#                     client.insert_dataframe(
#                         'INSERT INTO test.xbi_catchUp  VALUES', dff)
#                     print('\n\n\n CID\t\t--------\t', c)
#                     print('\n\n\n Year\t\t--------\t', y)
#                     sql = "DROP table if exists test.get_catch_up"
#                     client.execute(sql)

#                 # apiDataFrame = apiDataFrame.drop_duplicates(subset='ID', keep="first", inplace=False)
            



# dag = DAG(
#     'Prod_CatchUp_Automated',
#     schedule_interval='0 0 * * *',  # once a day at midnight
#     #schedule_interval='*/59 * * * *',  # will run after an hour[Every 60 minute].
#     default_args=default_args,
#     catchup=False)

# with dag:
#     GetAndInsertApiDataCatchUp = PythonOperator(
#         task_id='GetAndInsertApiDataCatchUp',
#         python_callable=GetAndInsertApiDataCatchUp,
#     )

# GetAndInsertApiDataCatchUp 

