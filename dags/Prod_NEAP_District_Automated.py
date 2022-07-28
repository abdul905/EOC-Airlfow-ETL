from airflow import DAG
import pandas as pd
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import requests
import numpy as np
from clickhouse_driver import connect
from clickhouse_driver import Client
import logging
import db_connection as dbConn
import sshtunnel as sshtunnel

default_args = {'owner': 'airflow',
                'start_date': datetime(2021, 1, 1)
                }
logger = logging.getLogger(__name__)
c_handler = logging.StreamHandler()
f_handler = logging.FileHandler('logger-file_name')
c_handler.setLevel(logging.WARNING)
c_handler.setLevel(logging.INFO)
c_format = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
f_format = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
c_handler.setFormatter(c_format)
f_handler.setFormatter(f_format)
logger.addHandler(c_handler)
logger.addHandler(f_handler)

#-------------------------------------------------------------------------------------------------------------------------------#
#----------------------------------------------- INSERT CAMPAIGN DATA District NEAP --------------------------------------------#
#-------------------------------------------- Author: Abdul Bari Malik ---------------------------------------------------------#
#-------------------------------------------------------------------------------------------------------------------------------#
def GetAndInsertApiDataDistrictNeap():
    logger.info('Function \' GetAndInsertApiDataDistrictNeap \' Started Off')

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

        for c, y in zip(campIDold_list, campYr_list):
            url = "http://idims.eoc.gov.pk/api_who/api/get_allplaningDist/5468XE2LN6CzR7qRG041/"+ str(c)+"/"+ str(y)
            logger.info('Requested Data From Api URL: '+url)

            r = requests.get(url)
            data = r.json()

            if data['data'] == "No data found":
                print("Not Data found for campaign: "+url)    
            else:
                print("Data found for campaign: "+url)
                logger.info('Received Data  From Api URL: '+url)

                rowsData = data["data"]["data"]
                apiDataFrame = pd.DataFrame(rowsData)
                sql = "CREATE TABLE if not exists test.get_district_neap (ID Int32,UserName String,ActivityID Int32,TimeStamp Date,Yr Int32,prov_id Int32,div_id Int32,dist_id Int32,day1 Date,dpec Date,dco Int32,edo Int32,dpo_dpec Int32,allMember Int32,rmDate Date,rmDC Int32,rmDHO Int32,rmSecurity Int32,rmAllMembers Int32,districtReady Int32,resheduleDate Date,actionLPUC Int32,actionType Int32,reviewSIA Int32,inaugrated_by String,OPVVilesReceived Int32,VaccArival Date,TeleSheets Int32,FingerMarker_before_sia Int32,FingerMarker Int32,SmMatrialReceived Date,avbl_vacc_carrier Int32,Remarks String,mov_dpec Int32,mov_redines Int32,status Int32,trash Int32,isSync Int32,VaccineType String)ENGINE = MergeTree PRIMARY KEY ID ORDER BY ID"
                client.execute(sql)

                df2 = client.query_dataframe("SELECT * FROM test.get_district_neap")
                    
                if df2.empty:
                    apiDataFrame['TimeStamp'] = pd.to_datetime(apiDataFrame['TimeStamp'], errors='coerce')
                    apiDataFrame['day1'] = pd.to_datetime(apiDataFrame['day1'], errors='coerce')
                    apiDataFrame['dpec'] = pd.to_datetime(apiDataFrame['dpec'], errors='coerce')
                    apiDataFrame['rmDate'] = pd.to_datetime(apiDataFrame['rmDate'], errors='coerce')
                    apiDataFrame['resheduleDate'] = pd.to_datetime(apiDataFrame['resheduleDate'], errors='coerce')
                    apiDataFrame['VaccArival'] = pd.to_datetime(apiDataFrame['VaccArival'], errors='coerce')
                    apiDataFrame['SmMatrialReceived'] = pd.to_datetime(apiDataFrame['SmMatrialReceived'], errors='coerce')
                    apiDataFrame = apiDataFrame.replace(r'^\s*$', np.nan, regex=True)
                    print('cid --------------------------->\t', c)
                    print('year--------------------------->\t', y)
                    client.insert_dataframe(
                        'INSERT INTO test.get_district_neap VALUES', apiDataFrame)
                else:
                    apiDataFrame['TimeStamp'] = pd.to_datetime(apiDataFrame['TimeStamp'], errors='coerce')
                    apiDataFrame['day1'] = pd.to_datetime(apiDataFrame['day1'], errors='coerce')
                    apiDataFrame['dpec'] = pd.to_datetime(apiDataFrame['dpec'], errors='coerce')
                    apiDataFrame['rmDate'] = pd.to_datetime(apiDataFrame['rmDate'], errors='coerce')
                    apiDataFrame['resheduleDate'] = pd.to_datetime(apiDataFrame['resheduleDate'], errors='coerce')
                    apiDataFrame['VaccArival'] = pd.to_datetime(apiDataFrame['VaccArival'], errors='coerce')
                    apiDataFrame['SmMatrialReceived'] = pd.to_datetime(apiDataFrame['SmMatrialReceived'], errors='coerce')
                    apiDataFrame = apiDataFrame.replace(r'^\s*$', np.nan, regex=True)
                    print('cid --------------------------->\t', c)
                    print('year--------------------------->\t', y)
                    client.insert_dataframe(
                        'INSERT INTO test.get_district_neap VALUES', apiDataFrame)
                                

def CreateJoinTableOfDistrictNEAP():
    
    logger.info('Function \' CreateJoinTableOfDistrictNEAP \' Started Off')

    with sshtunnel.SSHTunnelForwarder(
        ('172.16.3.68', 22),
        ssh_username="root",
        ssh_password="COV!D@19#",
        remote_bind_address=('localhost', 9000)) as server:

        local_port = server.local_bind_port
        print(local_port)
        #connection = 'clickhouse://{user}:{password}@{server_host}:{port}/{db}'.format(**conf)    
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
            sql = "CREATE TABLE if not exists test.xbi_neapDistrictPlan (ID Int32,UserName String,ActivityID Int32,TimeStamp Date,Yr Int32,prov_id Int32,div_id Int32,dist_id Int32,day1 Date,dpec Date,dco Int32,edo Int32,dpo_dpec Int32,allMember Int32,rmDate Date,rmDC Int32,rmDHO Int32,rmSecurity Int32,rmAllMembers Int32,districtReady Int32,resheduleDate Date,actionLPUC Int32,actionType Int32,reviewSIA Int32,inaugrated_by String,OPVVilesReceived Int32,VaccArival Date,TeleSheets Int32,FingerMarker_before_sia Int32,FingerMarker Int32,SmMatrialReceived Date,avbl_vacc_carrier Int32,Remarks String,mov_dpec Int32,mov_redines Int32,status Int32,trash Int32,isSync Int32,VaccineType String, campaign_ID Int32, campaign_ActivityName String, campaign_ActivityID_old Int32, campaign_Yr Int32, campaign_SubActivityName String, location_id Int32, location_code Int32, location_name String, location_type String, location_status Int32)ENGINE = MergeTree PRIMARY KEY ID ORDER BY ID"
            client.execute(sql)
            cols = "test.get_district_neap.ID,test.get_district_neap.UserName,test.get_district_neap.ActivityID,test.get_district_neap.TimeStamp,test.get_district_neap.Yr,test.get_district_neap.prov_id,test.get_district_neap.div_id,test.get_district_neap.dist_id,test.get_district_neap.day1,test.get_district_neap.dpec,test.get_district_neap.dco,test.get_district_neap.edo,test.get_district_neap.dpo_dpec,test.get_district_neap.allMember,test.get_district_neap.rmDate,test.get_district_neap.rmDC,test.get_district_neap.rmDHO,test.get_district_neap.rmSecurity,test.get_district_neap.rmAllMembers,test.get_district_neap.districtReady,test.get_district_neap.resheduleDate,test.get_district_neap.actionLPUC,test.get_district_neap.actionType,test.get_district_neap.reviewSIA,test.get_district_neap.inaugrated_by,test.get_district_neap.OPVVilesReceived,test.get_district_neap.VaccArival,test.get_district_neap.TeleSheets,test.get_district_neap.FingerMarker_before_sia,test.get_district_neap.FingerMarker,test.get_district_neap.SmMatrialReceived,test.get_district_neap.avbl_vacc_carrier,test.get_district_neap.Remarks,test.get_district_neap.mov_dpec,test.get_district_neap.mov_redines,test.get_district_neap.status,test.get_district_neap.trash,test.get_district_neap.isSync,test.get_district_neap.VaccineType,test.xbi_campaign.campaign_ID,test.xbi_campaign.campaign_ActivityName, test.xbi_campaign.campaign_ActivityID_old, test.xbi_campaign.campaign_Yr, test.xbi_campaign.campaign_SubActivityName, test.xbi_geolocation.ID, test.xbi_geolocation.code, test.xbi_geolocation.name, test.xbi_geolocation.type, test.xbi_geolocation.location_status"
            sql = "SELECT " + cols + " FROM test.get_district_neap tsm left JOIN test.xbi_geolocation gl1 ON (tsm.dist_id = gl1.ID) left JOIN test.xbi_campaign eoc_3 ON (tsm.ActivityID  = eoc_3.campaign_ActivityID_old And tsm.Yr = eoc_3.campaign_Yr) WHERE LENGTH(CAST(gl1.code AS VARCHAR(10)))  = '3' and gl1.type = 'District' AND tsm.ActivityID="+str(f)+" AND tsm.Yr="+str(b)
            data = client.execute(sql)
                    
            apiDataFrame = pd.DataFrame(data)

            if apiDataFrame.empty:
                print("No Data found for campaign: "+str(f))
                print("API DF Size: ",len(apiDataFrame))
            else:
                all_columns = list(apiDataFrame)  # Creates list of all column headers
                cols = apiDataFrame.iloc[0]
                apiDataFrame[all_columns] = apiDataFrame[all_columns].astype(str)
                        
                d =  'ID','UserName','ActivityID','TimeStamp','Yr','prov_id','div_id','dist_id','day1','dpec','dco','edo','dpo_dpec','allMember','rmDate','rmDC','rmDHO','rmSecurity','rmAllMembers','districtReady','resheduleDate','actionLPUC','actionType','reviewSIA','inaugrated_by','OPVVilesReceived','VaccArival','TeleSheets','FingerMarker_before_sia','FingerMarker','SmMatrialReceived','avbl_vacc_carrier','Remarks','mov_dpec','mov_redines','status','trash','isSync','VaccineType','campaign_ID', 'campaign_ActivityName', 'campaign_ActivityID_old', 'campaign_Yr', 'campaign_SubActivityName','location_id', 'location_code', 'location_name','location_type','location_status'
                
                dff = pd.DataFrame(columns=d)
                for index, item in enumerate(d):
                    dff[item] = apiDataFrame[index].values
                
                data_q = "SELECT * FROM test.xbi_neapDistrictPlan WHERE Yr = "+str(b)+" AND ActivityID = "+str(f)
                df2 = client.query_dataframe(data_q)  
                print("API DF Size: ",len(df2))     
                # df2 = client.query_dataframe(
                #     "SELECT * FROM  test.xbi_neapDistrictPlan WHERE Yr = 2022 and ActivityID = 2")
                        
                if df2.empty:
                    dff[["TimeStamp"]] = dff[["TimeStamp"]].apply(pd.to_datetime)
                    dff[["day1"]] = dff[["day1"]].apply(pd.to_datetime)
                    dff[["dpec"]] = dff[["dpec"]].apply(pd.to_datetime)
                    dff[["rmDate"]] = dff[["rmDate"]].apply(pd.to_datetime)
                    dff[["resheduleDate"]] = dff[["resheduleDate"]].apply(pd.to_datetime)
                    dff[["VaccArival"]] = dff[["VaccArival"]].apply(pd.to_datetime)
                    dff[["SmMatrialReceived"]] = dff[["SmMatrialReceived"]].apply(pd.to_datetime)

                    client.insert_dataframe(
                        'INSERT INTO  test.xbi_neapDistrictPlan  VALUES', dff)
                    logger.info(
                        ' Data has been inserted into Table\' INSERT INTO  test.xbi_neapDistrictPlan  VALUES \' ')

                    # sql = "DROP table if exists  test.get_district_neap"
                    # client.execute(sql)

                else:
                    sql = "ALTER TABLE test.xbi_neapDistrictPlan DELETE WHERE Yr = "+ str(b) + " AND ActivityID = "+str(f)
                    client.execute(sql)

                    dff[["TimeStamp"]] = dff[["TimeStamp"]].apply(pd.to_datetime)
                    dff[["day1"]] = dff[["day1"]].apply(pd.to_datetime)
                    dff[["dpec"]] = dff[["dpec"]].apply(pd.to_datetime)
                    dff[["rmDate"]] = dff[["rmDate"]].apply(pd.to_datetime)
                    dff[["resheduleDate"]] = dff[["resheduleDate"]].apply(pd.to_datetime)
                    dff[["VaccArival"]] = dff[["VaccArival"]].apply(pd.to_datetime)
                    dff[["SmMatrialReceived"]] = dff[["SmMatrialReceived"]].apply(pd.to_datetime)

                    client.insert_dataframe(
                        'INSERT INTO  test.xbi_neapDistrictPlan  VALUES', dff)
                    logger.info(
                        ' Data has been inserted into Table\' INSERT INTO  test.xbi_neapDistrictPlan  VALUES \' ')    
                    
        sql = "DROP table if exists  test.get_district_neap"
        client.execute(sql)
            
dag = DAG(
    'Prod_Neap_District_Automated',
    schedule_interval='0 0 * * *',  # will run every mid-night.
    default_args=default_args,
    catchup=False)

with dag:
    GetAndInsertApiDataDistrictNeap = PythonOperator(
        task_id='GetAndInsertApiDataDistrictNeap',
        python_callable=GetAndInsertApiDataDistrictNeap,
    )
    CreateJoinTableOfDistrictNEAP = PythonOperator(
        task_id='CreateJoinTableOfDistrictNEAP',
        python_callable=CreateJoinTableOfDistrictNEAP,
    )
GetAndInsertApiDataDistrictNeap >> CreateJoinTableOfDistrictNEAP








#-------------------------------------------------------------------------------------------------------------------------------#
#----------------------------------------------- INSERT CAMPAIGN DATA District NEAP --------------------------------------------#
#-------------------------------------------- Author: Abdul Bari Malik ---------------------------------------------------------#
#-------------------------------------------------------------------------------------------------------------------------------#

# Campaign ID OLD : 46 | Yr = 2020  --> Data Insertion Failed 
# def GetAndInsertApiDataDistrictNeap():
#     li1 = [28, 29, 31, 30, 1, 106, 34, 9, 37, 40, 43, 10, 2,  11, 3, 107, 1, 106, 2, 9, 8, 10, 3, 107, 28, 31, 4, 9, 1, 28, 31, 2, 257, 257, 10]
#     li2 = [2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2022, 2022, 2022, 2022, 2022, 2022, 2022, 2022]

#     logger.info('Function \' GetAndInsertApiDataDistrictNeap \' Started Off')
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
#         df = pd.DataFrame()
#         for c, y in zip(li1, li2):
#             url = "http://idims.eoc.gov.pk/api_who/api/get_allplaningDist/5468XE2LN6CzR7qRG041/"+ str(c)+"/"+ str(y)
#             logger.info('Requested Data From Api URL: '+url)

#             r = requests.get(url)
#             data = r.json()
#             if data['data'] == "No data found":
#                 print("Not Data found for campaign: "+url)
#             else:
#                 print('\nCampaign \t\t--------\t',c,'--',y,'\t-------- Data Found \n')
#                 print("Data found for campaign: "+url)
#                 logger.info('Received Data  From Api URL: '+url)
#                 rowsData = data["data"]["data"]
#                 apiDataFrame = pd.DataFrame(rowsData)
#                 print("DF API Size",len(apiDataFrame))
#                 df = df.append(apiDataFrame, ignore_index=True)
#                 print("Merge DF Size",len(df))
               
#                 sql = "CREATE TABLE if not exists test.get_district_neap (ID Int32,UserName String,ActivityID Int32,TimeStamp Date,Yr Int32,prov_id Int32,div_id Int32,dist_id Int32,day1 Date,dpec Date,dco Int32,edo Int32,dpo_dpec Int32,allMember Int32,rmDate Date,rmDC Int32,rmDHO Int32,rmSecurity Int32,rmAllMembers Int32,districtReady Int32,resheduleDate Date,actionLPUC Int32,actionType Int32,reviewSIA Int32,inaugrated_by String,OPVVilesReceived Int32,VaccArival Date,TeleSheets Int32,FingerMarker_before_sia Int32,FingerMarker Int32,SmMatrialReceived Date,avbl_vacc_carrier Int32,Remarks String,mov_dpec Int32,mov_redines Int32,status Int32,trash Int32,isSync Int32,VaccineType String)ENGINE = MergeTree PRIMARY KEY ID ORDER BY ID"
#                 client.execute(sql)

#                 df2 = client.query_dataframe(
#                     "SELECT * FROM test.get_district_neap")
#                 if df2.empty:
#                     apiDataFrame['TimeStamp'] = pd.to_datetime(apiDataFrame['TimeStamp'], errors='coerce')
#                     apiDataFrame['day1'] = pd.to_datetime(apiDataFrame['day1'], errors='coerce')
#                     apiDataFrame['dpec'] = pd.to_datetime(apiDataFrame['dpec'], errors='coerce')
#                     apiDataFrame['rmDate'] = pd.to_datetime(apiDataFrame['rmDate'], errors='coerce')
#                     apiDataFrame['resheduleDate'] = pd.to_datetime(apiDataFrame['resheduleDate'], errors='coerce')
#                     apiDataFrame['VaccArival'] = pd.to_datetime(apiDataFrame['VaccArival'], errors='coerce')
#                     apiDataFrame['SmMatrialReceived'] = pd.to_datetime(apiDataFrame['SmMatrialReceived'], errors='coerce')
#                     apiDataFrame = apiDataFrame.replace(r'^\s*$', np.nan, regex=True)
#                     print('cid --------------------------->\t', c)
#                     print('year--------------------------->\t', y)
#                     client.insert_dataframe(
#                         'INSERT INTO test.get_district_neap VALUES', apiDataFrame)


#                 sql = "CREATE TABLE if not exists test.xbi_neapDistrictPlan (ID Int32,UserName String,ActivityID Int32,TimeStamp Date,Yr Int32,prov_id Int32,div_id Int32,dist_id Int32,day1 Date,dpec Date,dco Int32,edo Int32,dpo_dpec Int32,allMember Int32,rmDate Date,rmDC Int32,rmDHO Int32,rmSecurity Int32,rmAllMembers Int32,districtReady Int32,resheduleDate Date,actionLPUC Int32,actionType Int32,reviewSIA Int32,inaugrated_by String,OPVVilesReceived Int32,VaccArival Date,TeleSheets Int32,FingerMarker_before_sia Int32,FingerMarker Int32,SmMatrialReceived Date,avbl_vacc_carrier Int32,Remarks String,mov_dpec Int32,mov_redines Int32,status Int32,trash Int32,isSync Int32,VaccineType String, campaign_ID Int32, campaign_ActivityName String, campaign_ActivityID_old Int32, campaign_Yr Int32, campaign_SubActivityName String, location_id Int32, location_code Int32, location_name String, location_type String, location_status Int32 )ENGINE = MergeTree PRIMARY KEY ID ORDER BY ID"
#                 client.execute(sql)
#                 cols = "test.get_district_neap.ID,test.get_district_neap.UserName,test.get_district_neap.ActivityID,test.get_district_neap.TimeStamp,test.get_district_neap.Yr,test.get_district_neap.prov_id,test.get_district_neap.div_id,test.get_district_neap.dist_id,test.get_district_neap.day1,test.get_district_neap.dpec,test.get_district_neap.dco,test.get_district_neap.edo,test.get_district_neap.dpo_dpec,test.get_district_neap.allMember,test.get_district_neap.rmDate,test.get_district_neap.rmDC,test.get_district_neap.rmDHO,test.get_district_neap.rmSecurity,test.get_district_neap.rmAllMembers,test.get_district_neap.districtReady,test.get_district_neap.resheduleDate,test.get_district_neap.actionLPUC,test.get_district_neap.actionType,test.get_district_neap.reviewSIA,test.get_district_neap.inaugrated_by,test.get_district_neap.OPVVilesReceived,test.get_district_neap.VaccArival,test.get_district_neap.TeleSheets,test.get_district_neap.FingerMarker_before_sia,test.get_district_neap.FingerMarker,test.get_district_neap.SmMatrialReceived,test.get_district_neap.avbl_vacc_carrier,test.get_district_neap.Remarks,test.get_district_neap.mov_dpec,test.get_district_neap.mov_redines,test.get_district_neap.status,test.get_district_neap.trash,test.get_district_neap.isSync,test.get_district_neap.VaccineType,test.xbi_campaign.campaign_ID,test.xbi_campaign.campaign_ActivityName, test.xbi_campaign.campaign_ActivityID_old, test.xbi_campaign.campaign_Yr, test.xbi_campaign.campaign_SubActivityName, test.xbi_geolocation.ID, test.xbi_geolocation.code, test.xbi_geolocation.name, test.xbi_geolocation.type, test.xbi_geolocation.location_status"
#                 sql = "SELECT " + cols + " FROM test.get_district_neap tsm left JOIN test.xbi_geolocation gl1 ON (tsm.dist_id = gl1.ID) left JOIN test.xbi_campaign eoc_3 ON (tsm.ActivityID  = eoc_3.campaign_ActivityID_old And tsm.Yr = eoc_3.campaign_Yr) WHERE LENGTH(CAST(gl1.code AS VARCHAR(10)))  = '3' and gl1.type = 'District' "
#                 data = client.execute(sql)
                
#                 apiDataFrame = pd.DataFrame(data)
#                 apiDataFrame = apiDataFrame.replace(r'^\s*$', np.nan, regex=True)
#                 all_columns = list(apiDataFrame)  # Creates list of all column headers
#                 cols = apiDataFrame.iloc[0]
#                 apiDataFrame[all_columns] = apiDataFrame[all_columns].astype(str)
                
#                 d =  'ID','UserName','ActivityID','TimeStamp','Yr','prov_id','div_id','dist_id','day1','dpec','dco','edo','dpo_dpec','allMember','rmDate','rmDC','rmDHO','rmSecurity','rmAllMembers','districtReady','resheduleDate','actionLPUC','actionType','reviewSIA','inaugrated_by','OPVVilesReceived','VaccArival','TeleSheets','FingerMarker_before_sia','FingerMarker','SmMatrialReceived','avbl_vacc_carrier','Remarks','mov_dpec','mov_redines','status','trash','isSync','VaccineType','campaign_ID', 'campaign_ActivityName', 'campaign_ActivityID_old', 'campaign_Yr', 'campaign_SubActivityName','location_id', 'location_code', 'location_name','location_type','location_status'
#                 dff = pd.DataFrame(columns=d)
#                 for index, item in enumerate(d):
#                     dff[item] = apiDataFrame[index].values
#                 df2 = client.query_dataframe(
#                     "SELECT * FROM  test.xbi_neapDistrictPlan")
                
#                 if df2.empty:
#                     dff[["TimeStamp"]] = dff[["TimeStamp"]].apply(pd.to_datetime)
#                     dff[["day1"]] = dff[["day1"]].apply(pd.to_datetime)
#                     dff[["dpec"]] = dff[["dpec"]].apply(pd.to_datetime)
#                     dff[["rmDate"]] = dff[["rmDate"]].apply(pd.to_datetime)
#                     dff[["resheduleDate"]] = dff[["resheduleDate"]].apply(pd.to_datetime)
#                     dff[["VaccArival"]] = dff[["VaccArival"]].apply(pd.to_datetime)
#                     dff[["SmMatrialReceived"]] = dff[["SmMatrialReceived"]].apply(pd.to_datetime)
                    
#                     client.insert_dataframe(
#                         'INSERT INTO  test.xbi_neapDistrictPlan  VALUES', dff)
#                     print('\nCampaign \t\t--------\t',c,'--',y,'\t-------- Data Inserted \n')    
#                     logger.info(
#                         ' Data has been inserted into Table\' INSERT INTO  test.xbi_neapDistrictPlan  VALUES \' ')
#                     print('cid --------------------------->\t', c)
#                     print('year--------------------------->\t', y)
#                     sql = "DROP table if exists  test.get_district_neap"
#                     client.execute(sql)
#                 else:
#                     dff[["TimeStamp"]] = dff[["TimeStamp"]].apply(pd.to_datetime)
#                     dff[["day1"]] = dff[["day1"]].apply(pd.to_datetime)
#                     dff[["dpec"]] = dff[["dpec"]].apply(pd.to_datetime)
#                     dff[["rmDate"]] = dff[["rmDate"]].apply(pd.to_datetime)
#                     dff[["resheduleDate"]] = dff[["resheduleDate"]].apply(pd.to_datetime)
#                     dff[["VaccArival"]] = dff[["VaccArival"]].apply(pd.to_datetime)
#                     dff[["SmMatrialReceived"]] = dff[["SmMatrialReceived"]].apply(pd.to_datetime)
                    
#                     client.insert_dataframe(
#                         'INSERT INTO  test.xbi_neapDistrictPlan  VALUES', dff)
#                     print('\nCampaign \t\t--------\t',c,'--',y,'\t-------- Data Inserted \n')    
#                     logger.info(
#                         ' Data has been inserted into Table\' INSERT INTO  test.xbi_neapDistrictPlan  VALUES \' ')    
#                     print('cid --------------------------->\t', c)
#                     print('year--------------------------->\t', y)
#                     sql = "DROP table if exists  test.get_district_neap"
#                     client.execute(sql)


# dag = DAG(
#     'Prod_Neap_District_Automated',
#     schedule_interval='0 0 * * *',  # will run every mid-night.
#     default_args=default_args,
#     catchup=False)

# with dag:
#     GetAndInsertApiDataDistrictNeap = PythonOperator(
#         task_id='GetAndInsertApiDataDistrictNeap',
#         python_callable=GetAndInsertApiDataDistrictNeap,
#     )
# GetAndInsertApiDataDistrictNeap
#-------------------------------------------------------------------#
# def GetAndInsertApiDataDistrictNeap():
#     # li1 = [28, 29, 31, 30, 1, 106, 34, 9, 37, 40,  43, 10, 2, 46, 11, 3, 107, 1, 106, 2, 9, 8, 10, 3, 107, 28, 31, 4, 9, 1, 28, 31, 2]
#     # li2 = [2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2022, 2022, 2022, 2022, 2022]

#     li1 = [46, 11, 3, 107, 1, 106, 2, 9, 8, 10, 3, 107, 28, 31, 4, 9, 1, 28, 31, 2]
#     li2 = [2020, 2020, 2020, 2020, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2022, 2022, 2022, 2022, 2022]
#         #, 2020, 2020, 2020, 2020, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2022, 2022, 2022, 2022, 2022]


#     logger.info('Function \' GetAndInsertApiDataDistrictNeap \' Started Off')
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
#             url = "http://idims.eoc.gov.pk/api_who/api/get_allplaningDist/5468XE2LN6CzR7qRG041/"+ str(c)+"/"+ str(y)
#             logger.info('Requested Data From Api URL: '+url)

#             r = requests.get(url)
#             data = r.json()
#             if data['data'] != "No data found":
#                 logger.info('Received Data  From Api URL: '+url)

#                 rowsData = data["data"]["data"]
#                 apiDataFrame = pd.DataFrame(rowsData)
#                 sql = "CREATE TABLE if not exists test.get_district_neap (ID Int32,UserName String,ActivityID Int32,TimeStamp Date,Yr Int32,prov_id Int32,div_id Int32,dist_id Int32,day1 Date,dpec Date,dco Int32,edo Int32,dpo_dpec Int32,allMember Int32,rmDate Date,rmDC Int32,rmDHO Int32,rmSecurity Int32,rmAllMembers Int32,districtReady Int32,resheduleDate Date,actionLPUC Int32,actionType Int32,reviewSIA Int32,inaugrated_by String,OPVVilesReceived Int32,VaccArival Date,TeleSheets Int32,FingerMarker_before_sia Int32,FingerMarker Int32,SmMatrialReceived Date,avbl_vacc_carrier Int32,Remarks String,mov_dpec Int32,mov_redines Int32,status Int32,trash Int32,isSync Int32,VaccineType String)ENGINE = MergeTree PRIMARY KEY ID ORDER BY ID"
#                 client.execute(sql)

#                 df2 = client.query_dataframe(
#                     "SELECT * FROM test.get_district_neap")
#                 if df2.empty:
#                     apiDataFrame = apiDataFrame.replace(r'^\s*$', np.nan, regex=True)
#                     apiDataFrame['TimeStamp'] = pd.to_datetime(apiDataFrame['TimeStamp'], errors='coerce')
#                     apiDataFrame['day1'] = pd.to_datetime(apiDataFrame['day1'], errors='coerce')
#                     apiDataFrame['dpec'] = pd.to_datetime(apiDataFrame['dpec'], errors='coerce')
#                     apiDataFrame['rmDate'] = pd.to_datetime(apiDataFrame['rmDate'], errors='coerce')
#                     apiDataFrame['resheduleDate'] = pd.to_datetime(apiDataFrame['resheduleDate'], errors='coerce')
#                     apiDataFrame['VaccArival'] = pd.to_datetime(apiDataFrame['VaccArival'], errors='coerce')
#                     apiDataFrame['SmMatrialReceived'] = pd.to_datetime(apiDataFrame['SmMatrialReceived'], errors='coerce')

#                     print('cid --------------------------->\t', c)
#                     print('year--------------------------->\t', y)
#                     client.insert_dataframe('INSERT INTO test.get_district_neap VALUES', apiDataFrame)

#                 sql = "CREATE TABLE if not exists test.xbi_neapDistrictPlan (ID Int32,UserName String,ActivityID Int32,TimeStamp Date,Yr Int32,prov_id Int32,div_id Int32,dist_id Int32,day1 Date,dpec Date,dco Int32,edo Int32,dpo_dpec Int32,allMember Int32,rmDate Date,rmDC Int32,rmDHO Int32,rmSecurity Int32,rmAllMembers Int32,districtReady Int32,resheduleDate Date,actionLPUC Int32,actionType Int32,reviewSIA Int32,inaugrated_by String,OPVVilesReceived Int32,VaccArival Date,TeleSheets Int32,FingerMarker_before_sia Int32,FingerMarker Int32,SmMatrialReceived Date,avbl_vacc_carrier Int32,Remarks String,mov_dpec Int32,mov_redines Int32,status Int32,trash Int32,isSync Int32,VaccineType String, campaign_ID Int32, campaign_ActivityName String, campaign_ActivityID_old Int32, campaign_Yr Int32, campaign_SubActivityName String, location_id Int32, location_code Int32, location_name String, location_type String, location_status Int32 )ENGINE = MergeTree PRIMARY KEY ID ORDER BY ID"
#                 client.execute(sql)
#                 cols = "test.get_district_neap.ID,test.get_district_neap.UserName,test.get_district_neap.ActivityID,test.get_district_neap.TimeStamp,test.get_district_neap.Yr,test.get_district_neap.prov_id,test.get_district_neap.div_id,test.get_district_neap.dist_id,test.get_district_neap.day1,test.get_district_neap.dpec,test.get_district_neap.dco,test.get_district_neap.edo,test.get_district_neap.dpo_dpec,test.get_district_neap.allMember,test.get_district_neap.rmDate,test.get_district_neap.rmDC,test.get_district_neap.rmDHO,test.get_district_neap.rmSecurity,test.get_district_neap.rmAllMembers,test.get_district_neap.districtReady,test.get_district_neap.resheduleDate,test.get_district_neap.actionLPUC,test.get_district_neap.actionType,test.get_district_neap.reviewSIA,test.get_district_neap.inaugrated_by,test.get_district_neap.OPVVilesReceived,test.get_district_neap.VaccArival,test.get_district_neap.TeleSheets,test.get_district_neap.FingerMarker_before_sia,test.get_district_neap.FingerMarker,test.get_district_neap.SmMatrialReceived,test.get_district_neap.avbl_vacc_carrier,test.get_district_neap.Remarks,test.get_district_neap.mov_dpec,test.get_district_neap.mov_redines,test.get_district_neap.status,test.get_district_neap.trash,test.get_district_neap.isSync,test.get_district_neap.VaccineType,test.xbi_campaign.campaign_ID,test.xbi_campaign.campaign_ActivityName, test.xbi_campaign.campaign_ActivityID_old, test.xbi_campaign.campaign_Yr, test.xbi_campaign.campaign_SubActivityName, test.xbi_geolocation.ID, test.xbi_geolocation.code, test.xbi_geolocation.name, test.xbi_geolocation.type, test.xbi_geolocation.location_status"
#                 sql = "SELECT " + cols + " FROM test.get_district_neap tsm left JOIN test.xbi_geolocation gl1 ON (tsm.dist_id = gl1.ID) left JOIN test.xbi_campaign eoc_3 ON (tsm.ActivityID  = eoc_3.campaign_ActivityID_old And tsm.Yr = eoc_3.campaign_Yr) WHERE LENGTH(CAST(gl1.code AS VARCHAR(10)))  = '3' and gl1.type = 'District' "
#                 data = client.execute(sql)
                
#                 apiDataFrame = pd.DataFrame(data)
#                 apiDataFrame = apiDataFrame.replace(r'^\s*$', np.nan, regex=True)
#                 all_columns = list(apiDataFrame)  # Creates list of all column headers
#                 cols = apiDataFrame.iloc[0]
#                 apiDataFrame[all_columns] = apiDataFrame[all_columns].astype(str)
                
#                 d =  'ID','UserName','ActivityID','TimeStamp','Yr','prov_id','div_id','dist_id','day1','dpec','dco','edo','dpo_dpec','allMember','rmDate','rmDC','rmDHO','rmSecurity','rmAllMembers','districtReady','resheduleDate','actionLPUC','actionType','reviewSIA','inaugrated_by','OPVVilesReceived','VaccArival','TeleSheets','FingerMarker_before_sia','FingerMarker','SmMatrialReceived','avbl_vacc_carrier','Remarks','mov_dpec','mov_redines','status','trash','isSync','VaccineType','campaign_ID', 'campaign_ActivityName', 'campaign_ActivityID_old', 'campaign_Yr', 'campaign_SubActivityName','location_id', 'location_code', 'location_name','location_type','location_status'
#                 dff = pd.DataFrame(columns=d)
#                 for index, item in enumerate(d):
#                     dff[item] = apiDataFrame[index].values
#                 df2 = client.query_dataframe(
#                     "SELECT * FROM  test.xbi_neapDistrictPlan")
                
#                 if df2.empty:
#                     dff[["TimeStamp"]] = dff[["TimeStamp"]].apply(pd.to_datetime)
#                     dff[["day1"]] = dff[["day1"]].apply(pd.to_datetime)
#                     dff[["dpec"]] = dff[["dpec"]].apply(pd.to_datetime)
#                     dff[["rmDate"]] = dff[["rmDate"]].apply(pd.to_datetime)
#                     dff[["resheduleDate"]] = dff[["resheduleDate"]].apply(pd.to_datetime)
#                     dff[["VaccArival"]] = dff[["VaccArival"]].apply(pd.to_datetime)
#                     dff[["SmMatrialReceived"]] = dff[["SmMatrialReceived"]].apply(pd.to_datetime)

#                     client.insert_dataframe(
#                         'INSERT INTO  test.xbi_neapDistrictPlan  VALUES', dff)
#                     # logger.info(
#                     #     ' Data has been inserted into Table\' INSERT INTO  test.xbi_neapDistrictPlan  VALUES \' ')
#                     # print('cid --------------------------->\t', c)
#                     # print('year--------------------------->\t', y)
#                     sql = "DROP table if exists  test.get_district_neap"
#                     client.execute(sql)
#                 else:
#                     dff[["TimeStamp"]] = dff[["TimeStamp"]].apply(pd.to_datetime)
#                     dff[["day1"]] = dff[["day1"]].apply(pd.to_datetime)
#                     dff[["dpec"]] = dff[["dpec"]].apply(pd.to_datetime)
#                     dff[["rmDate"]] = dff[["rmDate"]].apply(pd.to_datetime)
#                     dff[["resheduleDate"]] = dff[["resheduleDate"]].apply(pd.to_datetime)
#                     dff[["VaccArival"]] = dff[["VaccArival"]].apply(pd.to_datetime)
#                     dff[["SmMatrialReceived"]] = dff[["SmMatrialReceived"]].apply(pd.to_datetime)

#                     client.insert_dataframe(
#                         'INSERT INTO  test.xbi_neapDistrictPlan  VALUES', dff)
#                     # logger.info(
#                     #     ' Data has been inserted into Table\' INSERT INTO  test.xbi_neapDistrictPlan  VALUES \' ')    
#                     # print('cid --------------------------->\t', c)
#                     # print('year--------------------------->\t', y)
#                     sql = "DROP table if exists  test.get_district_neap"
#                     client.execute(sql)
# dag = DAG(
#     'Prod_Neap_District_Automated',
#     schedule_interval='0 0 * * *',  # will run every mid-night.
#     default_args=default_args,
#     catchup=False)

# with dag:
#     GetAndInsertApiDataDistrictNeap = PythonOperator(
#         task_id='GetAndInsertApiDataDistrictNeap',
#         python_callable=GetAndInsertApiDataDistrictNeap,
#     )
# GetAndInsertApiDataDistrictNeap
