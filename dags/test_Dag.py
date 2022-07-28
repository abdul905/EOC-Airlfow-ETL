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
# def GetAndInsertApiDataDistrictNeap():
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

#         get_q = "select * FROM (select * from test.xbi_campaign xc  order by xc.campaign_ID desc limit 4) ORDER BY campaign_ID ASC "
#         df2 = client.query_dataframe(get_q)
#         #df2 = client.execute(get_q)
#         print(df2)
#         df = pd.DataFrame(df2)

#         campID_list = df2.campaign_ID.tolist()
#         campName_list = df2.campaign_ActivityName.tolist()
#         campIDold_list = df2.campaign_ActivityID_old.tolist()
#         campYr_list = df2.campaign_Yr.tolist()

#         for c, y in zip(campIDold_list, campYr_list):
#             url = "http://idims.eoc.gov.pk/api_who/api/get_allplaningDist/5468XE2LN6CzR7qRG041/"+ str(c)+"/"+ str(y)
#             logger.info('Requested Data From Api URL: '+url)

#             r = requests.get(url)
#             data = r.json()

#             if data_c['data'] == "No data found":
#                 print("Not Data found for campaign: "+url_c)    
#             else:
#                 print("Data found for campaign: "+url_c)
#                 logger.info('Received Data  From Api URL: '+url)

#                 rowsData = data["data"]["data"]
#                 apiDataFrame = pd.DataFrame(rowsData)
#                 sql = "CREATE TABLE if not exists test.get_district_neap_test (ID Int32,UserName String,ActivityID Int32,TimeStamp Date,Yr Int32,prov_id Int32,div_id Int32,dist_id Int32,day1 Date,dpec Date,dco Int32,edo Int32,dpo_dpec Int32,allMember Int32,rmDate Date,rmDC Int32,rmDHO Int32,rmSecurity Int32,rmAllMembers Int32,districtReady Int32,resheduleDate Date,actionLPUC Int32,actionType Int32,reviewSIA Int32,inaugrated_by String,OPVVilesReceived Int32,VaccArival Date,TeleSheets Int32,FingerMarker_before_sia Int32,FingerMarker Int32,SmMatrialReceived Date,avbl_vacc_carrier Int32,Remarks String,mov_dpec Int32,mov_redines Int32,status Int32,trash Int32,isSync Int32,VaccineType String)ENGINE = MergeTree PRIMARY KEY ID ORDER BY ID"
#                 client.execute(sql)

#                 df2 = client.query_dataframe("SELECT * FROM test.get_district_neap_test")
                    
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
#                         'INSERT INTO test.get_district_neap_test VALUES', apiDataFrame)
#                 else:
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
#                         'INSERT INTO test.get_district_neap_test VALUES', apiDataFrame)
                                

# def CreateJoinTableOfDistrictNEAP():
    
#     logger.info('Function \' CreateJoinTableOfDistrictNEAP \' Started Off')

#     with sshtunnel.SSHTunnelForwarder(
#         ('172.16.3.68', 22),
#         ssh_username="root",
#         ssh_password="COV!D@19#",
#         remote_bind_address=('localhost', 9000)) as server:

#         local_port = server.local_bind_port
#         print(local_port)
#         #connection = 'clickhouse://{user}:{password}@{server_host}:{port}/{db}'.format(**conf)    
#         conn = connect(f'clickhouse://default:mm@1234@localhost:{local_port}/test')
#         #conn = connect(host='172.16.3.68', database='test', user='default', password='mm@1234')

#         cursor = conn.cursor()
#         # cursor.execute('SHOW TABLES')
#         # print(cursor.fetchall())

#         client = Client(host='localhost',port=local_port, database='test',
#                                 user='default',
#                                 password='mm@1234',
#                                 settings={"use_numpy": True})

#         get_q = "select * FROM (select * from test.xbi_campaign xc  order by xc.campaign_ID desc limit 4) ORDER BY campaign_ID ASC "
#         df2 = client.query_dataframe(get_q)
#         #df2 = client.execute(get_q)
#         print(df2)
#         df = pd.DataFrame(df2)

#         campID_list = df2.campaign_ID.tolist()
#         campName_list = df2.campaign_ActivityName.tolist()
#         campIDold_list = df2.campaign_ActivityID_old.tolist()
#         campYr_list = df2.campaign_Yr.tolist()

#         print(campID_list)
#         print(campName_list)
#         print(campIDold_list)
#         print(campYr_list)

#         for f, b in zip(campIDold_list, campYr_list):                        
#             sql = "CREATE TABLE if not exists test.xbi_Test (ID Int32,UserName String,ActivityID Int32,TimeStamp Date,Yr Int32,prov_id Int32,div_id Int32,dist_id Int32,day1 Date,dpec Date,dco Int32,edo Int32,dpo_dpec Int32,allMember Int32,rmDate Date,rmDC Int32,rmDHO Int32,rmSecurity Int32,rmAllMembers Int32,districtReady Int32,resheduleDate Date,actionLPUC Int32,actionType Int32,reviewSIA Int32,inaugrated_by String,OPVVilesReceived Int32,VaccArival Date,TeleSheets Int32,FingerMarker_before_sia Int32,FingerMarker Int32,SmMatrialReceived Date,avbl_vacc_carrier Int32,Remarks String,mov_dpec Int32,mov_redines Int32,status Int32,trash Int32,isSync Int32,VaccineType String, campaign_ID Int32, campaign_ActivityName String, campaign_ActivityID_old Int32, campaign_Yr Int32, campaign_SubActivityName String, location_id Int32, location_code Int32, location_name String, location_type String, location_status Int32)ENGINE = MergeTree PRIMARY KEY ID ORDER BY ID"
#             client.execute(sql)
#             cols = "test.get_district_neap_test.ID,test.get_district_neap_test.UserName,test.get_district_neap_test.ActivityID,test.get_district_neap_test.TimeStamp,test.get_district_neap_test.Yr,test.get_district_neap_test.prov_id,test.get_district_neap_test.div_id,test.get_district_neap_test.dist_id,test.get_district_neap_test.day1,test.get_district_neap_test.dpec,test.get_district_neap_test.dco,test.get_district_neap_test.edo,test.get_district_neap_test.dpo_dpec,test.get_district_neap_test.allMember,test.get_district_neap_test.rmDate,test.get_district_neap_test.rmDC,test.get_district_neap_test.rmDHO,test.get_district_neap_test.rmSecurity,test.get_district_neap_test.rmAllMembers,test.get_district_neap_test.districtReady,test.get_district_neap_test.resheduleDate,test.get_district_neap_test.actionLPUC,test.get_district_neap_test.actionType,test.get_district_neap_test.reviewSIA,test.get_district_neap_test.inaugrated_by,test.get_district_neap_test.OPVVilesReceived,test.get_district_neap_test.VaccArival,test.get_district_neap_test.TeleSheets,test.get_district_neap_test.FingerMarker_before_sia,test.get_district_neap_test.FingerMarker,test.get_district_neap_test.SmMatrialReceived,test.get_district_neap_test.avbl_vacc_carrier,test.get_district_neap_test.Remarks,test.get_district_neap_test.mov_dpec,test.get_district_neap_test.mov_redines,test.get_district_neap_test.status,test.get_district_neap_test.trash,test.get_district_neap_test.isSync,test.get_district_neap_test.VaccineType,test.xbi_campaign.campaign_ID,test.xbi_campaign.campaign_ActivityName, test.xbi_campaign.campaign_ActivityID_old, test.xbi_campaign.campaign_Yr, test.xbi_campaign.campaign_SubActivityName, test.xbi_geolocation.ID, test.xbi_geolocation.code, test.xbi_geolocation.name, test.xbi_geolocation.type, test.xbi_geolocation.location_status"
#             sql = "SELECT " + cols + " FROM test.get_district_neap_test tsm left JOIN test.xbi_geolocation gl1 ON (tsm.dist_id = gl1.ID) left JOIN test.xbi_campaign eoc_3 ON (tsm.ActivityID  = eoc_3.campaign_ActivityID_old And tsm.Yr = eoc_3.campaign_Yr) WHERE LENGTH(CAST(gl1.code AS VARCHAR(10)))  = '3' and gl1.type = 'District' AND tsm.ActivityID="+str(f)+" AND tsm.Yr="+str(b)
#             data = client.execute(sql)
                    
#             apiDataFrame = pd.DataFrame(data)

#             if apiDataFrame.empty:
#                 print("No Data found for campaign: "+str(f))
#                 print("API DF Size: ",len(apiDataFrame))
#             else:
#                 all_columns = list(apiDataFrame)  # Creates list of all column headers
#                 cols = apiDataFrame.iloc[0]
#                 apiDataFrame[all_columns] = apiDataFrame[all_columns].astype(str)
                        
#                 d =  'ID','UserName','ActivityID','TimeStamp','Yr','prov_id','div_id','dist_id','day1','dpec','dco','edo','dpo_dpec','allMember','rmDate','rmDC','rmDHO','rmSecurity','rmAllMembers','districtReady','resheduleDate','actionLPUC','actionType','reviewSIA','inaugrated_by','OPVVilesReceived','VaccArival','TeleSheets','FingerMarker_before_sia','FingerMarker','SmMatrialReceived','avbl_vacc_carrier','Remarks','mov_dpec','mov_redines','status','trash','isSync','VaccineType','campaign_ID', 'campaign_ActivityName', 'campaign_ActivityID_old', 'campaign_Yr', 'campaign_SubActivityName','location_id', 'location_code', 'location_name','location_type','location_status'
                
#                 dff = pd.DataFrame(columns=d)
#                 for index, item in enumerate(d):
#                     dff[item] = apiDataFrame[index].values
                
#                 data_q = "SELECT * FROM test.xbi_Test WHERE Yr = "+str(b)+" AND ActivityID = "+str(f)
#                 df2 = client.query_dataframe(data_q)  
#                 print("API DF Size: ",len(df2))     
#                 # df2 = client.query_dataframe(
#                 #     "SELECT * FROM  test.xbi_Test WHERE Yr = 2022 and ActivityID = 2")
                        
#                 if df2.empty:
#                     dff[["TimeStamp"]] = dff[["TimeStamp"]].apply(pd.to_datetime)
#                     dff[["day1"]] = dff[["day1"]].apply(pd.to_datetime)
#                     dff[["dpec"]] = dff[["dpec"]].apply(pd.to_datetime)
#                     dff[["rmDate"]] = dff[["rmDate"]].apply(pd.to_datetime)
#                     dff[["resheduleDate"]] = dff[["resheduleDate"]].apply(pd.to_datetime)
#                     dff[["VaccArival"]] = dff[["VaccArival"]].apply(pd.to_datetime)
#                     dff[["SmMatrialReceived"]] = dff[["SmMatrialReceived"]].apply(pd.to_datetime)

#                     client.insert_dataframe(
#                         'INSERT INTO  test.xbi_Test  VALUES', dff)
#                     logger.info(
#                         ' Data has been inserted into Table\' INSERT INTO  test.xbi_Test  VALUES \' ')

#                     # sql = "DROP table if exists  test.get_district_neap_test"
#                     # client.execute(sql)

#                 else:
#                     sql = "ALTER TABLE test.xbi_Test DELETE WHERE Yr = "+ str(b) + " AND ActivityID = "+str(f)
#                     client.execute(sql)

#                     dff[["TimeStamp"]] = dff[["TimeStamp"]].apply(pd.to_datetime)
#                     dff[["day1"]] = dff[["day1"]].apply(pd.to_datetime)
#                     dff[["dpec"]] = dff[["dpec"]].apply(pd.to_datetime)
#                     dff[["rmDate"]] = dff[["rmDate"]].apply(pd.to_datetime)
#                     dff[["resheduleDate"]] = dff[["resheduleDate"]].apply(pd.to_datetime)
#                     dff[["VaccArival"]] = dff[["VaccArival"]].apply(pd.to_datetime)
#                     dff[["SmMatrialReceived"]] = dff[["SmMatrialReceived"]].apply(pd.to_datetime)

#                     client.insert_dataframe(
#                         'INSERT INTO  test.xbi_Test  VALUES', dff)
#                     logger.info(
#                         ' Data has been inserted into Table\' INSERT INTO  test.xbi_Test  VALUES \' ')    
                    
#         sql = "DROP table if exists  test.get_district_neap_test"
#         client.execute(sql)
            
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
#     CreateJoinTableOfDistrictNEAP = PythonOperator(
#         task_id='CreateJoinTableOfDistrictNEAP',
#         python_callable=CreateJoinTableOfDistrictNEAP,
#     )
# GetAndInsertApiDataDistrictNeap >> CreateJoinTableOfDistrictNEAP



#-------------------------------------------------------------------------------------------------------------------------------#
#---------------------------------------- INSERT CAMPAIGN DATA PROCESS CUBE  ---------------------------------------------------#
#-------------------------------------------- Author: Abdul Bari Malik ---------------------------------------------------------#
#-------------------------------------------------------------------------------------------------------------------------------#

# def GetAndInsertApiDataTest():
    
#     # li3 = [222,223,224,225,226,227,228,229,230,231,232,233,234,235,236,237,238,239,240,241,242,243,244,245,247,248,249,250,251,252,253,254,255,256]
    
#     logger.info('Function \' GetAndInsertApiDataTest \' Started Off')
#     client = Client(host='161.97.136.95',
#                         user='default',
#                         password='pakistan',
#                         port='9000', settings={"use_numpy": True})

#     sql = """CREATE TABLE if not exists test.get_ProcessCubeTest ( ID Int32, IDcampCat Int32,ActivityID Int32,Yr Int32,TimeStamp DateTime('Asia/Karachi'), Cday Int32, TehsilID Int32, UCID Int32 
#             ,ccpvTargets Int32,TeamsRpt Int32,HH011_MP Int32,HH1259_MP Int32,HH011_TS Int32,HH1259_TS Int32,HH011 Int32,HH1259 Int32,OutofH011 Int32,OutofH1259 Int32, RecNA011 Int32,RecNA1259 Int32,RecRef011 Int32,RecRef1259 Int32,CovNA011 Int32,CovNA1259 Int32,CovRef011 Int32,CovRef1259 Int32,Guests Int32,VaccinatedSchool Int32,NewBorn Int32,AlreadyVaccinated Int32,FixSite011 Int32,FixSite1259 Int32,Transit011 Int32,Transit1259 Int32,CovMob Int32,HHvisit Int32,HHPlan Int32,MultipleFamily String,Weakness Int32,ZeroRt Int32,Stage34 Int32,Inaccessible Int32,V_Age611 Int32,v_Age1259 Int32,V_capGiven Int32,V_capUsed Int32,V_capRet Int32,trigers String,Covid19 Int32,campcat_name String
#             ,UserName String,DistID Int32,DivID Int32,ProvID Int32
#             ,CCovNA011 Int32,CCovNA1259 Int32,CCovRef011 Int32,CCovRef1259 Int32
#             ,COutofH011 Int32,COutofH1259 Int32,CGuests Int32,CCovMob Int32,CNewBorn Int32,CAlreadyVaccinated Int32,CFixSite011 Int32,CFixSite1259 Int32,CTransit011 Int32,CTransit1259 Int32,UnRecorded_Cov Int32,AFP_CaseRpt Int32,ZeroDos1 Int32,V_Age611_CP Int32,v_Age1259_CP Int32,missing_vacc_carr Int32,missing_vacc_carr_reason String
#             ,ACCovNA011 Int32,ACCovNA1259 Int32,ACCovRef011 Int32,ACCovRef1259 Int32,ACOutofH011 Int32,ACOutofH1259 Int32,ACGuests Int32,ACCovMob Int32,ACNewBorn Int32,ACAlreadyVaccinated Int32,UnRecCov Int32
#             ,PersistentlyMC Int32,PersistentlyMCHRMP Int32, fm_issued Int32,fm_retrieved Int32
#             ,VaccinationDate DateTime('Asia/Karachi'),OPVGiven Int32,OPVUsed Int32,OPVReturned Int32,status Int32,trash Int32,isSync Int32
#             ,UserID Int32,CategoryID Int32,Proposed_HH_Targets Int32,Final_HH_Targets Int32 ,Seasonal String,Nomads String,Agriculture String,Beggers String,IDPs String,Family String,Others String,Status String,Trash Int32
#             ,Remarks String,location_code Int32
#             ,campaign_ID Int32, campaign_ActivityName String, campaign_ActivityID_old Int32, campaign_Yr Int32, campaign_SubActivityName String
#             ,geoLocation_name String, geoLocation_type Int32, geoLocation_code Int32, geoLocation_census_pop Int32, geoLocation_target Int32, geoLocation_status Int32, geoLocation_pname String, geoLocation_dname String, geoLocation_namedistrict String, geoLocation_codedistrict String, geoLocation_tname String, geoLocation_provincecode Int32, geoLocation_districtcode Int32, geoLocation_tehsilcode Int32, geoLocation_priority Int32, geoLocation_commnet Int32, geoLocation_hr Int32, geoLocation_fcm Int32, geoLocation_tier Int32, geoLocation_block String, geoLocation_division String, geoLocation_cordinates String, geoLocation_latitude String, geoLocation_longitude String, geoLocation_x String, geoLocation_y String, geoLocation_imagepath String, geoLocation_isccpv Int32, geoLocation_rank Int32, geoLocation_rank_score String, geoLocation_ishealthcamp Int32, geoLocation_isdsc Int32, geoLocation_ucorg String, geoLocation_organization String, geoLocation_tierfromaug161 Int32, geoLocation_tierfromsep171 Int32, geoLocation_tierfromdec181 Int32, geoLocation_mtap Int32, geoLocation_rspuc Int32, geoLocation_issmt Int32, geoLocation_updateddatetime String, geoLocation_x_code Int32, geoLocation_draining_uc Int32, geoLocation_upap_districts Int32, geoLocation_shruc Int32, geoLocation_khidist_id String
    
#             )ENGINE = MergeTree PRIMARY KEY ID ORDER BY ID"""
#     client.execute(sql)

#     df = client.query_dataframe(
#                 """
#                     SELECT ID,	IDcampCat,	ActivityID,	Yr,	TimeStamp,	Cday,	TehsilID,	UCID, 
#                     -- Control Room --
#                     ccpvTargets,TeamsRpt,HH011_MP,HH1259_MP,HH011_TS,HH1259_TS,HH011,HH1259,OutofH011,OutofH1259,
#                     RecNA011,RecNA1259,RecRef011,RecRef1259,CovNA011,CovNA1259,CovRef011,CovRef1259,Guests, 
#                     VaccinatedSchool,NewBorn,AlreadyVaccinated,FixSite011,FixSite1259,Transit011,Transit1259,CovMob,HHvisit,HHPlan,MultipleFamily,Weakness,ZeroRt,Stage34,
#                     Inaccessible,V_Age611,v_Age1259,V_capGiven,V_capUsed,V_capRet,trigers,Covid19,campcat_name
#                     -- CatchUp --
#                     ,NULL AS UserName,NULL AS DistID,NULL AS DivID,NULL AS ProvID
#                     ,NULL AS CCovNA011, NULL AS CCovNA1259,NULL AS CCovRef011,NULL AS CCovRef1259
#                     ,NULL AS COutofH011,NULL AS COutofH1259,NULL AS CGuests,NULL AS CCovMob,NULL AS CNewBorn,NULL AS CAlreadyVaccinated,NULL AS CFixSite011,NULL AS CFixSite1259,NULL AS CTransit011,NULL AS CTransit1259,NULL AS UnRecorded_Cov,
#                     NULL AS AFP_CaseRpt,NULL AS ZeroDos1,NULL AS V_Age611_CP,NULL AS v_Age1259_CP,NULL AS missing_vacc_carr,NULL AS missing_vacc_carr_reason
#                     -- ACU --
#                     ,NULL AS ACCovNA011,NULL AS ACCovNA1259,NULL AS ACCovRef011,NULL AS ACCovRef1259,NULL AS ACOutofH011,NULL AS ACOutofH1259,NULL AS ACGuests,NULL AS ACCovMob,NULL AS ACNewBorn,NULL AS ACAlreadyVaccinated,NULL AS UnRecCov
#                     -- CU = ACU --
#                     ,NULL AS PersistentlyMC,NULL AS PersistentlyMCHRMP, NULL AS fm_issued,NULL AS fm_retrieved
#                     -- CR = CU = ACU --
#                     ,VaccinationDate,OPVGiven,OPVUsed,OPVReturned,status,trash,isSync,
#                     -- HH Target --
#                     NULL AS UserID,NULL AS CategoryID, NULL AS Proposed_HH_Targets, NULL AS Final_HH_Targets , NULL AS Seasonal, NULL AS Nomads, NULL AS Agriculture, NULL AS Beggers, NULL AS IDPs, NULL AS Family, NULL AS Others 
#                     ,NULL AS Status,NULL AS Trash 
#                     --- Common --
#                     ,Remarks,location_code
#                     ,campaign_ID, campaign_ActivityName, campaign_ActivityID_old, campaign_Yr, campaign_SubActivityName
#                     ,geoLocation_name, geoLocation_type, geoLocation_code, geoLocation_census_pop, geoLocation_target, geoLocation_status, geoLocation_pname, geoLocation_dname, geoLocation_namedistrict, geoLocation_codedistrict, geoLocation_tname, geoLocation_provincecode, geoLocation_districtcode, geoLocation_tehsilcode, geoLocation_priority, geoLocation_commnet, geoLocation_hr, geoLocation_fcm, geoLocation_tier, geoLocation_block, geoLocation_division, geoLocation_cordinates, geoLocation_latitude, geoLocation_longitude, geoLocation_x, geoLocation_y, geoLocation_imagepath, geoLocation_isccpv, geoLocation_rank, geoLocation_rank_score, geoLocation_ishealthcamp, geoLocation_isdsc, geoLocation_ucorg, geoLocation_organization, geoLocation_tierfromaug161, geoLocation_tierfromsep171, geoLocation_tierfromdec181, geoLocation_mtap, geoLocation_rspuc, geoLocation_issmt, geoLocation_updateddatetime, geoLocation_x_code, geoLocation_draining_uc, geoLocation_upap_districts, geoLocation_shruc, geoLocation_khidist_id
                                
#                     FROM test.xbi_controlroom
#                     where ActivityID = 2 and Yr = 2022
#                     ----------
#                     UNION ALL
#                     ----------
#                     SELECT ID,IDcampCat,ActivityID,Yr,TimeStamp,Cday,TehsilID,UCID, 
#                     -- Control Room --
#                     NULL AS ccpvTargets,NULL AS TeamsRpt,NULL AS HH011_MP,NULL AS HH1259_MP,NULL AS HH011_TS,NULL AS HH1259_TS,NULL AS HH011,NULL AS HH1259,NULL AS OutofH011,NULL AS OutofH1259,
#                     NULL AS RecNA011,NULL AS RecNA1259,NULL AS RecRef011,NULL AS RecRef1259,NULL AS CovNA011,NULL AS CovNA1259,NULL AS CovRef011,NULL AS CovRef1259,NULL AS Guests,
#                     NULL AS VaccinatedSchool,NULL AS NewBorn,NULL AS AlreadyVaccinated,NULL AS FixSite011,NULL AS FixSite1259,NULL AS Transit011,NULL AS Transit1259,NULL AS CovMob,NULL AS HHvisit,NULL AS HHPlan,NULL AS MultipleFamily,NULL AS Weakness,NULL AS ZeroRt,NULL AS Stage34,
#                     NULL AS Inaccessible,NULL AS V_Age611,NULL AS v_Age1259,NULL AS V_capGiven,NULL AS V_capUsed,NULL AS V_capRet,NULL AS trigers,NULL AS Covid19,NULL AS campcat_name
#                     -- CatchUp --
#                     ,UserName,DistID,DivID,ProvID
#                     ,CCovNA011,CCovNA1259,CCovRef011,CCovRef1259
#                     ,COutofH011,COutofH1259,CGuests,CCovMob,CNewBorn,CAlreadyVaccinated,CFixSite011,CFixSite1259,CTransit011,CTransit1259,UnRecorded_Cov,
#                     AFP_CaseRpt,ZeroDos1,V_Age611_CP,v_Age1259_CP,missing_vacc_carr,missing_vacc_carr_reason
#                     -- ACU --
#                     ,NULL AS ACCovNA011,NULL AS ACCovNA1259,NULL AS ACCovRef011,NULL AS ACCovRef1259,NULL AS ACOutofH011,NULL AS ACOutofH1259,NULL AS ACGuests,NULL AS ACCovMob,NULL AS ACNewBorn,NULL AS ACAlreadyVaccinated,NULL AS UnRecCov
#                     -- CU = ACU --
#                     ,PersistentlyMC,PersistentlyMCHRMP,fm_issued,fm_retrieved
#                     -- CR = CU = ACU --
#                     ,NULL AS VaccinationDate,OPVGiven,OPVUsed,CAST(OPVReturned AS INT)OPVReturned ,status,trash,isSync,
#                     -- HH Target --
#                     NULL AS UserID,NULL AS CategoryID, NULL AS Proposed_HH_Targets, NULL AS Final_HH_Targets , NULL AS Seasonal, NULL AS Nomads, NULL AS Agriculture, NULL AS Beggers, NULL AS IDPs, NULL AS Family, NULL AS Others 
#                     ,NULL AS Status,NULL AS Trash 
#                     --- Common --
#                     ,Remarks,location_code
#                     ,campaign_ID, campaign_ActivityName, campaign_ActivityID_old, campaign_Yr, campaign_SubActivityName
#                     ,geoLocation_name, geoLocation_type, geoLocation_code, geoLocation_census_pop, geoLocation_target, geoLocation_status, geoLocation_pname, geoLocation_dname, geoLocation_namedistrict, geoLocation_codedistrict, geoLocation_tname, geoLocation_provincecode, geoLocation_districtcode, geoLocation_tehsilcode, geoLocation_priority, geoLocation_commnet, geoLocation_hr, geoLocation_fcm, geoLocation_tier, geoLocation_block, CAST(geoLocation_division AS String)geoLocation_division, geoLocation_cordinates, geoLocation_latitude, geoLocation_longitude, geoLocation_x, geoLocation_y, geoLocation_imagepath, geoLocation_isccpv, geoLocation_rank, geoLocation_rank_score, geoLocation_ishealthcamp, geoLocation_isdsc, geoLocation_ucorg, geoLocation_organization, geoLocation_tierfromaug161, geoLocation_tierfromsep171, geoLocation_tierfromdec181, geoLocation_mtap, geoLocation_rspuc, geoLocation_issmt, geoLocation_updateddatetime, geoLocation_x_code, geoLocation_draining_uc, geoLocation_upap_districts, geoLocation_shruc, geoLocation_khidist_id

#                     FROM test.xbi_catchUp
#                     where ActivityID = 2 and Yr = 2022
#                     -----------
#                     UNION ALL
#                     ----------
#                     SELECT ID,IDcampCat,ActivityID,Yr,TimeStamp,cday AS Cday,TehsilID,UCID,
#                     -- Control Room --
#                     NULL AS ccpvTargets,NULL AS TeamsRpt,NULL AS HH011_MP,NULL AS HH1259_MP,NULL AS HH011_TS,NULL AS HH1259_TS,NULL AS HH011,NULL AS HH1259,NULL AS OutofH011,NULL AS OutofH1259,
#                     NULL AS RecNA011,NULL AS RecNA1259,NULL AS RecRef011,NULL AS RecRef1259,NULL AS CovNA011,NULL AS CovNA1259,NULL AS CovRef011,NULL AS CovRef1259,NULL AS Guests,
#                     NULL AS VaccinatedSchool,NULL AS NewBorn,NULL AS AlreadyVaccinated,NULL AS FixSite011,NULL AS FixSite1259,NULL AS Transit011,NULL AS Transit1259,NULL AS CovMob,NULL AS HHvisit,NULL AS HHPlan,NULL AS MultipleFamily,NULL AS Weakness,NULL AS ZeroRt,NULL AS Stage34,
#                     NULL AS Inaccessible,NULL AS V_Age611,NULL AS v_Age1259,NULL AS V_capGiven,NULL AS V_capUsed,NULL AS V_capRet,NULL AS trigers,NULL AS Covid19,NULL AS campcat_name
#                     -- CatchUp=afterCatchUp --
#                     ,UserName,DistID,DivID,ProvID
#                     -- CatchUp --
#                     ,NULL AS CCovNA011,NULL AS CCovNA1259,NULL AS CCovRef011,NULL AS CCovRef1259
#                     ,NULL AS COutofH011,NULL AS COutofH1259,NULL AS CGuests,NULL AS CCovMob,NULL AS CNewBorn,NULL AS CAlreadyVaccinated,NULL AS CFixSite011,NULL AS CFixSite1259,NULL AS CTransit011,NULL AS CTransit1259,NULL AS UnRecorded_Cov,
#                     NULL AS AFP_CaseRpt,NULL AS ZeroDos1,NULL AS V_Age611_CP,NULL AS v_Age1259_CP,NULL AS missing_vacc_carr,NULL AS missing_vacc_carr_reason
#                     -- ACU --
#                     ,ACCovNA011,ACCovNA1259,ACCovRef011,ACCovRef1259,ACOutofH011,ACOutofH1259,ACGuests,ACCovMob,ACNewBorn,ACAlreadyVaccinated,UnRecCov
#                     -- CU = ACU --
#                     ,PersistentlyMC,PersistentlyMCHRMP,fm_issued,fm_retrieved
#                     -- CR = CU = ACU --
#                     ,VaccinationDate,OPVGiven,OPVUsed,CAST(OPVReturned AS INT)OPVReturned,status,trash,isSync,
#                     -- HH Target --
#                     NULL AS UserID,NULL AS CategoryID, NULL AS Proposed_HH_Targets, NULL AS Final_HH_Targets , NULL AS Seasonal, NULL AS Nomads, NULL AS Agriculture, NULL AS Beggers, NULL AS IDPs, NULL AS Family, NULL AS Others 
#                     ,NULL AS Status,NULL AS Trash 
#                     --- Common --
#                     ,Remarks,location_code
#                     ,campaign_ID, campaign_ActivityName, campaign_ActivityID_old, campaign_Yr, campaign_SubActivityName
#                     ,geoLocation_name, geoLocation_type, geoLocation_code, geoLocation_census_pop, geoLocation_target, geoLocation_status, geoLocation_pname, geoLocation_dname, geoLocation_namedistrict, geoLocation_codedistrict, geoLocation_tname, geoLocation_provincecode, geoLocation_districtcode, geoLocation_tehsilcode, geoLocation_priority, geoLocation_commnet, geoLocation_hr, geoLocation_fcm, geoLocation_tier, geoLocation_block, geoLocation_division, geoLocation_cordinates, geoLocation_latitude, geoLocation_longitude, geoLocation_x, geoLocation_y, geoLocation_imagepath, geoLocation_isccpv, geoLocation_rank, geoLocation_rank_score, geoLocation_ishealthcamp, geoLocation_isdsc, geoLocation_ucorg, geoLocation_organization, geoLocation_tierfromaug161, geoLocation_tierfromsep171, geoLocation_tierfromdec181, geoLocation_mtap, geoLocation_rspuc, geoLocation_issmt, geoLocation_updateddatetime, geoLocation_x_code, geoLocation_draining_uc, geoLocation_upap_districts, geoLocation_shruc, geoLocation_khidist_id

#                     FROM test.xbi_after_catchUp
#                     where ActivityID = 2 and Yr = 2022
#                     ----------
#                     UNION ALL
#                     ----------
#                     SELECT ID,NULL AS IDcampCat,ActivityID_fk AS ActivityID,Yr,CAST (TimeStamp AS String)TimeStamp, NULL AS Cday,TehsilID,UCID,
#                     -- Control Room --
#                     NULL AS ccpvTargets,NULL AS TeamsRpt,NULL AS HH011_MP,NULL AS HH1259_MP,NULL AS HH011_TS,NULL AS HH1259_TS,NULL AS HH011,NULL AS HH1259,NULL AS OutofH011,NULL AS OutofH1259,
#                     NULL AS RecNA011,NULL AS RecNA1259,NULL AS RecRef011,NULL AS RecRef1259,NULL AS CovNA011,NULL AS CovNA1259,NULL AS CovRef011,NULL AS CovRef1259,NULL AS Guests,
#                     NULL AS VaccinatedSchool,NULL AS NewBorn,NULL AS AlreadyVaccinated,NULL AS FixSite011,NULL AS FixSite1259,NULL AS Transit011,NULL AS Transit1259,NULL AS CovMob,NULL AS HHvisit,NULL AS HHPlan,NULL AS MultipleFamily,NULL AS Weakness,NULL AS ZeroRt,NULL AS Stage34,
#                     NULL AS Inaccessible,NULL AS V_Age611,NULL AS v_Age1259,NULL AS V_capGiven,NULL AS V_capUsed,NULL AS V_capRet,NULL AS trigers,NULL AS Covid19,NULL AS campcat_name
#                     -- CatchUp=afterCatchUp --
#                     ,NULL AS UserName,DistID,DivID,ProvID
#                     -- CatchUp --
#                     ,NULL AS CCovNA011,NULL AS CCovNA1259,NULL AS CCovRef011,NULL AS CCovRef1259
#                     ,NULL AS COutofH011,NULL AS COutofH1259,NULL AS CGuests,NULL AS CCovMob,NULL AS CNewBorn,NULL AS CAlreadyVaccinated,NULL AS CFixSite011,NULL AS CFixSite1259,NULL AS CTransit011,NULL AS CTransit1259,NULL AS UnRecorded_Cov,
#                     NULL AS AFP_CaseRpt,NULL AS ZeroDos1,NULL AS V_Age611_CP,NULL AS v_Age1259_CP,NULL AS missing_vacc_carr,NULL AS missing_vacc_carr_reason
#                     -- ACU --
#                     ,NULL AS ACCovNA011,NULL AS ACCovNA1259,NULL AS ACCovRef011,NULL AS ACCovRef1259,NULL AS ACOutofH011,NULL AS ACOutofH1259,NULL AS ACGuests,NULL AS ACCovMob,NULL AS ACNewBorn,NULL AS ACAlreadyVaccinated,NULL AS UnRecCov
#                     -- CU = ACU --
#                     ,NULL AS PersistentlyMC,NULL AS PersistentlyMCHRMP,NULL AS fm_issued,NULL AS fm_retrieved
#                     -- CR = CU = ACU --
#                     ,NULL AS VaccinationDate,NULL AS OPVGiven,NULL AS OPVUsed,NULL AS OPVReturned,NULL AS status,NULL AS trash,NULL AS isSync,
#                     -- HH Target --
#                     UserID,CategoryID,Proposed_HH_Targets,Final_HH_Targets ,Seasonal,Nomads,Agriculture,Beggers,IDPs,Family,Others,Status,Trash 
#                     --- Common --
#                     ,Remarks,Ucode AS location_code
#                     ,campaign_ID, campaign_ActivityName, campaign_ActivityID_old, campaign_Yr, campaign_SubActivityName
#                     ,geoLocation_name, geoLocation_type, geoLocation_code, geoLocation_census_pop, geoLocation_target, geoLocation_status, geoLocation_pname, geoLocation_dname, geoLocation_namedistrict, geoLocation_codedistrict, geoLocation_tname, geoLocation_provincecode, geoLocation_districtcode, geoLocation_tehsilcode, geoLocation_priority, geoLocation_commnet, geoLocation_hr, geoLocation_fcm, geoLocation_tier, geoLocation_block, geoLocation_division, geoLocation_cordinates, geoLocation_latitude, geoLocation_longitude, geoLocation_x, geoLocation_y, geoLocation_imagepath, geoLocation_isccpv, geoLocation_rank, geoLocation_rank_score, geoLocation_ishealthcamp, geoLocation_isdsc, geoLocation_ucorg, geoLocation_organization, geoLocation_tierfromaug161, geoLocation_tierfromsep171, geoLocation_tierfromdec181, geoLocation_mtap, geoLocation_rspuc, geoLocation_issmt, geoLocation_updateddatetime, geoLocation_x_code, geoLocation_draining_uc, geoLocation_upap_districts, geoLocation_shruc, geoLocation_khidist_id

#                     FROM test.xbi_hhtarget
#                     where ActivityID = 2 and Yr = 2022
#                 """)
#     #df['TimeStamp'] = datetime.strptime(df['TimeStamp'], '%d/%m/%y %H:%M:%S')
#     df['TimeStamp'] = pd.to_datetime(df['TimeStamp'], errors='coerce')
#     df['VaccinationDate'] = pd.to_datetime(df['VaccinationDate'], errors='coerce')
#     df[["TimeStamp"]] = df[["TimeStamp"]].apply(pd.to_datetime)
#     df[["VaccinationDate"]] = df[["VaccinationDate"]].apply(pd.to_datetime)         
#     client.insert_dataframe(
#         'INSERT INTO test.get_ProcessCubeTest  VALUES', df)
#     logger.info(
#             ' Data has been inserted into Table\' INSERT INTO test.get_ProcessCubeTest VALUES \' ')            
    
#     #---------------- xbi_table --------------------#
#     sql = """CREATE TABLE if not exists test.xbi_ProcessCubeTest ( ID Int32, IDcampCat Int32,ActivityID Int32,Yr Int32,TimeStamp DateTime('Asia/Karachi'), Cday Int32, TehsilID Int32, UCID Int32 
#             ,ccpvTargets Int32,TeamsRpt Int32,HH011_MP Int32,HH1259_MP Int32,HH011_TS Int32,HH1259_TS Int32,HH011 Int32,HH1259 Int32,OutofH011 Int32,OutofH1259 Int32, RecNA011 Int32,RecNA1259 Int32,RecRef011 Int32,RecRef1259 Int32,CovNA011 Int32,CovNA1259 Int32,CovRef011 Int32,CovRef1259 Int32,Guests Int32,VaccinatedSchool Int32,NewBorn Int32,AlreadyVaccinated Int32,FixSite011 Int32,FixSite1259 Int32,Transit011 Int32,Transit1259 Int32,CovMob Int32,HHvisit Int32,HHPlan Int32,MultipleFamily String,Weakness Int32,ZeroRt Int32,Stage34 Int32,Inaccessible Int32,V_Age611 Int32,v_Age1259 Int32,V_capGiven Int32,V_capUsed Int32,V_capRet Int32,trigers String,Covid19 Int32,campcat_name String
#             ,UserName String,DistID Int32,DivID Int32,ProvID Int32
#             ,CCovNA011 Int32,CCovNA1259 Int32,CCovRef011 Int32,CCovRef1259 Int32,COutofH011 Int32,COutofH1259 Int32,CGuests Int32,CCovMob Int32,CNewBorn Int32,CAlreadyVaccinated Int32,CFixSite011 Int32,CFixSite1259 Int32,CTransit011 Int32,CTransit1259 Int32,UnRecorded_Cov Int32,AFP_CaseRpt Int32,ZeroDos1 Int32,V_Age611_CP Int32,v_Age1259_CP Int32,missing_vacc_carr Int32,missing_vacc_carr_reason String
#             ,ACCovNA011 Int32,ACCovNA1259 Int32,ACCovRef011 Int32,ACCovRef1259 Int32,ACOutofH011 Int32,ACOutofH1259 Int32,ACGuests Int32,ACCovMob Int32,ACNewBorn Int32,ACAlreadyVaccinated Int32,UnRecCov Int32
#             ,PersistentlyMC Int32,PersistentlyMCHRMP Int32, fm_issued Int32,fm_retrieved Int32
#             ,VaccinationDate DateTime('Asia/Karachi'),OPVGiven Int32,OPVUsed Int32,OPVReturned Int32,status Int32,trash Int32,isSync Int32
            
#             ,UserID Int32,CategoryID Int32,Proposed_HH_Targets Int32,Final_HH_Targets Int32 ,Seasonal String,Nomads String,Agriculture String,Beggers String,IDPs String,Family String,Others String,Status String,Trash Int32
            
#             ,Remarks String,location_code Int32
#             ,campaign_ID Int32, campaign_ActivityName String, campaign_ActivityID_old Int32, campaign_Yr Int32, campaign_SubActivityName String
#             ,geoLocation_name String, geoLocation_type Int32, geoLocation_code Int32, geoLocation_census_pop Int32, geoLocation_target Int32, geoLocation_status Int32, geoLocation_pname String, geoLocation_dname String, geoLocation_namedistrict String, geoLocation_codedistrict String, geoLocation_tname String, geoLocation_provincecode Int32, geoLocation_districtcode Int32, geoLocation_tehsilcode Int32, geoLocation_priority Int32, geoLocation_commnet Int32, geoLocation_hr Int32, geoLocation_fcm Int32, geoLocation_tier Int32, geoLocation_block String, geoLocation_division String, geoLocation_cordinates String, geoLocation_latitude String, geoLocation_longitude String, geoLocation_x String, geoLocation_y String, geoLocation_imagepath String, geoLocation_isccpv Int32, geoLocation_rank Int32, geoLocation_rank_score String, geoLocation_ishealthcamp Int32, geoLocation_isdsc Int32, geoLocation_ucorg String, geoLocation_organization String, geoLocation_tierfromaug161 Int32, geoLocation_tierfromsep171 Int32, geoLocation_tierfromdec181 Int32, geoLocation_mtap Int32, geoLocation_rspuc Int32, geoLocation_issmt Int32, geoLocation_updateddatetime String, geoLocation_x_code Int32, geoLocation_draining_uc Int32, geoLocation_upap_districts Int32, geoLocation_shruc Int32, geoLocation_khidist_id String
            
#             )ENGINE = MergeTree PRIMARY KEY ID ORDER BY ID"""
#     client.execute(sql)

#     cols = """
#             eoc_1.ID,	eoc_1.IDcampCat,	eoc_1.ActivityID,	eoc_1.Yr,	eoc_1.TimeStamp,	eoc_1.Cday,	eoc_1.TehsilID,	eoc_1.UCID, 
#             eoc_1.ccpvTargets,eoc_1.TeamsRpt,eoc_1.HH011_MP,eoc_1.HH1259_MP,eoc_1.HH011_TS,eoc_1.HH1259_TS,eoc_1.HH011,eoc_1.HH1259,eoc_1.OutofH011,eoc_1.OutofH1259,
#             eoc_1.RecNA011,eoc_1.RecNA1259,eoc_1.RecRef011,eoc_1.RecRef1259,eoc_1.CovNA011,eoc_1.CovNA1259,eoc_1.CovRef011,eoc_1.CovRef1259,eoc_1.Guests, 
#             eoc_1.VaccinatedSchool,eoc_1.NewBorn,eoc_1.AlreadyVaccinated,eoc_1.FixSite011,eoc_1.FixSite1259,eoc_1.Transit011,eoc_1.Transit1259,eoc_1.CovMob,eoc_1.HHvisit,eoc_1.HHPlan,eoc_1.MultipleFamily,eoc_1.Weakness,eoc_1.ZeroRt,eoc_1.Stage34,
#             eoc_1.Inaccessible,eoc_1.V_Age611,eoc_1.v_Age1259,eoc_1.V_capGiven,eoc_1.V_capUsed,eoc_1.V_capRet,eoc_1.trigers,eoc_1.Covid19,eoc_1.campcat_name

#             ,eoc_1.UserName,eoc_1.DistID,eoc_1.DivID,eoc_1.ProvID
#             ,eoc_1.CCovNA011, eoc_1.CCovNA1259,eoc_1.CCovRef011,eoc_1.CCovRef1259
#             ,eoc_1.COutofH011,eoc_1.COutofH1259,eoc_1.CGuests,eoc_1.CCovMob,eoc_1.CNewBorn,eoc_1.CAlreadyVaccinated,eoc_1.CFixSite011,eoc_1.CFixSite1259,eoc_1.CTransit011,eoc_1.CTransit1259,eoc_1.UnRecorded_Cov
#             ,eoc_1.AFP_CaseRpt,eoc_1.ZeroDos1,eoc_1.V_Age611_CP,eoc_1.v_Age1259_CP,eoc_1.missing_vacc_carr,eoc_1.missing_vacc_carr_reason

#             ,eoc_1.ACCovNA011,eoc_1.ACCovNA1259,eoc_1.ACCovRef011,eoc_1.ACCovRef1259,eoc_1.ACOutofH011,eoc_1.ACOutofH1259,eoc_1.ACGuests,eoc_1.ACCovMob,eoc_1.ACNewBorn,eoc_1.ACAlreadyVaccinated,eoc_1.UnRecCov

#             ,eoc_1.PersistentlyMC,eoc_1.PersistentlyMCHRMP, eoc_1.fm_issued,eoc_1.fm_retrieved

#             ,eoc_1.VaccinationDate,eoc_1.OPVGiven,eoc_1.OPVUsed,eoc_1.OPVReturned,eoc_1.status,eoc_1.trash,eoc_1.isSync

#             ,eoc_1.UserID,CategoryID,eoc_1.Proposed_HH_Targets,eoc_1.Final_HH_Targets ,eoc_1.Seasonal,eoc_1.Nomads,eoc_1.Agriculture,eoc_1.Beggers,eoc_1.IDPs,eoc_1.Family,eoc_1.Others,eoc_1.Status,eoc_1.Trash 
#             ,eoc_1.Remarks,eoc_1.location_code 
#             ,eoc_1.campaign_ID, eoc_1.campaign_ActivityName, eoc_1.campaign_ActivityID_old, eoc_1.campaign_Yr, eoc_1.campaign_SubActivityName,  
            
#             eoc_1.geoLocation_name, eoc_1.geoLocation_type, eoc_1.geoLocation_code, eoc_1.geoLocation_census_pop, eoc_1.geoLocation_target, eoc_1.geoLocation_status, eoc_1.geoLocation_pname, eoc_1.geoLocation_dname, eoc_1.geoLocation_namedistrict, eoc_1.geoLocation_codedistrict, eoc_1.geoLocation_tname, eoc_1.geoLocation_provincecode, eoc_1.geoLocation_districtcode, eoc_1.geoLocation_tehsilcode, eoc_1.geoLocation_priority, eoc_1.geoLocation_commnet, eoc_1.geoLocation_hr, eoc_1.geoLocation_fcm, eoc_1.geoLocation_tier, eoc_1.geoLocation_block, eoc_1.geoLocation_division, eoc_1.geoLocation_cordinates, eoc_1.geoLocation_latitude, eoc_1.geoLocation_longitude, eoc_1.geoLocation_x, eoc_1.geoLocation_y, eoc_1.geoLocation_imagepath, eoc_1.geoLocation_isccpv, eoc_1.geoLocation_rank, eoc_1.geoLocation_rank_score, eoc_1.geoLocation_ishealthcamp, eoc_1.geoLocation_isdsc, eoc_1.geoLocation_ucorg, eoc_1.geoLocation_organization, eoc_1.geoLocation_tierfromaug161, eoc_1.geoLocation_tierfromsep171, eoc_1.geoLocation_tierfromdec181, eoc_1.geoLocation_mtap, eoc_1.geoLocation_rspuc, eoc_1.geoLocation_issmt, eoc_1.geoLocation_updateddatetime, eoc_1.geoLocation_x_code, eoc_1.geoLocation_draining_uc, eoc_1.geoLocation_upap_districts, eoc_1.geoLocation_shruc, eoc_1.geoLocation_khidist_id
#             """
#     sql = "SELECT " + cols + "  FROM test.get_ProcessCubeTest  eoc_1"# left JOIN test.xbi_hhtarget hht on (hht.Yr = eoc_1.Yr and hht.ActivityID_fk = eoc_1.ActivityID and hht.Ucode = eoc_1.location_code) left JOIN test.eoc_geolocation_t eoc_2 ON eoc_1.location_code  = eoc_1.code left JOIN test.xbi_campaign eoc_3 ON (eoc_1.ActivityID  = eoc_3.campaign_ActivityID_old And eoc_1.Yr = eoc_3.campaign_Yr) "
#     data = client.execute(sql)

#     xbiDataFrame = pd.DataFrame(data)
#     all_columns = list(xbiDataFrame)  # Creates list of all column headers
#     cols = xbiDataFrame.iloc[0]

#     xbiDataFrame[all_columns] = xbiDataFrame[all_columns].astype(str)

#     d = ['ID', 'IDcampCat',	'ActivityID','Yr','TimeStamp','Cday', 'TehsilID', 'UCID', 
#         'ccpvTargets','TeamsRpt','HH011_MP','HH1259_MP','HH011_TS','HH1259_TS','HH011','HH1259','OutofH011','OutofH1259',
#         'RecNA011','RecNA1259','RecRef011','RecRef1259','CovNA011','CovNA1259','CovRef011','CovRef1259','Guests', 
#         'VaccinatedSchool','NewBorn','AlreadyVaccinated','FixSite011','FixSite1259','Transit011','Transit1259','CovMob','HHvisit','HHPlan','MultipleFamily','Weakness','ZeroRt','Stage34',
#         'Inaccessible','V_Age611','v_Age1259','V_capGiven','V_capUsed','V_capRet','trigers','Covid19','campcat_name'
#         ,'UserName','DistID','DivID','ProvID'
#         ,'CCovNA011', 'CCovNA1259','CCovRef011','CCovRef1259'
#         ,'COutofH011','COutofH1259','CGuests','CCovMob','CNewBorn','CAlreadyVaccinated','CFixSite011','CFixSite1259','CTransit011','CTransit1259','UnRecorded_Cov'
#         ,'AFP_CaseRpt','ZeroDos1','V_Age611_CP','v_Age1259_CP','missing_vacc_carr','missing_vacc_carr_reason'
#         ,'ACCovNA011','ACCovNA1259','ACCovRef011','ACCovRef1259','ACOutofH011','ACOutofH1259','ACGuests','ACCovMob','ACNewBorn','ACAlreadyVaccinated','UnRecCov'
#         ,'PersistentlyMC','PersistentlyMCHRMP', 'fm_issued','fm_retrieved'
#         ,'VaccinationDate','OPVGiven','OPVUsed','OPVReturned','status','trash','isSync'
        
#         ,'UserID','CategoryID','Proposed_HH_Targets','Final_HH_Targets','Seasonal','Nomads','Agriculture','Beggers','IDPs','Family','Others','Status','Trash'
            
#         ,'Remarks','location_code'
#         ,'campaign_ID', 'campaign_ActivityName', 'campaign_ActivityID_old', 'campaign_Yr', 'campaign_SubActivityName'
#         ,'geoLocation_name', 'geoLocation_type', 'geoLocation_code', 'geoLocation_census_pop', 'geoLocation_target', 'geoLocation_status', 'geoLocation_pname', 'geoLocation_dname', 'geoLocation_namedistrict', 'geoLocation_codedistrict', 'geoLocation_tname', 'geoLocation_provincecode', 'geoLocation_districtcode', 'geoLocation_tehsilcode', 'geoLocation_priority', 'geoLocation_commnet', 'geoLocation_hr', 'geoLocation_fcm', 'geoLocation_tier', 'geoLocation_block', 'geoLocation_division', 'geoLocation_cordinates', 'geoLocation_latitude', 'geoLocation_longitude', 'geoLocation_x', 'geoLocation_y', 'geoLocation_imagepath', 'geoLocation_isccpv', 'geoLocation_rank', 'geoLocation_rank_score', 'geoLocation_ishealthcamp', 'geoLocation_isdsc', 'geoLocation_ucorg', 'geoLocation_organization', 'geoLocation_tierfromaug161', 'geoLocation_tierfromsep171', 'geoLocation_tierfromdec181', 'geoLocation_mtap', 'geoLocation_rspuc', 'geoLocation_issmt', 'geoLocation_updateddatetime', 'geoLocation_x_code', 'geoLocation_draining_uc', 'geoLocation_upap_districts', 'geoLocation_shruc', 'geoLocation_khidist_id' 
#         ]
#     dff = pd.DataFrame(columns=d)
#     for index, item in enumerate(d):
#         dff[item] = xbiDataFrame[index].values
#     logger.info(
#             'Get Data from test.get_ProcessCubeTest for campaign')#+str(cid))

#     df3 = client.query_dataframe(
#                     "SELECT * FROM test.xbi_ProcessCubeTest")
#     if df3.empty:
#         client.insert_dataframe(
#             'INSERT INTO test.xbi_ProcessCubeTest  VALUES', dff)
#         logger.info(
#             'Data has been inserted into Table test.xbi_ProcessCubeTest for campaign ')#+str(cid))

#         sql = "DROP table if exists test.get_ProcessCubeTest"
#         client.execute(sql)
    
#     else:
#         client.insert_dataframe(
#             'INSERT INTO test.xbi_ProcessCubeTest VALUES', dff)
#         logger.info(
#             ' Data has been inserted into Table test.xbi_ProcessCubeTest for campaign ')#+str(cid))    

#         sql = "DROP table if exists test.get_ProcessCubeTest"
#         client.execute(sql)
                       
                        
 
# dag = DAG(
#     'TestDag_Automated',
#     #schedule_interval='*/10 * * * *',# will run every 10 min.
#     schedule_interval='0 0 * * *',  
#     default_args=default_args,
#     catchup=False)

# with dag:
#     GetAndInsertApiDataTest = PythonOperator(
#         task_id='GetAndInsertApiDataTest',
#         python_callable=GetAndInsertApiDataTest,
#     )
# GetAndInsertApiDataTest


#-------------------------------------------------------------------------------------------------------------------------------#
#---------------------------------------- INSERT CAMPAIGN DATA ICM Supervisor   ------------------------------------------------#
#-------------------------------------------- Author: Abdul Bari Malik ---------------------------------------------------------#
#-------------------------------------------------------------------------------------------------------------------------------#
# def GetAndInsertApiDataTest():
    
#     li3 = [222,223,224,225,226,227,228,229,230,231,232,233,234,235,236,237,238,239,240,241,242,243,244,245,247,248,249,250,251,252,253,254,255,256]
    
#     logger.info('Function \' GetAndInsertApiDataTest \' Started Off')
#     client = Client(host='161.97.136.95',
#                     user='default',
#                     password='pakistan',
#                     port='9000', settings={"use_numpy": True})

#     for cid in li3:                                
#         url = "http://idims.eoc.gov.pk/api_who/api/get_icm/5468XE2LN6CzR7qRG041/supervisor/"+ str(cid)+"/"
#         print('\n\n\n Campaign-ID\t\t--------\t', cid)
#         logger.info('Requested Data From Api URL:'+url)

#         r = requests.get(url)
#         data = r.json()
        
#         if data['data'] == "No data found":
#             print("Not Data found for campaign: "+str(cid))
#             logger.info('Not Data found for campaign: '+str(cid))
#         else:
#             print("Data found for campaign: "+str(cid))
#             df = pd.DataFrame()
#             print("Campaign_Total_Pages",data['total_page'])
#             i = 1
#             while int(i) <= data['total_page']:
#                 url_c = "http://idims.eoc.gov.pk/api_who/api/get_icm/5468XE2LN6CzR7qRG041/supervisor/"+str(cid)+'/'+str(i)
#                 r2 = requests.get(url_c)
#                 data_c = r2.json()
#                 if data_c['data'] == "No data found":
#                     print("Not Data found for campaign: "+url_c)
#                     break
#                 else:
#                     rowsData = data_c["data"]["data"]
#                     print("Data found for campaign: "+url_c)
#                     print("Data found for Page: "+str(i))
#                     apiDataFrame = pd.DataFrame(rowsData)
#                     print("DF Page Size",len(apiDataFrame))
#                     df = df.append(apiDataFrame, ignore_index=True)
#                     print("DF API Size",len(df))
#                     i+=1
                    
#                     logger.info('Received Data  From Api URL: '+url_c)    

#             sql = "CREATE TABLE if not exists test.get_ICMTest (pk_icm_supervisor_21_id Int32, fk_form_id Int32, fk_user_id Int32,date_created DateTime('Asia/Karachi'), campaign_id Int32, surveyor_name String,designation String, surveyor_affliate String,fk_prov_id Int32,fk_dist_id Int32, fk_tehsil_id Int32, fk_uc_id Int32, g1_Cday String,g1_supv_name String,g1_supv_desg String,g1_supv_agency String,g1_sup_trained_count Int32,g1_sup_t_date String,g1_sup_ttrained_count Int32,g1_sup_tt_date String,g1_items_tsupport_count Int32,g1_track_mopv2 Int32,g1_sup_suprvsn Int32,g1_Comp_tm_hhform Int32,g1_hrp_present String,g1_Comment String,col_covid_brief_guidline Int32,col_covid_wear_mask Int32,col_covid_use_santzr Int32,col_covid_social_dist Int32,g1_gis_mapavail_count String,g1_gis_mapuse String,g1_gis_mapworkload String,unique_id String,dev_remarks String,app_version String,vac_type Int32,signed_vm03 Int32,supp_clear Int32,is_archive Int32, update_by Int32, archive_reason String,date_updated DateTime('Asia/Karachi'))ENGINE = MergeTree PRIMARY KEY pk_icm_supervisor_21_id ORDER BY pk_icm_supervisor_21_id"
#             client.execute(sql)

#             df2 = client.query_dataframe(
#                 "SELECT * FROM test.get_ICMTest")
#             if df2.empty:
#                 df['date_created'] = pd.to_datetime(df['date_created'],unit='s')
#                 df['date_created'] = df['date_created'].dt.strftime('%d-%m-%Y  %H:%M:%S')
#                 df['date_updated'] = pd.to_datetime(df['date_updated'],unit='s')
#                 df['date_updated'] = df['date_updated'].dt.strftime('%d-%m-%Y %H:%M:%S')
#                 # df['date_created'] = pd.to_datetime(df['date_created'], errors='coerce')
#                 # df['date_updated'] = pd.to_datetime(df['date_updated'], errors='coerce')
#                 df = df.replace(r'^\s*$', np.nan, regex=True)
#                 client.insert_dataframe(
#                     'INSERT INTO test.get_ICMTest VALUES', df)
#                 logger.info(
#                         ' Data has been inserted into Table test.get_ICMTest for campaign'+str(cid))    
#             sql = "CREATE TABLE if not exists test.xbi_Test (pk_icm_supervisor_21_id Int32, fk_form_id Int32, fk_user_id Int32,date_created DateTime('Asia/Karachi'), campaign_id Int32, surveyor_name String,designation String, surveyor_affliate String,fk_prov_id Int32,fk_dist_id Int32, fk_tehsil_id Int32, fk_uc_id Int32, g1_Cday String,g1_supv_name String,g1_supv_desg String,g1_supv_agency String,g1_sup_trained_count Int32,g1_sup_t_date String,g1_sup_ttrained_count Int32,g1_sup_tt_date String,g1_items_tsupport_count Int32,g1_track_mopv2 Int32,g1_sup_suprvsn Int32,g1_Comp_tm_hhform Int32,g1_hrp_present String,g1_Comment String,col_covid_brief_guidline Int32,col_covid_wear_mask Int32,col_covid_use_santzr Int32,col_covid_social_dist Int32,g1_gis_mapavail_count String,g1_gis_mapuse String,g1_gis_mapworkload String,unique_id String,dev_remarks String,app_version String,vac_type Int32,signed_vm03 Int32,supp_clear Int32,is_archive Int32, update_by Int32, archive_reason String,date_updated DateTime('Asia/Karachi'),campaign_ID Int32, campaign_ActivityName String, campaign_ActivityID_old Int32, campaign_Yr Int32, campaign_SubActivityName String, geoLocation_name String, geoLocation_type String, geoLocation_code String, geoLocation_census_pop String, geoLocation_target String, geoLocation_status String, geoLocation_pname String, geoLocation_dname String, geoLocation_namedistrict String, geoLocation_codedistrict String, geoLocation_tname String, geoLocation_provincecode String, geoLocation_districtcode String, geoLocation_tehsilcode String, geoLocation_priority String, geoLocation_commnet String, geoLocation_hr String, geoLocation_fcm String, geoLocation_tier String, geoLocation_block String, geoLocation_division String, geoLocation_cordinates String, geoLocation_latitude String, geoLocation_longitude String, geoLocation_x String, geoLocation_y String, geoLocation_imagepath String, geoLocation_isccpv String, geoLocation_rank String, geoLocation_rank_score String, geoLocation_ishealthcamp String, geoLocation_isdsc String, geoLocation_ucorg String, geoLocation_organization String, geoLocation_tierfromaug161 String, geoLocation_tierfromsep171 String, geoLocation_tierfromdec181 String, geoLocation_mtap String, geoLocation_rspuc String, geoLocation_issmt String, geoLocation_updateddatetime String, geoLocation_x_code String, geoLocation_draining_uc String, geoLocation_upap_districts String, geoLocation_shruc String, geoLocation_khidist_id String )ENGINE = MergeTree PRIMARY KEY pk_icm_supervisor_21_id ORDER BY pk_icm_supervisor_21_id"
#             client.execute(sql)
#             cols = "test.get_ICMTest.pk_icm_supervisor_21_id, test.get_ICMTest.fk_form_id, test.get_ICMTest.test.get_ICMTest.fk_user_id,test.get_ICMTest.date_created, test.get_ICMTest.campaign_id, test.get_ICMTest.surveyor_name,test.get_ICMTest.designation, test.get_ICMTest.surveyor_affliate,test.get_ICMTest.fk_prov_id,test.get_ICMTest.fk_dist_id,test.get_ICMTest.fk_tehsil_id,test.get_ICMTest.fk_uc_id, test.get_ICMTest.g1_Cday,test.get_ICMTest.g1_supv_name,test.get_ICMTest.g1_supv_desg,test.get_ICMTest.g1_supv_agency,test.get_ICMTest.g1_sup_trained_count,test.get_ICMTest.g1_sup_t_date,test.get_ICMTest.g1_sup_ttrained_count,test.get_ICMTest.g1_sup_tt_date,test.get_ICMTest.g1_items_tsupport_count,test.get_ICMTest.g1_track_mopv2,test.get_ICMTest.g1_sup_suprvsn,test.get_ICMTest.g1_Comp_tm_hhform,test.get_ICMTest.g1_hrp_present,test.get_ICMTest.g1_Comment,test.get_ICMTest.col_covid_brief_guidline,test.get_ICMTest.col_covid_wear_mask,test.get_ICMTest.col_covid_use_santzr,test.get_ICMTest.col_covid_social_dist,test.get_ICMTest.g1_gis_mapavail_count,test.get_ICMTest.g1_gis_mapuse,test.get_ICMTest.g1_gis_mapworkload,test.get_ICMTest.unique_id, test.get_ICMTest.dev_remarks,test.get_ICMTest.app_version,test.get_ICMTest.vac_type,test.get_ICMTest.signed_vm03,test.get_ICMTest.supp_clear,test.get_ICMTest.is_archive,test.get_ICMTest.update_by,test.get_ICMTest.archive_reason,test.get_ICMTest.date_updated,test.xbi_campaign.campaign_ID,test.xbi_campaign.campaign_ActivityName,test.xbi_campaign.campaign_ActivityID_old,test.xbi_campaign.campaign_Yr,test.xbi_campaign.campaign_SubActivityName,test.eoc_geolocation_t.name, test.eoc_geolocation_t.type, test.eoc_geolocation_t.code, test.eoc_geolocation_t.census_pop, test.eoc_geolocation_t.target, test.eoc_geolocation_t.status, test.eoc_geolocation_t.pname, test.eoc_geolocation_t.dname, test.eoc_geolocation_t.namedistrict, test.eoc_geolocation_t.codedistrict, test.eoc_geolocation_t.tname, test.eoc_geolocation_t.provincecode, test.eoc_geolocation_t.districtcode, test.eoc_geolocation_t.tehsilcode, test.eoc_geolocation_t.priority, test.eoc_geolocation_t.commnet, test.eoc_geolocation_t.hr, test.eoc_geolocation_t.fcm, test.eoc_geolocation_t.tier, test.eoc_geolocation_t.block, test.eoc_geolocation_t.division, test.eoc_geolocation_t.cordinates, test.eoc_geolocation_t.latitude, test.eoc_geolocation_t.longitude, test.eoc_geolocation_t.x, test.eoc_geolocation_t.y, test.eoc_geolocation_t.imagepath, test.eoc_geolocation_t.isccpv, test.eoc_geolocation_t.rank, test.eoc_geolocation_t.rank_score, test.eoc_geolocation_t.ishealthcamp, test.eoc_geolocation_t.isdsc, test.eoc_geolocation_t.ucorg, test.eoc_geolocation_t.organization, test.eoc_geolocation_t.tierfromaug161, test.eoc_geolocation_t.tierfromsep171, test.eoc_geolocation_t.tierfromdec181, test.eoc_geolocation_t.mtap, test.eoc_geolocation_t.rspuc, test.eoc_geolocation_t.issmt, test.eoc_geolocation_t.updateddatetime, test.eoc_geolocation_t.x_code, test.eoc_geolocation_t.draining_uc, test.eoc_geolocation_t.upap_districts, test.eoc_geolocation_t.shruc, test.eoc_geolocation_t.khidist_id     "
#             sql = "SELECT " + cols + "  FROM test.get_ICMTest eoc_1 left join test.eoc_geolocation_t eoc_2 ON eoc_1.fk_uc_id  = eoc_1.code left JOIN test.xbi_campaign eoc_3 ON eoc_1.campaign_id  = eoc_3.campaign_ID "

#             data = client.execute(sql)
            
#             apiDataFrame = pd.DataFrame(data)
#             apiDataFrame = apiDataFrame.replace(r'^\s*$', np.nan, regex=True)
#             all_columns = list(apiDataFrame)  # Creates list of all column headers
            
#             cols = apiDataFrame.iloc[0]
#             apiDataFrame[all_columns] = apiDataFrame[all_columns].astype(str)
#             d = 'pk_icm_supervisor_21_id','fk_form_id','fk_user_id','date_created','campaign_id','surveyor_name','designation','surveyor_affliate','fk_prov_id','fk_dist_id','fk_tehsil_id','fk_uc_id','g1_Cday','g1_supv_name','g1_supv_desg','g1_supv_agency','g1_sup_trained_count','g1_sup_t_date','g1_sup_ttrained_count','g1_sup_tt_date','g1_items_tsupport_count','g1_track_mopv2','g1_sup_suprvsn','g1_Comp_tm_hhform','g1_hrp_present','g1_Comment','col_covid_brief_guidline','col_covid_wear_mask','col_covid_use_santzr','col_covid_social_dist','g1_gis_mapavail_count','g1_gis_mapuse','g1_gis_mapworkload','unique_id','dev_remarks','app_version','vac_type','signed_vm03','supp_clear','is_archive','update_by','archive_reason','date_updated', 'campaign_ID', 'campaign_ActivityName', 'campaign_ActivityID_old', 'campaign_Yr', 'campaign_SubActivityName','geoLocation_name', 'geoLocation_type', 'geoLocation_code', 'geoLocation_census_pop', 'geoLocation_target', 'geoLocation_status', 'geoLocation_pname', 'geoLocation_dname', 'geoLocation_namedistrict', 'geoLocation_codedistrict', 'geoLocation_tname', 'geoLocation_provincecode', 'geoLocation_districtcode', 'geoLocation_tehsilcode', 'geoLocation_priority', 'geoLocation_commnet', 'geoLocation_hr', 'geoLocation_fcm','geoLocation_tier','geoLocation_block','geoLocation_division','geoLocation_cordinates','geoLocation_latitude','geoLocation_longitude','geoLocation_x','geoLocation_y','geoLocation_imagepath','geoLocation_isccpv','geoLocation_rank','geoLocation_rank_score','geoLocation_ishealthcamp','geoLocation_isdsc','geoLocation_ucorg','geoLocation_organization','geoLocation_tierfromaug161','geoLocation_tierfromsep171','geoLocation_tierfromdec181','geoLocation_mtap','geoLocation_rspuc','geoLocation_issmt','geoLocation_updateddatetime','geoLocation_x_code','geoLocation_draining_uc','geoLocation_upap_districts','geoLocation_shruc','geoLocation_khidist_id' 
#             dff = pd.DataFrame(columns=d)
#             for index, item in enumerate(d):
#                 dff[item] = apiDataFrame[index].values
#             df3 = client.query_dataframe(
#                 "SELECT * FROM test.xbi_Test")
#             if df3.empty:
#                 dff[["date_created"]] = dff[["date_created"]].apply(pd.to_datetime)
#                 dff[["date_updated"]] = dff[["date_updated"]].apply(pd.to_datetime)
#                 client.insert_dataframe(
#                     'INSERT INTO test.xbi_Test  VALUES', dff)
#                 logger.info(
#                     ' Data has been inserted into Table\' INSERT INTO test.xbi_Test  VALUES \' ')

#                 sql = "DROP table if exists test.get_ICMTest"
#                 client.execute(sql)
#                 print('\n\n\n CID\t\t--------\t', cid)
#                 print('\n\n\n Page\t\t--------\t', i)
#             else:
#                 dff[["date_created"]] = dff[["date_created"]].apply(pd.to_datetime)
#                 dff[["date_updated"]] = dff[["date_updated"]].apply(pd.to_datetime)
#                 client.insert_dataframe(
#                     'INSERT INTO test.xbi_Test  VALUES', dff)
#                 logger.info(
#                         ' Data has been inserted into Table test.xbi_Test for campaign '+str(cid))    
#                 print('\n\n\n CID\t\t--------\t', cid)
#                 print('\n\n\n Page\t\t--------\t', i)
                
#                 sql = "DROP table if exists test.get_ICMTest"
#                 client.execute(sql)
                        
# dag = DAG(
#     'TestDag_Automated',
#     #schedule_interval='*/10 * * * *',# will run every 10 min.
#     schedule_interval='0 0 * * *',  
#     default_args=default_args,
#     catchup=False)

# with dag:
#     GetAndInsertApiDataTest = PythonOperator(
#         task_id='GetAndInsertApiDataTest',
#         python_callable=GetAndInsertApiDataTest,
#     )
# GetAndInsertApiDataTest

#-------------------------------------------------------------------------------------------------------------------------------#
#-------------------------------------- INSERT LATEST CAMPAIGN DATA ICM_Supervisor_Monitoring ----------------------------------#
#----------------------------------------------- Author: Abdul Bari Malik ------------------------------------------------------#
#-------------------------------------------------------------------------------------------------------------------------------#
import db_connection as dbConn


def GetAndInsertApiDataTest():
    logger.info('Function \' GetAndInsertApiDataTest \' Started Off')
    client = dbConn.getConnectionDev()
    # client = Client(host='161.97.136.95',
    #                 user='default',
    #                 password='pakistan',
    #                 port='9000', settings={"use_numpy": True})
    
    li3 = [256]
    merged_df = pd.DataFrame()

    for cid in li3:                                
        url = "http://idims.eoc.gov.pk/api_who/api/get_icm/5468XE2LN6CzR7qRG041/supervisor/"+ str(cid)+"/"
        print('\n\n\n Campaign-ID\t\t--------\t', cid)
        logger.info('Requested Data From Api URL:'+url)

        r = requests.get(url)
        data = r.json()
        
        if data['data'] == "No data found":
            print("Not Data found for campaign: "+str(cid))
            logger.info('Not Data found for campaign: '+str(cid))
        else:
            print("Data found for campaign: "+str(cid))
            df = pd.DataFrame()
            print("Campaign_Total_Pages",data['total_page'])
            i = 1
            while int(i) <= data['total_page']:
                url_c = "http://idims.eoc.gov.pk/api_who/api/get_icm/5468XE2LN6CzR7qRG041/supervisor/"+str(cid)+'/'+str(i)
                r2 = requests.get(url_c)
                data_c = r2.json()
                if data_c['data'] == "No data found":
                    print("Not Data found for campaign: "+url_c)
                    break
                else:
                    rowsData = data_c["data"]["data"]
                    print("Data found for campaign: "+url_c)
                    print("Data found for Page: "+str(i))
                    apiDataFrame = pd.DataFrame(rowsData)
                    print("DF Page Size",len(apiDataFrame))
                    df = df.append(apiDataFrame, ignore_index=True)
                    print("DF API Size",len(df))
                    i+=1
                    
                    logger.info('Received Data  From Api URL: '+url_c)    

            merged_df = merged_df.append(df, ignore_index=True)
            print("Merged DF Size",len(merged_df))
    
            sql = "CREATE TABLE if not exists test.get_ICMTest (pk_icm_supervisor_21_id Int32, fk_form_id Int32, fk_user_id Int32,date_created  DateTime, campaign_id Int32, surveyor_name String,designation String, surveyor_affliate String,fk_prov_id Int32,fk_dist_id Int32, fk_tehsil_id Int32, fk_uc_id Int32, g1_Cday String,g1_supv_name String,g1_supv_desg String,g1_supv_agency String,g1_sup_trained_count Int32,g1_sup_t_date String,g1_sup_ttrained_count Int32,g1_sup_tt_date String,g1_items_tsupport_count Int32,g1_track_mopv2 Int32,g1_sup_suprvsn Int32,g1_Comp_tm_hhform Int32,g1_hrp_present String,g1_Comment String,col_covid_brief_guidline Int32,col_covid_wear_mask Int32,col_covid_use_santzr Int32,col_covid_social_dist Int32,g1_gis_mapavail_count String,g1_gis_mapuse String,g1_gis_mapworkload String,unique_id String,dev_remarks String,app_version String,vac_type Int32,signed_vm03 Int32,supp_clear Int32,is_archive Int32, update_by Int32, archive_reason String,date_updated DateTime('Asia/Karachi'))ENGINE = MergeTree PRIMARY KEY pk_icm_supervisor_21_id ORDER BY pk_icm_supervisor_21_id"
            client.execute(sql)

            df2 = client.query_dataframe(
                "SELECT * FROM test.get_ICMTest")
            if df2.empty:
                df['date_created'] = pd.to_datetime(df['date_created'],unit='s')
                df['date_created'] = df['date_created'].dt.strftime('%d-%m-%Y  %H:%M:%S')
                df['date_updated'] = pd.to_datetime(df['date_updated'],unit='s')
                df['date_updated'] = df['date_updated'].dt.strftime('%d-%m-%Y %H:%M:%S')
                df = df.replace(r'^\s*$', np.nan, regex=True)
                client.insert_dataframe(
                    'INSERT INTO test.get_ICMTest VALUES', df)
                logger.info(
                        ' Data has been inserted into Table test.xbi_Test for campaign'+str(cid))    

def CreateJoinTableOfApiDataTest():
    logger.info(' Function  \' CreateJoinTableOfApiDataTest \' has been Initiated')
    client = dbConn.getConnectionDev()
    # client = Client(host='161.97.136.95',
    #                 user='default',
    #                 password='pakistan',
    #                 port='9000', settings={"use_numpy": True})

    cid = 256

    sql = "CREATE TABLE if not exists test.xbi_Test (pk_icm_supervisor_21_id Int32, fk_form_id Int32, fk_user_id Int32,date_created DateTime, campaign_id Int32, surveyor_name String,designation String, surveyor_affliate String,fk_prov_id Int32,fk_dist_id Int32, fk_tehsil_id Int32, fk_uc_id Int32, g1_Cday String,g1_supv_name String,g1_supv_desg String,g1_supv_agency String,g1_sup_trained_count Int32,g1_sup_t_date String,g1_sup_ttrained_count Int32,g1_sup_tt_date String,g1_items_tsupport_count Int32,g1_track_mopv2 Int32,g1_sup_suprvsn Int32,g1_Comp_tm_hhform Int32,g1_hrp_present String,g1_Comment String,col_covid_brief_guidline Int32,col_covid_wear_mask Int32,col_covid_use_santzr Int32,col_covid_social_dist Int32,g1_gis_mapavail_count String,g1_gis_mapuse String,g1_gis_mapworkload String,unique_id String,dev_remarks String,app_version String,vac_type Int32,signed_vm03 Int32,supp_clear Int32,is_archive Int32, update_by Int32, archive_reason String,date_updated DateTime('Asia/Karachi'),campaign_ID Int32, campaign_ActivityName String, campaign_ActivityID_old Int32, campaign_Yr Int32, campaign_SubActivityName String, geoLocation_name String, geoLocation_type String, geoLocation_code String, geoLocation_census_pop String, geoLocation_target String, geoLocation_status String, geoLocation_pname String, geoLocation_dname String, geoLocation_namedistrict String, geoLocation_codedistrict String, geoLocation_tname String, geoLocation_provincecode String, geoLocation_districtcode String, geoLocation_tehsilcode String, geoLocation_priority String, geoLocation_commnet String, geoLocation_hr String, geoLocation_fcm String, geoLocation_tier String, geoLocation_block String, geoLocation_division String, geoLocation_cordinates String, geoLocation_latitude String, geoLocation_longitude String, geoLocation_x String, geoLocation_y String, geoLocation_imagepath String, geoLocation_isccpv String, geoLocation_rank String, geoLocation_rank_score String, geoLocation_ishealthcamp String, geoLocation_isdsc String, geoLocation_ucorg String, geoLocation_organization String, geoLocation_tierfromaug161 String, geoLocation_tierfromsep171 String, geoLocation_tierfromdec181 String, geoLocation_mtap String, geoLocation_rspuc String, geoLocation_issmt String, geoLocation_updateddatetime String, geoLocation_x_code String, geoLocation_draining_uc String, geoLocation_upap_districts String, geoLocation_shruc String, geoLocation_khidist_id String )ENGINE = MergeTree PRIMARY KEY pk_icm_supervisor_21_id ORDER BY pk_icm_supervisor_21_id"
    client.execute(sql)
    cols = "test.get_ICMTest.pk_icm_supervisor_21_id, test.get_ICMTest.fk_form_id, test.get_ICMTest.test.get_ICMTest.fk_user_id,test.get_ICMTest.date_created, test.get_ICMTest.campaign_id, test.get_ICMTest.surveyor_name,test.get_ICMTest.designation, test.get_ICMTest.surveyor_affliate,test.get_ICMTest.fk_prov_id, test.get_ICMTest.fk_dist_id, test.get_ICMTest.fk_tehsil_id, test.get_ICMTest.fk_uc_id, test.get_ICMTest.g1_Cday,test.get_ICMTest.g1_supv_name,test.get_ICMTest.g1_supv_desg,test.get_ICMTest.g1_supv_agency,test.get_ICMTest.g1_sup_trained_count, test.get_ICMTest.g1_sup_t_date,test.get_ICMTest.g1_sup_ttrained_count, test.get_ICMTest.g1_sup_tt_date, test.get_ICMTest.g1_items_tsupport_count,test.get_ICMTest.g1_track_mopv2, test.get_ICMTest.g1_sup_suprvsn,test.get_ICMTest.g1_Comp_tm_hhform, test.get_ICMTest.g1_hrp_present,test.get_ICMTest.g1_Comment,test.get_ICMTest.col_covid_brief_guidline,test.get_ICMTest.col_covid_wear_mask,test.get_ICMTest.col_covid_use_santzr,test.get_ICMTest.col_covid_social_dist, test.get_ICMTest.g1_gis_mapavail_count, test.get_ICMTest.g1_gis_mapuse, test.get_ICMTest.g1_gis_mapworkload, test.get_ICMTest.unique_id, test.get_ICMTest.dev_remarks, test.get_ICMTest.app_version, test.get_ICMTest.vac_type, test.get_ICMTest.signed_vm03, test.get_ICMTest.supp_clear, test.get_ICMTest.is_archive, test.get_ICMTest.update_by, test.get_ICMTest.archive_reason, test.get_ICMTest.date_updated, test.xbi_campaign.campaign_ID, test.xbi_campaign.campaign_ActivityName, test.xbi_campaign.campaign_ActivityID_old, test.xbi_campaign.campaign_Yr, test.xbi_campaign.campaign_SubActivityName, test.eoc_geolocation_t.name, test.eoc_geolocation_t.type, test.eoc_geolocation_t.code, test.eoc_geolocation_t.census_pop, test.eoc_geolocation_t.target, test.eoc_geolocation_t.status, test.eoc_geolocation_t.pname, test.eoc_geolocation_t.dname, test.eoc_geolocation_t.namedistrict, test.eoc_geolocation_t.codedistrict, test.eoc_geolocation_t.tname, test.eoc_geolocation_t.provincecode, test.eoc_geolocation_t.districtcode, test.eoc_geolocation_t.tehsilcode, test.eoc_geolocation_t.priority, test.eoc_geolocation_t.commnet, test.eoc_geolocation_t.hr, test.eoc_geolocation_t.fcm, test.eoc_geolocation_t.tier, test.eoc_geolocation_t.block, test.eoc_geolocation_t.division, test.eoc_geolocation_t.cordinates, test.eoc_geolocation_t.latitude, test.eoc_geolocation_t.longitude, test.eoc_geolocation_t.x, test.eoc_geolocation_t.y, test.eoc_geolocation_t.imagepath, test.eoc_geolocation_t.isccpv, test.eoc_geolocation_t.rank, test.eoc_geolocation_t.rank_score, test.eoc_geolocation_t.ishealthcamp, test.eoc_geolocation_t.isdsc, test.eoc_geolocation_t.ucorg, test.eoc_geolocation_t.organization, test.eoc_geolocation_t.tierfromaug161, test.eoc_geolocation_t.tierfromsep171, test.eoc_geolocation_t.tierfromdec181, test.eoc_geolocation_t.mtap, test.eoc_geolocation_t.rspuc, test.eoc_geolocation_t.issmt, test.eoc_geolocation_t.updateddatetime, test.eoc_geolocation_t.x_code, test.eoc_geolocation_t.draining_uc, test.eoc_geolocation_t.upap_districts, test.eoc_geolocation_t.shruc, test.eoc_geolocation_t.khidist_id     "
    sql = "SELECT " + cols + "  FROM test.get_ICMTest eoc_1 left join test.eoc_geolocation_t eoc_2 ON eoc_1.fk_uc_id  = eoc_1.code left JOIN test.xbi_campaign eoc_3 ON eoc_1.campaign_id  = eoc_3.campaign_ID "

    data = client.execute(sql)
            
    apiDataFrame = pd.DataFrame(data)
    apiDataFrame = apiDataFrame.replace(r'^\s*$', np.nan, regex=True)
    all_columns = list(apiDataFrame)  # Creates list of all column headers
            
    cols = apiDataFrame.iloc[0]
    apiDataFrame[all_columns] = apiDataFrame[all_columns].astype(str)
    d = 'pk_icm_supervisor_21_id','fk_form_id','fk_user_id','date_created','campaign_id','surveyor_name','designation','surveyor_affliate','fk_prov_id','fk_dist_id','fk_tehsil_id','fk_uc_id','g1_Cday','g1_supv_name','g1_supv_desg','g1_supv_agency','g1_sup_trained_count','g1_sup_t_date','g1_sup_ttrained_count','g1_sup_tt_date','g1_items_tsupport_count','g1_track_mopv2','g1_sup_suprvsn','g1_Comp_tm_hhform','g1_hrp_present','g1_Comment','col_covid_brief_guidline','col_covid_wear_mask','col_covid_use_santzr','col_covid_social_dist','g1_gis_mapavail_count','g1_gis_mapuse','g1_gis_mapworkload','unique_id','dev_remarks','app_version','vac_type','signed_vm03','supp_clear','is_archive','update_by','archive_reason','date_updated', 'campaign_ID', 'campaign_ActivityName', 'campaign_ActivityID_old', 'campaign_Yr', 'campaign_SubActivityName','geoLocation_name', 'geoLocation_type', 'geoLocation_code', 'geoLocation_census_pop', 'geoLocation_target', 'geoLocation_status', 'geoLocation_pname', 'geoLocation_dname', 'geoLocation_namedistrict', 'geoLocation_codedistrict', 'geoLocation_tname', 'geoLocation_provincecode', 'geoLocation_districtcode', 'geoLocation_tehsilcode', 'geoLocation_priority', 'geoLocation_commnet', 'geoLocation_hr', 'geoLocation_fcm','geoLocation_tier','geoLocation_block','geoLocation_division','geoLocation_cordinates','geoLocation_latitude','geoLocation_longitude','geoLocation_x','geoLocation_y','geoLocation_imagepath','geoLocation_isccpv','geoLocation_rank','geoLocation_rank_score','geoLocation_ishealthcamp','geoLocation_isdsc','geoLocation_ucorg','geoLocation_organization','geoLocation_tierfromaug161','geoLocation_tierfromsep171','geoLocation_tierfromdec181','geoLocation_mtap','geoLocation_rspuc','geoLocation_issmt','geoLocation_updateddatetime','geoLocation_x_code','geoLocation_draining_uc','geoLocation_upap_districts','geoLocation_shruc','geoLocation_khidist_id' 
    dff = pd.DataFrame(columns=d)
    for index, item in enumerate(d):
        dff[item] = apiDataFrame[index].values
    df3 = client.query_dataframe(
        "SELECT * FROM test.xbi_Test")
    if df3.empty:
        dff[["date_created"]] = dff[["date_created"]].apply(pd.to_datetime)
        dff[["date_updated"]] = dff[["date_updated"]].apply(pd.to_datetime)
        client.insert_dataframe(
        'INSERT INTO test.xbi_Test  VALUES', dff)
        logger.info(' Data has been inserted into Table\' INSERT INTO test.xbi_Test  VALUES \' ')

        sql = "DROP table if exists test.get_ICMTest"
        client.execute(sql)
        print('\n\n\n CID\t\t--------\t', cid)
        print('\n\n\n Page\t\t--------\t', i)
    else:
        sql = "ALTER TABLE test.xbi_Test DELETE WHERE campaign_id = 256"
        client.execute(sql)
        
        dff[["date_created"]] = dff[["date_created"]].apply(pd.to_datetime)
        dff[["date_updated"]] = dff[["date_updated"]].apply(pd.to_datetime)
        
        dff = dff.replace(r'^\s*$', np.nan, regex=True)
        client.insert_dataframe(
            'INSERT INTO test.xbi_Test  VALUES', dff)
        logger.info(
            ' Data has been inserted into Table test.xbi_Test for campaign '+str(cid))
        print('\n\n Campaign \t\t--------\t', cid, '\t--------\t Data Inserted \n\n')    
                
        sql = "DROP table if exists test.get_ICMTest"
        client.execute(sql)

dag = DAG(
    'TestDag_Automated',
    schedule_interval='0 0 * * *', # will run daily at mid-night
    #schedule_interval='*/59 * * * *',  # will run every 10 min.
    default_args=default_args,
    catchup=False)

with dag:
    GetAndInsertApiDataTest = PythonOperator(
        task_id='GetAndInsertApiDataTest',
        python_callable=GetAndInsertApiDataTest,
    )
    CreateJoinTableOfApiDataTest = PythonOperator(
        task_id='CreateJoinTableOfApiDataTest',
        python_callable=CreateJoinTableOfApiDataTest,
    )
GetAndInsertApiDataTest >> CreateJoinTableOfApiDataTest


#-------------------------------------------------------------------------------------------------------------------------------#
#---------------------------------------- INSERT CAMPAIGN DATA ICM Transit Site ------------------------------------------------#
#-------------------------------------------------------------------------------------------------------------------------------#

# def GetAndInsertApiDataTest():
    
#     li3 = [222,223,224,225,226,227,228,229,230,231,232,233,234,235,236,237,238,239,240,241,242,243,244,245,247,248,249,250,251,252,253,254,255,256]
    
#     logger.info('Function \' GetAndInsertApiDataTest \' Started Off')
#     client = Client(host='161.97.136.95',
#                         user='default',
#                         password='pakistan',
#                         port='9000', settings={"use_numpy": True})
        
#     for cid in li3:                                
#         url = "http://idims.eoc.gov.pk/api_who/api/get_icm/5468XE2LN6CzR7qRG041/transit/"+ str(cid)+"/"
#         print('\n\n\n F\t\t--------\t', cid)
#         logger.info('Requested Data From Api URL:'+url)

#         r = requests.get(url)
#         data = r.json()
        
#         if data['data'] == "No data found":
#             print("Not Data found for campaign: "+str(cid))
#             logger.info('Not Data found for campaign: '+str(cid))
#         else:
#             print("Data found for campaign: "+str(cid))
#             df = pd.DataFrame()
#             print("Campaign_Total_Pages",data['total_page'])
#             i = 1
#             while int(i) <= data['total_page']:
#                 url_c = "http://idims.eoc.gov.pk/api_who/api/get_icm/5468XE2LN6CzR7qRG041/transit/"+str(cid)+'/'+str(i)
#                 r2 = requests.get(url_c)
#                 data_c = r2.json()
#                 if data_c['data'] == "No data found":
#                     print("Not Data found for campaign: "+url_c)
#                     break
#                 else:
#                     rowsData = data_c["data"]["data"]
#                     print("Data found for campaign: "+url_c)
#                     print("Data found for Page: "+str(i))
#                     apiDataFrame = pd.DataFrame(rowsData)
#                     print("DF Page Size",len(apiDataFrame))
#                     df = df.append(apiDataFrame, ignore_index=True)
#                     print("DF API Size",len(df))
#                     i+=1
                    
#                     logger.info('Received Data  From Api URL: '+url_c)    
#             sql = "CREATE TABLE if not exists test.get_ICMTest ( pk_icm_transit_21_id Int32,fk_form_id Int32,fk_user_id Int32,date_created String,campaign_id Int32,sur_name String,designation String,survyor_affliate String,fk_prov_id Int32,fk_dist_id Int32,fk_tehsil_id Int32,fk_uc_id Int32,g1_postname String,g1_pnametype String,g1_ssupname String,g1_tum_name_1 String,g1_tum_name_2 String,  g1_sup_visit_count String,col_g1_Comments1 String,g2_loc_appr String,g2_loc_ident String,g2_loc_ident_ptp String,g2_loc_train_adlt String,g2_loc_train_zero String,g2_tmem_cnic Int32,g2_tmem_lang Int32,g2_vaccine_carr String,g2_finger_marker String,g2_tally_sheet String,g2_docs_avail String,g2_vial_SOP_cool String,g2_vial_SOP_valid_vvm Int32,g2_vial_SOP_dry Int32,g2_child_vs_doses Int32,g2_transit_point Int32,g2_security_lea String,g2_Comments3 String,unique_id String,dev_remarks String,g2_loc_appr_accessable Int32,g2_loc_appr_visiable Int32,g2_loc_appr_placement Int32,g2_loc_ident_badges Int32,g2_loc_ident_poster Int32,g2_loc_ident_ptp_shade Int32,g2_loc_ident_ptp_caps Int32,g2_loc_train_adlt_trained Int32,g2_loc_train_adlt_adult Int32,g2_loc_train_adlt_local Int32,g2_docs_avail_attendance_sheet String,g2_docs_avail_sup_checklist String,g2_docs_avail_mp String,app_version String,coldchain_protocol String,g2_vial_SOP_intact Int32,g2_vial_SOP_valid_recap Int32, is_archive Int32, update_by Int32, archive_reason String, vacc_ts_cc Int32, date_updated Int32 )ENGINE = MergeTree PRIMARY KEY pk_icm_transit_21_id ORDER BY pk_icm_transit_21_id"
#             client.execute(sql)
#             df2 = client.query_dataframe("SELECT * FROM test.get_ICMTest")
#             if df2.empty:
#                 df = df.replace(r'^\s*$', np.nan, regex=True)
#                 client.insert_dataframe(
#                     'INSERT INTO test.get_ICMTest VALUES', df)
#                 logger.info(
#                         ' Data has been inserted into Table get_ICMTest for campaign'+str(cid))    
#             sql = "CREATE TABLE if not exists test.xbi_Test (pk_icm_transit_21_id Int32,fk_form_id Int32,fk_user_id Int32,date_created String,campaign_id Int32,sur_name String,designation String,survyor_affliate String,fk_prov_id Int32,fk_dist_id Int32,fk_tehsil_id Int32,fk_uc_id Int32,g1_postname String,g1_pnametype String,g1_ssupname String,g1_tum_name_1 String,g1_tum_name_2 String,  g1_sup_visit_count String,col_g1_Comments1 String,g2_loc_appr String,g2_loc_ident String,g2_loc_ident_ptp String,g2_loc_train_adlt String,g2_loc_train_zero String,g2_tmem_cnic Int32,g2_tmem_lang Int32,g2_vaccine_carr String,g2_finger_marker String,g2_tally_sheet String,g2_docs_avail String,g2_vial_SOP_cool String,g2_vial_SOP_valid_vvm Int32,g2_vial_SOP_dry Int32,g2_child_vs_doses Int32,g2_transit_point Int32,g2_security_lea String,g2_Comments3 String,unique_id String,dev_remarks String,g2_loc_appr_accessable Int32,g2_loc_appr_visiable Int32,g2_loc_appr_placement Int32,g2_loc_ident_badges Int32,g2_loc_ident_poster Int32,g2_loc_ident_ptp_shade Int32,g2_loc_ident_ptp_caps Int32,g2_loc_train_adlt_trained Int32,g2_loc_train_adlt_adult Int32,g2_loc_train_adlt_local Int32,g2_docs_avail_attendance_sheet String,g2_docs_avail_sup_checklist String,g2_docs_avail_mp String,app_version String,coldchain_protocol String,g2_vial_SOP_intact Int32,g2_vial_SOP_valid_recap Int32, is_archive Int32, update_by Int32, archive_reason String, vacc_ts_cc Int32, date_updated Int32, campaign_ID Int32, campaign_ActivityName String, campaign_ActivityID_old Int32, campaign_Yr Int32, campaign_SubActivityName String, geoLocation_name String, geoLocation_type String, geoLocation_code String, geoLocation_census_pop String, geoLocation_target String, geoLocation_status String, geoLocation_pname String, geoLocation_dname String, geoLocation_namedistrict String, geoLocation_codedistrict String, geoLocation_tname String, geoLocation_provincecode String, geoLocation_districtcode String, geoLocation_tehsilcode String, geoLocation_priority String, geoLocation_commnet String, geoLocation_hr String, geoLocation_fcm String, geoLocation_tier String, geoLocation_block String, geoLocation_division String, geoLocation_cordinates String, geoLocation_latitude String, geoLocation_longitude String, geoLocation_x String, geoLocation_y String, geoLocation_imagepath String, geoLocation_isccpv String, geoLocation_rank String, geoLocation_rank_score String, geoLocation_ishealthcamp String, geoLocation_isdsc String, geoLocation_ucorg String, geoLocation_organization String, geoLocation_tierfromaug161 String, geoLocation_tierfromsep171 String, geoLocation_tierfromdec181 String, geoLocation_mtap String, geoLocation_rspuc String, geoLocation_issmt String, geoLocation_updateddatetime String, geoLocation_x_code String, geoLocation_draining_uc String, geoLocation_upap_districts String, geoLocation_shruc String, geoLocation_khidist_id String)ENGINE = MergeTree PRIMARY KEY pk_icm_transit_21_id ORDER BY pk_icm_transit_21_id"
#             client.execute(sql)
#             cols = "test.get_ICMTest.pk_icm_transit_21_id,test.get_ICMTest.fk_form_id,test.get_ICMTest.fk_user_id,test.get_ICMTest.date_created,test.get_ICMTest.campaign_id,test.get_ICMTest.sur_name,test.get_ICMTest.designation,test.get_ICMTest.survyor_affliate,test.get_ICMTest.fk_prov_id,test.get_ICMTest.fk_dist_id,test.get_ICMTest.fk_tehsil_id,test.get_ICMTest.fk_uc_id,test.get_ICMTest.g1_postname,test.get_ICMTest.g1_pnametype,test.get_ICMTest.g1_ssupname,test.get_ICMTest.g1_tum_name_1,test.get_ICMTest.g1_tum_name_2,test.get_ICMTest.g1_sup_visit_count,test.get_ICMTest.col_g1_Comments1,test.get_ICMTest.g2_loc_appr,test.get_ICMTest.g2_loc_ident,test.get_ICMTest.g2_loc_ident_ptp,test.get_ICMTest.g2_loc_train_adlt,test.get_ICMTest.g2_loc_train_zero,test.get_ICMTest.g2_tmem_cnic,test.get_ICMTest.g2_tmem_lang,test.get_ICMTest.g2_vaccine_carr,test.get_ICMTest.g2_finger_marker,test.get_ICMTest.g2_tally_sheet,test.get_ICMTest.g2_docs_avail,test.get_ICMTest.g2_vial_SOP_cool,test.get_ICMTest.g2_vial_SOP_valid_vvm,test.get_ICMTest.g2_vial_SOP_dry,test.get_ICMTest.g2_child_vs_doses,test.get_ICMTest.g2_transit_point,test.get_ICMTest.g2_security_lea,test.get_ICMTest.g2_Comments3,test.get_ICMTest.unique_id,test.get_ICMTest.dev_remarks,test.get_ICMTest.g2_loc_appr_accessable,test.get_ICMTest.g2_loc_appr_visiable,test.get_ICMTest.g2_loc_appr_placement,test.get_ICMTest.g2_loc_ident_badges,test.get_ICMTest.g2_loc_ident_poster, test.get_ICMTest.g2_loc_ident_ptp_shade,test.get_ICMTest.g2_loc_ident_ptp_caps,test.get_ICMTest.g2_loc_train_adlt_trained,test.get_ICMTest.g2_loc_train_adlt_adult,test.get_ICMTest.g2_loc_train_adlt_local,test.get_ICMTest.g2_docs_avail_attendance_sheet,test.get_ICMTest.g2_docs_avail_sup_checklist,test.get_ICMTest.g2_docs_avail_mp,test.get_ICMTest.app_version,test.get_ICMTest.coldchain_protocol,test.get_ICMTest.g2_vial_SOP_intact,test.get_ICMTest.g2_vial_SOP_valid_recap, test.get_ICMTest.vacc_ts_cc, test.get_ICMTest.date_updated, test.xbi_campaign.campaign_ID, test.xbi_campaign.campaign_ActivityName, test.xbi_campaign.campaign_ActivityID_old, test.xbi_campaign.campaign_Yr, test.xbi_campaign.campaign_SubActivityName,  test.eoc_geolocation_t.name, test.eoc_geolocation_t.type, test.eoc_geolocation_t.code, test.eoc_geolocation_t.census_pop, test.eoc_geolocation_t.target, test.eoc_geolocation_t.status, test.eoc_geolocation_t.pname, test.eoc_geolocation_t.dname, test.eoc_geolocation_t.namedistrict, test.eoc_geolocation_t.codedistrict, test.eoc_geolocation_t.tname, test.eoc_geolocation_t.provincecode, test.eoc_geolocation_t.districtcode, test.eoc_geolocation_t.tehsilcode, test.eoc_geolocation_t.priority, test.eoc_geolocation_t.commnet, test.eoc_geolocation_t.hr, test.eoc_geolocation_t.fcm, test.eoc_geolocation_t.tier, test.eoc_geolocation_t.block, test.eoc_geolocation_t.division, test.eoc_geolocation_t.cordinates, test.eoc_geolocation_t.latitude, test.eoc_geolocation_t.longitude, test.eoc_geolocation_t.x, test.eoc_geolocation_t.y, test.eoc_geolocation_t.imagepath, test.eoc_geolocation_t.isccpv, test.eoc_geolocation_t.rank, test.eoc_geolocation_t.rank_score, test.eoc_geolocation_t.ishealthcamp, test.eoc_geolocation_t.isdsc, test.eoc_geolocation_t.ucorg, test.eoc_geolocation_t.organization, test.eoc_geolocation_t.tierfromaug161, test.eoc_geolocation_t.tierfromsep171, test.eoc_geolocation_t.tierfromdec181, test.eoc_geolocation_t.mtap, test.eoc_geolocation_t.rspuc, test.eoc_geolocation_t.issmt, test.eoc_geolocation_t.updateddatetime, test.eoc_geolocation_t.x_code, test.eoc_geolocation_t.draining_uc, test.eoc_geolocation_t.upap_districts, test.eoc_geolocation_t.shruc, test.eoc_geolocation_t.khidist_id"
#             sql = "SELECT " + cols + "  FROM test.get_ICMTest eoc_1 left join test.eoc_geolocation_t eoc_2 ON eoc_1.fk_uc_id  = eoc_1.code left JOIN test.xbi_campaign eoc_3 ON eoc_1.campaign_id  = eoc_3.campaign_ID "

#             data = client.execute(sql)
#             apiDataFrame = pd.DataFrame(data)
#             apiDataFrame = apiDataFrame.replace(r'^\s*$', np.nan, regex=True)
#             all_columns = list(apiDataFrame)  # Creates list of all column headers
#             cols = apiDataFrame.iloc[0]
#             apiDataFrame[all_columns] = apiDataFrame[all_columns].astype(str)
#             d = 'pk_icm_transit_21_id','fk_form_id','fk_user_id','date_created','campaign_id','sur_name','designation','survyor_affliate','fk_prov_id','fk_dist_id','fk_tehsil_id','fk_uc_id','g1_postname','g1_pnametype','g1_ssupname','g1_tum_name_1','g1_tum_name_2','g1_sup_visit_count','col_g1_Comments1','g2_loc_appr','g2_loc_ident','g2_loc_ident_ptp','g2_loc_train_adlt','g2_loc_train_zero','g2_tmem_cnic','g2_tmem_lang','g2_vaccine_carr','g2_finger_marker','g2_tally_sheet','g2_docs_avail','g2_vial_SOP_cool','g2_vial_SOP_valid_vvm','g2_vial_SOP_dry','g2_child_vs_doses','g2_transit_point','g2_security_lea','g2_Comments3','unique_id','dev_remarks','g2_loc_appr_accessable','g2_loc_appr_visiable','g2_loc_appr_placement','g2_loc_ident_badges','g2_loc_ident_poster','g2_loc_ident_ptp_shade','g2_loc_ident_ptp_caps','g2_loc_train_adlt_trained','g2_loc_train_adlt_adult','g2_loc_train_adlt_local','g2_docs_avail_attendance_sheet','g2_docs_avail_sup_checklist','g2_docs_avail_mp','app_version','coldchain_protocol','g2_vial_SOP_intact','g2_vial_SOP_valid_recap', 'vacc_ts_cc', 'date_updated', 'campaign_ID', 'campaign_ActivityName', 'campaign_ActivityID_old', 'campaign_Yr', 'campaign_SubActivityName','geoLocation_name', 'geoLocation_type', 'geoLocation_code', 'geoLocation_census_pop', 'geoLocation_target', 'geoLocation_status', 'geoLocation_pname', 'geoLocation_dname', 'geoLocation_namedistrict', 'geoLocation_codedistrict', 'geoLocation_tname', 'geoLocation_provincecode', 'geoLocation_districtcode', 'geoLocation_tehsilcode', 'geoLocation_priority', 'geoLocation_commnet', 'geoLocation_hr', 'geoLocation_fcm', 'geoLocation_tier', 'geoLocation_block', 'geoLocation_division', 'geoLocation_cordinates', 'geoLocation_latitude', 'geoLocation_longitude', 'geoLocation_x', 'geoLocation_y', 'geoLocation_imagepath', 'geoLocation_isccpv', 'geoLocation_rank', 'geoLocation_rank_score', 'geoLocation_ishealthcamp', 'geoLocation_isdsc', 'geoLocation_ucorg', 'geoLocation_organization', 'geoLocation_tierfromaug161', 'geoLocation_tierfromsep171', 'geoLocation_tierfromdec181', 'geoLocation_mtap', 'geoLocation_rspuc', 'geoLocation_issmt', 'geoLocation_updateddatetime', 'geoLocation_x_code', 'geoLocation_draining_uc', 'geoLocation_upap_districts', 'geoLocation_shruc', 'geoLocation_khidist_id' 
#             dff = pd.DataFrame(columns=d)
#             for index, item in enumerate(d):
#                 dff[item] = apiDataFrame[index].values
#                 logger.info(
#                         'Get Data from get_ICMTest for campaign'+str(cid))
#                 #dff[item]= dff[item].replace(r'^\s*$', np.nan, regex=True)
#             df3 = client.query_dataframe(
#                     "SELECT * FROM test.xbi_Test")
#             if df3.empty:
#                 client.insert_dataframe(
#                         'INSERT INTO test.xbi_Test  VALUES', dff)
#                 logger.info(
#                         'Data has been inserted into Table test.xbi_Test for campaign '+str(cid))

#                 sql = "DROP table if exists test.get_ICMTest"
#                 client.execute(sql)
#                 print('\n\n\n F\t\t--------\t', cid)
#                 print('\n\n\n B\t\t--------\t', i)    
#             else:
#                     # df = pd.concat([dff, df2])
#                     # df = df.astype('str')
#                     # df = df.drop_duplicates(subset='pk_icm_fixed_21_id',
#                     #                         keep="first", inplace=False)
#                     # sql = "DROP TABLE if exists  test.xbi_Test;"
#                     # client.execute(sql)
#                     # sql = "CREATE TABLE if not exists test.xbi_Test (pk_icm_fixed_21_id Int32,fk_form_id Int32,fk_user_id Int32,date_created String, icm_campaign_id Int32,surveyor_name String,designation String,surveyor_affliate String,fk_prov_id Int32,fk_dist_id Int32,fk_tehsil_id Int32,fk_uc_id Int32,g1_aic_name String,g1_team_no String,g1_day String,g1_site_name String, g1_fsloc String, g1_vac_chld String,g1_doses String,g1_site_fun Int32,g1_vac_trained_government_accountable Int32,g1_vac_trained_trained Int32,g1_vac_trained_adult Int32,g1_vac_trained_local Int32,g1_opv_dry_valid_vvm Int32,g1_opv_dry_cool String, g1_opv_dry_dry Int32,g1_FS_address Int32,g1_Svisit_fixsite Int32,g1_cat String, g1_micro_avail String,g1_vitaminA String, g1_comments String, epi_center_count Int32,g2_temp_ilr String, g2_r_imm_given String,g2_reg_record_cvacc String,g2_ipv_penta_chart String,site_afp_surv_net Int32,g3_zero_site String, g3_afp_def_visible String,g3_zero_report String,g3_afp_reported String,g3_Comments1 String, covid_brief_guidline Int32,covid_disinfct_matrial Int32,wear_ppe Int32,covid_social_dist Int32,covid_hold_child Int32,unique_id String, dev_remarks String, app_version String, g1_opv_dry_intact Int32,g1_opv_dry_re_capped Int32,match_admin_team Int32,mp_sk_ucmo Int32,mp_ask_ice_aic String, chk_epi_stock String,chk_downloading String,chk_vac_fridge String,chk_ilr_logic String,chk_ilr_logic_reason String, is_archive Int32, update_by Int32, archive_reason String, campaign_ID Int32, campaign_ActivityName String, campaign_ActivityID_old Int32, campaign_Yr Int32, campaign_SubActivityName String, location_ID Int32, location_code Int32, location_name String, location_type String, location_target Int32, location_status Int32, location_priority String, hr_status String)ENGINE = MergeTree PRIMARY KEY pk_icm_fixed_21_id ORDER BY pk_icm_fixed_21_id"
#                     # client.execute(sql)
#                 dff = dff.replace(r'^\s*$', np.nan, regex=True)
#                 client.insert_dataframe(
#                     'INSERT INTO test.xbi_Test  VALUES', dff)
#                 logger.info(
#                         ' Data has been inserted into Table test.xbi_Test for campaign '+str(cid))    
#                 print('\n\n\n F\t\t--------\t', cid)
#                 print('\n\n\n B\t\t--------\t', i)
#                 sql = "DROP table if exists test.get_ICMTest"
#                 client.execute(sql)
                       
                        
 
# dag = DAG(
#     'TestDag_Automated',
#     #schedule_interval='*/10 * * * *',# will run every 10 min.
#     schedule_interval='0 0 * * *',  
#     default_args=default_args,
#     catchup=False)

# with dag:
#     GetAndInsertApiDataTest = PythonOperator(
#         task_id='GetAndInsertApiDataTest',
#         python_callable=GetAndInsertApiDataTest,
#     )
# GetAndInsertApiDataTest

#-------------------------------------------------------------------------------------------------------------------------------#
#-------------------------------------- INSERT LATEST CAMPAIGN DATA ICM_Fixed_Site_Monitoring ----------------------------------#
#-------------------------------------------------------------------------------------------------------------------------------#

# def GetAndInsertApiDataTest():
#     logger.info('Function \' GetAndInsertApiDataTest \' Started Off')
#     client = Client(host='161.97.136.95',
#                     user='default',
#                     password='pakistan',
#                     port='9000', settings={"use_numpy": True})

#     clist = [256]
#     merged_df = pd.DataFrame()

#     for cid in clist:
#         url = "http://idims.eoc.gov.pk/api_who/api/get_icm/5468XE2LN6CzR7qRG041/fixed/"+ str(cid)+"/"
#         print('\n\n\n Campaign-ID\t\t--------\t', cid)
#         logger.info('Requested Data From Api URL:'+url)

#         r = requests.get(url)
#         data = r.json()
        
#         if data['data'] == "No data found":
#             print("No Data found for campaign: "+str(cid))
#             logger.info('No Data found for campaign: '+str(cid))
#         else:
#             print("Data found for campaign: "+str(cid))
#             df = pd.DataFrame()
#             print("Campaign_Total_Pages",data['total_page'])
#             i = 1
#             while int(i) <= data['total_page']:
#                 url_c = "http://idims.eoc.gov.pk/api_who/api/get_icm/5468XE2LN6CzR7qRG041/fixed/"+str(cid)+'/'+str(i)
#                 r2 = requests.get(url_c)
#                 data_c = r2.json()
#                 if data_c['data'] == "No data found":
#                     print("Not Data found for campaign: "+url_c)
#                     break
#                 else:
#                     rowsData = data_c["data"]["data"]
#                     print("Data found for campaign: "+url_c)
#                     print("Data found for Page: "+str(i))
#                     apiDataFrame = pd.DataFrame(rowsData)
#                     print("DF Page Size",len(apiDataFrame))
#                     df = df.append(apiDataFrame, ignore_index=True)
#                     print("DF API Size",len(df))
#                     i+=1
                    
#                     logger.info('Received Data  From Api URL: '+url_c)
#             merged_df = merged_df.append(df, ignore_index=True)
#             print("Merged DF Size",len(merged_df))
#             sql = "CREATE TABLE if not exists test.get_ICMTest ( pk_icm_fixed_21_id Int32, fk_form_id Int32, fk_user_id Int32, date_created String, campaign_id Int32, surveyor_name String, designation String, surveyor_affliate String, fk_prov_id Int32, fk_dist_id Int32, fk_tehsil_id Int32, fk_uc_id Int32, g1_aic_name String, g1_team_no String, g1_day String, g1_site_name String, g1_fsloc String, g1_vac_chld String, g1_doses String, g1_site_fun Int32, g1_vac_trained_government_accountable Int32, g1_vac_trained_trained Int32, g1_vac_trained_adult Int32, g1_vac_trained_local Int32, g1_opv_dry_valid_vvm Int32 ,g1_opv_dry_cool String, g1_opv_dry_dry Int32, g1_FS_address Int32, g1_Svisit_fixsite Int32, g1_cat String, g1_micro_avail String, g1_vitaminA String, g1_comments String, epi_center_count Int32, g2_temp_ilr String, g2_r_imm_given String, g2_reg_record_cvacc String, g2_ipv_penta_chart String, site_afp_surv_net Int32, g3_zero_site String, g3_afp_def_visible String, g3_zero_report String, g3_afp_reported String,g3_Comments1 String,covid_brief_guidline Int32, covid_disinfct_matrial Int32, covid_wear_ppe Int32, covid_social_dist Int32, covid_hold_child Int32, unique_id String, dev_remarks String, app_version String, g1_opv_dry_intact Int32, g1_opv_dry_re_capped Int32, match_admin_team Int32, mp_sk_ucmo Int32, mp_ask_ice_aic String, chk_epi_stock String, chk_downloading String, chk_vac_fridge String, chk_ilr_logic String, chk_ilr_logic_reason String, is_archive Int32, update_by Int32, archive_reason String )ENGINE = MergeTree PRIMARY KEY pk_icm_fixed_21_id ORDER BY pk_icm_fixed_21_id"
#             client.execute(sql)
#             df2 = client.query_dataframe("SELECT * FROM test.get_ICMTest")
#             if df2.empty:
#                 df = df.replace(r'^\s*$', np.nan, regex=True)
#                 client.insert_dataframe(
#                     'INSERT INTO test.get_ICMTest VALUES', df)
#                 logger.info(
#                         ' Data has been inserted into Table get_icm_fixed_site_monitoring for campaign'+str(cid))


# def CreateJoinTableOfIApiDataTest():
#     cid = 256
#     logger.info(' Function  \' CreateJoinTableOfIApiDataTest \' has been Initiated')
#     client = Client(host='161.97.136.95',
#                     user='default',
#                     password='pakistan',
#                     port='9000', settings={"use_numpy": True})
#     sql = "CREATE TABLE if not exists test.xbi_Test (pk_icm_fixed_21_id Int32,fk_form_id Int32,fk_user_id Int32,date_created String, icm_campaign_id Int32,surveyor_name String,designation String,surveyor_affliate String,fk_prov_id Int32,fk_dist_id Int32,fk_tehsil_id Int32,fk_uc_id Int32,g1_aic_name String,g1_team_no String,g1_day String,g1_site_name String, g1_fsloc String, g1_vac_chld String,g1_doses String,g1_site_fun Int32,g1_vac_trained_government_accountable Int32,g1_vac_trained_trained Int32,g1_vac_trained_adult Int32,g1_vac_trained_local Int32,g1_opv_dry_valid_vvm Int32,g1_opv_dry_cool String, g1_opv_dry_dry Int32,g1_FS_address Int32,g1_Svisit_fixsite Int32,g1_cat String, g1_micro_avail String,g1_vitaminA String, g1_comments String, epi_center_count Int32,g2_temp_ilr String, g2_r_imm_given String,g2_reg_record_cvacc String,g2_ipv_penta_chart String,site_afp_surv_net Int32,g3_zero_site String, g3_afp_def_visible String,g3_zero_report String,g3_afp_reported String,g3_Comments1 String, covid_brief_guidline Int32,covid_disinfct_matrial Int32,wear_ppe Int32,covid_social_dist Int32,covid_hold_child Int32,unique_id String, dev_remarks String, app_version String, g1_opv_dry_intact Int32,g1_opv_dry_re_capped Int32,match_admin_team Int32,mp_sk_ucmo Int32,mp_ask_ice_aic String, chk_epi_stock String,chk_downloading String,chk_vac_fridge String,chk_ilr_logic String,chk_ilr_logic_reason String, is_archive Int32, update_by Int32, archive_reason String, campaign_ID Int32, campaign_ActivityName String, campaign_ActivityID_old Int32, campaign_Yr Int32, campaign_SubActivityName String, geoLocation_name String, geoLocation_type String, geoLocation_code String, geoLocation_census_pop String, geoLocation_target String, geoLocation_status String, geoLocation_pname String, geoLocation_dname String, geoLocation_namedistrict String, geoLocation_codedistrict String, geoLocation_tname String, geoLocation_provincecode String, geoLocation_districtcode String, geoLocation_tehsilcode String, geoLocation_priority String, geoLocation_commnet String, geoLocation_hr String, geoLocation_fcm String, geoLocation_tier String, geoLocation_block String, geoLocation_division String, geoLocation_cordinates String, geoLocation_latitude String, geoLocation_longitude String, geoLocation_x String, geoLocation_y String, geoLocation_imagepath String, geoLocation_isccpv String, geoLocation_rank String, geoLocation_rank_score String, geoLocation_ishealthcamp String, geoLocation_isdsc String, geoLocation_ucorg String, geoLocation_organization String, geoLocation_tierfromaug161 String, geoLocation_tierfromsep171 String, geoLocation_tierfromdec181 String, geoLocation_mtap String, geoLocation_rspuc String, geoLocation_issmt String, geoLocation_updateddatetime String, geoLocation_x_code String, geoLocation_draining_uc String, geoLocation_upap_districts String, geoLocation_shruc String, geoLocation_khidist_id String)ENGINE = MergeTree PRIMARY KEY pk_icm_fixed_21_id ORDER BY pk_icm_fixed_21_id"
#     client.execute(sql)
            
#     logger.info('Get Data from get_icm_fixed_site_monitoring for campaign'+str(cid))
#     cols = "test.get_ICMTest.pk_icm_fixed_21_id, test.get_ICMTest.fk_form_id, test.get_ICMTest.fk_user_id, test.get_ICMTest.date_created, test.get_ICMTest.campaign_id, test.get_ICMTest.surveyor_name, test.get_ICMTest.designation, test.get_ICMTest.surveyor_affliate, test.get_ICMTest.fk_prov_id, test.get_ICMTest.fk_dist_id, test.get_ICMTest.fk_tehsil_id, test.get_ICMTest.fk_uc_id, test.get_ICMTest.g1_aic_name, test.get_ICMTest.g1_team_no, test.get_ICMTest.g1_day, test.get_ICMTest.g1_site_name, test.get_ICMTest.g1_fsloc, test.get_ICMTest.g1_vac_chld, test.get_ICMTest.g1_doses, test.get_ICMTest.g1_site_fun, test.get_ICMTest.g1_vac_trained_government_accountable, test.get_ICMTest.g1_vac_trained_trained, test.get_ICMTest.g1_vac_trained_adult, test.get_ICMTest.g1_vac_trained_local, test.get_ICMTest.g1_opv_dry_valid_vvm, test.get_ICMTest.g1_opv_dry_cool, test.get_ICMTest.g1_opv_dry_dry, test.get_ICMTest.g1_FS_address, test.get_ICMTest.g1_Svisit_fixsite, test.get_ICMTest.g1_cat, test.get_ICMTest.g1_micro_avail, test.get_ICMTest.g1_vitaminA, test.get_ICMTest.g1_comments, test.get_ICMTest.epi_center_count, test.get_ICMTest.g2_temp_ilr, test.get_ICMTest.g2_r_imm_given, test.get_ICMTest.g2_reg_record_cvacc, test.get_ICMTest.g2_ipv_penta_chart, test.get_ICMTest.site_afp_surv_net, test.get_ICMTest.g3_zero_site, test.get_ICMTest.g3_afp_def_visible, test.get_ICMTest.g3_zero_report, test.get_ICMTest.g3_afp_reported,test.get_ICMTest.g3_Comments1, test.get_ICMTest.covid_brief_guidline, test.get_ICMTest.covid_disinfct_matrial, test.get_ICMTest.covid_wear_ppe, test.get_ICMTest.covid_social_dist, test.get_ICMTest.covid_hold_child, test.get_ICMTest.unique_id, test.get_ICMTest.dev_remarks, test.get_ICMTest.app_version, test.get_ICMTest.g1_opv_dry_intact, test.get_ICMTest.g1_opv_dry_re_capped, test.get_ICMTest.match_admin_team, test.get_ICMTest.mp_sk_ucmo, test.get_ICMTest.mp_ask_ice_aic, test.get_ICMTest.chk_epi_stock, test.get_ICMTest.chk_downloading,test.get_ICMTest.chk_vac_fridge, test.get_ICMTest.chk_ilr_logic, test.get_ICMTest.chk_ilr_logic_reason, test.get_ICMTest.is_archive, test.get_ICMTest.update_by, test.get_ICMTest.archive_reason, test.xbi_campaign.campaign_ID, test.xbi_campaign.campaign_ActivityName, test.xbi_campaign.campaign_ActivityID_old, test.xbi_campaign.campaign_Yr, test.xbi_campaign.campaign_SubActivityName,  test.eoc_geolocation_t.name, test.eoc_geolocation_t.type, test.eoc_geolocation_t.code, test.eoc_geolocation_t.census_pop, test.eoc_geolocation_t.target, test.eoc_geolocation_t.status, test.eoc_geolocation_t.pname, test.eoc_geolocation_t.dname, test.eoc_geolocation_t.namedistrict, test.eoc_geolocation_t.codedistrict, test.eoc_geolocation_t.tname, test.eoc_geolocation_t.provincecode, test.eoc_geolocation_t.districtcode, test.eoc_geolocation_t.tehsilcode, test.eoc_geolocation_t.priority, test.eoc_geolocation_t.commnet, test.eoc_geolocation_t.hr, test.eoc_geolocation_t.fcm, test.eoc_geolocation_t.tier, test.eoc_geolocation_t.block, test.eoc_geolocation_t.division, test.eoc_geolocation_t.cordinates, test.eoc_geolocation_t.latitude, test.eoc_geolocation_t.longitude, test.eoc_geolocation_t.x, test.eoc_geolocation_t.y, test.eoc_geolocation_t.imagepath, test.eoc_geolocation_t.isccpv, test.eoc_geolocation_t.rank, test.eoc_geolocation_t.rank_score, test.eoc_geolocation_t.ishealthcamp, test.eoc_geolocation_t.isdsc, test.eoc_geolocation_t.ucorg, test.eoc_geolocation_t.organization, test.eoc_geolocation_t.tierfromaug161, test.eoc_geolocation_t.tierfromsep171, test.eoc_geolocation_t.tierfromdec181, test.eoc_geolocation_t.mtap, test.eoc_geolocation_t.rspuc, test.eoc_geolocation_t.issmt, test.eoc_geolocation_t.updateddatetime, test.eoc_geolocation_t.x_code, test.eoc_geolocation_t.draining_uc, test.eoc_geolocation_t.upap_districts, test.eoc_geolocation_t.shruc, test.eoc_geolocation_t.khidist_id"
#     sql = "SELECT " + cols + "  FROM test.get_ICMTest eoc_1 left join test.eoc_geolocation_t eoc_2 ON eoc_1.fk_uc_id  = eoc_1.code left JOIN test.xbi_campaign eoc_3 ON eoc_1.campaign_id  = eoc_3.campaign_ID "
#     data = client.execute(sql)

#     apiDataFrame = pd.DataFrame(data)
#     apiDataFrame = apiDataFrame.replace(r'^\s*$', np.nan, regex=True)
#     all_columns = list(apiDataFrame)  # Creates list of all column headers
#     cols = apiDataFrame.iloc[0]
#     apiDataFrame[all_columns] = apiDataFrame[all_columns].astype(str)
#     d = 'pk_icm_fixed_21_id', 'fk_form_id', 'fk_user_id', 'date_created', 'icm_campaign_id', 'surveyor_name', 'designation', 'surveyor_affliate', 'fk_prov_id', 'fk_dist_id', 'fk_tehsil_id', 'fk_uc_id', 'g1_aic_name', 'g1_team_no', 'g1_day', 'g1_site_name','g1_fsloc','g1_vac_chld','g1_doses','g1_site_fun', 'g1_vac_trained_government_accountable','g1_vac_trained_trained', 'g1_vac_trained_adult', 'g1_vac_trained_local', 'g1_opv_dry_valid_vvm', 'g1_opv_dry_cool', 'g1_opv_dry_dry', 'g1_FS_address', 'g1_Svisit_fixsite', 'g1_cat', 'g1_micro_avail', 'g1_vitaminA', 'g1_comments', 'epi_center_count', 'g2_temp_ilr', 'g2_r_imm_given', 'g2_reg_record_cvacc', 'g2_ipv_penta_chart', 'site_afp_surv_net', 'g3_zero_site','g3_afp_def_visible','g3_zero_report', 'g3_afp_reported', 'g3_Comments1', 'covid_brief_guidline', 'covid_disinfct_matrial', 'wear_ppe', 'covid_social_dist', 'covid_hold_child', 'unique_id', 'dev_remarks', 'app_version', 'g1_opv_dry_intact', 'g1_opv_dry_re_capped', 'match_admin_team','mp_sk_ucmo','mp_ask_ice_aic','chk_epi_stock','chk_downloading', 'chk_vac_fridge','chk_ilr_logic','chk_ilr_logic_reason', 'is_archive', 'update_by', 'archive_reason', 'campaign_ID', 'campaign_ActivityName', 'campaign_ActivityID_old', 'campaign_Yr', 'campaign_SubActivityName','geoLocation_name', 'geoLocation_type', 'geoLocation_code', 'geoLocation_census_pop', 'geoLocation_target', 'geoLocation_status', 'geoLocation_pname', 'geoLocation_dname', 'geoLocation_namedistrict', 'geoLocation_codedistrict', 'geoLocation_tname', 'geoLocation_provincecode', 'geoLocation_districtcode', 'geoLocation_tehsilcode', 'geoLocation_priority', 'geoLocation_commnet', 'geoLocation_hr', 'geoLocation_fcm', 'geoLocation_tier', 'geoLocation_block', 'geoLocation_division', 'geoLocation_cordinates', 'geoLocation_latitude', 'geoLocation_longitude', 'geoLocation_x', 'geoLocation_y', 'geoLocation_imagepath', 'geoLocation_isccpv', 'geoLocation_rank', 'geoLocation_rank_score', 'geoLocation_ishealthcamp', 'geoLocation_isdsc', 'geoLocation_ucorg', 'geoLocation_organization', 'geoLocation_tierfromaug161', 'geoLocation_tierfromsep171', 'geoLocation_tierfromdec181', 'geoLocation_mtap', 'geoLocation_rspuc', 'geoLocation_issmt', 'geoLocation_updateddatetime', 'geoLocation_x_code', 'geoLocation_draining_uc', 'geoLocation_upap_districts', 'geoLocation_shruc', 'geoLocation_khidist_id'
#     dff = pd.DataFrame(columns=d)
#     for index, item in enumerate(d):
#         dff[item] = apiDataFrame[index].values
    
#     df3 = client.query_dataframe("SELECT * FROM test.xbi_Test WHERE icm_campaign_id = 256")
    
#     if df3.empty:
#         client.insert_dataframe('INSERT INTO test.xbi_Test  VALUES', dff)
#         logger.info('Data has been inserted into Table test.xbi_Test for campaign '+str(cid))

#         sql = "DROP table if exists test.get_ICMTest"
#         client.execute(sql)
#         print('\n\n Campaign \t\t--------\t', cid, '\t--------\tData Inserted \n\n')
       
#     else:
#         sql = "ALTER TABLE test.xbi_Test DELETE WHERE icm_campaign_id = 256"
#         client.execute(sql)

#         dff = dff.replace(r'^\s*$', np.nan, regex=True)
#         client.insert_dataframe('INSERT INTO test.xbi_Test  VALUES', dff)
#         logger.info(' Data has been inserted into Table test.xbi_Test for campaign '+str(cid))
#         print('\n\n Campaign \t\t--------\t', cid, '\t--------\t Data Inserted \n\n')
        
#         sql = "DROP table if exists test.get_ICMTest"
#         client.execute(sql)


# dag = DAG(
#     'TestDag_Automated',
#     #schedule_interval='*/59 * * * *',  # will run every 10 min.
#     schedule_interval='0 0 * * *',  # once a day at midnight.
#     #schedule_interval='0 * * * *', # Run once an hour at the beginning of the hour 
#     default_args=default_args,
#     catchup=False)

# with dag:
#     GetAndInsertApiDataTest = PythonOperator(
#         task_id='GetAndInsertApiDataTest',
#         python_callable=GetAndInsertApiDataTest,
#     )
#     CreateJoinTableOfIApiDataTest = PythonOperator(
#         task_id='CreateJoinTableOfIApiDataTest',
#         python_callable=CreateJoinTableOfIApiDataTest,
#     )
# GetAndInsertApiDataTest >> CreateJoinTableOfIApiDataTest

#-------------------------------------------------------------------------------------------------------------------------------#
#------------------------------------------- INSERT LATEST CAMPAIGN DATA ICM Fixed ---------------------------------------------#
#-------------------------------------------------------------------------------------------------------------------------------#

# def GetAndInsertApiDataTest():
#     #li1 = [28, 29, 31, 30, 1, 106, 34, 9, 37, 40, 40, 43, 10, 2, 46, 11, 3, 107, 1, 106, 2, 9, 8, 10, 3, 107, 28, 31, 4, 9, 1, 28, 31, 2]
#     #li2 = [2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2022, 2022, 2022, 2022, 2022]
#     li3 = [222,223,224,225,226,227,228,229,230,231,232,233,234,235,236,237,238,239,240,241,242,243,244,245,247,248,249,250,251,252,253,254,255,256]
#     #li3 = [243,245]
#     #li4 = [1,2,3,4,5,6] zip(li3, li4):
#     for cid in li3:
#         logger.info('Function \' GetAndInsertApiDataTest \' Started Off')
#         client = Client(host='161.97.136.95',
#                         user='default',
#                         password='pakistan',
#                         port='9000', settings={"use_numpy": True})
                                        
#         url = "http://idims.eoc.gov.pk/api_who/api/get_icm/5468XE2LN6CzR7qRG041/fixed/"+ str(cid)+"/"
#         print('\n\n\n F\t\t--------\t', cid)
#         logger.info('Requested Data From Api URL:'+url)

#         r = requests.get(url)
#         data = r.json()
        
#         if data['data'] == "No data found":
#             print("Not Data found for campaign: "+str(cid))
#             logger.info('Not Data found for campaign: '+str(cid))
#         else:
#             print("Data found for campaign: "+str(cid))
#             df = pd.DataFrame()
#             print("Campaign_Total_Pages",data['total_page'])
#             i = 1
#             while int(i) <= data['total_page']:
#                 url_c = "http://idims.eoc.gov.pk/api_who/api/get_icm/5468XE2LN6CzR7qRG041/fixed/"+str(cid)+'/'+str(i)
#                 r2 = requests.get(url_c)
#                 data_c = r2.json()
#                 if data_c['data'] == "No data found":
#                     print("Not Data found for campaign: "+url_c)
#                     break
#                 else:
#                     rowsData = data_c["data"]["data"]
#                     print("Data found for campaign: "+url_c)
#                     print("Data found for Page: "+str(i))
#                     apiDataFrame = pd.DataFrame(rowsData)
#                     print("DF Page Size",len(apiDataFrame))
#                     df = df.append(apiDataFrame, ignore_index=True)
#                     print("DF API Size",len(df))
#                     i+=1
                    
#                     logger.info('Received Data  From Api URL: '+url_c)    
#             sql = "CREATE TABLE if not exists test.get_ICMTest ( pk_icm_fixed_21_id Int32, fk_form_id Int32, fk_user_id Int32, date_created String, campaign_id Int32, surveyor_name String, designation String, surveyor_affliate String, fk_prov_id Int32, fk_dist_id Int32, fk_tehsil_id Int32, fk_uc_id Int32, g1_aic_name String, g1_team_no String, g1_day String, g1_site_name String, g1_fsloc String, g1_vac_chld String, g1_doses String, g1_site_fun Int32, g1_vac_trained_government_accountable Int32, g1_vac_trained_trained Int32, g1_vac_trained_adult Int32, g1_vac_trained_local Int32, g1_opv_dry_valid_vvm Int32 ,g1_opv_dry_cool String, g1_opv_dry_dry Int32, g1_FS_address Int32, g1_Svisit_fixsite Int32, g1_cat String, g1_micro_avail String, g1_vitaminA String, g1_comments String, epi_center_count Int32, g2_temp_ilr String, g2_r_imm_given String, g2_reg_record_cvacc String, g2_ipv_penta_chart String, site_afp_surv_net Int32, g3_zero_site String, g3_afp_def_visible String, g3_zero_report String, g3_afp_reported String,g3_Comments1 String,covid_brief_guidline Int32, covid_disinfct_matrial Int32, covid_wear_ppe Int32, covid_social_dist Int32, covid_hold_child Int32, unique_id String, dev_remarks String, app_version String, g1_opv_dry_intact Int32, g1_opv_dry_re_capped Int32, match_admin_team Int32, mp_sk_ucmo Int32, mp_ask_ice_aic String, chk_epi_stock String, chk_downloading String, chk_vac_fridge String, chk_ilr_logic String, chk_ilr_logic_reason String, is_archive Int32, update_by Int32, archive_reason String )ENGINE = MergeTree PRIMARY KEY pk_icm_fixed_21_id ORDER BY pk_icm_fixed_21_id"
#             client.execute(sql)
#             df2 = client.query_dataframe("SELECT * FROM test.get_ICMTest")
#             if df2.empty:
#                 df = df.replace(r'^\s*$', np.nan, regex=True)
#                 client.insert_dataframe(
#                     'INSERT INTO test.get_ICMTest VALUES', df)
#                 logger.info(
#                         ' Data has been inserted into Table get_ICMTest for campaign'+str(cid))    
#             sql = "CREATE TABLE if not exists test.xbi_Test (pk_icm_fixed_21_id Int32,fk_form_id Int32,fk_user_id Int32,date_created String, icm_campaign_id Int32,surveyor_name String,designation String,surveyor_affliate String,fk_prov_id Int32,fk_dist_id Int32,fk_tehsil_id Int32,fk_uc_id Int32,g1_aic_name String,g1_team_no String,g1_day String,g1_site_name String, g1_fsloc String, g1_vac_chld String,g1_doses String,g1_site_fun Int32,g1_vac_trained_government_accountable Int32,g1_vac_trained_trained Int32,g1_vac_trained_adult Int32,g1_vac_trained_local Int32,g1_opv_dry_valid_vvm Int32,g1_opv_dry_cool String, g1_opv_dry_dry Int32,g1_FS_address Int32,g1_Svisit_fixsite Int32,g1_cat String, g1_micro_avail String,g1_vitaminA String, g1_comments String, epi_center_count Int32,g2_temp_ilr String, g2_r_imm_given String,g2_reg_record_cvacc String,g2_ipv_penta_chart String,site_afp_surv_net Int32,g3_zero_site String, g3_afp_def_visible String,g3_zero_report String,g3_afp_reported String,g3_Comments1 String, covid_brief_guidline Int32,covid_disinfct_matrial Int32,wear_ppe Int32,covid_social_dist Int32,covid_hold_child Int32,unique_id String, dev_remarks String, app_version String, g1_opv_dry_intact Int32,g1_opv_dry_re_capped Int32,match_admin_team Int32,mp_sk_ucmo Int32,mp_ask_ice_aic String, chk_epi_stock String,chk_downloading String,chk_vac_fridge String,chk_ilr_logic String,chk_ilr_logic_reason String, is_archive Int32, update_by Int32, archive_reason String, campaign_ID Int32, campaign_ActivityName String, campaign_ActivityID_old Int32, campaign_Yr Int32, campaign_SubActivityName String, geoLocation_name String, geoLocation_type String, geoLocation_code String, geoLocation_census_pop String, geoLocation_target String, geoLocation_status String, geoLocation_pname String, geoLocation_dname String, geoLocation_namedistrict String, geoLocation_codedistrict String, geoLocation_tname String, geoLocation_provincecode String, geoLocation_districtcode String, geoLocation_tehsilcode String, geoLocation_priority String, geoLocation_commnet String, geoLocation_hr String, geoLocation_fcm String, geoLocation_tier String, geoLocation_block String, geoLocation_division String, geoLocation_cordinates String, geoLocation_latitude String, geoLocation_longitude String, geoLocation_x String, geoLocation_y String, geoLocation_imagepath String, geoLocation_isccpv String, geoLocation_rank String, geoLocation_rank_score String, geoLocation_ishealthcamp String, geoLocation_isdsc String, geoLocation_ucorg String, geoLocation_organization String, geoLocation_tierfromaug161 String, geoLocation_tierfromsep171 String, geoLocation_tierfromdec181 String, geoLocation_mtap String, geoLocation_rspuc String, geoLocation_issmt String, geoLocation_updateddatetime String, geoLocation_x_code String, geoLocation_draining_uc String, geoLocation_upap_districts String, geoLocation_shruc String, geoLocation_khidist_id String)ENGINE = MergeTree PRIMARY KEY pk_icm_fixed_21_id ORDER BY pk_icm_fixed_21_id"
#             client.execute(sql)
#             cols = "test.get_ICMTest.pk_icm_fixed_21_id, test.get_ICMTest.fk_form_id, test.get_ICMTest.fk_user_id, test.get_ICMTest.date_created, test.get_ICMTest.campaign_id, test.get_ICMTest.surveyor_name, test.get_ICMTest.designation, test.get_ICMTest.surveyor_affliate, test.get_ICMTest.fk_prov_id, test.get_ICMTest.fk_dist_id, test.get_ICMTest.fk_tehsil_id, test.get_ICMTest.fk_uc_id, test.get_ICMTest.g1_aic_name, test.get_ICMTest.g1_team_no, test.get_ICMTest.g1_day, test.get_ICMTest.g1_site_name, test.get_ICMTest.g1_fsloc, test.get_ICMTest.g1_vac_chld, test.get_ICMTest.g1_doses, test.get_ICMTest.g1_site_fun, test.get_ICMTest.g1_vac_trained_government_accountable, test.get_ICMTest.g1_vac_trained_trained, test.get_ICMTest.g1_vac_trained_adult, test.get_ICMTest.g1_vac_trained_local, test.get_ICMTest.g1_opv_dry_valid_vvm, test.get_ICMTest.g1_opv_dry_cool, test.get_ICMTest.g1_opv_dry_dry, test.get_ICMTest.g1_FS_address, test.get_ICMTest.g1_Svisit_fixsite, test.get_ICMTest.g1_cat, test.get_ICMTest.g1_micro_avail, test.get_ICMTest.g1_vitaminA, test.get_ICMTest.g1_comments, test.get_ICMTest.epi_center_count, test.get_ICMTest.g2_temp_ilr, test.get_ICMTest.g2_r_imm_given, test.get_ICMTest.g2_reg_record_cvacc, test.get_ICMTest.g2_ipv_penta_chart, test.get_ICMTest.site_afp_surv_net, test.get_ICMTest.g3_zero_site, test.get_ICMTest.g3_afp_def_visible, test.get_ICMTest.g3_zero_report, test.get_ICMTest.g3_afp_reported,test.get_ICMTest.g3_Comments1, test.get_ICMTest.covid_brief_guidline, test.get_ICMTest.covid_disinfct_matrial, test.get_ICMTest.covid_wear_ppe, test.get_ICMTest.covid_social_dist, test.get_ICMTest.covid_hold_child, test.get_ICMTest.unique_id, test.get_ICMTest.dev_remarks, test.get_ICMTest.app_version, test.get_ICMTest.g1_opv_dry_intact, test.get_ICMTest.g1_opv_dry_re_capped, test.get_ICMTest.match_admin_team, test.get_ICMTest.mp_sk_ucmo, test.get_ICMTest.mp_ask_ice_aic, test.get_ICMTest.chk_epi_stock, test.get_ICMTest.chk_downloading,test.get_ICMTest.chk_vac_fridge, test.get_ICMTest.chk_ilr_logic, test.get_ICMTest.chk_ilr_logic_reason, test.get_ICMTest.is_archive, test.get_ICMTest.update_by, test.get_ICMTest.archive_reason, test.xbi_campaign.campaign_ID, test.xbi_campaign.campaign_ActivityName, test.xbi_campaign.campaign_ActivityID_old, test.xbi_campaign.campaign_Yr, test.xbi_campaign.campaign_SubActivityName,  test.eoc_geolocation_t.name, test.eoc_geolocation_t.type, test.eoc_geolocation_t.code, test.eoc_geolocation_t.census_pop, test.eoc_geolocation_t.target, test.eoc_geolocation_t.status, test.eoc_geolocation_t.pname, test.eoc_geolocation_t.dname, test.eoc_geolocation_t.namedistrict, test.eoc_geolocation_t.codedistrict, test.eoc_geolocation_t.tname, test.eoc_geolocation_t.provincecode, test.eoc_geolocation_t.districtcode, test.eoc_geolocation_t.tehsilcode, test.eoc_geolocation_t.priority, test.eoc_geolocation_t.commnet, test.eoc_geolocation_t.hr, test.eoc_geolocation_t.fcm, test.eoc_geolocation_t.tier, test.eoc_geolocation_t.block, test.eoc_geolocation_t.division, test.eoc_geolocation_t.cordinates, test.eoc_geolocation_t.latitude, test.eoc_geolocation_t.longitude, test.eoc_geolocation_t.x, test.eoc_geolocation_t.y, test.eoc_geolocation_t.imagepath, test.eoc_geolocation_t.isccpv, test.eoc_geolocation_t.rank, test.eoc_geolocation_t.rank_score, test.eoc_geolocation_t.ishealthcamp, test.eoc_geolocation_t.isdsc, test.eoc_geolocation_t.ucorg, test.eoc_geolocation_t.organization, test.eoc_geolocation_t.tierfromaug161, test.eoc_geolocation_t.tierfromsep171, test.eoc_geolocation_t.tierfromdec181, test.eoc_geolocation_t.mtap, test.eoc_geolocation_t.rspuc, test.eoc_geolocation_t.issmt, test.eoc_geolocation_t.updateddatetime, test.eoc_geolocation_t.x_code, test.eoc_geolocation_t.draining_uc, test.eoc_geolocation_t.upap_districts, test.eoc_geolocation_t.shruc, test.eoc_geolocation_t.khidist_id"
#             sql = "SELECT " + cols + "  FROM test.get_ICMTest eoc_1 left join test.eoc_geolocation_t eoc_2 ON eoc_1.fk_uc_id  = eoc_1.code left JOIN test.xbi_campaign eoc_3 ON eoc_1.campaign_id  = eoc_3.campaign_ID "

#             data = client.execute(sql)
#             apiDataFrame = pd.DataFrame(data)
#             apiDataFrame = apiDataFrame.replace(r'^\s*$', np.nan, regex=True)
#             all_columns = list(apiDataFrame)  # Creates list of all column headers
#             cols = apiDataFrame.iloc[0]
#             apiDataFrame[all_columns] = apiDataFrame[all_columns].astype(str)
#             d = 'pk_icm_fixed_21_id', 'fk_form_id', 'fk_user_id', 'date_created', 'icm_campaign_id', 'surveyor_name', 'designation', 'surveyor_affliate', 'fk_prov_id', 'fk_dist_id', 'fk_tehsil_id', 'fk_uc_id', 'g1_aic_name', 'g1_team_no', 'g1_day', 'g1_site_name','g1_fsloc','g1_vac_chld','g1_doses','g1_site_fun', 'g1_vac_trained_government_accountable','g1_vac_trained_trained', 'g1_vac_trained_adult', 'g1_vac_trained_local', 'g1_opv_dry_valid_vvm', 'g1_opv_dry_cool', 'g1_opv_dry_dry', 'g1_FS_address', 'g1_Svisit_fixsite', 'g1_cat', 'g1_micro_avail', 'g1_vitaminA', 'g1_comments', 'epi_center_count', 'g2_temp_ilr', 'g2_r_imm_given', 'g2_reg_record_cvacc', 'g2_ipv_penta_chart', 'site_afp_surv_net', 'g3_zero_site','g3_afp_def_visible','g3_zero_report', 'g3_afp_reported', 'g3_Comments1', 'covid_brief_guidline', 'covid_disinfct_matrial', 'wear_ppe', 'covid_social_dist', 'covid_hold_child', 'unique_id', 'dev_remarks', 'app_version', 'g1_opv_dry_intact', 'g1_opv_dry_re_capped', 'match_admin_team','mp_sk_ucmo','mp_ask_ice_aic','chk_epi_stock','chk_downloading', 'chk_vac_fridge','chk_ilr_logic','chk_ilr_logic_reason', 'is_archive', 'update_by', 'archive_reason', 'campaign_ID', 'campaign_ActivityName', 'campaign_ActivityID_old', 'campaign_Yr', 'campaign_SubActivityName','geoLocation_name', 'geoLocation_type', 'geoLocation_code', 'geoLocation_census_pop', 'geoLocation_target', 'geoLocation_status', 'geoLocation_pname', 'geoLocation_dname', 'geoLocation_namedistrict', 'geoLocation_codedistrict', 'geoLocation_tname', 'geoLocation_provincecode', 'geoLocation_districtcode', 'geoLocation_tehsilcode', 'geoLocation_priority', 'geoLocation_commnet', 'geoLocation_hr', 'geoLocation_fcm', 'geoLocation_tier', 'geoLocation_block', 'geoLocation_division', 'geoLocation_cordinates', 'geoLocation_latitude', 'geoLocation_longitude', 'geoLocation_x', 'geoLocation_y', 'geoLocation_imagepath', 'geoLocation_isccpv', 'geoLocation_rank', 'geoLocation_rank_score', 'geoLocation_ishealthcamp', 'geoLocation_isdsc', 'geoLocation_ucorg', 'geoLocation_organization', 'geoLocation_tierfromaug161', 'geoLocation_tierfromsep171', 'geoLocation_tierfromdec181', 'geoLocation_mtap', 'geoLocation_rspuc', 'geoLocation_issmt', 'geoLocation_updateddatetime', 'geoLocation_x_code', 'geoLocation_draining_uc', 'geoLocation_upap_districts', 'geoLocation_shruc', 'geoLocation_khidist_id' 
#             dff = pd.DataFrame(columns=d)
#             for index, item in enumerate(d):
#                 dff[item] = apiDataFrame[index].values
#                 logger.info(
#                         'Get Data from get_ICMTest for campaign'+str(cid))
#                 #dff[item]= dff[item].replace(r'^\s*$', np.nan, regex=True)
#             df3 = client.query_dataframe(
#                     "SELECT * FROM test.xbi_Test")
#             if df3.empty:
#                 client.insert_dataframe(
#                         'INSERT INTO test.xbi_Test  VALUES', dff)
#                 logger.info(
#                         'Data has been inserted into Table test.xbi_Test for campaign '+str(cid))

#                 sql = "DROP table if exists test.get_ICMTest"
#                 client.execute(sql)
#                 print('\n\n\n F\t\t--------\t', cid)
#                 print('\n\n\n B\t\t--------\t', i)    
#             else:
#                     # df = pd.concat([dff, df2])
#                     # df = df.astype('str')
#                     # df = df.drop_duplicates(subset='pk_icm_fixed_21_id',
#                     #                         keep="first", inplace=False)
#                     # sql = "DROP TABLE if exists  test.xbi_Test;"
#                     # client.execute(sql)
#                     # sql = "CREATE TABLE if not exists test.xbi_Test (pk_icm_fixed_21_id Int32,fk_form_id Int32,fk_user_id Int32,date_created String, icm_campaign_id Int32,surveyor_name String,designation String,surveyor_affliate String,fk_prov_id Int32,fk_dist_id Int32,fk_tehsil_id Int32,fk_uc_id Int32,g1_aic_name String,g1_team_no String,g1_day String,g1_site_name String, g1_fsloc String, g1_vac_chld String,g1_doses String,g1_site_fun Int32,g1_vac_trained_government_accountable Int32,g1_vac_trained_trained Int32,g1_vac_trained_adult Int32,g1_vac_trained_local Int32,g1_opv_dry_valid_vvm Int32,g1_opv_dry_cool String, g1_opv_dry_dry Int32,g1_FS_address Int32,g1_Svisit_fixsite Int32,g1_cat String, g1_micro_avail String,g1_vitaminA String, g1_comments String, epi_center_count Int32,g2_temp_ilr String, g2_r_imm_given String,g2_reg_record_cvacc String,g2_ipv_penta_chart String,site_afp_surv_net Int32,g3_zero_site String, g3_afp_def_visible String,g3_zero_report String,g3_afp_reported String,g3_Comments1 String, covid_brief_guidline Int32,covid_disinfct_matrial Int32,wear_ppe Int32,covid_social_dist Int32,covid_hold_child Int32,unique_id String, dev_remarks String, app_version String, g1_opv_dry_intact Int32,g1_opv_dry_re_capped Int32,match_admin_team Int32,mp_sk_ucmo Int32,mp_ask_ice_aic String, chk_epi_stock String,chk_downloading String,chk_vac_fridge String,chk_ilr_logic String,chk_ilr_logic_reason String, is_archive Int32, update_by Int32, archive_reason String, campaign_ID Int32, campaign_ActivityName String, campaign_ActivityID_old Int32, campaign_Yr Int32, campaign_SubActivityName String, location_ID Int32, location_code Int32, location_name String, location_type String, location_target Int32, location_status Int32, location_priority String, hr_status String)ENGINE = MergeTree PRIMARY KEY pk_icm_fixed_21_id ORDER BY pk_icm_fixed_21_id"
#                     # client.execute(sql)
#                 dff = dff.replace(r'^\s*$', np.nan, regex=True)
#                 client.insert_dataframe(
#                     'INSERT INTO test.xbi_Test  VALUES', dff)
#                 logger.info(
#                         ' Data has been inserted into Table test.xbi_Test for campaign '+str(cid))    
#                 print('\n\n\n F\t\t--------\t', cid)
#                 print('\n\n\n B\t\t--------\t', i)
#                 sql = "DROP table if exists test.get_ICMTest"
#                 client.execute(sql)
                       
                        
 
# dag = DAG(
#     'TestDag_Automated',
#     #schedule_interval='*/10 * * * *',# will run every 10 min.
#     schedule_interval='0 0 * * *',  
#     default_args=default_args,
#     catchup=False)

# with dag:
#     GetAndInsertApiDataTest = PythonOperator(
#         task_id='GetAndInsertApiDataTest',
#         python_callable=GetAndInsertApiDataTest,
#     )
# GetAndInsertApiDataTest


#-------------------------------------------------------------------------------------------------------------------------------#
#------------------------------------------- INSERT LATEST CAMPAIGN DATA After_CatchUp -----------------------------------------#
#-------------------------------------------------------------------------------------------------------------------------------#

# def GetAndInsertApiDataTest():
#     logger.info('Function \' GetAndInsertApiDataTest \' Started Off')
#     client = Client(host='161.97.136.95',
#                     user='default',
#                     password='pakistan',
#                     port='9000', settings={"use_numpy": True}) 

#     c = 2
#     y = 2022
#     url = "http://idims.eoc.gov.pk/api_who/api/get_allaftercatchup/5468XE2LN6CzR7qRG041/"+ str(c)+"/"+ str(y)
#     #url = 'http://idims.eoc.gov.pk/api_who/api/get_allaftercatchup/5468XE2LN6CzR7qRG041/2/2022'
#     logger.info('Requested Data From Api URL:  \' http://idims.eoc.gov.pk/api_who/api/get_allcatchup/5468XE2LN6CzR7qRG041/"+ str(c)+"/"+ str(y) \' ')

#     r = requests.get(url)
#     data = r.json()
    
#     logger.info('Requested Data From Api URL:  \' http://idims.eoc.gov.pk/api_who/api/get_allcatchup/5468XE2LN6CzR7qRG041/"+ str(c)+"/"+ str(y) \' ')

#     rowsData = data["data"]["data"]
#     apiDataFrame = pd.DataFrame(rowsData)

#     count_row = apiDataFrame.shape[0]
#     count_col = apiDataFrame.shape[1]
#     print("DF Rows",count_row)
#     print("DF Cols",count_col)
#     print('\n\n\n CID\t\t--------\t', c)
#     print('\n\n\n Yr\t\t--------\t', y) 
    
#     sql = " Create Table if not exists test.get_testData (ID Int32, UserName String, IDcampCat Int32, ActivityID Int32, TimeStamp String, Yr Int32, TehsilID Int32, UCID Int32, DistID Int32, DivID Int32, ProvID Int32, cday Int32,VaccinationDate String,ACCovNA011 Int32, ACCovNA1259 Int32, ACCovRef011 Int32, ACCovRef1259 Int32, ACOutofH011 Int32, ACOutofH1259 Int32, ACGuests Int32, ACCovMob Int32, ACNewBorn Int32, PersistentlyMC Int32, PersistentlyMCHRMP Int32, ACAlreadyVaccinated Int32, UnRecCov Int32, OPVGiven Int32, OPVUsed Int32, OPVReturned Int32, status Int32, trash Int32, isSync Int32, fm_issued Int32, fm_retrieved Int32, Remarks String, location_code Int32 NULL)ENGINE = MergeTree PRIMARY KEY ID ORDER BY ID"
#     client.execute(sql)

#     df2 = client.query_dataframe(
#         "SELECT * FROM test.get_testData")
#     if df2.empty:
#         apiDataFrame = apiDataFrame.replace(r'^\s*$', np.nan, regex=True)
#         client.insert_dataframe(
#             'INSERT INTO test.get_testData VALUES', apiDataFrame)

# def CreateJoinTableOfTest():
#     logger.info(' Function  \' CreateJoinTableOfTest \' has been Initiated')
#     client = Client(host='161.97.136.95',
#                     user='default',
#                     password='pakistan',
#                     port='9000', settings={"use_numpy": True})
    
#     sql = "CREATE TABLE if not exists test.xbi_Test (ID Int32, UserName String, IDcampCat Int32, ActivityID Int32, TimeStamp String, Yr Int32, TehsilID Int32, UCID Int32, DistID Int32, DivID Int32, ProvID Int32, cday Int32, VaccinationDate String, ACCovNA011 Int32, ACCovNA1259 Int32, ACCovRef011 Int32, ACCovRef1259 Int32, ACOutofH011 Int32, ACOutofH1259 Int32, ACGuests Int32, ACCovMob Int32, ACNewBorn Int32, PersistentlyMC Int32, PersistentlyMCHRMP Int32, ACAlreadyVaccinated Int32,  UnRecCov Int32, OPVGiven Int32, OPVUsed Int32, OPVReturned String, status Int32, trash Int32, isSync Int32,fm_issued Int32,fm_retrieved Int32, Remarks String, location_code Int32 NULL, campaign_ID Int32, campaign_ActivityName String, campaign_ActivityID_old Int32, campaign_Yr Int32, campaign_SubActivityName String, geoLocation_name String, geoLocation_type Int32, geoLocation_code Int32, geoLocation_census_pop Int32, geoLocation_target Int32, geoLocation_status Int32, geoLocation_pname String, geoLocation_dname String, geoLocation_namedistrict String, geoLocation_codedistrict String, geoLocation_tname String, geoLocation_provincecode Int32, geoLocation_districtcode Int32, geoLocation_tehsilcode Int32, geoLocation_priority Int32, geoLocation_commnet Int32, geoLocation_hr Int32, geoLocation_fcm Int32, geoLocation_tier Int32, geoLocation_block String, geoLocation_division String, geoLocation_cordinates String, geoLocation_latitude String, geoLocation_longitude String, geoLocation_x String, geoLocation_y String, geoLocation_imagepath String, geoLocation_isccpv Int32, geoLocation_rank Int32, geoLocation_rank_score String, geoLocation_ishealthcamp Int32, geoLocation_isdsc Int32, geoLocation_ucorg String, geoLocation_organization String, geoLocation_tierfromaug161 Int32, geoLocation_tierfromsep171 Int32, geoLocation_tierfromdec181 Int32, geoLocation_mtap Int32, geoLocation_rspuc Int32, geoLocation_issmt Int32, geoLocation_updateddatetime String, geoLocation_x_code Int32, geoLocation_draining_uc Int32, geoLocation_upap_districts Int32, geoLocation_shruc Int32, geoLocation_khidist_id String )ENGINE = MergeTree PRIMARY KEY ID ORDER BY ID"
#     client.execute(sql)

#     cols = "test.get_testData.ID, test.get_testData.UserName, test.get_testData.IDcampCat, test.get_testData.ActivityID, test.get_testData.TimeStamp, test.get_testData.Yr, test.get_testData.TehsilID, test.get_testData.UCID, test.get_testData.DistID, test.get_testData.DivID, test.get_testData.ProvID, test.get_testData.cday, test.get_testData.VaccinationDate,test.get_testData.ACCovNA011, test.get_testData.ACCovNA1259, test.get_testData.ACCovRef011, test.get_testData.ACCovRef1259, test.get_testData.ACOutofH011, test.get_testData.ACOutofH1259, test.get_testData.ACGuests, test.get_testData.ACCovMob, test.get_testData.ACNewBorn, test.get_testData.PersistentlyMC, test.get_testData.PersistentlyMCHRMP, test.get_testData.ACAlreadyVaccinated, test.get_testData.UnRecCov, test.get_testData.OPVGiven, test.get_testData.OPVUsed, test.get_testData.OPVReturned,test.get_testData.status, test.get_testData.trash, test.get_testData.isSync,test.get_testData.fm_issued, test.get_testData.fm_retrieved,test.get_testData.Remarks, test.get_testData.location_code, test.xbi_campaign.campaign_ID, test.xbi_campaign.campaign_ActivityName, test.xbi_campaign.campaign_ActivityID_old, test.xbi_campaign.campaign_Yr, test.xbi_campaign.campaign_SubActivityName, test.eoc_geolocation_t.name, test.eoc_geolocation_t.type, test.eoc_geolocation_t.code, test.eoc_geolocation_t.census_pop, test.eoc_geolocation_t.target, test.eoc_geolocation_t.status, test.eoc_geolocation_t.pname, test.eoc_geolocation_t.dname, test.eoc_geolocation_t.namedistrict, test.eoc_geolocation_t.codedistrict, test.eoc_geolocation_t.tname, test.eoc_geolocation_t.provincecode, test.eoc_geolocation_t.districtcode, test.eoc_geolocation_t.tehsilcode, test.eoc_geolocation_t.priority, test.eoc_geolocation_t.commnet, test.eoc_geolocation_t.hr, test.eoc_geolocation_t.fcm, test.eoc_geolocation_t.tier, test.eoc_geolocation_t.block, test.eoc_geolocation_t.division, test.eoc_geolocation_t.cordinates, test.eoc_geolocation_t.latitude, test.eoc_geolocation_t.longitude, test.eoc_geolocation_t.x, test.eoc_geolocation_t.y, test.eoc_geolocation_t.imagepath, test.eoc_geolocation_t.isccpv, test.eoc_geolocation_t.rank, test.eoc_geolocation_t.rank_score, test.eoc_geolocation_t.ishealthcamp, test.eoc_geolocation_t.isdsc, test.eoc_geolocation_t.ucorg, test.eoc_geolocation_t.organization, test.eoc_geolocation_t.tierfromaug161, test.eoc_geolocation_t.tierfromsep171, test.eoc_geolocation_t.tierfromdec181, test.eoc_geolocation_t.mtap, test.eoc_geolocation_t.rspuc, test.eoc_geolocation_t.issmt, test.eoc_geolocation_t.updateddatetime, test.eoc_geolocation_t.x_code, test.eoc_geolocation_t.draining_uc, test.eoc_geolocation_t.upap_districts, test.eoc_geolocation_t.shruc, test.eoc_geolocation_t.khidist_id"
            
#     sql = "SELECT " + cols + "  FROM test.get_testData eoc_1 left JOIN test.eoc_geolocation_t eoc_2 ON eoc_1.location_code  = eoc_1.code JOIN test.xbi_campaign eoc_3 ON (eoc_1.ActivityID  = eoc_3.campaign_ActivityID_old And eoc_1.Yr = eoc_3.campaign_Yr) "

#     data = client.execute(sql)
#     apiDataFrame = pd.DataFrame(data)
#     apiDataFrame = apiDataFrame.replace(r'^\s*$', np.nan, regex=True)
#     all_columns = list(apiDataFrame)  # Creates list of all column headers
#     cols = apiDataFrame.iloc[0]
#     apiDataFrame[all_columns] = apiDataFrame[all_columns].astype(str)
    
#     d = 'ID', 'UserName', 'IDcampCat', 'ActivityID', 'TimeStamp', 'Yr', 'TehsilID', 'UCID', 'DistID', 'DivID', 'ProvID', 'cday', 'VaccinationDate', 'ACCovNA011', 'ACCovNA1259', 'ACCovRef011', 'ACCovRef1259', 'ACOutofH011', 'ACOutofH1259', 'ACGuests', 'ACCovMob', 'ACNewBorn', 'PersistentlyMC', 'PersistentlyMCHRMP', 'ACAlreadyVaccinated',  'UnRecCov', 'OPVGiven', 'OPVUsed', 'OPVReturned', 'status', 'trash', 'isSync','fm_issued','fm_retrieved', 'Remarks', 'location_code', 'campaign_ID', 'campaign_ActivityName', 'campaign_ActivityID_old', 'campaign_Yr', 'campaign_SubActivityName','geoLocation_name', 'geoLocation_type', 'geoLocation_code', 'geoLocation_census_pop', 'geoLocation_target', 'geoLocation_status', 'geoLocation_pname', 'geoLocation_dname', 'geoLocation_namedistrict', 'geoLocation_codedistrict', 'geoLocation_tname', 'geoLocation_provincecode', 'geoLocation_districtcode', 'geoLocation_tehsilcode', 'geoLocation_priority', 'geoLocation_commnet', 'geoLocation_hr', 'geoLocation_fcm', 'geoLocation_tier', 'geoLocation_block', 'geoLocation_division', 'geoLocation_cordinates', 'geoLocation_latitude', 'geoLocation_longitude', 'geoLocation_x', 'geoLocation_y', 'geoLocation_imagepath', 'geoLocation_isccpv', 'geoLocation_rank', 'geoLocation_rank_score', 'geoLocation_ishealthcamp', 'geoLocation_isdsc', 'geoLocation_ucorg', 'geoLocation_organization', 'geoLocation_tierfromaug161', 'geoLocation_tierfromsep171', 'geoLocation_tierfromdec181', 'geoLocation_mtap', 'geoLocation_rspuc', 'geoLocation_issmt', 'geoLocation_updateddatetime', 'geoLocation_x_code', 'geoLocation_draining_uc', 'geoLocation_upap_districts', 'geoLocation_shruc', 'geoLocation_khidist_id'  
            
#     dff = pd.DataFrame(columns=d)
#     for index, item in enumerate(d):
#         dff[item] = apiDataFrame[index].values
#     df2 = client.query_dataframe(
#         "SELECT * FROM test.xbi_Test WHERE Yr = 2022 and ActivityID = 2")
#     if df2.empty:
#         client.insert_dataframe(
#             'INSERT INTO test.xbi_Test  VALUES', dff)
#         logger.info(
#             ' Data has been inserted into Table\' INSERT INTO test.xbi_Test  VALUES \' ')

#         sql = "DROP table if exists test.get_testData"
#         client.execute(sql)
#         print('\n\n\n F\t\t--------\t', c)
#         print('\n\n\n B\t\t--------\t', y)

#     else:
#         sql = "ALTER TABLE test.xbi_Test DELETE WHERE Yr = 2022 and ActivityID = 2"
#         client.execute(sql)

#         # df = pd.concat([dff, df2])
#         # df = df.astype('str')
#         # df = df.drop_duplicates(subset='ID',
#         #                         keep="first", inplace=False)
#         # sql = "DROP TABLE test.xbi_Test;"
#         # client.execute(sql)
#         # sql = "CREATE TABLE if not exists test.xbi_Test (ID Int32, UserName String, IDcampCat Int32, ActivityID Int32, TimeStamp String, Yr Int32, TehsilID Int32, UCID Int32, DistID Int32, DivID Int32, ProvID Int32, cday Int32, VaccinationDate String, ACCovNA011 Int32, ACCovNA1259 Int32, ACCovRef011 Int32, ACCovRef1259 Int32, ACOutofH011 Int32, ACOutofH1259 Int32, ACGuests Int32, ACCovMob Int32, ACNewBorn Int32, PersistentlyMC Int32, PersistentlyMCHRMP Int32, ACAlreadyVaccinated Int32,  UnRecCov Int32, OPVGiven Int32, OPVUsed Int32, OPVReturned String, status Int32, trash Int32, isSync Int32,fm_issued Int32,fm_retrieved Int32, Remarks String, location_code Int32 NULL, campaign_ID Int32, campaign_ActivityName String, campaign_ActivityID_old Int32, campaign_Yr Int32, campaign_SubActivityName String,  location_ID Int32, geo_location_code Int32, location_name String, location_type String, location_target Int32, location_status Int32, location_priority String, hr_status String)ENGINE = MergeTree PRIMARY KEY ID ORDER BY ID"
#         # client.execute(sql)
#         dff = dff.replace(r'^\s*$', np.nan, regex=True)
#         dff['location_code'] = pd.to_numeric(dff['location_code'], errors='coerce').fillna(0)
#         client.insert_dataframe(
#             'INSERT INTO test.xbi_Test  VALUES', dff)

#         sql = "DROP table if exists test.get_testData"
#         client.execute(sql)


# dag = DAG(
#     'TestDag_Automated',
#     schedule_interval='0 0 * * *',  # once a day at midnight.
#     #schedule_interval='0 * * * *', # Run once an hour at the beginning of the hour 
#     default_args=default_args,
#     catchup=False)

# with dag:
#     GetAndInsertApiDataTest = PythonOperator(
#         task_id='GetAndInsertApiDataTest', 
#         python_callable=GetAndInsertApiDataTest,
#     )
#     CreateJoinTableOfTest = PythonOperator(
#         task_id='CreateJoinTableOfTest',
#         python_callable=CreateJoinTableOfTest,
#     )

# GetAndInsertApiDataTest >> CreateJoinTableOfTest


#-------------------------------------------------------------------------------------------------------------------------------#
#---------------------------------------------DATA INSERTION: After_CatchUp-----------------------------------------------------#
#-------------------------------------------------------------------------------------------------------------------------------#

# def GetAndInsertApiDataTest():
#     li1 = [28, 29, 31, 30, 1, 106, 34, 9, 37, 40, 40, 43, 10, 2, 46, 11, 3, 107, 1, 106, 2, 9, 8, 10, 3, 107, 28, 31, 4, 9, 1, 28, 31, 2]
#     li2 = [2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2022, 2022, 2022, 2022, 2022]

#     for c, y in zip(li1, li2):
#         logger.info('Function \' GetAndInsertApiDataTest \' Started Off')
#         client = Client(host='161.97.136.95',
#                         user='default',
#                         password='pakistan',
#                         port='9000', settings={"use_numpy": True})

#         url = "http://idims.eoc.gov.pk/api_who/api/get_allaftercatchup/5468XE2LN6CzR7qRG041/"+ str(c)+"/"+ str(y)
#         #url = 'http://idims.eoc.gov.pk/api_who/api/get_allaftercatchup/5468XE2LN6CzR7qRG041/2/2022'
#         print('\n\n\n F\t\t--------\t', c)
#         print('\n\n\n B\t\t--------\t', y)
        
#         logger.info('Requested Data From Api URL:  \' http://idims.eoc.gov.pk/api_who/api/get_allaftercatchup/5468XE2LN6CzR7qRG041/"+ str(c)+"/"+ str(y) \' ')

#         r = requests.get(url)
#         data = r.json()
#         print('\n\n\n F\t\t--------\t', c)
#         print('\n\n\n B\t\t--------\t', y)
#         if data['data'] != "No data found":
#             logger.info('Received Data  From Api URL:  \' http://idims.eoc.gov.pk/api_who/api/get_allaftercatchup/5468XE2LN6CzR7qRG041/"+ str(c)+"/"+ str(y) \' ')

#             rowsData = data["data"]["data"]
#             apiDataFrame = pd.DataFrame(rowsData)
            
#             sql = " Create Table if not exists test.get_testData (ID Int32, UserName String, IDcampCat Int32, ActivityID Int32, TimeStamp String, Yr Int32, TehsilID Int32, UCID Int32, DistID Int32, DivID Int32, ProvID Int32, cday Int32,VaccinationDate String,ACCovNA011 Int32, ACCovNA1259 Int32, ACCovRef011 Int32, ACCovRef1259 Int32, ACOutofH011 Int32, ACOutofH1259 Int32, ACGuests Int32, ACCovMob Int32, ACNewBorn Int32, PersistentlyMC Int32, PersistentlyMCHRMP Int32, ACAlreadyVaccinated Int32, UnRecCov Int32, OPVGiven Int32, OPVUsed Int32, OPVReturned Int32, status Int32, trash Int32, isSync Int32, fm_issued Int32, fm_retrieved Int32, Remarks String, location_code Int32 NULL)ENGINE = MergeTree PRIMARY KEY ID ORDER BY ID"
#             client.execute(sql)

#             df2 = client.query_dataframe(
#                 "SELECT * FROM test.get_testData")
#             if df2.empty:
#                 apiDataFrame = apiDataFrame.replace(r'^\s*$', np.nan, regex=True)
#                 client.insert_dataframe(
#                     'INSERT INTO test.get_testData VALUES', apiDataFrame)
            
#             sql = "CREATE TABLE if not exists test.xbi_Test (ID Int32, UserName String, IDcampCat Int32, ActivityID Int32, TimeStamp String, Yr Int32, TehsilID Int32, UCID Int32, DistID Int32, DivID Int32, ProvID Int32, cday Int32, VaccinationDate String, ACCovNA011 Int32, ACCovNA1259 Int32, ACCovRef011 Int32, ACCovRef1259 Int32, ACOutofH011 Int32, ACOutofH1259 Int32, ACGuests Int32, ACCovMob Int32, ACNewBorn Int32, PersistentlyMC Int32, PersistentlyMCHRMP Int32, ACAlreadyVaccinated Int32,  UnRecCov Int32, OPVGiven Int32, OPVUsed Int32, OPVReturned String, status Int32, trash Int32, isSync Int32,fm_issued Int32,fm_retrieved Int32, Remarks String, location_code Int32 NULL, campaign_ID Int32, campaign_ActivityName String, campaign_ActivityID_old Int32, campaign_Yr Int32, campaign_SubActivityName String, geoLocation_name String, geoLocation_type Int32, geoLocation_code Int32, geoLocation_census_pop Int32, geoLocation_target Int32, geoLocation_status Int32, geoLocation_pname String, geoLocation_dname String, geoLocation_namedistrict String, geoLocation_codedistrict String, geoLocation_tname String, geoLocation_provincecode Int32, geoLocation_districtcode Int32, geoLocation_tehsilcode Int32, geoLocation_priority Int32, geoLocation_commnet Int32, geoLocation_hr Int32, geoLocation_fcm Int32, geoLocation_tier Int32, geoLocation_block String, geoLocation_division String, geoLocation_cordinates String, geoLocation_latitude String, geoLocation_longitude String, geoLocation_x String, geoLocation_y String, geoLocation_imagepath String, geoLocation_isccpv Int32, geoLocation_rank Int32, geoLocation_rank_score String, geoLocation_ishealthcamp Int32, geoLocation_isdsc Int32, geoLocation_ucorg String, geoLocation_organization String, geoLocation_tierfromaug161 Int32, geoLocation_tierfromsep171 Int32, geoLocation_tierfromdec181 Int32, geoLocation_mtap Int32, geoLocation_rspuc Int32, geoLocation_issmt Int32, geoLocation_updateddatetime String, geoLocation_x_code Int32, geoLocation_draining_uc Int32, geoLocation_upap_districts Int32, geoLocation_shruc Int32, geoLocation_khidist_id String )ENGINE = MergeTree PRIMARY KEY ID ORDER BY ID"
#             client.execute(sql)

#             cols = "test.get_testData.ID, test.get_testData.UserName, test.get_testData.IDcampCat, test.get_testData.ActivityID, test.get_testData.TimeStamp, test.get_testData.Yr, test.get_testData.TehsilID, test.get_testData.UCID, test.get_testData.DistID, test.get_testData.DivID, test.get_testData.ProvID, test.get_testData.cday, test.get_testData.VaccinationDate,test.get_testData.ACCovNA011, test.get_testData.ACCovNA1259, test.get_testData.ACCovRef011, test.get_testData.ACCovRef1259, test.get_testData.ACOutofH011, test.get_testData.ACOutofH1259, test.get_testData.ACGuests, test.get_testData.ACCovMob, test.get_testData.ACNewBorn, test.get_testData.PersistentlyMC, test.get_testData.PersistentlyMCHRMP, test.get_testData.ACAlreadyVaccinated, test.get_testData.UnRecCov, test.get_testData.OPVGiven, test.get_testData.OPVUsed, test.get_testData.OPVReturned,test.get_testData.status, test.get_testData.trash, test.get_testData.isSync,test.get_testData.fm_issued, test.get_testData.fm_retrieved,test.get_testData.Remarks, test.get_testData.location_code, test.xbi_campaign.campaign_ID, test.xbi_campaign.campaign_ActivityName, test.xbi_campaign.campaign_ActivityID_old, test.xbi_campaign.campaign_Yr, test.xbi_campaign.campaign_SubActivityName, test.eoc_geolocation_t.name, test.eoc_geolocation_t.type, test.eoc_geolocation_t.code, test.eoc_geolocation_t.census_pop, test.eoc_geolocation_t.target, test.eoc_geolocation_t.status, test.eoc_geolocation_t.pname, test.eoc_geolocation_t.dname, test.eoc_geolocation_t.namedistrict, test.eoc_geolocation_t.codedistrict, test.eoc_geolocation_t.tname, test.eoc_geolocation_t.provincecode, test.eoc_geolocation_t.districtcode, test.eoc_geolocation_t.tehsilcode, test.eoc_geolocation_t.priority, test.eoc_geolocation_t.commnet, test.eoc_geolocation_t.hr, test.eoc_geolocation_t.fcm, test.eoc_geolocation_t.tier, test.eoc_geolocation_t.block, test.eoc_geolocation_t.division, test.eoc_geolocation_t.cordinates, test.eoc_geolocation_t.latitude, test.eoc_geolocation_t.longitude, test.eoc_geolocation_t.x, test.eoc_geolocation_t.y, test.eoc_geolocation_t.imagepath, test.eoc_geolocation_t.isccpv, test.eoc_geolocation_t.rank, test.eoc_geolocation_t.rank_score, test.eoc_geolocation_t.ishealthcamp, test.eoc_geolocation_t.isdsc, test.eoc_geolocation_t.ucorg, test.eoc_geolocation_t.organization, test.eoc_geolocation_t.tierfromaug161, test.eoc_geolocation_t.tierfromsep171, test.eoc_geolocation_t.tierfromdec181, test.eoc_geolocation_t.mtap, test.eoc_geolocation_t.rspuc, test.eoc_geolocation_t.issmt, test.eoc_geolocation_t.updateddatetime, test.eoc_geolocation_t.x_code, test.eoc_geolocation_t.draining_uc, test.eoc_geolocation_t.upap_districts, test.eoc_geolocation_t.shruc, test.eoc_geolocation_t.khidist_id"
            
#             sql = "SELECT " + cols + "  FROM test.get_testData eoc_1 left JOIN test.eoc_geolocation_t eoc_2 ON eoc_1.location_code  = eoc_1.code JOIN test.xbi_campaign eoc_3 ON (eoc_1.ActivityID  = eoc_3.campaign_ActivityID_old And eoc_1.Yr = eoc_3.campaign_Yr) "

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
#                 "SELECT * FROM test.xbi_Test")
#             if df2.empty:
#                 client.insert_dataframe(
#                     'INSERT INTO test.xbi_Test  VALUES', dff)
#                 logger.info(
#                     'Data has been inserted into Table\' INSERT INTO test.xbi_Test  VALUES \' ')

#                 sql = "DROP table if exists test.get_testData"
#                 client.execute(sql)
#                 print('\n\n\n F\t\t--------\t', c)
#                 print('\n\n\n B\t\t--------\t', y)
#             else:
#                 # df = pd.concat([dff, df2])
#                 # df = df.astype('str')
#                 # df = df.drop_duplicates(subset='ID',
#                 #                         keep="first", inplace=False)
#                 # sql = "DROP TABLE test.xbi_Test ;"
#                 # client.execute(sql)
#                 dff = dff.replace(r'^\s*$', np.nan, regex=True)
#                 dff['location_code'] = pd.to_numeric(dff['location_code'], errors='coerce').fillna(0)
#                 client.insert_dataframe(
#                     'INSERT INTO test.xbi_Test  VALUES', dff)

#                 sql = "DROP table if exists test.get_testData"
#                 client.execute(sql)

#             # apiDataFrame = apiDataFrame.drop_duplicates(subset='ID', keep="first", inplace=False)
           



# dag = DAG(
#     'TestDag_Automated',
#     schedule_interval='0 0 * * *',  # once a day at midnight
#     #schedule_interval='*/59 * * * *',  # will run after an hour[Every 60 minute].
#     default_args=default_args,
#     catchup=False)

# with dag:
#     GetAndInsertApiDataTest = PythonOperator(
#         task_id='GetAndInsertApiDataTest',
#         python_callable=GetAndInsertApiDataTest,
#     )

# GetAndInsertApiDataTest 

