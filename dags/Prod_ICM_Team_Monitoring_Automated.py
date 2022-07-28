
from airflow import DAG
import pandas as pd
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import requests
import logging
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
#---------------------------------------- INSERT CAMPAIGN DATA ICM Team   ------------------------------------------------------#
#-------------------------------------------- Author: Abdul Bari Malik ---------------------------------------------------------#
#-------------------------------------------------------------------------------------------------------------------------------#

# def GetAndInsertApiDataICMTeamMonitoring():
    
#     li3 = [222,223,224,225,226,227,228,229,230,231,232,233,234,235,236,237,238,239,240,241,242,243,244,245,247,248,249,250,251,252,253,254,255,256,257,258,259]
    
#     logger.info('Function \' GetAndInsertApiDataICMTeamMonitoring \' Started Off')

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

#         for cid in li3:                                
#             url = "http://idims.eoc.gov.pk/api_who/api/get_icm/5468XE2LN6CzR7qRG041/team/"+ str(cid)+"/"
#             print('\n\n\n Campaign-ID\t\t--------\t', cid)
#             logger.info('Requested Data From Api URL:'+url)

#             r = requests.get(url)
#             data = r.json()
            
#             if data['data'] == "No data found":
#                 print("Not Data found for campaign: "+str(cid))
#                 logger.info('Not Data found for campaign: '+str(cid))
#             else:
#                 print("Data found for campaign: "+str(cid))
#                 df = pd.DataFrame()
#                 print("Campaign_Total_Pages",data['total_page'])
#                 i = 1
#                 while int(i) <= data['total_page']:
#                     url_c = "http://idims.eoc.gov.pk/api_who/api/get_icm/5468XE2LN6CzR7qRG041/team/"+str(cid)+'/'+str(i)
#                     r2 = requests.get(url_c)
#                     data_c = r2.json()
#                     if data_c['data'] == "No data found":
#                         print("Not Data found for campaign: "+url_c)
#                         break
#                     else:
#                         rowsData = data_c["data"]["data"]
#                         print("Data found for campaign: "+url_c)
#                         print("Data found for Page: "+str(i))
#                         apiDataFrame = pd.DataFrame(rowsData)
#                         print("DF Page Size",len(apiDataFrame))
#                         df = df.append(apiDataFrame, ignore_index=True)
#                         print("DF API Size",len(df))
#                         i+=1
                        
#                         logger.info('Received Data  From Api URL: '+url_c)    

#                 sql = "CREATE TABLE if not exists test.get_icm_team_monitoring (pk_icm_team_21_id Int32, fk_form_id Int32, fk_user_id Int32, date_created Date, campaign_id Int32, surveyor_name String, designation String, surveyor_affliate String, fk_prov_id Int32, fk_dist_id Int32, fk_tehsil_id Int32, fk_uc_id Int32, g1_cbv_noncbv Int32, g1_aic_name String, g1_team_no String, g1_day String, g1_team_composition_government Int32, g1_team_composition_local Int32, g1_team_composition_female Int32, g1_team_composition_adult Int32, g1_NamesMP_match Int32, g1_valid_marker Int32, g1_T_memTrained Int32, g1_Tdate String, g1_opv_dry_valid_vvm Int32, g1_opv_dry_cool String, g1_opv_dry_dry Int32, g1_t_sheetcorrect Int32, g1_vitamin_a String, g1_T_recordedMissed Int32, g1_T_mapAvailable Int32, g1_Tworkload Int32, g1_Comment String, g2_obsorve Int32, g2_T_chiledAwayHome  Int32, g2_T_rmAFP Int32, g2_T_markerUsed Int32, g2_T_CfMarked Int32, g2_T_childVaccinatedToday Int32, g2_Comments String, covid_indi Int32, covid_brief_guidline Int32, covid_symptoms Int32, covid_wear_mask Int32, covid_use_santzr Int32, covid_age_member Int32, covid_hold_child Int32, covid_chllenges Int32, covid_chllenges_dscrptn String, unique_id String, dev_remarks String, app_version String, cold_chain String, g1_opv_dry_intact Int32, g1_opv_dry_re_capped Int32, g1_matched_admin_team Int32, team_vacc_ts_cc Int32, date_updated Date)ENGINE = MergeTree PRIMARY KEY pk_icm_team_21_id ORDER BY pk_icm_team_21_id"
#                 client.execute(sql)

#                 df2 = client.query_dataframe(
#                     "SELECT * FROM test.get_icm_team_monitoring")
#                 if df2.empty:
#                     df['date_created'] = pd.to_datetime(df['date_created'],unit='s')
#                     df['date_created'] = df['date_created'].dt.date
#                     #df['date_created'] = df['date_created'].dt.strftime('%d-%m-%Y')
#                     df['date_updated'] = pd.to_datetime(df['date_updated'],unit='s')
#                     df['date_updated'] = df['date_updated'].dt.date
#                     df = df.replace(r'^\s*$', np.nan, regex=True)
#                     client.insert_dataframe(
#                         'INSERT INTO test.get_icm_team_monitoring VALUES', df)
#                     logger.info(
#                             ' Data has been inserted into Table test.xbi_icm_team_monitoring for campaign'+str(cid))    
#                 sql = "CREATE TABLE if not exists test.xbi_icm_team_monitoring (pk_icm_team_21_id Int32, fk_form_id Int32, fk_user_id Int32, date_created Date, campaign_id Int32, surveyor_name String, designation String, surveyor_affliate String, fk_prov_id Int32, fk_dist_id Int32, fk_tehsil_id Int32, fk_uc_id Int32, g1_cbv_noncbv Int32, g1_aic_name String, g1_team_no String, g1_day String, g1_team_composition_government Int32, g1_team_composition_local Int32, g1_team_composition_female Int32, g1_team_composition_adult Int32, g1_NamesMP_match Int32, g1_valid_marker Int32, g1_T_memTrained Int32, g1_Tdate String, g1_opv_dry_valid_vvm Int32, g1_opv_dry_cool String, g1_opv_dry_dry Int32, g1_t_sheetcorrect Int32, g1_vitamin_a String, g1_T_recordedMissed Int32, g1_T_mapAvailable Int32, g1_Tworkload Int32, g1_Comment String, g2_obsorve Int32, g2_T_chiledAwayHome  Int32, g2_T_rmAFP Int32, g2_T_markerUsed Int32, g2_T_CfMarked Int32, g2_T_childVaccinatedToday Int32, g2_Comments String, covid_indi Int32, covid_brief_guidline Int32, covid_symptoms Int32, covid_wear_mask Int32, covid_use_santzr Int32, covid_age_member Int32, covid_hold_child Int32, covid_chllenges Int32, covid_chllenges_dscrptn String, unique_id String, dev_remarks String, app_version String, cold_chain String, g1_opv_dry_intact Int32, g1_opv_dry_re_capped Int32, g1_matched_admin_team Int32, team_vacc_ts_cc Int32, date_updated Date,campaign_ID Int32, campaign_ActivityName String, campaign_ActivityID_old Int32, campaign_Yr Int32, campaign_SubActivityName String, geoLocation_name String,geoLocation_type Int32,geoLocation_code Int32,geoLocation_census_pop Int32,geoLocation_target Int32,geoLocation_status Int32,geoLocation_pname String,geoLocation_dname String,geoLocation_namedistrict String,geoLocation_codedistrict String,geoLocation_tname String,geoLocation_provincecode Int32,geoLocation_districtcode Int32,geoLocation_tehsilcode Int32,geoLocation_priority Int32,geoLocation_commnet Int32,geoLocation_hr Int32,geoLocation_fcm Int8,geoLocation_tier Int32,geoLocation_block String,geoLocation_division String,geoLocation_cordinates String,geoLocation_latitude String,geoLocation_longitude String,geoLocation_x String,geoLocation_y String,geoLocation_imagepath String,geoLocation_isccpv Int32,geoLocation_rank Int32,geoLocation_rank_score Float64,geoLocation_ishealthcamp Int32,geoLocation_isdsc Int32,geoLocation_ucorg String,geoLocation_organization String,geoLocation_tierfromaug161 Int32,geoLocation_tierfromsep171 Int32,geoLocation_tierfromdec181 Int32,geoLocation_mtap Int32,geoLocation_rspuc Int32,geoLocation_issmt Int32,geoLocation_updateddatetime String,geoLocation_x_code Int32,geoLocation_draining_uc Int32,geoLocation_upap_districts Int32,geoLocation_shruc Int32,geoLocation_khidist_id String )ENGINE = MergeTree PRIMARY KEY pk_icm_team_21_id ORDER BY pk_icm_team_21_id"
#                 client.execute(sql)
#                 cols = "test.get_icm_team_monitoring.pk_icm_team_21_id, test.get_icm_team_monitoring.fk_form_id, test.get_icm_team_monitoring.fk_user_id, test.get_icm_team_monitoring.date_created, test.get_icm_team_monitoring.campaign_id, test.get_icm_team_monitoring.surveyor_name, test.get_icm_team_monitoring.designation, test.get_icm_team_monitoring.surveyor_affliate, test.get_icm_team_monitoring.fk_prov_id, test.get_icm_team_monitoring.fk_dist_id, test.get_icm_team_monitoring.fk_tehsil_id, test.get_icm_team_monitoring.fk_uc_id, test.get_icm_team_monitoring.g1_cbv_noncbv, test.get_icm_team_monitoring.g1_aic_name, test.get_icm_team_monitoring.g1_team_no, test.get_icm_team_monitoring.g1_day, test.get_icm_team_monitoring.g1_team_composition_government, test.get_icm_team_monitoring.g1_team_composition_local, test.get_icm_team_monitoring.g1_team_composition_female, test.get_icm_team_monitoring.g1_team_composition_adult, test.get_icm_team_monitoring.g1_NamesMP_match, test.get_icm_team_monitoring.g1_valid_marker, test.get_icm_team_monitoring.g1_T_memTrained, test.get_icm_team_monitoring.g1_Tdate, test.get_icm_team_monitoring.g1_opv_dry_valid_vvm, test.get_icm_team_monitoring.g1_opv_dry_cool, test.get_icm_team_monitoring.g1_opv_dry_dry, test.get_icm_team_monitoring.g1_t_sheetcorrect, test.get_icm_team_monitoring.g1_vitamin_a, test.get_icm_team_monitoring.g1_T_recordedMissed, test.get_icm_team_monitoring.g1_T_mapAvailable, test.get_icm_team_monitoring.g1_Tworkload, test.get_icm_team_monitoring.g1_Comment, test.get_icm_team_monitoring.g2_obsorve, test.get_icm_team_monitoring.g2_T_chiledAwayHome, test.get_icm_team_monitoring.g2_T_rmAFP, test.get_icm_team_monitoring.g2_T_markerUsed, test.get_icm_team_monitoring.g2_T_CfMarked, test.get_icm_team_monitoring.g2_T_childVaccinatedToday, test.get_icm_team_monitoring.g2_Comments, test.get_icm_team_monitoring.covid_indi, test.get_icm_team_monitoring.covid_brief_guidline, test.get_icm_team_monitoring.covid_symptoms, test.get_icm_team_monitoring.covid_wear_mask, test.get_icm_team_monitoring.covid_use_santzr, test.get_icm_team_monitoring.covid_age_member, test.get_icm_team_monitoring.covid_hold_child, test.get_icm_team_monitoring.covid_chllenges, test.get_icm_team_monitoring.covid_chllenges_dscrptn, test.get_icm_team_monitoring.unique_id, test.get_icm_team_monitoring.dev_remarks, test.get_icm_team_monitoring.app_version, test.get_icm_team_monitoring.cold_chain, test.get_icm_team_monitoring.g1_opv_dry_intact, test.get_icm_team_monitoring.g1_opv_dry_re_capped, test.get_icm_team_monitoring.g1_matched_admin_team, test.get_icm_team_monitoring.team_vacc_ts_cc , test.get_icm_team_monitoring.date_updated, test.xbi_campaign.campaign_ID, test.xbi_campaign.campaign_ActivityName, test.xbi_campaign.campaign_ActivityID_old, test.xbi_campaign.campaign_Yr, test.xbi_campaign.campaign_SubActivityName, test.eoc_geolocation_tbl.name, test.eoc_geolocation_tbl.type, test.eoc_geolocation_tbl.code, test.eoc_geolocation_tbl.census_pop, test.eoc_geolocation_tbl.target, test.eoc_geolocation_tbl.status, test.eoc_geolocation_tbl.pname, test.eoc_geolocation_tbl.dname, test.eoc_geolocation_tbl.namedistrict, test.eoc_geolocation_tbl.codedistrict, test.eoc_geolocation_tbl.tname, test.eoc_geolocation_tbl.provincecode, test.eoc_geolocation_tbl.districtcode, test.eoc_geolocation_tbl.tehsilcode, test.eoc_geolocation_tbl.priority, test.eoc_geolocation_tbl.commnet, test.eoc_geolocation_tbl.hr, test.eoc_geolocation_tbl.fcm, test.eoc_geolocation_tbl.tier, test.eoc_geolocation_tbl.block, test.eoc_geolocation_tbl.division, test.eoc_geolocation_tbl.cordinates, test.eoc_geolocation_tbl.latitude, test.eoc_geolocation_tbl.longitude, test.eoc_geolocation_tbl.x, test.eoc_geolocation_tbl.y, test.eoc_geolocation_tbl.imagepath, test.eoc_geolocation_tbl.isccpv, test.eoc_geolocation_tbl.rank, test.eoc_geolocation_tbl.rank_score, test.eoc_geolocation_tbl.ishealthcamp, test.eoc_geolocation_tbl.isdsc, test.eoc_geolocation_tbl.ucorg, test.eoc_geolocation_tbl.organization, test.eoc_geolocation_tbl.tierfromaug161, test.eoc_geolocation_tbl.tierfromsep171, test.eoc_geolocation_tbl.tierfromdec181, test.eoc_geolocation_tbl.mtap, test.eoc_geolocation_tbl.rspuc, test.eoc_geolocation_tbl.issmt, test.eoc_geolocation_tbl.updateddatetime, test.eoc_geolocation_tbl.x_code, test.eoc_geolocation_tbl.draining_uc, test.eoc_geolocation_tbl.upap_districts, test.eoc_geolocation_tbl.shruc, test.eoc_geolocation_tbl.khidist_id     "
#                 sql = "SELECT " + cols + "  FROM test.get_icm_team_monitoring eoc_1 left join test.eoc_geolocation_tbl eoc_2 ON eoc_1.fk_uc_id  = eoc_2.code left JOIN test.xbi_campaign eoc_3 ON eoc_1.campaign_id  = eoc_3.campaign_ID "

#                 data = client.execute(sql)
                
#                 apiDataFrame = pd.DataFrame(data)
#                 apiDataFrame = apiDataFrame.replace(r'^\s*$', np.nan, regex=True)
#                 all_columns = list(apiDataFrame)  # Creates list of all column headers
                
#                 cols = apiDataFrame.iloc[0]
#                 apiDataFrame[all_columns] = apiDataFrame[all_columns].astype(str)
#                 d = 'pk_icm_team_21_id', 'fk_form_id', 'fk_user_id', 'date_created', 'campaign_id', 'surveyor_name', 'designation', 'surveyor_affliate', 'fk_prov_id', 'fk_dist_id', 'fk_tehsil_id', 'fk_uc_id', 'g1_cbv_noncbv', 'g1_aic_name', 'g1_team_no', 'g1_day', 'g1_team_composition_government', 'g1_team_composition_local', 'g1_team_composition_female', 'g1_team_composition_adult', 'g1_NamesMP_match', 'g1_valid_marker', 'g1_T_memTrained', 'g1_Tdate', 'g1_opv_dry_valid_vvm', 'g1_opv_dry_cool', 'g1_opv_dry_dry', 'g1_t_sheetcorrect', 'g1_vitamin_a', 'g1_T_recordedMissed', 'g1_T_mapAvailable', 'g1_Tworkload', 'g1_Comment', 'g2_obsorve', 'g2_T_chiledAwayHome', 'g2_T_rmAFP', 'g2_T_markerUsed', 'g2_T_CfMarked', 'g2_T_childVaccinatedToday', 'g2_Comments', 'covid_indi', 'covid_brief_guidline', 'covid_symptoms', 'covid_wear_mask', 'covid_use_santzr', 'covid_age_member', 'covid_hold_child', 'covid_chllenges', 'covid_chllenges_dscrptn', 'unique_id', 'dev_remarks', 'app_version', 'cold_chain', 'g1_opv_dry_intact', 'g1_opv_dry_re_capped', 'g1_matched_admin_team','team_vacc_ts_cc','date_updated', 'campaign_ID', 'campaign_ActivityName', 'campaign_ActivityID_old', 'campaign_Yr', 'campaign_SubActivityName','geoLocation_name', 'geoLocation_type', 'geoLocation_code', 'geoLocation_census_pop', 'geoLocation_target', 'geoLocation_status', 'geoLocation_pname', 'geoLocation_dname', 'geoLocation_namedistrict', 'geoLocation_codedistrict', 'geoLocation_tname', 'geoLocation_provincecode', 'geoLocation_districtcode', 'geoLocation_tehsilcode', 'geoLocation_priority', 'geoLocation_commnet', 'geoLocation_hr', 'geoLocation_fcm','geoLocation_tier','geoLocation_block','geoLocation_division','geoLocation_cordinates','geoLocation_latitude','geoLocation_longitude','geoLocation_x','geoLocation_y','geoLocation_imagepath','geoLocation_isccpv','geoLocation_rank','geoLocation_rank_score','geoLocation_ishealthcamp','geoLocation_isdsc','geoLocation_ucorg','geoLocation_organization','geoLocation_tierfromaug161','geoLocation_tierfromsep171','geoLocation_tierfromdec181','geoLocation_mtap','geoLocation_rspuc','geoLocation_issmt','geoLocation_updateddatetime','geoLocation_x_code','geoLocation_draining_uc','geoLocation_upap_districts','geoLocation_shruc','geoLocation_khidist_id' 
#                 dff = pd.DataFrame(columns=d)
#                 for index, item in enumerate(d):
#                     dff[item] = apiDataFrame[index].values
#                 df3 = client.query_dataframe(
#                     "SELECT * FROM test.xbi_icm_team_monitoring")
#                 if df3.empty:
#                     dff[["date_created"]] = dff[["date_created"]].apply(pd.to_datetime)
#                     dff[["date_updated"]] = dff[["date_updated"]].apply(pd.to_datetime)
#                     client.insert_dataframe(
#                         'INSERT INTO test.xbi_icm_team_monitoring  VALUES', dff)
#                     logger.info(
#                         ' Data has been inserted into Table\' INSERT INTO test.xbi_icm_team_monitoring  VALUES \' ')

#                     sql = "DROP table if exists test.get_icm_team_monitoring"
#                     client.execute(sql)
#                     print('\n\n\n CID\t\t--------\t', cid)
#                     print('\n\n\n Page\t\t--------\t', i)
#                 else:
#                     dff[["date_created"]] = dff[["date_created"]].apply(pd.to_datetime)
#                     dff[["date_updated"]] = dff[["date_updated"]].apply(pd.to_datetime)
#                     client.insert_dataframe(
#                         'INSERT INTO test.xbi_icm_team_monitoring  VALUES', dff)
#                     logger.info(
#                             ' Data has been inserted into Table test.xbi_icm_team_monitoring for campaign '+str(cid))    
#                     print('\n\n\n CID\t\t--------\t', cid)
#                     print('\n\n\n Page\t\t--------\t', i)
                    
#                     sql = "DROP table if exists test.get_icm_team_monitoring"
#                     client.execute(sql)
                        

# dag = DAG(
#     'Prod_ICM_Team_Monitoring_Automated',
#     schedule_interval='0 0 * * *', # will run daily at mid-night
#     #schedule_interval='*/59 * * * *',  # will run every 10 min.
#     default_args=default_args,
#     catchup=False)

# with dag:
#     GetAndInsertApiDataICMTeamMonitoring = PythonOperator(
#         task_id='GetAndInsertApiDataICMTeamMonitoring',
#         python_callable=GetAndInsertApiDataICMTeamMonitoring,
#     )
    
# GetAndInsertApiDataICMTeamMonitoring



#-------------------------------------------------------------------------------------------------------------------------------#
#-------------------------------------- INSERT LATEST CAMPAIGN DATA ICM_Team_Monitoring ---------------------------------------#
#----------------------------------------------- Author: Abdul Bari Malik ------------------------------------------------------#
#-------------------------------------------------------------------------------------------------------------------------------#

def GetAndInsertApiDataICMTeamMonitoring():
    #li3 = [256]
    merged_df = pd.DataFrame()
    
    logger.info('Function \' teGetAndInsertApiDataICMTeamMonitoring \' Started Off')
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

        get_q = "select campaign_ID FROM (select * from test.xbi_campaign xc  order by xc.campaign_ID desc limit 4) ORDER BY campaign_ID ASC "
        df2 = client.query_dataframe(get_q)
        print(df2)
        df = pd.DataFrame(df2)

        campID_list = df2.campaign_ID.tolist()
        print(campID_list)   



        for cid in campID_list:                                
            url = "http://idims.eoc.gov.pk/api_who/api/get_icm/5468XE2LN6CzR7qRG041/team/"+ str(cid)+"/"
            print('\n\n Campaign-ID\t\t--------\t', cid,'\t--------\t ')
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
                    url_c = "http://idims.eoc.gov.pk/api_who/api/get_icm/5468XE2LN6CzR7qRG041/team/"+str(cid)+'/'+str(i)
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
                
                sql = "CREATE TABLE if not exists test.get_icm_team_monitoring (pk_icm_team_21_id Int32, fk_form_id Int32, fk_user_id Int32, date_created Date, campaign_id Int32, surveyor_name String, designation String, surveyor_affliate String, fk_prov_id Int32, fk_dist_id Int32, fk_tehsil_id Int32, fk_uc_id Int32, g1_cbv_noncbv Int32, g1_aic_name String, g1_team_no String, g1_day String, g1_team_composition_government Int32, g1_team_composition_local Int32, g1_team_composition_female Int32, g1_team_composition_adult Int32, g1_NamesMP_match Int32, g1_valid_marker Int32, g1_T_memTrained Int32, g1_Tdate String, g1_opv_dry_valid_vvm Int32, g1_opv_dry_cool String, g1_opv_dry_dry Int32, g1_t_sheetcorrect Int32, g1_vitamin_a String, g1_T_recordedMissed Int32, g1_T_mapAvailable Int32, g1_Tworkload Int32, g1_Comment String, g2_obsorve Int32, g2_T_chiledAwayHome  Int32, g2_T_rmAFP Int32, g2_T_markerUsed Int32, g2_T_CfMarked Int32, g2_T_childVaccinatedToday Int32, g2_Comments String, covid_indi Int32, covid_brief_guidline Int32, covid_symptoms Int32, covid_wear_mask Int32, covid_use_santzr Int32, covid_age_member Int32, covid_hold_child Int32, covid_chllenges Int32, covid_chllenges_dscrptn String, unique_id String, dev_remarks String, app_version String, cold_chain String, g1_opv_dry_intact Int32, g1_opv_dry_re_capped Int32, g1_matched_admin_team Int32, team_vacc_ts_cc Int32, date_updated Date)ENGINE = MergeTree PRIMARY KEY pk_icm_team_21_id ORDER BY pk_icm_team_21_id"
                client.execute(sql)

                df2 = client.query_dataframe(
                    "SELECT * FROM test.get_icm_team_monitoring")
                if df2.empty:
                    df['date_created'] = pd.to_datetime(df['date_created'],unit='s')
                    df['date_created'] = df['date_created'].dt.date
                    df['date_updated'] = pd.to_datetime(df['date_updated'],unit='s')
                    df['date_updated'] = df['date_updated'].dt.date
                    df = df.replace(r'^\s*$', np.nan, regex=True)
                    client.insert_dataframe(
                        'INSERT INTO test.get_icm_team_monitoring VALUES', df)
                    logger.info(
                            ' Data has been inserted into Table test.get_icm_team_monitoring for campaign'+str(cid))    
                else:
                    df['date_created'] = pd.to_datetime(df['date_created'],unit='s')
                    df['date_created'] = df['date_created'].dt.date
                    df['date_updated'] = pd.to_datetime(df['date_updated'],unit='s')
                    df['date_updated'] = df['date_updated'].dt.date
                    df = df.replace(r'^\s*$', np.nan, regex=True)
                    client.insert_dataframe(
                        'INSERT INTO test.get_icm_team_monitoring VALUES', df)
                    logger.info(
                            ' Data has been inserted into Table test.get_icm_team_monitoring for campaign'+str(cid))    


def CreateJoinTableOfICMTeamMonitoring():
    #cid = 256
    logger.info(' Function  \' CreateJoinTableOfICMTeamMonitoring \' has been Initiated')
    
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

        get_q = "select campaign_ID FROM (select * from test.xbi_campaign xc  order by xc.campaign_ID desc limit 4) ORDER BY campaign_ID ASC "
        df2 = client.query_dataframe(get_q)
        print(df2)
        df = pd.DataFrame(df2)

        campID_list = df2.campaign_ID.tolist()
        print(campID_list)

        for cid in campID_list:
            sql = "CREATE TABLE if not exists test.xbi_icm_team_monitoring (pk_icm_team_21_id Int32, fk_form_id Int32, fk_user_id Int32, date_created Date, campaign_id Int32, surveyor_name String, designation String, surveyor_affliate String, fk_prov_id Int32, fk_dist_id Int32, fk_tehsil_id Int32, fk_uc_id Int32, g1_cbv_noncbv Int32, g1_aic_name String, g1_team_no String, g1_day String, g1_team_composition_government Int32, g1_team_composition_local Int32, g1_team_composition_female Int32, g1_team_composition_adult Int32, g1_NamesMP_match Int32, g1_valid_marker Int32, g1_T_memTrained Int32, g1_Tdate String, g1_opv_dry_valid_vvm Int32, g1_opv_dry_cool String, g1_opv_dry_dry Int32, g1_t_sheetcorrect Int32, g1_vitamin_a String, g1_T_recordedMissed Int32, g1_T_mapAvailable Int32, g1_Tworkload Int32, g1_Comment String, g2_obsorve Int32, g2_T_chiledAwayHome  Int32, g2_T_rmAFP Int32, g2_T_markerUsed Int32, g2_T_CfMarked Int32, g2_T_childVaccinatedToday Int32, g2_Comments String, covid_indi Int32, covid_brief_guidline Int32, covid_symptoms Int32, covid_wear_mask Int32, covid_use_santzr Int32, covid_age_member Int32, covid_hold_child Int32, covid_chllenges Int32, covid_chllenges_dscrptn String, unique_id String, dev_remarks String, app_version String, cold_chain String, g1_opv_dry_intact Int32, g1_opv_dry_re_capped Int32, g1_matched_admin_team Int32, team_vacc_ts_cc Int32, date_updated Date,campaign_ID Int32, campaign_ActivityName String, campaign_ActivityID_old Int32, campaign_Yr Int32, campaign_SubActivityName String, geoLocation_name String,geoLocation_type Int32,geoLocation_code Int32,geoLocation_census_pop Int32,geoLocation_target Int32,geoLocation_status Int32,geoLocation_pname String,geoLocation_dname String,geoLocation_namedistrict String,geoLocation_codedistrict String,geoLocation_tname String,geoLocation_provincecode Int32,geoLocation_districtcode Int32,geoLocation_tehsilcode Int32,geoLocation_priority Int32,geoLocation_commnet Int32,geoLocation_hr Int32,geoLocation_fcm Int8,geoLocation_tier Int32,geoLocation_block String,geoLocation_division String,geoLocation_cordinates String,geoLocation_latitude String,geoLocation_longitude String,geoLocation_x String,geoLocation_y String,geoLocation_imagepath String,geoLocation_isccpv Int32,geoLocation_rank Int32,geoLocation_rank_score Float64,geoLocation_ishealthcamp Int32,geoLocation_isdsc Int32,geoLocation_ucorg String,geoLocation_organization String,geoLocation_tierfromaug161 Int32,geoLocation_tierfromsep171 Int32,geoLocation_tierfromdec181 Int32,geoLocation_mtap Int32,geoLocation_rspuc Int32,geoLocation_issmt Int32,geoLocation_updateddatetime String,geoLocation_x_code Int32,geoLocation_draining_uc Int32,geoLocation_upap_districts Int32,geoLocation_shruc Int32,geoLocation_khidist_id String )ENGINE = MergeTree PRIMARY KEY pk_icm_team_21_id ORDER BY pk_icm_team_21_id"
            client.execute(sql)
            cols = "test.get_icm_team_monitoring.pk_icm_team_21_id, test.get_icm_team_monitoring.fk_form_id, test.get_icm_team_monitoring.fk_user_id, test.get_icm_team_monitoring.date_created, test.get_icm_team_monitoring.campaign_id, test.get_icm_team_monitoring.surveyor_name, test.get_icm_team_monitoring.designation, test.get_icm_team_monitoring.surveyor_affliate, test.get_icm_team_monitoring.fk_prov_id, test.get_icm_team_monitoring.fk_dist_id, test.get_icm_team_monitoring.fk_tehsil_id, test.get_icm_team_monitoring.fk_uc_id, test.get_icm_team_monitoring.g1_cbv_noncbv, test.get_icm_team_monitoring.g1_aic_name, test.get_icm_team_monitoring.g1_team_no, test.get_icm_team_monitoring.g1_day, test.get_icm_team_monitoring.g1_team_composition_government, test.get_icm_team_monitoring.g1_team_composition_local, test.get_icm_team_monitoring.g1_team_composition_female, test.get_icm_team_monitoring.g1_team_composition_adult, test.get_icm_team_monitoring.g1_NamesMP_match, test.get_icm_team_monitoring.g1_valid_marker, test.get_icm_team_monitoring.g1_T_memTrained, test.get_icm_team_monitoring.g1_Tdate, test.get_icm_team_monitoring.g1_opv_dry_valid_vvm, test.get_icm_team_monitoring.g1_opv_dry_cool, test.get_icm_team_monitoring.g1_opv_dry_dry, test.get_icm_team_monitoring.g1_t_sheetcorrect, test.get_icm_team_monitoring.g1_vitamin_a, test.get_icm_team_monitoring.g1_T_recordedMissed, test.get_icm_team_monitoring.g1_T_mapAvailable, test.get_icm_team_monitoring.g1_Tworkload, test.get_icm_team_monitoring.g1_Comment, test.get_icm_team_monitoring.g2_obsorve, test.get_icm_team_monitoring.g2_T_chiledAwayHome, test.get_icm_team_monitoring.g2_T_rmAFP, test.get_icm_team_monitoring.g2_T_markerUsed, test.get_icm_team_monitoring.g2_T_CfMarked, test.get_icm_team_monitoring.g2_T_childVaccinatedToday, test.get_icm_team_monitoring.g2_Comments, test.get_icm_team_monitoring.covid_indi, test.get_icm_team_monitoring.covid_brief_guidline, test.get_icm_team_monitoring.covid_symptoms, test.get_icm_team_monitoring.covid_wear_mask, test.get_icm_team_monitoring.covid_use_santzr, test.get_icm_team_monitoring.covid_age_member, test.get_icm_team_monitoring.covid_hold_child, test.get_icm_team_monitoring.covid_chllenges, test.get_icm_team_monitoring.covid_chllenges_dscrptn, test.get_icm_team_monitoring.unique_id, test.get_icm_team_monitoring.dev_remarks, test.get_icm_team_monitoring.app_version, test.get_icm_team_monitoring.cold_chain, test.get_icm_team_monitoring.g1_opv_dry_intact, test.get_icm_team_monitoring.g1_opv_dry_re_capped, test.get_icm_team_monitoring.g1_matched_admin_team, test.get_icm_team_monitoring.team_vacc_ts_cc , test.get_icm_team_monitoring.date_updated, test.xbi_campaign.campaign_ID, test.xbi_campaign.campaign_ActivityName, test.xbi_campaign.campaign_ActivityID_old, test.xbi_campaign.campaign_Yr, test.xbi_campaign.campaign_SubActivityName, test.eoc_geolocation_tbl.name, test.eoc_geolocation_tbl.type, test.eoc_geolocation_tbl.code, test.eoc_geolocation_tbl.census_pop, test.eoc_geolocation_tbl.target, test.eoc_geolocation_tbl.status, test.eoc_geolocation_tbl.pname, test.eoc_geolocation_tbl.dname, test.eoc_geolocation_tbl.namedistrict, test.eoc_geolocation_tbl.codedistrict, test.eoc_geolocation_tbl.tname, test.eoc_geolocation_tbl.provincecode, test.eoc_geolocation_tbl.districtcode, test.eoc_geolocation_tbl.tehsilcode, test.eoc_geolocation_tbl.priority, test.eoc_geolocation_tbl.commnet, test.eoc_geolocation_tbl.hr, test.eoc_geolocation_tbl.fcm, test.eoc_geolocation_tbl.tier, test.eoc_geolocation_tbl.block, test.eoc_geolocation_tbl.division, test.eoc_geolocation_tbl.cordinates, test.eoc_geolocation_tbl.latitude, test.eoc_geolocation_tbl.longitude, test.eoc_geolocation_tbl.x, test.eoc_geolocation_tbl.y, test.eoc_geolocation_tbl.imagepath, test.eoc_geolocation_tbl.isccpv, test.eoc_geolocation_tbl.rank, test.eoc_geolocation_tbl.rank_score, test.eoc_geolocation_tbl.ishealthcamp, test.eoc_geolocation_tbl.isdsc, test.eoc_geolocation_tbl.ucorg, test.eoc_geolocation_tbl.organization, test.eoc_geolocation_tbl.tierfromaug161, test.eoc_geolocation_tbl.tierfromsep171, test.eoc_geolocation_tbl.tierfromdec181, test.eoc_geolocation_tbl.mtap, test.eoc_geolocation_tbl.rspuc, test.eoc_geolocation_tbl.issmt, test.eoc_geolocation_tbl.updateddatetime, test.eoc_geolocation_tbl.x_code, test.eoc_geolocation_tbl.draining_uc, test.eoc_geolocation_tbl.upap_districts, test.eoc_geolocation_tbl.shruc, test.eoc_geolocation_tbl.khidist_id     "
            sql = "SELECT " + cols + "  FROM test.get_icm_team_monitoring eoc_1 left join test.eoc_geolocation_tbl eoc_2 ON eoc_1.fk_uc_id  = eoc_2.code left JOIN test.xbi_campaign eoc_3 ON eoc_1.campaign_id  = eoc_3.campaign_ID WHERE eoc_1.campaign_id ="+str(cid)

            data = client.execute(sql)
                    
            apiDataFrame = pd.DataFrame(data)
            apiDataFrame = apiDataFrame.replace(r'^\s*$', np.nan, regex=True)
            all_columns = list(apiDataFrame)  # Creates list of all column headers
                    
            cols = apiDataFrame.iloc[0]
            apiDataFrame[all_columns] = apiDataFrame[all_columns].astype(str)
            d = 'pk_icm_team_21_id', 'fk_form_id', 'fk_user_id', 'date_created', 'campaign_id', 'surveyor_name', 'designation', 'surveyor_affliate', 'fk_prov_id', 'fk_dist_id', 'fk_tehsil_id', 'fk_uc_id', 'g1_cbv_noncbv', 'g1_aic_name', 'g1_team_no', 'g1_day', 'g1_team_composition_government', 'g1_team_composition_local', 'g1_team_composition_female', 'g1_team_composition_adult', 'g1_NamesMP_match', 'g1_valid_marker', 'g1_T_memTrained', 'g1_Tdate', 'g1_opv_dry_valid_vvm', 'g1_opv_dry_cool', 'g1_opv_dry_dry', 'g1_t_sheetcorrect', 'g1_vitamin_a', 'g1_T_recordedMissed', 'g1_T_mapAvailable', 'g1_Tworkload', 'g1_Comment', 'g2_obsorve', 'g2_T_chiledAwayHome', 'g2_T_rmAFP', 'g2_T_markerUsed', 'g2_T_CfMarked', 'g2_T_childVaccinatedToday', 'g2_Comments', 'covid_indi', 'covid_brief_guidline', 'covid_symptoms', 'covid_wear_mask', 'covid_use_santzr', 'covid_age_member', 'covid_hold_child', 'covid_chllenges', 'covid_chllenges_dscrptn', 'unique_id', 'dev_remarks', 'app_version', 'cold_chain', 'g1_opv_dry_intact', 'g1_opv_dry_re_capped', 'g1_matched_admin_team','team_vacc_ts_cc','date_updated', 'campaign_ID', 'campaign_ActivityName', 'campaign_ActivityID_old', 'campaign_Yr', 'campaign_SubActivityName','geoLocation_name', 'geoLocation_type', 'geoLocation_code', 'geoLocation_census_pop', 'geoLocation_target', 'geoLocation_status', 'geoLocation_pname', 'geoLocation_dname', 'geoLocation_namedistrict', 'geoLocation_codedistrict', 'geoLocation_tname', 'geoLocation_provincecode', 'geoLocation_districtcode', 'geoLocation_tehsilcode', 'geoLocation_priority', 'geoLocation_commnet', 'geoLocation_hr', 'geoLocation_fcm','geoLocation_tier','geoLocation_block','geoLocation_division','geoLocation_cordinates','geoLocation_latitude','geoLocation_longitude','geoLocation_x','geoLocation_y','geoLocation_imagepath','geoLocation_isccpv','geoLocation_rank','geoLocation_rank_score','geoLocation_ishealthcamp','geoLocation_isdsc','geoLocation_ucorg','geoLocation_organization','geoLocation_tierfromaug161','geoLocation_tierfromsep171','geoLocation_tierfromdec181','geoLocation_mtap','geoLocation_rspuc','geoLocation_issmt','geoLocation_updateddatetime','geoLocation_x_code','geoLocation_draining_uc','geoLocation_upap_districts','geoLocation_shruc','geoLocation_khidist_id' 
            dff = pd.DataFrame(columns=d)
            for index, item in enumerate(d):
                dff[item] = apiDataFrame[index].values
            df3 = client.query_dataframe("SELECT * FROM test.xbi_icm_team_monitoring WHERE campaign_id ="+str(cid))

            print('\n\n Campaign \t\t--------\t', cid, '\t--------\tData Retrieved \t--------\t','Data Size: ',len(df3))

            if df3.empty:
                client.insert_dataframe(
                    'INSERT INTO test.xbi_icm_team_monitoring  VALUES', dff)
                logger.info(
                    ' Data has been inserted into Table\' INSERT INTO test.xbi_icm_team_monitoring  VALUES \' ')

                # sql = "DROP table if exists test.get_icm_team_monitoring"
                # client.execute(sql)
                print('\n\n Campaign \t\t--------\t', cid, '\t--------\tData Inserted \n\n')

            else:
                sql = "ALTER TABLE test.xbi_icm_team_monitoring DELETE WHERE campaign_id =  "+str(cid)
                client.execute(sql)
                
                client.insert_dataframe(
                    'INSERT INTO test.xbi_icm_team_monitoring  VALUES', dff)
                logger.info(
                    ' Data has been inserted into Table test.xbi_icm_team_monitoring for campaign '+str(cid))    
                
                print('\n\n Campaign \t\t--------\t', cid, '\t--------\t Data Inserted \n\n')
                        
        sql = "DROP table if exists test.get_icm_team_monitoring"
        client.execute(sql)

dag = DAG(
    'Prod_ICM_Team_Monitoring_Automated',
    schedule_interval='0 0 * * *', # will run daily at mid-night
    #schedule_interval='*/59 * * * *',  # will run every 10 min.
    default_args=default_args,
    catchup=False)

with dag:
    GetAndInsertApiDataICMTeamMonitoring = PythonOperator(
        task_id='GetAndInsertApiDataICMTeamMonitoring',
        python_callable=GetAndInsertApiDataICMTeamMonitoring,
    )
    CreateJoinTableOfICMTeamMonitoring = PythonOperator(
        task_id='CreateJoinTableOfICMTeamMonitoring',
        python_callable=CreateJoinTableOfICMTeamMonitoring,
    )
GetAndInsertApiDataICMTeamMonitoring >> CreateJoinTableOfICMTeamMonitoring
