
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
#---------------------------------------- INSERT CAMPAIGN DATA ICM Supervisor   ------------------------------------------------#
#-------------------------------------------- Author: Abdul Bari Malik ---------------------------------------------------------#
#-------------------------------------------------------------------------------------------------------------------------------#
# def GetAndInsertApiDataICMSupervisorMonitoring():
    
#     li3 = [222,223,224,225,226,227,228,229,230,231,232,233,234,235,236,237,238,239,240,241,242,243,244,245,247,248,249,250,251,252,253,254,255,256]
    
#     logger.info('Function \' GetAndInsertApiDataICMSupervisorMonitoring \' Started Off')
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

#             sql = "CREATE TABLE if not exists test.get_icm_supervisor_monitoring (pk_icm_supervisor_21_id Int32, fk_form_id Int32, fk_user_id Int32,date_created String, campaign_id Int32, surveyor_name String,designation String, surveyor_affliate String,fk_prov_id Int32,fk_dist_id Int32, fk_tehsil_id Int32, fk_uc_id Int32, g1_Cday String,g1_supv_name String,g1_supv_desg String,g1_supv_agency String,g1_sup_trained_count Int32,g1_sup_t_date String,g1_sup_ttrained_count Int32,g1_sup_tt_date String,g1_items_tsupport_count Int32,g1_track_mopv2 Int32,g1_sup_suprvsn Int32,g1_Comp_tm_hhform Int32,g1_hrp_present String,g1_Comment String,col_covid_brief_guidline Int32,col_covid_wear_mask Int32,col_covid_use_santzr Int32,col_covid_social_dist Int32,g1_gis_mapavail_count String,g1_gis_mapuse String,g1_gis_mapworkload String,unique_id String,dev_remarks String,app_version String,vac_type Int32,signed_vm03 Int32,supp_clear Int32,is_archive Int32, update_by Int32, archive_reason String,date_updated Int32)ENGINE = MergeTree PRIMARY KEY pk_icm_supervisor_21_id ORDER BY pk_icm_supervisor_21_id"
#             client.execute(sql)

#             df2 = client.query_dataframe(
#                 "SELECT * FROM test.get_icm_supervisor_monitoring")
#             if df2.empty:
#                 df = df.replace(r'^\s*$', np.nan, regex=True)
#                 client.insert_dataframe(
#                     'INSERT INTO test.get_icm_supervisor_monitoring VALUES', df)
#                 logger.info(
#                         ' Data has been inserted into Table test.xbi_icm_supervisor_monitoring for campaign'+str(cid))    
#             sql = "CREATE TABLE if not exists test.xbi_icm_supervisor_monitoring (pk_icm_supervisor_21_id Int32, fk_form_id Int32, fk_user_id Int32,date_created String, campaign_id Int32, surveyor_name String,designation String, surveyor_affliate String,fk_prov_id Int32,fk_dist_id Int32, fk_tehsil_id Int32, fk_uc_id Int32, g1_Cday String,g1_supv_name String,g1_supv_desg String,g1_supv_agency String,g1_sup_trained_count Int32,g1_sup_t_date String,g1_sup_ttrained_count Int32,g1_sup_tt_date String,g1_items_tsupport_count Int32,g1_track_mopv2 Int32,g1_sup_suprvsn Int32,g1_Comp_tm_hhform Int32,g1_hrp_present String,g1_Comment String,col_covid_brief_guidline Int32,col_covid_wear_mask Int32,col_covid_use_santzr Int32,col_covid_social_dist Int32,g1_gis_mapavail_count String,g1_gis_mapuse String,g1_gis_mapworkload String,unique_id String,dev_remarks String,app_version String,vac_type Int32,signed_vm03 Int32,supp_clear Int32,is_archive Int32, update_by Int32, archive_reason String,date_updated Int32,campaign_ID Int32, campaign_ActivityName String, campaign_ActivityID_old Int32, campaign_Yr Int32, campaign_SubActivityName String, geoLocation_name String, geoLocation_type String, geoLocation_code String, geoLocation_census_pop String, geoLocation_target String, geoLocation_status String, geoLocation_pname String, geoLocation_dname String, geoLocation_namedistrict String, geoLocation_codedistrict String, geoLocation_tname String, geoLocation_provincecode String, geoLocation_districtcode String, geoLocation_tehsilcode String, geoLocation_priority String, geoLocation_commnet String, geoLocation_hr String, geoLocation_fcm String, geoLocation_tier String, geoLocation_block String, geoLocation_division String, geoLocation_cordinates String, geoLocation_latitude String, geoLocation_longitude String, geoLocation_x String, geoLocation_y String, geoLocation_imagepath String, geoLocation_isccpv String, geoLocation_rank String, geoLocation_rank_score String, geoLocation_ishealthcamp String, geoLocation_isdsc String, geoLocation_ucorg String, geoLocation_organization String, geoLocation_tierfromaug161 String, geoLocation_tierfromsep171 String, geoLocation_tierfromdec181 String, geoLocation_mtap String, geoLocation_rspuc String, geoLocation_issmt String, geoLocation_updateddatetime String, geoLocation_x_code String, geoLocation_draining_uc String, geoLocation_upap_districts String, geoLocation_shruc String, geoLocation_khidist_id String )ENGINE = MergeTree PRIMARY KEY pk_icm_supervisor_21_id ORDER BY pk_icm_supervisor_21_id"
#             client.execute(sql)
#             cols = "test.get_icm_supervisor_monitoring.pk_icm_supervisor_21_id, test.get_icm_supervisor_monitoring.fk_form_id, test.get_icm_supervisor_monitoring.test.get_icm_supervisor_monitoring.fk_user_id,test.get_icm_supervisor_monitoring.date_created, test.get_icm_supervisor_monitoring.campaign_id, test.get_icm_supervisor_monitoring.surveyor_name,test.get_icm_supervisor_monitoring.designation, test.get_icm_supervisor_monitoring.surveyor_affliate,test.get_icm_supervisor_monitoring.fk_prov_id, test.get_icm_supervisor_monitoring.fk_dist_id, test.get_icm_supervisor_monitoring.fk_tehsil_id, test.get_icm_supervisor_monitoring.fk_uc_id, test.get_icm_supervisor_monitoring.g1_Cday,test.get_icm_supervisor_monitoring.g1_supv_name,test.get_icm_supervisor_monitoring.g1_supv_desg,test.get_icm_supervisor_monitoring.g1_supv_agency,test.get_icm_supervisor_monitoring.g1_sup_trained_count, test.get_icm_supervisor_monitoring.g1_sup_t_date,test.get_icm_supervisor_monitoring.g1_sup_ttrained_count, test.get_icm_supervisor_monitoring.g1_sup_tt_date, test.get_icm_supervisor_monitoring.g1_items_tsupport_count,test.get_icm_supervisor_monitoring.g1_track_mopv2, test.get_icm_supervisor_monitoring.g1_sup_suprvsn,test.get_icm_supervisor_monitoring.g1_Comp_tm_hhform, test.get_icm_supervisor_monitoring.g1_hrp_present,test.get_icm_supervisor_monitoring.g1_Comment,test.get_icm_supervisor_monitoring.col_covid_brief_guidline,test.get_icm_supervisor_monitoring.col_covid_wear_mask,test.get_icm_supervisor_monitoring.col_covid_use_santzr,test.get_icm_supervisor_monitoring.col_covid_social_dist, test.get_icm_supervisor_monitoring.g1_gis_mapavail_count, test.get_icm_supervisor_monitoring.g1_gis_mapuse, test.get_icm_supervisor_monitoring.g1_gis_mapworkload, test.get_icm_supervisor_monitoring.unique_id, test.get_icm_supervisor_monitoring.dev_remarks, test.get_icm_supervisor_monitoring.app_version, test.get_icm_supervisor_monitoring.vac_type, test.get_icm_supervisor_monitoring.signed_vm03, test.get_icm_supervisor_monitoring.supp_clear, test.get_icm_supervisor_monitoring.is_archive, test.get_icm_supervisor_monitoring.update_by, test.get_icm_supervisor_monitoring.archive_reason, test.get_icm_supervisor_monitoring.date_updated, test.xbi_campaign.campaign_ID, test.xbi_campaign.campaign_ActivityName, test.xbi_campaign.campaign_ActivityID_old, test.xbi_campaign.campaign_Yr, test.xbi_campaign.campaign_SubActivityName, test.eoc_geolocation_t.name, test.eoc_geolocation_t.type, test.eoc_geolocation_t.code, test.eoc_geolocation_t.census_pop, test.eoc_geolocation_t.target, test.eoc_geolocation_t.status, test.eoc_geolocation_t.pname, test.eoc_geolocation_t.dname, test.eoc_geolocation_t.namedistrict, test.eoc_geolocation_t.codedistrict, test.eoc_geolocation_t.tname, test.eoc_geolocation_t.provincecode, test.eoc_geolocation_t.districtcode, test.eoc_geolocation_t.tehsilcode, test.eoc_geolocation_t.priority, test.eoc_geolocation_t.commnet, test.eoc_geolocation_t.hr, test.eoc_geolocation_t.fcm, test.eoc_geolocation_t.tier, test.eoc_geolocation_t.block, test.eoc_geolocation_t.division, test.eoc_geolocation_t.cordinates, test.eoc_geolocation_t.latitude, test.eoc_geolocation_t.longitude, test.eoc_geolocation_t.x, test.eoc_geolocation_t.y, test.eoc_geolocation_t.imagepath, test.eoc_geolocation_t.isccpv, test.eoc_geolocation_t.rank, test.eoc_geolocation_t.rank_score, test.eoc_geolocation_t.ishealthcamp, test.eoc_geolocation_t.isdsc, test.eoc_geolocation_t.ucorg, test.eoc_geolocation_t.organization, test.eoc_geolocation_t.tierfromaug161, test.eoc_geolocation_t.tierfromsep171, test.eoc_geolocation_t.tierfromdec181, test.eoc_geolocation_t.mtap, test.eoc_geolocation_t.rspuc, test.eoc_geolocation_t.issmt, test.eoc_geolocation_t.updateddatetime, test.eoc_geolocation_t.x_code, test.eoc_geolocation_t.draining_uc, test.eoc_geolocation_t.upap_districts, test.eoc_geolocation_t.shruc, test.eoc_geolocation_t.khidist_id     "
#             sql = "SELECT " + cols + "  FROM test.get_icm_supervisor_monitoring eoc_1 left join test.eoc_geolocation_t eoc_2 ON eoc_1.fk_uc_id  = eoc_2.code left JOIN test.xbi_campaign eoc_3 ON eoc_1.campaign_id  = eoc_3.campaign_ID "

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
#                 "SELECT * FROM test.xbi_icm_supervisor_monitoring")
#             if df3.empty:
#                 client.insert_dataframe(
#                     'INSERT INTO test.xbi_icm_supervisor_monitoring  VALUES', dff)
#                 logger.info(
#                     ' Data has been inserted into Table\' INSERT INTO test.xbi_icm_supervisor_monitoring  VALUES \' ')

#                 sql = "DROP table if exists test.get_icm_supervisor_monitoring"
#                 client.execute(sql)
#                 print('\n\n\n CID\t\t--------\t', cid)
#                 print('\n\n\n Page\t\t--------\t', i)
#             else:
#                 client.insert_dataframe(
#                     'INSERT INTO test.xbi_icm_supervisor_monitoring  VALUES', dff)
#                 logger.info(
#                         ' Data has been inserted into Table test.xbi_icm_supervisor_monitoring for campaign '+str(cid))    
#                 print('\n\n\n CID\t\t--------\t', cid)
#                 print('\n\n\n Page\t\t--------\t', i)
                
#                 sql = "DROP table if exists test.get_icm_supervisor_monitoring"
#                 client.execute(sql)
                        


# dag = DAG(
#     'ICM_Supervisor_Monitoring_Automated',
#     schedule_interval='0 0 * * *', # will run daily at mid-night
#     #schedule_interval='*/59 * * * *',  # will run every 10 min.
#     default_args=default_args,
#     catchup=False)

# with dag:
#     GetAndInsertApiDataICMSupervisorMonitoring = PythonOperator(
#         task_id='GetAndInsertApiDataICMSupervisorMonitoring',
#         python_callable=GetAndInsertApiDataICMSupervisorMonitoring,
#     )
    
# GetAndInsertApiDataICMSupervisorMonitoring



#-------------------------------------------------------------------------------------------------------------------------------#
#-------------------------------------- INSERT LATEST CAMPAIGN DATA ICM_Supervisor_Monitoring ----------------------------------#
#----------------------------------------------- Author: Abdul Bari Malik ------------------------------------------------------#
#-------------------------------------------------------------------------------------------------------------------------------#

def GetAndInsertApiDataICMSupervisorMonitoring():
    logger.info('Function \' GetAndInsertApiDataICMSupervisorMonitoring \' Started Off')
    client = Client(host='161.97.136.95',
                    user='default',
                    password='pakistan',
                    port='9000', settings={"use_numpy": True})
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
    
            sql = "CREATE TABLE if not exists test.get_icm_supervisor_monitoring (pk_icm_supervisor_21_id Int32, fk_form_id Int32, fk_user_id Int32,date_created String, campaign_id Int32, surveyor_name String,designation String, surveyor_affliate String,fk_prov_id Int32,fk_dist_id Int32, fk_tehsil_id Int32, fk_uc_id Int32, g1_Cday String,g1_supv_name String,g1_supv_desg String,g1_supv_agency String,g1_sup_trained_count Int32,g1_sup_t_date String,g1_sup_ttrained_count Int32,g1_sup_tt_date String,g1_items_tsupport_count Int32,g1_track_mopv2 Int32,g1_sup_suprvsn Int32,g1_Comp_tm_hhform Int32,g1_hrp_present String,g1_Comment String,col_covid_brief_guidline Int32,col_covid_wear_mask Int32,col_covid_use_santzr Int32,col_covid_social_dist Int32,g1_gis_mapavail_count String,g1_gis_mapuse String,g1_gis_mapworkload String,unique_id String,dev_remarks String,app_version String,vac_type Int32,signed_vm03 Int32,supp_clear Int32,is_archive Int32, update_by Int32, archive_reason String,date_updated Int32)ENGINE = MergeTree PRIMARY KEY pk_icm_supervisor_21_id ORDER BY pk_icm_supervisor_21_id"
            client.execute(sql)

            df2 = client.query_dataframe(
                "SELECT * FROM test.get_icm_supervisor_monitoring")
            if df2.empty:
                df = df.replace(r'^\s*$', np.nan, regex=True)
                client.insert_dataframe(
                    'INSERT INTO test.get_icm_supervisor_monitoring VALUES', df)
                logger.info(
                        ' Data has been inserted into Table test.xbi_icm_supervisor_monitoring for campaign'+str(cid))    

def CreateJoinTableOfICMSupervisorMonitoring():
    logger.info(' Function  \' CreateJoinTableOfICMSupervisorMonitoring \' has been Initiated')
    client = Client(host='161.97.136.95',
                    user='default',
                    password='pakistan',
                    port='9000', settings={"use_numpy": True})

    cid = 256

    sql = "CREATE TABLE if not exists test.xbi_icm_supervisor_monitoring (pk_icm_supervisor_21_id Int32, fk_form_id Int32, fk_user_id Int32,date_created String, campaign_id Int32, surveyor_name String,designation String, surveyor_affliate String,fk_prov_id Int32,fk_dist_id Int32, fk_tehsil_id Int32, fk_uc_id Int32, g1_Cday String,g1_supv_name String,g1_supv_desg String,g1_supv_agency String,g1_sup_trained_count Int32,g1_sup_t_date String,g1_sup_ttrained_count Int32,g1_sup_tt_date String,g1_items_tsupport_count Int32,g1_track_mopv2 Int32,g1_sup_suprvsn Int32,g1_Comp_tm_hhform Int32,g1_hrp_present String,g1_Comment String,col_covid_brief_guidline Int32,col_covid_wear_mask Int32,col_covid_use_santzr Int32,col_covid_social_dist Int32,g1_gis_mapavail_count String,g1_gis_mapuse String,g1_gis_mapworkload String,unique_id String,dev_remarks String,app_version String,vac_type Int32,signed_vm03 Int32,supp_clear Int32,is_archive Int32, update_by Int32, archive_reason String,date_updated Int32,campaign_ID Int32, campaign_ActivityName String, campaign_ActivityID_old Int32, campaign_Yr Int32, campaign_SubActivityName String, geoLocation_name String, geoLocation_type String, geoLocation_code String, geoLocation_census_pop String, geoLocation_target String, geoLocation_status String, geoLocation_pname String, geoLocation_dname String, geoLocation_namedistrict String, geoLocation_codedistrict String, geoLocation_tname String, geoLocation_provincecode String, geoLocation_districtcode String, geoLocation_tehsilcode String, geoLocation_priority String, geoLocation_commnet String, geoLocation_hr String, geoLocation_fcm String, geoLocation_tier String, geoLocation_block String, geoLocation_division String, geoLocation_cordinates String, geoLocation_latitude String, geoLocation_longitude String, geoLocation_x String, geoLocation_y String, geoLocation_imagepath String, geoLocation_isccpv String, geoLocation_rank String, geoLocation_rank_score String, geoLocation_ishealthcamp String, geoLocation_isdsc String, geoLocation_ucorg String, geoLocation_organization String, geoLocation_tierfromaug161 String, geoLocation_tierfromsep171 String, geoLocation_tierfromdec181 String, geoLocation_mtap String, geoLocation_rspuc String, geoLocation_issmt String, geoLocation_updateddatetime String, geoLocation_x_code String, geoLocation_draining_uc String, geoLocation_upap_districts String, geoLocation_shruc String, geoLocation_khidist_id String )ENGINE = MergeTree PRIMARY KEY pk_icm_supervisor_21_id ORDER BY pk_icm_supervisor_21_id"
    client.execute(sql)
    cols = "test.get_icm_supervisor_monitoring.pk_icm_supervisor_21_id, test.get_icm_supervisor_monitoring.fk_form_id, test.get_icm_supervisor_monitoring.test.get_icm_supervisor_monitoring.fk_user_id,test.get_icm_supervisor_monitoring.date_created, test.get_icm_supervisor_monitoring.campaign_id, test.get_icm_supervisor_monitoring.surveyor_name,test.get_icm_supervisor_monitoring.designation, test.get_icm_supervisor_monitoring.surveyor_affliate,test.get_icm_supervisor_monitoring.fk_prov_id, test.get_icm_supervisor_monitoring.fk_dist_id, test.get_icm_supervisor_monitoring.fk_tehsil_id, test.get_icm_supervisor_monitoring.fk_uc_id, test.get_icm_supervisor_monitoring.g1_Cday,test.get_icm_supervisor_monitoring.g1_supv_name,test.get_icm_supervisor_monitoring.g1_supv_desg,test.get_icm_supervisor_monitoring.g1_supv_agency,test.get_icm_supervisor_monitoring.g1_sup_trained_count, test.get_icm_supervisor_monitoring.g1_sup_t_date,test.get_icm_supervisor_monitoring.g1_sup_ttrained_count, test.get_icm_supervisor_monitoring.g1_sup_tt_date, test.get_icm_supervisor_monitoring.g1_items_tsupport_count,test.get_icm_supervisor_monitoring.g1_track_mopv2, test.get_icm_supervisor_monitoring.g1_sup_suprvsn,test.get_icm_supervisor_monitoring.g1_Comp_tm_hhform, test.get_icm_supervisor_monitoring.g1_hrp_present,test.get_icm_supervisor_monitoring.g1_Comment,test.get_icm_supervisor_monitoring.col_covid_brief_guidline,test.get_icm_supervisor_monitoring.col_covid_wear_mask,test.get_icm_supervisor_monitoring.col_covid_use_santzr,test.get_icm_supervisor_monitoring.col_covid_social_dist, test.get_icm_supervisor_monitoring.g1_gis_mapavail_count, test.get_icm_supervisor_monitoring.g1_gis_mapuse, test.get_icm_supervisor_monitoring.g1_gis_mapworkload, test.get_icm_supervisor_monitoring.unique_id, test.get_icm_supervisor_monitoring.dev_remarks, test.get_icm_supervisor_monitoring.app_version, test.get_icm_supervisor_monitoring.vac_type, test.get_icm_supervisor_monitoring.signed_vm03, test.get_icm_supervisor_monitoring.supp_clear, test.get_icm_supervisor_monitoring.is_archive, test.get_icm_supervisor_monitoring.update_by, test.get_icm_supervisor_monitoring.archive_reason, test.get_icm_supervisor_monitoring.date_updated, test.xbi_campaign.campaign_ID, test.xbi_campaign.campaign_ActivityName, test.xbi_campaign.campaign_ActivityID_old, test.xbi_campaign.campaign_Yr, test.xbi_campaign.campaign_SubActivityName, test.eoc_geolocation_t.name, test.eoc_geolocation_t.type, test.eoc_geolocation_t.code, test.eoc_geolocation_t.census_pop, test.eoc_geolocation_t.target, test.eoc_geolocation_t.status, test.eoc_geolocation_t.pname, test.eoc_geolocation_t.dname, test.eoc_geolocation_t.namedistrict, test.eoc_geolocation_t.codedistrict, test.eoc_geolocation_t.tname, test.eoc_geolocation_t.provincecode, test.eoc_geolocation_t.districtcode, test.eoc_geolocation_t.tehsilcode, test.eoc_geolocation_t.priority, test.eoc_geolocation_t.commnet, test.eoc_geolocation_t.hr, test.eoc_geolocation_t.fcm, test.eoc_geolocation_t.tier, test.eoc_geolocation_t.block, test.eoc_geolocation_t.division, test.eoc_geolocation_t.cordinates, test.eoc_geolocation_t.latitude, test.eoc_geolocation_t.longitude, test.eoc_geolocation_t.x, test.eoc_geolocation_t.y, test.eoc_geolocation_t.imagepath, test.eoc_geolocation_t.isccpv, test.eoc_geolocation_t.rank, test.eoc_geolocation_t.rank_score, test.eoc_geolocation_t.ishealthcamp, test.eoc_geolocation_t.isdsc, test.eoc_geolocation_t.ucorg, test.eoc_geolocation_t.organization, test.eoc_geolocation_t.tierfromaug161, test.eoc_geolocation_t.tierfromsep171, test.eoc_geolocation_t.tierfromdec181, test.eoc_geolocation_t.mtap, test.eoc_geolocation_t.rspuc, test.eoc_geolocation_t.issmt, test.eoc_geolocation_t.updateddatetime, test.eoc_geolocation_t.x_code, test.eoc_geolocation_t.draining_uc, test.eoc_geolocation_t.upap_districts, test.eoc_geolocation_t.shruc, test.eoc_geolocation_t.khidist_id     "
    sql = "SELECT " + cols + "  FROM test.get_icm_supervisor_monitoring eoc_1 left join test.eoc_geolocation_t eoc_2 ON eoc_1.fk_uc_id  = eoc_2.code left JOIN test.xbi_campaign eoc_3 ON eoc_1.campaign_id  = eoc_3.campaign_ID "

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
    
    df3 = client.query_dataframe("SELECT * FROM test.xbi_icm_supervisor_monitoring")
    if df3.empty:
        client.insert_dataframe(
        'INSERT INTO test.xbi_icm_supervisor_monitoring  VALUES', dff)
        logger.info(' Data has been inserted into Table\' INSERT INTO test.xbi_icm_supervisor_monitoring  VALUES \' ')

        sql = "DROP table if exists test.get_icm_supervisor_monitoring"
        client.execute(sql)
        print('\n\n\n CID\t\t--------\t', cid)
        print('\n\n\n Page\t\t--------\t', i)
    else:
        sql = "ALTER TABLE test.xbi_icm_supervisor_monitoring DELETE WHERE campaign_id = 256"
        client.execute(sql)

        dff = dff.replace(r'^\s*$', np.nan, regex=True)
        client.insert_dataframe(
            'INSERT INTO test.xbi_icm_supervisor_monitoring  VALUES', dff)
        logger.info(
            ' Data has been inserted into Table test.xbi_icm_supervisor_monitoring for campaign '+str(cid))
        print('\n\n Campaign \t\t--------\t', cid, '\t--------\t Data Inserted \n\n')    
                
        sql = "DROP table if exists test.get_icm_supervisor_monitoring"
        client.execute(sql)

    

dag = DAG(
    'ICM_Supervisor_Monitoring_Automated',
    schedule_interval='0 0 * * *', # will run daily at mid-night
    #schedule_interval='*/59 * * * *',  # will run every 10 min.
    default_args=default_args,
    catchup=False)

with dag:
    GetAndInsertApiDataICMSupervisorMonitoring = PythonOperator(
        task_id='GetAndInsertApiDataICMSupervisorMonitoring',
        python_callable=GetAndInsertApiDataICMSupervisorMonitoring,
    )
    CreateJoinTableOfICMSupervisorMonitoring = PythonOperator(
        task_id='CreateJoinTableOfICMSupervisorMonitoring',
        python_callable=CreateJoinTableOfICMSupervisorMonitoring,
    )
GetAndInsertApiDataICMSupervisorMonitoring >> CreateJoinTableOfICMSupervisorMonitoring
