
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
#---------------------------------------- INSERT CAMPAIGN DATA ICM Transit Site ------------------------------------------------#
#-------------------------------------------- Author: Abdul Bari Malik ---------------------------------------------------------#
#-------------------------------------------------------------------------------------------------------------------------------#

# def GetAndInsertApiDataICMTransitSiteMonitoring():
    
#     li3 = [222,223,224,225,226,227,228,229,230,231,232,233,234,235,236,237,238,239,240,241,242,243,244,245,247,248,249,250,251,252,253,254,255,256]
    
#     logger.info('Function \' GetAndInsertApiDataICMTransitSiteMonitoring \' Started Off')
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
#             sql = "CREATE TABLE if not exists test.get_icm_transit_site_monitoring ( pk_icm_transit_21_id Int32,fk_form_id Int32,fk_user_id Int32,date_created String,campaign_id Int32,sur_name String,designation String,survyor_affliate String,fk_prov_id Int32,fk_dist_id Int32,fk_tehsil_id Int32,fk_uc_id Int32,g1_postname String,g1_pnametype String,g1_ssupname String,g1_tum_name_1 String,g1_tum_name_2 String,  g1_sup_visit_count String,col_g1_Comments1 String,g2_loc_appr String,g2_loc_ident String,g2_loc_ident_ptp String,g2_loc_train_adlt String,g2_loc_train_zero String,g2_tmem_cnic Int32,g2_tmem_lang Int32,g2_vaccine_carr String,g2_finger_marker String,g2_tally_sheet String,g2_docs_avail String,g2_vial_SOP_cool String,g2_vial_SOP_valid_vvm Int32,g2_vial_SOP_dry Int32,g2_child_vs_doses Int32,g2_transit_point Int32,g2_security_lea String,g2_Comments3 String,unique_id String,dev_remarks String,g2_loc_appr_accessable Int32,g2_loc_appr_visiable Int32,g2_loc_appr_placement Int32,g2_loc_ident_badges Int32,g2_loc_ident_poster Int32,g2_loc_ident_ptp_shade Int32,g2_loc_ident_ptp_caps Int32,g2_loc_train_adlt_trained Int32,g2_loc_train_adlt_adult Int32,g2_loc_train_adlt_local Int32,g2_docs_avail_attendance_sheet String,g2_docs_avail_sup_checklist String,g2_docs_avail_mp String,app_version String,coldchain_protocol String,g2_vial_SOP_intact Int32,g2_vial_SOP_valid_recap Int32, is_archive Int32, update_by Int32, archive_reason String, vacc_ts_cc Int32, date_updated Int32 )ENGINE = MergeTree PRIMARY KEY pk_icm_transit_21_id ORDER BY pk_icm_transit_21_id"
#             client.execute(sql)
#             df2 = client.query_dataframe("SELECT * FROM test.get_icm_transit_site_monitoring")
#             if df2.empty:
#                 df = df.replace(r'^\s*$', np.nan, regex=True)
#                 client.insert_dataframe(
#                     'INSERT INTO test.get_icm_transit_site_monitoring VALUES', df)
#                 logger.info(
#                         ' Data has been inserted into Table test.get_icm_transit_site_monitoring for campaign'+str(cid))    
#             sql = "CREATE TABLE if not exists test.xbi_icm_transit_site_monitoring (pk_icm_transit_21_id Int32,fk_form_id Int32,fk_user_id Int32,date_created String,campaign_id Int32,sur_name String,designation String,survyor_affliate String,fk_prov_id Int32,fk_dist_id Int32,fk_tehsil_id Int32,fk_uc_id Int32,g1_postname String,g1_pnametype String,g1_ssupname String,g1_tum_name_1 String,g1_tum_name_2 String,  g1_sup_visit_count String,col_g1_Comments1 String,g2_loc_appr String,g2_loc_ident String,g2_loc_ident_ptp String,g2_loc_train_adlt String,g2_loc_train_zero String,g2_tmem_cnic Int32,g2_tmem_lang Int32,g2_vaccine_carr String,g2_finger_marker String,g2_tally_sheet String,g2_docs_avail String,g2_vial_SOP_cool String,g2_vial_SOP_valid_vvm Int32,g2_vial_SOP_dry Int32,g2_child_vs_doses Int32,g2_transit_point Int32,g2_security_lea String,g2_Comments3 String,unique_id String,dev_remarks String,g2_loc_appr_accessable Int32,g2_loc_appr_visiable Int32,g2_loc_appr_placement Int32,g2_loc_ident_badges Int32,g2_loc_ident_poster Int32,g2_loc_ident_ptp_shade Int32,g2_loc_ident_ptp_caps Int32,g2_loc_train_adlt_trained Int32,g2_loc_train_adlt_adult Int32,g2_loc_train_adlt_local Int32,g2_docs_avail_attendance_sheet String,g2_docs_avail_sup_checklist String,g2_docs_avail_mp String,app_version String,coldchain_protocol String,g2_vial_SOP_intact Int32,g2_vial_SOP_valid_recap Int32, is_archive Int32, update_by Int32, archive_reason String, vacc_ts_cc Int32, date_updated Int32, campaign_ID Int32, campaign_ActivityName String, campaign_ActivityID_old Int32, campaign_Yr Int32, campaign_SubActivityName String, geoLocation_name String, geoLocation_type String, geoLocation_code String, geoLocation_census_pop String, geoLocation_target String, geoLocation_status String, geoLocation_pname String, geoLocation_dname String, geoLocation_namedistrict String, geoLocation_codedistrict String, geoLocation_tname String, geoLocation_provincecode String, geoLocation_districtcode String, geoLocation_tehsilcode String, geoLocation_priority String, geoLocation_commnet String, geoLocation_hr String, geoLocation_fcm String, geoLocation_tier String, geoLocation_block String, geoLocation_division String, geoLocation_cordinates String, geoLocation_latitude String, geoLocation_longitude String, geoLocation_x String, geoLocation_y String, geoLocation_imagepath String, geoLocation_isccpv String, geoLocation_rank String, geoLocation_rank_score String, geoLocation_ishealthcamp String, geoLocation_isdsc String, geoLocation_ucorg String, geoLocation_organization String, geoLocation_tierfromaug161 String, geoLocation_tierfromsep171 String, geoLocation_tierfromdec181 String, geoLocation_mtap String, geoLocation_rspuc String, geoLocation_issmt String, geoLocation_updateddatetime String, geoLocation_x_code String, geoLocation_draining_uc String, geoLocation_upap_districts String, geoLocation_shruc String, geoLocation_khidist_id String)ENGINE = MergeTree PRIMARY KEY pk_icm_transit_21_id ORDER BY pk_icm_transit_21_id"
#             client.execute(sql)
#             logger.info(
#                         'Get Data from test.get_icm_transit_site_monitoring for campaign'+str(cid))
#             cols = "test.get_icm_transit_site_monitoring.pk_icm_transit_21_id,test.get_icm_transit_site_monitoring.fk_form_id,test.get_icm_transit_site_monitoring.fk_user_id,test.get_icm_transit_site_monitoring.date_created,test.get_icm_transit_site_monitoring.campaign_id,test.get_icm_transit_site_monitoring.sur_name,test.get_icm_transit_site_monitoring.designation,test.get_icm_transit_site_monitoring.survyor_affliate,test.get_icm_transit_site_monitoring.fk_prov_id,test.get_icm_transit_site_monitoring.fk_dist_id,test.get_icm_transit_site_monitoring.fk_tehsil_id,test.get_icm_transit_site_monitoring.fk_uc_id,test.get_icm_transit_site_monitoring.g1_postname,test.get_icm_transit_site_monitoring.g1_pnametype,test.get_icm_transit_site_monitoring.g1_ssupname,test.get_icm_transit_site_monitoring.g1_tum_name_1,test.get_icm_transit_site_monitoring.g1_tum_name_2,test.get_icm_transit_site_monitoring.g1_sup_visit_count,test.get_icm_transit_site_monitoring.col_g1_Comments1,test.get_icm_transit_site_monitoring.g2_loc_appr,test.get_icm_transit_site_monitoring.g2_loc_ident,test.get_icm_transit_site_monitoring.g2_loc_ident_ptp,test.get_icm_transit_site_monitoring.g2_loc_train_adlt,test.get_icm_transit_site_monitoring.g2_loc_train_zero,test.get_icm_transit_site_monitoring.g2_tmem_cnic,test.get_icm_transit_site_monitoring.g2_tmem_lang,test.get_icm_transit_site_monitoring.g2_vaccine_carr,test.get_icm_transit_site_monitoring.g2_finger_marker,test.get_icm_transit_site_monitoring.g2_tally_sheet,test.get_icm_transit_site_monitoring.g2_docs_avail,test.get_icm_transit_site_monitoring.g2_vial_SOP_cool,test.get_icm_transit_site_monitoring.g2_vial_SOP_valid_vvm,test.get_icm_transit_site_monitoring.g2_vial_SOP_dry,test.get_icm_transit_site_monitoring.g2_child_vs_doses,test.get_icm_transit_site_monitoring.g2_transit_point,test.get_icm_transit_site_monitoring.g2_security_lea,test.get_icm_transit_site_monitoring.g2_Comments3,test.get_icm_transit_site_monitoring.unique_id,test.get_icm_transit_site_monitoring.dev_remarks,test.get_icm_transit_site_monitoring.g2_loc_appr_accessable,test.get_icm_transit_site_monitoring.g2_loc_appr_visiable,test.get_icm_transit_site_monitoring.g2_loc_appr_placement,test.get_icm_transit_site_monitoring.g2_loc_ident_badges,test.get_icm_transit_site_monitoring.g2_loc_ident_poster, test.get_icm_transit_site_monitoring.g2_loc_ident_ptp_shade,test.get_icm_transit_site_monitoring.g2_loc_ident_ptp_caps,test.get_icm_transit_site_monitoring.g2_loc_train_adlt_trained,test.get_icm_transit_site_monitoring.g2_loc_train_adlt_adult,test.get_icm_transit_site_monitoring.g2_loc_train_adlt_local,test.get_icm_transit_site_monitoring.g2_docs_avail_attendance_sheet,test.get_icm_transit_site_monitoring.g2_docs_avail_sup_checklist,test.get_icm_transit_site_monitoring.g2_docs_avail_mp,test.get_icm_transit_site_monitoring.app_version,test.get_icm_transit_site_monitoring.coldchain_protocol,test.get_icm_transit_site_monitoring.g2_vial_SOP_intact,test.get_icm_transit_site_monitoring.g2_vial_SOP_valid_recap,test.get_icm_transit_site_monitoring.is_archive, test.get_icm_transit_site_monitoring.update_by, test.get_icm_transit_site_monitoring.archive_reason, test.get_icm_transit_site_monitoring.vacc_ts_cc, test.get_icm_transit_site_monitoring.date_updated, test.xbi_campaign.campaign_ID, test.xbi_campaign.campaign_ActivityName, test.xbi_campaign.campaign_ActivityID_old, test.xbi_campaign.campaign_Yr, test.xbi_campaign.campaign_SubActivityName,  test.eoc_geolocation_t.name, test.eoc_geolocation_t.type, test.eoc_geolocation_t.code, test.eoc_geolocation_t.census_pop, test.eoc_geolocation_t.target, test.eoc_geolocation_t.status, test.eoc_geolocation_t.pname, test.eoc_geolocation_t.dname, test.eoc_geolocation_t.namedistrict, test.eoc_geolocation_t.codedistrict, test.eoc_geolocation_t.tname, test.eoc_geolocation_t.provincecode, test.eoc_geolocation_t.districtcode, test.eoc_geolocation_t.tehsilcode, test.eoc_geolocation_t.priority, test.eoc_geolocation_t.commnet, test.eoc_geolocation_t.hr, test.eoc_geolocation_t.fcm, test.eoc_geolocation_t.tier, test.eoc_geolocation_t.block, test.eoc_geolocation_t.division, test.eoc_geolocation_t.cordinates, test.eoc_geolocation_t.latitude, test.eoc_geolocation_t.longitude, test.eoc_geolocation_t.x, test.eoc_geolocation_t.y, test.eoc_geolocation_t.imagepath, test.eoc_geolocation_t.isccpv, test.eoc_geolocation_t.rank, test.eoc_geolocation_t.rank_score, test.eoc_geolocation_t.ishealthcamp, test.eoc_geolocation_t.isdsc, test.eoc_geolocation_t.ucorg, test.eoc_geolocation_t.organization, test.eoc_geolocation_t.tierfromaug161, test.eoc_geolocation_t.tierfromsep171, test.eoc_geolocation_t.tierfromdec181, test.eoc_geolocation_t.mtap, test.eoc_geolocation_t.rspuc, test.eoc_geolocation_t.issmt, test.eoc_geolocation_t.updateddatetime, test.eoc_geolocation_t.x_code, test.eoc_geolocation_t.draining_uc, test.eoc_geolocation_t.upap_districts, test.eoc_geolocation_t.shruc, test.eoc_geolocation_t.khidist_id"
#             sql = "SELECT " + cols + "  FROM test.get_icm_transit_site_monitoring eoc_1 left join test.eoc_geolocation_t eoc_2 ON eoc_1.fk_uc_id  = eoc_2.code left JOIN test.xbi_campaign eoc_3 ON eoc_1.campaign_id  = eoc_3.campaign_ID "

#             data = client.execute(sql)
#             apiDataFrame = pd.DataFrame(data)
#             apiDataFrame = apiDataFrame.replace(r'^\s*$', np.nan, regex=True)
#             all_columns = list(apiDataFrame)  # Creates list of all column headers
#             cols = apiDataFrame.iloc[0]
#             apiDataFrame[all_columns] = apiDataFrame[all_columns].astype(str)
#             d = 'pk_icm_transit_21_id','fk_form_id','fk_user_id','date_created','campaign_id','sur_name','designation','survyor_affliate','fk_prov_id','fk_dist_id','fk_tehsil_id','fk_uc_id','g1_postname','g1_pnametype','g1_ssupname','g1_tum_name_1','g1_tum_name_2','g1_sup_visit_count','col_g1_Comments1','g2_loc_appr','g2_loc_ident','g2_loc_ident_ptp','g2_loc_train_adlt','g2_loc_train_zero','g2_tmem_cnic','g2_tmem_lang','g2_vaccine_carr','g2_finger_marker','g2_tally_sheet','g2_docs_avail','g2_vial_SOP_cool','g2_vial_SOP_valid_vvm','g2_vial_SOP_dry','g2_child_vs_doses','g2_transit_point','g2_security_lea','g2_Comments3','unique_id','dev_remarks','g2_loc_appr_accessable','g2_loc_appr_visiable','g2_loc_appr_placement','g2_loc_ident_badges','g2_loc_ident_poster','g2_loc_ident_ptp_shade','g2_loc_ident_ptp_caps','g2_loc_train_adlt_trained','g2_loc_train_adlt_adult','g2_loc_train_adlt_local','g2_docs_avail_attendance_sheet','g2_docs_avail_sup_checklist','g2_docs_avail_mp','app_version','coldchain_protocol','g2_vial_SOP_intact','g2_vial_SOP_valid_recap','is_archive', 'update_by', 'archive_reason', 'vacc_ts_cc', 'date_updated', 'campaign_ID', 'campaign_ActivityName', 'campaign_ActivityID_old', 'campaign_Yr', 'campaign_SubActivityName','geoLocation_name', 'geoLocation_type', 'geoLocation_code', 'geoLocation_census_pop', 'geoLocation_target', 'geoLocation_status', 'geoLocation_pname', 'geoLocation_dname', 'geoLocation_namedistrict', 'geoLocation_codedistrict', 'geoLocation_tname', 'geoLocation_provincecode', 'geoLocation_districtcode', 'geoLocation_tehsilcode', 'geoLocation_priority', 'geoLocation_commnet', 'geoLocation_hr', 'geoLocation_fcm', 'geoLocation_tier', 'geoLocation_block', 'geoLocation_division', 'geoLocation_cordinates', 'geoLocation_latitude', 'geoLocation_longitude', 'geoLocation_x', 'geoLocation_y', 'geoLocation_imagepath', 'geoLocation_isccpv', 'geoLocation_rank', 'geoLocation_rank_score', 'geoLocation_ishealthcamp', 'geoLocation_isdsc', 'geoLocation_ucorg', 'geoLocation_organization', 'geoLocation_tierfromaug161', 'geoLocation_tierfromsep171', 'geoLocation_tierfromdec181', 'geoLocation_mtap', 'geoLocation_rspuc', 'geoLocation_issmt', 'geoLocation_updateddatetime', 'geoLocation_x_code', 'geoLocation_draining_uc', 'geoLocation_upap_districts', 'geoLocation_shruc', 'geoLocation_khidist_id' 
#             dff = pd.DataFrame(columns=d)
#             for index, item in enumerate(d):
#                 dff[item] = apiDataFrame[index].values
#                 #dff[item]= dff[item].replace(r'^\s*$', np.nan, regex=True)
#             df3 = client.query_dataframe(
#                     "SELECT * FROM test.xbi_icm_transit_site_monitoring")
#             if df3.empty:
#                 client.insert_dataframe(
#                         'INSERT INTO test.xbi_icm_transit_site_monitoring  VALUES', dff)
#                 logger.info(
#                         'Data has been inserted into Table test.xbi_icm_transit_site_monitoring for campaign '+str(cid))

#                 sql = "DROP table if exists test.get_icm_transit_site_monitoring"
#                 client.execute(sql)
#                 print('\n\n\n F\t\t--------\t', cid)
#                 print('\n\n\n B\t\t--------\t', i)    
#             else:
#                 dff = dff.replace(r'^\s*$', np.nan, regex=True)
#                 client.insert_dataframe(
#                     'INSERT INTO test.xbi_icm_transit_site_monitoring  VALUES', dff)
#                 logger.info(
#                         ' Data has been inserted into Table test.xbi_icm_transit_site_monitoring for campaign '+str(cid))    
#                 print('\n\n\n F\t\t--------\t', cid)
#                 print('\n\n\n B\t\t--------\t', i)
#                 sql = "DROP table if exists test.get_icm_transit_site_monitoring"
#                 client.execute(sql)
                       
                        
 
# dag = DAG(
#     'ICM_Transit_Site_Monitoring_Automated',
#     #schedule_interval='*/10 * * * *',# will run every 10 min.
#     schedule_interval='0 0 * * *',  
#     default_args=default_args,
#     catchup=False)

# with dag:
#     GetAndInsertApiDataICMTransitSiteMonitoring = PythonOperator(
#         task_id='GetAndInsertApiDataICMTransitSiteMonitoring',
#         python_callable=GetAndInsertApiDataICMTransitSiteMonitoring,
#     )
# GetAndInsertApiDataICMTransitSiteMonitoring

#-------------------------------------------------------------------------------------------------------------------------------#
#-------------------------------------- INSERT LATEST CAMPAIGN DATA ICM_Transit_Site_Monitoring --------------------------------#
#-------------------------------------------- Author: Abdul Bari Malik ---------------------------------------------------------#
#-------------------------------------------------------------------------------------------------------------------------------#

def GetAndInsertApiDataICMTransitSiteMonitoring():
    logger.info('Function \' GetAndInsertApiDataICMTransitSiteMonitoring \' Started Off')
    client = Client(host='161.97.136.95',
                    user='default',
                    password='pakistan',
                    port='9000', settings={"use_numpy": True})

    li3 = [256]
    merged_df = pd.DataFrame()

    for cid in li3:                                
        url = "http://idims.eoc.gov.pk/api_who/api/get_icm/5468XE2LN6CzR7qRG041/transit/"+ str(cid)+"/"
        print('\n\n\n F\t\t--------\t', cid)
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
                url_c = "http://idims.eoc.gov.pk/api_who/api/get_icm/5468XE2LN6CzR7qRG041/transit/"+str(cid)+'/'+str(i)
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
            sql = "CREATE TABLE if not exists test.get_icm_transit_site_monitoring ( pk_icm_transit_21_id Int32,fk_form_id Int32,fk_user_id Int32,date_created String,campaign_id Int32,sur_name String,designation String,survyor_affliate String,fk_prov_id Int32,fk_dist_id Int32,fk_tehsil_id Int32,fk_uc_id Int32,g1_postname String,g1_pnametype String,g1_ssupname String,g1_tum_name_1 String,g1_tum_name_2 String,  g1_sup_visit_count String,col_g1_Comments1 String,g2_loc_appr String,g2_loc_ident String,g2_loc_ident_ptp String,g2_loc_train_adlt String,g2_loc_train_zero String,g2_tmem_cnic Int32,g2_tmem_lang Int32,g2_vaccine_carr String,g2_finger_marker String,g2_tally_sheet String,g2_docs_avail String,g2_vial_SOP_cool String,g2_vial_SOP_valid_vvm Int32,g2_vial_SOP_dry Int32,g2_child_vs_doses Int32,g2_transit_point Int32,g2_security_lea String,g2_Comments3 String,unique_id String,dev_remarks String,g2_loc_appr_accessable Int32,g2_loc_appr_visiable Int32,g2_loc_appr_placement Int32,g2_loc_ident_badges Int32,g2_loc_ident_poster Int32,g2_loc_ident_ptp_shade Int32,g2_loc_ident_ptp_caps Int32,g2_loc_train_adlt_trained Int32,g2_loc_train_adlt_adult Int32,g2_loc_train_adlt_local Int32,g2_docs_avail_attendance_sheet String,g2_docs_avail_sup_checklist String,g2_docs_avail_mp String,app_version String,coldchain_protocol String,g2_vial_SOP_intact Int32,g2_vial_SOP_valid_recap Int32, is_archive Int32, update_by Int32, archive_reason String, vacc_ts_cc Int32, date_updated Int32 )ENGINE = MergeTree PRIMARY KEY pk_icm_transit_21_id ORDER BY pk_icm_transit_21_id"
            client.execute(sql)
            df2 = client.query_dataframe("SELECT * FROM test.get_icm_transit_site_monitoring")
            if df2.empty:
                df = df.replace(r'^\s*$', np.nan, regex=True)
                client.insert_dataframe(
                    'INSERT INTO test.get_icm_transit_site_monitoring VALUES', df)
                logger.info(
                        ' Data has been inserted into Table test.get_icm_transit_site_monitoring for campaign'+str(cid))    
    
def CreateJoinTableOfICMTransitSiteMonitoring():
    cid = 256
    logger.info(' Function  \' CreateJoinTableOfICMTransitSiteMonitoring \' has been Initiated')
    client = Client(host='161.97.136.95',
                    user='default',
                    password='pakistan',
                    port='9000', settings={"use_numpy": True})
    
    sql = "CREATE TABLE if not exists test.xbi_icm_transit_site_monitoring (pk_icm_transit_21_id Int32,fk_form_id Int32,fk_user_id Int32,date_created String,campaign_id Int32,sur_name String,designation String,survyor_affliate String,fk_prov_id Int32,fk_dist_id Int32,fk_tehsil_id Int32,fk_uc_id Int32,g1_postname String,g1_pnametype String,g1_ssupname String,g1_tum_name_1 String,g1_tum_name_2 String,  g1_sup_visit_count String,col_g1_Comments1 String,g2_loc_appr String,g2_loc_ident String,g2_loc_ident_ptp String,g2_loc_train_adlt String,g2_loc_train_zero String,g2_tmem_cnic Int32,g2_tmem_lang Int32,g2_vaccine_carr String,g2_finger_marker String,g2_tally_sheet String,g2_docs_avail String,g2_vial_SOP_cool String,g2_vial_SOP_valid_vvm Int32,g2_vial_SOP_dry Int32,g2_child_vs_doses Int32,g2_transit_point Int32,g2_security_lea String,g2_Comments3 String,unique_id String,dev_remarks String,g2_loc_appr_accessable Int32,g2_loc_appr_visiable Int32,g2_loc_appr_placement Int32,g2_loc_ident_badges Int32,g2_loc_ident_poster Int32,g2_loc_ident_ptp_shade Int32,g2_loc_ident_ptp_caps Int32,g2_loc_train_adlt_trained Int32,g2_loc_train_adlt_adult Int32,g2_loc_train_adlt_local Int32,g2_docs_avail_attendance_sheet String,g2_docs_avail_sup_checklist String,g2_docs_avail_mp String,app_version String,coldchain_protocol String,g2_vial_SOP_intact Int32,g2_vial_SOP_valid_recap Int32, is_archive Int32, update_by Int32, archive_reason String, vacc_ts_cc Int32, date_updated Int32, campaign_ID Int32, campaign_ActivityName String, campaign_ActivityID_old Int32, campaign_Yr Int32, campaign_SubActivityName String, geoLocation_name String, geoLocation_type String, geoLocation_code String, geoLocation_census_pop String, geoLocation_target String, geoLocation_status String, geoLocation_pname String, geoLocation_dname String, geoLocation_namedistrict String, geoLocation_codedistrict String, geoLocation_tname String, geoLocation_provincecode String, geoLocation_districtcode String, geoLocation_tehsilcode String, geoLocation_priority String, geoLocation_commnet String, geoLocation_hr String, geoLocation_fcm String, geoLocation_tier String, geoLocation_block String, geoLocation_division String, geoLocation_cordinates String, geoLocation_latitude String, geoLocation_longitude String, geoLocation_x String, geoLocation_y String, geoLocation_imagepath String, geoLocation_isccpv String, geoLocation_rank String, geoLocation_rank_score String, geoLocation_ishealthcamp String, geoLocation_isdsc String, geoLocation_ucorg String, geoLocation_organization String, geoLocation_tierfromaug161 String, geoLocation_tierfromsep171 String, geoLocation_tierfromdec181 String, geoLocation_mtap String, geoLocation_rspuc String, geoLocation_issmt String, geoLocation_updateddatetime String, geoLocation_x_code String, geoLocation_draining_uc String, geoLocation_upap_districts String, geoLocation_shruc String, geoLocation_khidist_id String)ENGINE = MergeTree PRIMARY KEY pk_icm_transit_21_id ORDER BY pk_icm_transit_21_id"
    client.execute(sql)
    
    logger.info('Get Data from test.get_icm_transit_site_monitoring for campaign'+str(cid))
    
    cols = "test.get_icm_transit_site_monitoring.pk_icm_transit_21_id,test.get_icm_transit_site_monitoring.fk_form_id,test.get_icm_transit_site_monitoring.fk_user_id,test.get_icm_transit_site_monitoring.date_created,test.get_icm_transit_site_monitoring.campaign_id,test.get_icm_transit_site_monitoring.sur_name,test.get_icm_transit_site_monitoring.designation,test.get_icm_transit_site_monitoring.survyor_affliate,test.get_icm_transit_site_monitoring.fk_prov_id,test.get_icm_transit_site_monitoring.fk_dist_id,test.get_icm_transit_site_monitoring.fk_tehsil_id,test.get_icm_transit_site_monitoring.fk_uc_id,test.get_icm_transit_site_monitoring.g1_postname,test.get_icm_transit_site_monitoring.g1_pnametype,test.get_icm_transit_site_monitoring.g1_ssupname,test.get_icm_transit_site_monitoring.g1_tum_name_1,test.get_icm_transit_site_monitoring.g1_tum_name_2,test.get_icm_transit_site_monitoring.g1_sup_visit_count,test.get_icm_transit_site_monitoring.col_g1_Comments1,test.get_icm_transit_site_monitoring.g2_loc_appr,test.get_icm_transit_site_monitoring.g2_loc_ident,test.get_icm_transit_site_monitoring.g2_loc_ident_ptp,test.get_icm_transit_site_monitoring.g2_loc_train_adlt,test.get_icm_transit_site_monitoring.g2_loc_train_zero,test.get_icm_transit_site_monitoring.g2_tmem_cnic,test.get_icm_transit_site_monitoring.g2_tmem_lang,test.get_icm_transit_site_monitoring.g2_vaccine_carr,test.get_icm_transit_site_monitoring.g2_finger_marker,test.get_icm_transit_site_monitoring.g2_tally_sheet,test.get_icm_transit_site_monitoring.g2_docs_avail,test.get_icm_transit_site_monitoring.g2_vial_SOP_cool,test.get_icm_transit_site_monitoring.g2_vial_SOP_valid_vvm,test.get_icm_transit_site_monitoring.g2_vial_SOP_dry,test.get_icm_transit_site_monitoring.g2_child_vs_doses,test.get_icm_transit_site_monitoring.g2_transit_point,test.get_icm_transit_site_monitoring.g2_security_lea,test.get_icm_transit_site_monitoring.g2_Comments3,test.get_icm_transit_site_monitoring.unique_id,test.get_icm_transit_site_monitoring.dev_remarks,test.get_icm_transit_site_monitoring.g2_loc_appr_accessable,test.get_icm_transit_site_monitoring.g2_loc_appr_visiable,test.get_icm_transit_site_monitoring.g2_loc_appr_placement,test.get_icm_transit_site_monitoring.g2_loc_ident_badges,test.get_icm_transit_site_monitoring.g2_loc_ident_poster, test.get_icm_transit_site_monitoring.g2_loc_ident_ptp_shade,test.get_icm_transit_site_monitoring.g2_loc_ident_ptp_caps,test.get_icm_transit_site_monitoring.g2_loc_train_adlt_trained,test.get_icm_transit_site_monitoring.g2_loc_train_adlt_adult,test.get_icm_transit_site_monitoring.g2_loc_train_adlt_local,test.get_icm_transit_site_monitoring.g2_docs_avail_attendance_sheet,test.get_icm_transit_site_monitoring.g2_docs_avail_sup_checklist,test.get_icm_transit_site_monitoring.g2_docs_avail_mp,test.get_icm_transit_site_monitoring.app_version,test.get_icm_transit_site_monitoring.coldchain_protocol,test.get_icm_transit_site_monitoring.g2_vial_SOP_intact,test.get_icm_transit_site_monitoring.g2_vial_SOP_valid_recap,test.get_icm_transit_site_monitoring.is_archive,test.get_icm_transit_site_monitoring.update_by,test.get_icm_transit_site_monitoring.archive_reason,test.get_icm_transit_site_monitoring.vacc_ts_cc,test.get_icm_transit_site_monitoring.date_updated,test.xbi_campaign.campaign_ID,test.xbi_campaign.campaign_ActivityName,test.xbi_campaign.campaign_ActivityID_old,test.xbi_campaign.campaign_Yr,test.xbi_campaign.campaign_SubActivityName,test.eoc_geolocation_t.name, test.eoc_geolocation_t.type, test.eoc_geolocation_t.code, test.eoc_geolocation_t.census_pop, test.eoc_geolocation_t.target, test.eoc_geolocation_t.status, test.eoc_geolocation_t.pname, test.eoc_geolocation_t.dname, test.eoc_geolocation_t.namedistrict, test.eoc_geolocation_t.codedistrict, test.eoc_geolocation_t.tname, test.eoc_geolocation_t.provincecode, test.eoc_geolocation_t.districtcode, test.eoc_geolocation_t.tehsilcode, test.eoc_geolocation_t.priority, test.eoc_geolocation_t.commnet, test.eoc_geolocation_t.hr, test.eoc_geolocation_t.fcm, test.eoc_geolocation_t.tier, test.eoc_geolocation_t.block, test.eoc_geolocation_t.division, test.eoc_geolocation_t.cordinates, test.eoc_geolocation_t.latitude, test.eoc_geolocation_t.longitude, test.eoc_geolocation_t.x, test.eoc_geolocation_t.y, test.eoc_geolocation_t.imagepath, test.eoc_geolocation_t.isccpv, test.eoc_geolocation_t.rank, test.eoc_geolocation_t.rank_score, test.eoc_geolocation_t.ishealthcamp, test.eoc_geolocation_t.isdsc, test.eoc_geolocation_t.ucorg, test.eoc_geolocation_t.organization, test.eoc_geolocation_t.tierfromaug161, test.eoc_geolocation_t.tierfromsep171, test.eoc_geolocation_t.tierfromdec181, test.eoc_geolocation_t.mtap, test.eoc_geolocation_t.rspuc, test.eoc_geolocation_t.issmt, test.eoc_geolocation_t.updateddatetime, test.eoc_geolocation_t.x_code, test.eoc_geolocation_t.draining_uc, test.eoc_geolocation_t.upap_districts, test.eoc_geolocation_t.shruc, test.eoc_geolocation_t.khidist_id"
    sql = "SELECT " + cols + "  FROM test.get_icm_transit_site_monitoring eoc_1 left join test.eoc_geolocation_t eoc_2 ON eoc_1.fk_uc_id  = eoc_2.code left JOIN test.xbi_campaign eoc_3 ON eoc_1.campaign_id  = eoc_3.campaign_ID "
    data = client.execute(sql)
    
    apiDataFrame = pd.DataFrame(data)
    apiDataFrame = apiDataFrame.replace(r'^\s*$', np.nan, regex=True)
    all_columns = list(apiDataFrame)  # Creates list of all column headers
    cols = apiDataFrame.iloc[0]
    apiDataFrame[all_columns] = apiDataFrame[all_columns].astype(str)
    d = 'pk_icm_transit_21_id','fk_form_id','fk_user_id','date_created','campaign_id','sur_name','designation','survyor_affliate','fk_prov_id','fk_dist_id','fk_tehsil_id','fk_uc_id','g1_postname','g1_pnametype','g1_ssupname','g1_tum_name_1','g1_tum_name_2','g1_sup_visit_count','col_g1_Comments1','g2_loc_appr','g2_loc_ident','g2_loc_ident_ptp','g2_loc_train_adlt','g2_loc_train_zero','g2_tmem_cnic','g2_tmem_lang','g2_vaccine_carr','g2_finger_marker','g2_tally_sheet','g2_docs_avail','g2_vial_SOP_cool','g2_vial_SOP_valid_vvm','g2_vial_SOP_dry','g2_child_vs_doses','g2_transit_point','g2_security_lea','g2_Comments3','unique_id','dev_remarks','g2_loc_appr_accessable','g2_loc_appr_visiable','g2_loc_appr_placement','g2_loc_ident_badges','g2_loc_ident_poster','g2_loc_ident_ptp_shade','g2_loc_ident_ptp_caps','g2_loc_train_adlt_trained','g2_loc_train_adlt_adult','g2_loc_train_adlt_local','g2_docs_avail_attendance_sheet','g2_docs_avail_sup_checklist','g2_docs_avail_mp','app_version','coldchain_protocol','g2_vial_SOP_intact','g2_vial_SOP_valid_recap','is_archive', 'update_by', 'archive_reason', 'vacc_ts_cc', 'date_updated', 'campaign_ID', 'campaign_ActivityName', 'campaign_ActivityID_old', 'campaign_Yr', 'campaign_SubActivityName','geoLocation_name', 'geoLocation_type', 'geoLocation_code', 'geoLocation_census_pop', 'geoLocation_target', 'geoLocation_status', 'geoLocation_pname', 'geoLocation_dname', 'geoLocation_namedistrict', 'geoLocation_codedistrict', 'geoLocation_tname', 'geoLocation_provincecode', 'geoLocation_districtcode', 'geoLocation_tehsilcode', 'geoLocation_priority', 'geoLocation_commnet', 'geoLocation_hr', 'geoLocation_fcm', 'geoLocation_tier', 'geoLocation_block', 'geoLocation_division', 'geoLocation_cordinates', 'geoLocation_latitude', 'geoLocation_longitude', 'geoLocation_x', 'geoLocation_y', 'geoLocation_imagepath', 'geoLocation_isccpv', 'geoLocation_rank', 'geoLocation_rank_score', 'geoLocation_ishealthcamp', 'geoLocation_isdsc', 'geoLocation_ucorg', 'geoLocation_organization', 'geoLocation_tierfromaug161', 'geoLocation_tierfromsep171', 'geoLocation_tierfromdec181', 'geoLocation_mtap', 'geoLocation_rspuc', 'geoLocation_issmt', 'geoLocation_updateddatetime', 'geoLocation_x_code', 'geoLocation_draining_uc', 'geoLocation_upap_districts', 'geoLocation_shruc', 'geoLocation_khidist_id' 
    dff = pd.DataFrame(columns=d)
    for index, item in enumerate(d):
        dff[item] = apiDataFrame[index].values
    df3 = client.query_dataframe("SELECT * FROM test.xbi_icm_transit_site_monitoring")
        
    if df3.empty:
        client.insert_dataframe('INSERT INTO test.xbi_icm_transit_site_monitoring  VALUES', dff)
        logger.info('Data has been inserted into Table test.xbi_icm_transit_site_monitoring for campaign '+str(cid))

        sql = "DROP table if exists test.get_icm_transit_site_monitoring"
        client.execute(sql)
        print('\n\n Campaign \t\t--------\t', cid, '\t--------\tData Inserted \n\n')
               
    else:
        sql = "ALTER TABLE test.xbi_icm_transit_site_monitoring DELETE WHERE campaign_id = 256"
        client.execute(sql)

        dff = dff.replace(r'^\s*$', np.nan, regex=True)
        client.insert_dataframe('INSERT INTO test.xbi_icm_transit_site_monitoring  VALUES', dff)
            
        logger.info(' Data has been inserted into Table test.xbi_icm_transit_site_monitoring for campaign '+str(cid))    
        print('\n\n Campaign \t\t--------\t', cid, '\t--------\t Data Inserted \n\n')
            
        sql = "DROP table if exists test.get_icm_transit_site_monitoring"
        client.execute(sql)

dag = DAG(
    'ICM_Transit_Site_Monitoring_Automated',
    schedule_interval='0 0 * * *', 
    #schedule_interval='*/59 * * * *',  # will run every 10 min.
    default_args=default_args,
    catchup=False)

with dag:
    GetAndInsertApiDataICMTransitSiteMonitoring = PythonOperator(
        task_id='GetAndInsertApiDataICMTransitSiteMonitoring',
        python_callable=GetAndInsertApiDataICMTransitSiteMonitoring,
    )
    CreateJoinTableOfICMTransitSiteMonitoring = PythonOperator(
        task_id='CreateJoinTableOfICMTransitSiteMonitoring',
        python_callable=CreateJoinTableOfICMTransitSiteMonitoring,
    )
GetAndInsertApiDataICMTransitSiteMonitoring >> CreateJoinTableOfICMTransitSiteMonitoring
