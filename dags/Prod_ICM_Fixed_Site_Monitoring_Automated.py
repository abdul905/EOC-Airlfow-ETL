
from airflow import DAG
import pandas as pd
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import requests
import logging
import numpy as np
from clickhouse_driver import connect
from clickhouse_driver import Client
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
#-------------------------------------------------- INSERT CAMPAIGN DATA ICM Fixed Site ----------------------------------------#
#-------------------------------------------- Author: Abdul Bari Malik ---------------------------------------------------------#
#-------------------------------------------------------------------------------------------------------------------------------#

# def GetAndInsertApiDataICMFixedSiteMonitoring():
#     li3 = [222,223,224,225,226,227,228,229,230,231,232,233,234,235,236,237,238,239,240,241,242,243,244,245,247,248,249,250,251,252,253,254,255,256,257,258,259]
#     merged_df = pd.DataFrame()

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
#             url = "http://idims.eoc.gov.pk/api_who/api/get_icm/5468XE2LN6CzR7qRG041/fixed/"+ str(cid)+"/"
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
#                     url_c = "http://idims.eoc.gov.pk/api_who/api/get_icm/5468XE2LN6CzR7qRG041/fixed/"+str(cid)+'/'+str(i)
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
#                 merged_df = merged_df.append(df, ignore_index=True)
#                 print("Merged DF Size",len(merged_df))            
#                 sql = "CREATE TABLE if not exists test.get_icm_fixed_site_monitoring ( pk_icm_fixed_21_id Int32, fk_form_id Int32, fk_user_id Int32, date_created Date, campaign_id Int32, surveyor_name String, designation String, surveyor_affliate String, fk_prov_id Int32, fk_dist_id Int32, fk_tehsil_id Int32, fk_uc_id Int32, g1_aic_name String, g1_team_no String, g1_day String, g1_site_name String, g1_fsloc String, g1_vac_chld String, g1_doses String, g1_site_fun Int32, g1_vac_trained_government_accountable Int32, g1_vac_trained_trained Int32, g1_vac_trained_adult Int32, g1_vac_trained_local Int32, g1_opv_dry_valid_vvm Int32 ,g1_opv_dry_cool String, g1_opv_dry_dry Int32, g1_FS_address Int32, g1_Svisit_fixsite Int32, g1_cat String, g1_micro_avail String, g1_vitaminA String, g1_comments String, epi_center_count Int32, g2_temp_ilr String, g2_r_imm_given String, g2_reg_record_cvacc String, g2_ipv_penta_chart String, site_afp_surv_net Int32, g3_zero_site String, g3_afp_def_visible String, g3_zero_report String, g3_afp_reported String,g3_Comments1 String,covid_brief_guidline Int32, covid_disinfct_matrial Int32, covid_wear_ppe Int32, covid_social_dist Int32, covid_hold_child Int32, unique_id String, dev_remarks String, app_version String, g1_opv_dry_intact Int32, g1_opv_dry_re_capped Int32, match_admin_team Int32, mp_sk_ucmo Int32, mp_ask_ice_aic String, chk_epi_stock String, chk_downloading String, chk_vac_fridge String, chk_ilr_logic String, chk_ilr_logic_reason String, is_archive Int32, update_by Int32, archive_reason String, chk_epi_stock_leftover Int32, date_updated Date )ENGINE = MergeTree PRIMARY KEY pk_icm_fixed_21_id ORDER BY pk_icm_fixed_21_id"
#                 client.execute(sql)
#                 df2 = client.query_dataframe("SELECT * FROM test.get_icm_fixed_site_monitoring")
#                 if df2.empty:
#                     df['date_created'] = pd.to_datetime(df['date_created'],unit='s')
#                     df['date_created'] = df['date_created'].dt.date
#                     #df['date_created'] = df['date_created'].dt.strftime('%d-%m-%Y')
#                     df['date_updated'] = pd.to_datetime(df['date_updated'],unit='s')
#                     df['date_updated'] = df['date_updated'].dt.date
#                     df = df.replace(r'^\s*$', np.nan, regex=True)
#                     client.insert_dataframe(
#                         'INSERT INTO test.get_icm_fixed_site_monitoring VALUES', df)
#                     logger.info(
#                             ' Data has been inserted into Table get_icm_fixed_site_monitoring for campaign'+str(cid))    
#                 sql = "CREATE TABLE if not exists test.xbi_icm_fixed_site_monitoring (pk_icm_fixed_21_id Int32,fk_form_id Int32,fk_user_id Int32,date_created Date, icm_campaign_id Int32,surveyor_name String,designation String,surveyor_affliate String,fk_prov_id Int32,fk_dist_id Int32,fk_tehsil_id Int32,fk_uc_id Int32,g1_aic_name String,g1_team_no String,g1_day String,g1_site_name String, g1_fsloc String, g1_vac_chld String,g1_doses String,g1_site_fun Int32,g1_vac_trained_government_accountable Int32,g1_vac_trained_trained Int32,g1_vac_trained_adult Int32,g1_vac_trained_local Int32,g1_opv_dry_valid_vvm Int32,g1_opv_dry_cool String, g1_opv_dry_dry Int32,g1_FS_address Int32,g1_Svisit_fixsite Int32,g1_cat String, g1_micro_avail String,g1_vitaminA String, g1_comments String, epi_center_count Int32,g2_temp_ilr String, g2_r_imm_given String,g2_reg_record_cvacc String,g2_ipv_penta_chart String,site_afp_surv_net Int32,g3_zero_site String, g3_afp_def_visible String,g3_zero_report String,g3_afp_reported String,g3_Comments1 String, covid_brief_guidline Int32,covid_disinfct_matrial Int32,wear_ppe Int32,covid_social_dist Int32,covid_hold_child Int32,unique_id String, dev_remarks String, app_version String, g1_opv_dry_intact Int32,g1_opv_dry_re_capped Int32,match_admin_team Int32,mp_sk_ucmo Int32,mp_ask_ice_aic String, chk_epi_stock String,chk_downloading String,chk_vac_fridge String,chk_ilr_logic String,chk_ilr_logic_reason String, is_archive Int32, update_by Int32, archive_reason String, chk_epi_stock_leftover Int32, date_updated Date, campaign_ID Int32, campaign_ActivityName String, campaign_ActivityID_old Int32, campaign_Yr Int32, campaign_SubActivityName String, geoLocation_name String,geoLocation_type Int32,geoLocation_code Int32,geoLocation_census_pop Int32,geoLocation_target Int32,geoLocation_status Int32,geoLocation_pname String,geoLocation_dname String,geoLocation_namedistrict String,geoLocation_codedistrict String,geoLocation_tname String,geoLocation_provincecode Int32,geoLocation_districtcode Int32,geoLocation_tehsilcode Int32,geoLocation_priority Int32,geoLocation_commnet Int32,geoLocation_hr Int32,geoLocation_fcm Int8,geoLocation_tier Int32,geoLocation_block String,geoLocation_division String,geoLocation_cordinates String,geoLocation_latitude String,geoLocation_longitude String,geoLocation_x String,geoLocation_y String,geoLocation_imagepath String,geoLocation_isccpv Int32,geoLocation_rank Int32,geoLocation_rank_score Float64,geoLocation_ishealthcamp Int32,geoLocation_isdsc Int32,geoLocation_ucorg String,geoLocation_organization String,geoLocation_tierfromaug161 Int32,geoLocation_tierfromsep171 Int32,geoLocation_tierfromdec181 Int32,geoLocation_mtap Int32,geoLocation_rspuc Int32,geoLocation_issmt Int32,geoLocation_updateddatetime String,geoLocation_x_code Int32,geoLocation_draining_uc Int32,geoLocation_upap_districts Int32,geoLocation_shruc Int32,geoLocation_khidist_id String)ENGINE = MergeTree PRIMARY KEY pk_icm_fixed_21_id ORDER BY pk_icm_fixed_21_id"
#                 client.execute(sql)
                
#                 logger.info('Get Data from get_icm_fixed_site_monitoring for campaign'+str(cid))
#                 cols = "test.get_icm_fixed_site_monitoring.pk_icm_fixed_21_id, test.get_icm_fixed_site_monitoring.fk_form_id, test.get_icm_fixed_site_monitoring.fk_user_id, test.get_icm_fixed_site_monitoring.date_created, test.get_icm_fixed_site_monitoring.campaign_id, test.get_icm_fixed_site_monitoring.surveyor_name, test.get_icm_fixed_site_monitoring.designation, test.get_icm_fixed_site_monitoring.surveyor_affliate, test.get_icm_fixed_site_monitoring.fk_prov_id, test.get_icm_fixed_site_monitoring.fk_dist_id, test.get_icm_fixed_site_monitoring.fk_tehsil_id, test.get_icm_fixed_site_monitoring.fk_uc_id, test.get_icm_fixed_site_monitoring.g1_aic_name, test.get_icm_fixed_site_monitoring.g1_team_no, test.get_icm_fixed_site_monitoring.g1_day, test.get_icm_fixed_site_monitoring.g1_site_name, test.get_icm_fixed_site_monitoring.g1_fsloc, test.get_icm_fixed_site_monitoring.g1_vac_chld, test.get_icm_fixed_site_monitoring.g1_doses, test.get_icm_fixed_site_monitoring.g1_site_fun, test.get_icm_fixed_site_monitoring.g1_vac_trained_government_accountable, test.get_icm_fixed_site_monitoring.g1_vac_trained_trained, test.get_icm_fixed_site_monitoring.g1_vac_trained_adult, test.get_icm_fixed_site_monitoring.g1_vac_trained_local, test.get_icm_fixed_site_monitoring.g1_opv_dry_valid_vvm, test.get_icm_fixed_site_monitoring.g1_opv_dry_cool, test.get_icm_fixed_site_monitoring.g1_opv_dry_dry, test.get_icm_fixed_site_monitoring.g1_FS_address, test.get_icm_fixed_site_monitoring.g1_Svisit_fixsite, test.get_icm_fixed_site_monitoring.g1_cat, test.get_icm_fixed_site_monitoring.g1_micro_avail, test.get_icm_fixed_site_monitoring.g1_vitaminA, test.get_icm_fixed_site_monitoring.g1_comments, test.get_icm_fixed_site_monitoring.epi_center_count, test.get_icm_fixed_site_monitoring.g2_temp_ilr, test.get_icm_fixed_site_monitoring.g2_r_imm_given, test.get_icm_fixed_site_monitoring.g2_reg_record_cvacc, test.get_icm_fixed_site_monitoring.g2_ipv_penta_chart, test.get_icm_fixed_site_monitoring.site_afp_surv_net, test.get_icm_fixed_site_monitoring.g3_zero_site, test.get_icm_fixed_site_monitoring.g3_afp_def_visible, test.get_icm_fixed_site_monitoring.g3_zero_report, test.get_icm_fixed_site_monitoring.g3_afp_reported,test.get_icm_fixed_site_monitoring.g3_Comments1, test.get_icm_fixed_site_monitoring.covid_brief_guidline, test.get_icm_fixed_site_monitoring.covid_disinfct_matrial, test.get_icm_fixed_site_monitoring.covid_wear_ppe, test.get_icm_fixed_site_monitoring.covid_social_dist, test.get_icm_fixed_site_monitoring.covid_hold_child, test.get_icm_fixed_site_monitoring.unique_id, test.get_icm_fixed_site_monitoring.dev_remarks, test.get_icm_fixed_site_monitoring.app_version, test.get_icm_fixed_site_monitoring.g1_opv_dry_intact, test.get_icm_fixed_site_monitoring.g1_opv_dry_re_capped, test.get_icm_fixed_site_monitoring.match_admin_team, test.get_icm_fixed_site_monitoring.mp_sk_ucmo, test.get_icm_fixed_site_monitoring.mp_ask_ice_aic, test.get_icm_fixed_site_monitoring.chk_epi_stock, test.get_icm_fixed_site_monitoring.chk_downloading,test.get_icm_fixed_site_monitoring.chk_vac_fridge, test.get_icm_fixed_site_monitoring.chk_ilr_logic, test.get_icm_fixed_site_monitoring.chk_ilr_logic_reason, test.get_icm_fixed_site_monitoring.is_archive, test.get_icm_fixed_site_monitoring.update_by, test.get_icm_fixed_site_monitoring.archive_reason, test.get_icm_fixed_site_monitoring.chk_epi_stock_leftover, test.get_icm_fixed_site_monitoring.date_updated, test.xbi_campaign.campaign_ID, test.xbi_campaign.campaign_ActivityName, test.xbi_campaign.campaign_ActivityID_old, test.xbi_campaign.campaign_Yr, test.xbi_campaign.campaign_SubActivityName,  test.eoc_geolocation_tbl.name, test.eoc_geolocation_tbl.type, test.eoc_geolocation_tbl.code, test.eoc_geolocation_tbl.census_pop, test.eoc_geolocation_tbl.target, test.eoc_geolocation_tbl.status, test.eoc_geolocation_tbl.pname, test.eoc_geolocation_tbl.dname, test.eoc_geolocation_tbl.namedistrict, test.eoc_geolocation_tbl.codedistrict, test.eoc_geolocation_tbl.tname, test.eoc_geolocation_tbl.provincecode, test.eoc_geolocation_tbl.districtcode, test.eoc_geolocation_tbl.tehsilcode, test.eoc_geolocation_tbl.priority, test.eoc_geolocation_tbl.commnet, test.eoc_geolocation_tbl.hr, test.eoc_geolocation_tbl.fcm, test.eoc_geolocation_tbl.tier, test.eoc_geolocation_tbl.block, test.eoc_geolocation_tbl.division, test.eoc_geolocation_tbl.cordinates, test.eoc_geolocation_tbl.latitude, test.eoc_geolocation_tbl.longitude, test.eoc_geolocation_tbl.x, test.eoc_geolocation_tbl.y, test.eoc_geolocation_tbl.imagepath, test.eoc_geolocation_tbl.isccpv, test.eoc_geolocation_tbl.rank, test.eoc_geolocation_tbl.rank_score, test.eoc_geolocation_tbl.ishealthcamp, test.eoc_geolocation_tbl.isdsc, test.eoc_geolocation_tbl.ucorg, test.eoc_geolocation_tbl.organization, test.eoc_geolocation_tbl.tierfromaug161, test.eoc_geolocation_tbl.tierfromsep171, test.eoc_geolocation_tbl.tierfromdec181, test.eoc_geolocation_tbl.mtap, test.eoc_geolocation_tbl.rspuc, test.eoc_geolocation_tbl.issmt, test.eoc_geolocation_tbl.updateddatetime, test.eoc_geolocation_tbl.x_code, test.eoc_geolocation_tbl.draining_uc, test.eoc_geolocation_tbl.upap_districts, test.eoc_geolocation_tbl.shruc, test.eoc_geolocation_tbl.khidist_id"
#                 sql = "SELECT " + cols + "  FROM test.get_icm_fixed_site_monitoring eoc_1 left join test.eoc_geolocation_tbl eoc_2 ON eoc_1.fk_uc_id  = eoc_2.code left JOIN test.xbi_campaign eoc_3 ON eoc_1.campaign_id  = eoc_3.campaign_ID "
#                 data = client.execute(sql)

#                 apiDataFrame = pd.DataFrame(data)
#                 apiDataFrame = apiDataFrame.replace(r'^\s*$', np.nan, regex=True)
#                 all_columns = list(apiDataFrame)  # Creates list of all column headers
#                 cols = apiDataFrame.iloc[0]
#                 apiDataFrame[all_columns] = apiDataFrame[all_columns].astype(str)
#                 d = 'pk_icm_fixed_21_id', 'fk_form_id', 'fk_user_id', 'date_created', 'icm_campaign_id', 'surveyor_name', 'designation', 'surveyor_affliate', 'fk_prov_id', 'fk_dist_id', 'fk_tehsil_id', 'fk_uc_id', 'g1_aic_name', 'g1_team_no', 'g1_day', 'g1_site_name','g1_fsloc','g1_vac_chld','g1_doses','g1_site_fun', 'g1_vac_trained_government_accountable','g1_vac_trained_trained', 'g1_vac_trained_adult', 'g1_vac_trained_local', 'g1_opv_dry_valid_vvm', 'g1_opv_dry_cool', 'g1_opv_dry_dry', 'g1_FS_address', 'g1_Svisit_fixsite', 'g1_cat', 'g1_micro_avail', 'g1_vitaminA', 'g1_comments', 'epi_center_count', 'g2_temp_ilr', 'g2_r_imm_given', 'g2_reg_record_cvacc', 'g2_ipv_penta_chart', 'site_afp_surv_net', 'g3_zero_site','g3_afp_def_visible','g3_zero_report', 'g3_afp_reported', 'g3_Comments1', 'covid_brief_guidline', 'covid_disinfct_matrial', 'wear_ppe', 'covid_social_dist', 'covid_hold_child', 'unique_id', 'dev_remarks', 'app_version', 'g1_opv_dry_intact', 'g1_opv_dry_re_capped', 'match_admin_team','mp_sk_ucmo','mp_ask_ice_aic','chk_epi_stock','chk_downloading', 'chk_vac_fridge','chk_ilr_logic','chk_ilr_logic_reason', 'is_archive', 'update_by', 'archive_reason','chk_epi_stock_leftover', 'date_updated', 'campaign_ID', 'campaign_ActivityName', 'campaign_ActivityID_old', 'campaign_Yr', 'campaign_SubActivityName','geoLocation_name', 'geoLocation_type', 'geoLocation_code', 'geoLocation_census_pop', 'geoLocation_target', 'geoLocation_status', 'geoLocation_pname', 'geoLocation_dname', 'geoLocation_namedistrict', 'geoLocation_codedistrict', 'geoLocation_tname', 'geoLocation_provincecode', 'geoLocation_districtcode', 'geoLocation_tehsilcode', 'geoLocation_priority', 'geoLocation_commnet', 'geoLocation_hr', 'geoLocation_fcm', 'geoLocation_tier', 'geoLocation_block', 'geoLocation_division', 'geoLocation_cordinates', 'geoLocation_latitude', 'geoLocation_longitude', 'geoLocation_x', 'geoLocation_y', 'geoLocation_imagepath', 'geoLocation_isccpv', 'geoLocation_rank', 'geoLocation_rank_score', 'geoLocation_ishealthcamp', 'geoLocation_isdsc', 'geoLocation_ucorg', 'geoLocation_organization', 'geoLocation_tierfromaug161', 'geoLocation_tierfromsep171', 'geoLocation_tierfromdec181', 'geoLocation_mtap', 'geoLocation_rspuc', 'geoLocation_issmt', 'geoLocation_updateddatetime', 'geoLocation_x_code', 'geoLocation_draining_uc', 'geoLocation_upap_districts', 'geoLocation_shruc', 'geoLocation_khidist_id' 
#                 dff = pd.DataFrame(columns=d)
#                 for index, item in enumerate(d):
#                     dff[item] = apiDataFrame[index].values
#                     #dff[item]= dff[item].replace(r'^\s*$', np.nan, regex=True)
#                 df3 = client.query_dataframe(
#                         "SELECT * FROM test.xbi_icm_fixed_site_monitoring")
#                 if df3.empty:
#                     dff[["date_created"]] = dff[["date_created"]].apply(pd.to_datetime)
#                     dff[["date_updated"]] = dff[["date_updated"]].apply(pd.to_datetime)
#                     client.insert_dataframe(
#                             'INSERT INTO test.xbi_icm_fixed_site_monitoring  VALUES', dff)
#                     logger.info(
#                             'Data has been inserted into Table test.xbi_icm_fixed_site_monitoring for campaign '+str(cid))

#                     sql = "DROP table if exists test.get_icm_fixed_site_monitoring"
#                     client.execute(sql)
#                     print('\n\n\n CampaignID\t\t--------\t', cid)
#                     print('\n\n\n Campaign Record Page:\t\t--------\t', i)    
#                 else:
#                     dff[["date_created"]] = dff[["date_created"]].apply(pd.to_datetime)
#                     dff[["date_updated"]] = dff[["date_updated"]].apply(pd.to_datetime)
#                     dff = dff.replace(r'^\s*$', np.nan, regex=True)
#                     client.insert_dataframe(
#                         'INSERT INTO test.xbi_icm_fixed_site_monitoring  VALUES', dff)
#                     logger.info(
#                             ' Data has been inserted into Table test.xbi_icm_fixed_site_monitoring for campaign '+str(cid))    
#                     print('\n\n\n CampaignID\t\t--------\t', cid)
#                     print('\n\n\n Campaign Page\t\t--------\t', i)
#                     sql = "DROP table if exists test.get_icm_fixed_site_monitoring"
#                     client.execute(sql)
#                 #----------------------------------------------------#
#                     # df = pd.concat([dff, df2])
#                     # df = df.astype('str')
#                     # df = df.drop_duplicates(subset='pk_icm_fixed_21_id',
#                     #                         keep="first", inplace=False)
#                     # sql = "DROP TABLE if exists  test.xbi_icm_fixed_site_monitoring;"
#                     # client.execute(sql)
#                     # sql = "CREATE TABLE if not exists test.xbi_icm_fixed_site_monitoring (pk_icm_fixed_21_id Int32,fk_form_id Int32,fk_user_id Int32,date_created Date, icm_campaign_id Int32,surveyor_name String,designation String,surveyor_affliate String,fk_prov_id Int32,fk_dist_id Int32,fk_tehsil_id Int32,fk_uc_id Int32,g1_aic_name String,g1_team_no String,g1_day String,g1_site_name String, g1_fsloc String, g1_vac_chld String,g1_doses String,g1_site_fun Int32,g1_vac_trained_government_accountable Int32,g1_vac_trained_trained Int32,g1_vac_trained_adult Int32,g1_vac_trained_local Int32,g1_opv_dry_valid_vvm Int32,g1_opv_dry_cool String, g1_opv_dry_dry Int32,g1_FS_address Int32,g1_Svisit_fixsite Int32,g1_cat String, g1_micro_avail String,g1_vitaminA String, g1_comments String, epi_center_count Int32,g2_temp_ilr String, g2_r_imm_given String,g2_reg_record_cvacc String,g2_ipv_penta_chart String,site_afp_surv_net Int32,g3_zero_site String, g3_afp_def_visible String,g3_zero_report String,g3_afp_reported String,g3_Comments1 String, covid_brief_guidline Int32,covid_disinfct_matrial Int32,wear_ppe Int32,covid_social_dist Int32,covid_hold_child Int32,unique_id String, dev_remarks String, app_version String, g1_opv_dry_intact Int32,g1_opv_dry_re_capped Int32,match_admin_team Int32,mp_sk_ucmo Int32,mp_ask_ice_aic String, chk_epi_stock String,chk_downloading String,chk_vac_fridge String,chk_ilr_logic String,chk_ilr_logic_reason String, is_archive Int32, update_by Int32, archive_reason String, campaign_ID Int32, campaign_ActivityName String, campaign_ActivityID_old Int32, campaign_Yr Int32, campaign_SubActivityName String, location_ID Int32, location_code Int32, location_name String, location_type String, location_target Int32, location_status Int32, location_priority String, hr_status String)ENGINE = MergeTree PRIMARY KEY pk_icm_fixed_21_id ORDER BY pk_icm_fixed_21_id"
#                     # client.execute(sql)    
                        
 
# dag = DAG(
#     'Prod_ICM_Fixed_Team_Monitoring_Automated',
#     #schedule_interval='*/10 * * * *',# will run every 10 min.
#     schedule_interval='0 0 * * *',  
#     default_args=default_args,
#     catchup=False)

# with dag:
#     GetAndInsertApiDataICMFixedSiteMonitoring = PythonOperator(
#         task_id='GetAndInsertApiDataICMFixedSiteMonitoring',
#         python_callable=GetAndInsertApiDataICMFixedSiteMonitoring,
#     )
# GetAndInsertApiDataICMFixedSiteMonitoring

#-------------------------------------------------------------------------------------------------------------------------------#
#-------------------------------------- INSERT LATEST CAMPAIGN DATA ICM_Fixed_Site_Monitoring ----------------------------------#
#-------------------------------------------- Author: Abdul Bari Malik ---------------------------------------------------------#
#-------------------------------------------------------------------------------------------------------------------------------#

def GetAndInsertApiDataICMFixedSiteMonitoring():
    logger.info('Function \' GetAndInsertApiDataICMFixedSiteMonitoring \' Started Off')
    
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

        #clist = [256, 257, 258, 259]
        merged_df = pd.DataFrame()

        get_q = "select campaign_ID FROM (select * from test.xbi_campaign xc  order by xc.campaign_ID desc limit 4) ORDER BY campaign_ID ASC "
        df2 = client.query_dataframe(get_q)
        print(df2)
        df = pd.DataFrame(df2)

        campID_list = df2.campaign_ID.tolist()
        print(campID_list)

        for cid in campID_list:
            url = "http://idims.eoc.gov.pk/api_who/api/get_icm/5468XE2LN6CzR7qRG041/fixed/"+ str(cid)+"/"
            print('\n\n Campaign-ID\t\t--------\t', cid,'\t--------\t ')
            logger.info('Requested Data From Api URL:'+url)

            r = requests.get(url)
            data = r.json()
            
            if data['data'] == "No data found":
                print("No Data found for campaign: "+str(cid))
                logger.info('No Data found for campaign: '+str(cid))
            else:
                print("Data found for campaign: "+str(cid))
                df = pd.DataFrame()
                print("Campaign_Total_Pages",data['total_page'])
                i = 1
                while int(i) <= data['total_page']:
                    url_c = "http://idims.eoc.gov.pk/api_who/api/get_icm/5468XE2LN6CzR7qRG041/fixed/"+str(cid)+'/'+str(i)
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
                sql = "CREATE TABLE if not exists test.get_icm_fixed_site_monitoring ( pk_icm_fixed_21_id Int32, fk_form_id Int32, fk_user_id Int32, date_created Date, campaign_id Int32, surveyor_name String, designation String, surveyor_affliate String, fk_prov_id Int32, fk_dist_id Int32, fk_tehsil_id Int32, fk_uc_id Int32, g1_aic_name String, g1_team_no String, g1_day String, g1_site_name String, g1_fsloc String, g1_vac_chld String, g1_doses String, g1_site_fun Int32, g1_vac_trained_government_accountable Int32, g1_vac_trained_trained Int32, g1_vac_trained_adult Int32, g1_vac_trained_local Int32, g1_opv_dry_valid_vvm Int32 ,g1_opv_dry_cool String, g1_opv_dry_dry Int32, g1_FS_address Int32, g1_Svisit_fixsite Int32, g1_cat String, g1_micro_avail String, g1_vitaminA String, g1_comments String, epi_center_count Int32, g2_temp_ilr String, g2_r_imm_given String, g2_reg_record_cvacc String, g2_ipv_penta_chart String, site_afp_surv_net Int32, g3_zero_site String, g3_afp_def_visible String, g3_zero_report String, g3_afp_reported String,g3_Comments1 String,covid_brief_guidline Int32, covid_disinfct_matrial Int32, covid_wear_ppe Int32, covid_social_dist Int32, covid_hold_child Int32, unique_id String, dev_remarks String, app_version String, g1_opv_dry_intact Int32, g1_opv_dry_re_capped Int32, match_admin_team Int32, mp_sk_ucmo Int32, mp_ask_ice_aic String, chk_epi_stock String, chk_downloading String, chk_vac_fridge String, chk_ilr_logic String, chk_ilr_logic_reason String, is_archive Int32, update_by Int32, archive_reason String, chk_epi_stock_leftover Int32, date_updated Date )ENGINE = MergeTree PRIMARY KEY pk_icm_fixed_21_id ORDER BY pk_icm_fixed_21_id"
                client.execute(sql)
                df2 = client.query_dataframe("SELECT * FROM test.get_icm_fixed_site_monitoring")
                if df2.empty:
                    df['date_created'] = pd.to_datetime(df['date_created'],unit='s')
                    df['date_created'] = df['date_created'].dt.date
                    df['date_updated'] = pd.to_datetime(df['date_updated'],unit='s')
                    df['date_updated'] = df['date_updated'].dt.date
                    df = df.replace(r'^\s*$', np.nan, regex=True)
                    client.insert_dataframe(
                        'INSERT INTO test.get_icm_fixed_site_monitoring VALUES', df)
                    logger.info(
                            ' Data has been inserted into Table get_icm_fixed_site_monitoring for campaign'+str(cid))
                else:
                    df['date_created'] = pd.to_datetime(df['date_created'],unit='s')
                    df['date_created'] = df['date_created'].dt.date
                    df['date_updated'] = pd.to_datetime(df['date_updated'],unit='s')
                    df['date_updated'] = df['date_updated'].dt.date
                    df = df.replace(r'^\s*$', np.nan, regex=True)
                    client.insert_dataframe(
                        'INSERT INTO test.get_icm_fixed_site_monitoring VALUES', df)
                    logger.info(
                            ' Data has been inserted into Table get_icm_fixed_site_monitoring for campaign'+str(cid))            


def CreateJoinTableOfICMFixedSiteMonitoring():
    #cid = 256
    logger.info(' Function  \' CreateJoinTableOfICMFixedSiteMonitoring \' has been Initiated')

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

        #clist = [256, 257, 258, 259]
        
        get_q = "select campaign_ID FROM (select * from test.xbi_campaign xc  order by xc.campaign_ID desc limit 4) ORDER BY campaign_ID ASC "
        df2 = client.query_dataframe(get_q)
        print(df2)
        df = pd.DataFrame(df2)

        campID_list = df2.campaign_ID.tolist()
        print(campID_list)

        for cid in campID_list:
            sql = "CREATE TABLE if not exists test.xbi_icm_fixed_site_monitoring (pk_icm_fixed_21_id Int32,fk_form_id Int32,fk_user_id Int32,date_created Date, icm_campaign_id Int32,surveyor_name String,designation String,surveyor_affliate String,fk_prov_id Int32,fk_dist_id Int32,fk_tehsil_id Int32,fk_uc_id Int32,g1_aic_name String,g1_team_no String,g1_day String,g1_site_name String, g1_fsloc String, g1_vac_chld String,g1_doses String,g1_site_fun Int32,g1_vac_trained_government_accountable Int32,g1_vac_trained_trained Int32,g1_vac_trained_adult Int32,g1_vac_trained_local Int32,g1_opv_dry_valid_vvm Int32,g1_opv_dry_cool String, g1_opv_dry_dry Int32,g1_FS_address Int32,g1_Svisit_fixsite Int32,g1_cat String, g1_micro_avail String,g1_vitaminA String, g1_comments String, epi_center_count Int32,g2_temp_ilr String, g2_r_imm_given String,g2_reg_record_cvacc String,g2_ipv_penta_chart String,site_afp_surv_net Int32,g3_zero_site String, g3_afp_def_visible String,g3_zero_report String,g3_afp_reported String,g3_Comments1 String, covid_brief_guidline Int32,covid_disinfct_matrial Int32,wear_ppe Int32,covid_social_dist Int32,covid_hold_child Int32,unique_id String, dev_remarks String, app_version String, g1_opv_dry_intact Int32,g1_opv_dry_re_capped Int32,match_admin_team Int32,mp_sk_ucmo Int32,mp_ask_ice_aic String, chk_epi_stock String,chk_downloading String,chk_vac_fridge String,chk_ilr_logic String,chk_ilr_logic_reason String, is_archive Int32, update_by Int32, archive_reason String, chk_epi_stock_leftover Int32, date_updated Date, campaign_ID Int32, campaign_ActivityName String, campaign_ActivityID_old Int32, campaign_Yr Int32, campaign_SubActivityName String, geoLocation_name String,geoLocation_type Int32,geoLocation_code Int32,geoLocation_census_pop Int32,geoLocation_target Int32,geoLocation_status Int32,geoLocation_pname String,geoLocation_dname String,geoLocation_namedistrict String,geoLocation_codedistrict String,geoLocation_tname String,geoLocation_provincecode Int32,geoLocation_districtcode Int32,geoLocation_tehsilcode Int32,geoLocation_priority Int32,geoLocation_commnet Int32,geoLocation_hr Int32,geoLocation_fcm Int8,geoLocation_tier Int32,geoLocation_block String,geoLocation_division String,geoLocation_cordinates String,geoLocation_latitude String,geoLocation_longitude String,geoLocation_x String,geoLocation_y String,geoLocation_imagepath String,geoLocation_isccpv Int32,geoLocation_rank Int32,geoLocation_rank_score Float64,geoLocation_ishealthcamp Int32,geoLocation_isdsc Int32,geoLocation_ucorg String,geoLocation_organization String,geoLocation_tierfromaug161 Int32,geoLocation_tierfromsep171 Int32,geoLocation_tierfromdec181 Int32,geoLocation_mtap Int32,geoLocation_rspuc Int32,geoLocation_issmt Int32,geoLocation_updateddatetime String,geoLocation_x_code Int32,geoLocation_draining_uc Int32,geoLocation_upap_districts Int32,geoLocation_shruc Int32,geoLocation_khidist_id String)ENGINE = MergeTree PRIMARY KEY pk_icm_fixed_21_id ORDER BY pk_icm_fixed_21_id"
            client.execute(sql)
                    
            logger.info('Get Data from get_icm_fixed_site_monitoring for campaign'+str(cid))
            cols = "test.get_icm_fixed_site_monitoring.pk_icm_fixed_21_id, test.get_icm_fixed_site_monitoring.fk_form_id, test.get_icm_fixed_site_monitoring.fk_user_id, test.get_icm_fixed_site_monitoring.date_created, test.get_icm_fixed_site_monitoring.campaign_id, test.get_icm_fixed_site_monitoring.surveyor_name, test.get_icm_fixed_site_monitoring.designation, test.get_icm_fixed_site_monitoring.surveyor_affliate, test.get_icm_fixed_site_monitoring.fk_prov_id, test.get_icm_fixed_site_monitoring.fk_dist_id, test.get_icm_fixed_site_monitoring.fk_tehsil_id, test.get_icm_fixed_site_monitoring.fk_uc_id, test.get_icm_fixed_site_monitoring.g1_aic_name, test.get_icm_fixed_site_monitoring.g1_team_no, test.get_icm_fixed_site_monitoring.g1_day, test.get_icm_fixed_site_monitoring.g1_site_name, test.get_icm_fixed_site_monitoring.g1_fsloc, test.get_icm_fixed_site_monitoring.g1_vac_chld, test.get_icm_fixed_site_monitoring.g1_doses, test.get_icm_fixed_site_monitoring.g1_site_fun, test.get_icm_fixed_site_monitoring.g1_vac_trained_government_accountable, test.get_icm_fixed_site_monitoring.g1_vac_trained_trained, test.get_icm_fixed_site_monitoring.g1_vac_trained_adult, test.get_icm_fixed_site_monitoring.g1_vac_trained_local, test.get_icm_fixed_site_monitoring.g1_opv_dry_valid_vvm, test.get_icm_fixed_site_monitoring.g1_opv_dry_cool, test.get_icm_fixed_site_monitoring.g1_opv_dry_dry, test.get_icm_fixed_site_monitoring.g1_FS_address, test.get_icm_fixed_site_monitoring.g1_Svisit_fixsite, test.get_icm_fixed_site_monitoring.g1_cat, test.get_icm_fixed_site_monitoring.g1_micro_avail, test.get_icm_fixed_site_monitoring.g1_vitaminA, test.get_icm_fixed_site_monitoring.g1_comments, test.get_icm_fixed_site_monitoring.epi_center_count, test.get_icm_fixed_site_monitoring.g2_temp_ilr, test.get_icm_fixed_site_monitoring.g2_r_imm_given, test.get_icm_fixed_site_monitoring.g2_reg_record_cvacc, test.get_icm_fixed_site_monitoring.g2_ipv_penta_chart, test.get_icm_fixed_site_monitoring.site_afp_surv_net, test.get_icm_fixed_site_monitoring.g3_zero_site, test.get_icm_fixed_site_monitoring.g3_afp_def_visible, test.get_icm_fixed_site_monitoring.g3_zero_report, test.get_icm_fixed_site_monitoring.g3_afp_reported,test.get_icm_fixed_site_monitoring.g3_Comments1, test.get_icm_fixed_site_monitoring.covid_brief_guidline, test.get_icm_fixed_site_monitoring.covid_disinfct_matrial, test.get_icm_fixed_site_monitoring.covid_wear_ppe, test.get_icm_fixed_site_monitoring.covid_social_dist, test.get_icm_fixed_site_monitoring.covid_hold_child, test.get_icm_fixed_site_monitoring.unique_id, test.get_icm_fixed_site_monitoring.dev_remarks, test.get_icm_fixed_site_monitoring.app_version, test.get_icm_fixed_site_monitoring.g1_opv_dry_intact, test.get_icm_fixed_site_monitoring.g1_opv_dry_re_capped, test.get_icm_fixed_site_monitoring.match_admin_team, test.get_icm_fixed_site_monitoring.mp_sk_ucmo, test.get_icm_fixed_site_monitoring.mp_ask_ice_aic, test.get_icm_fixed_site_monitoring.chk_epi_stock, test.get_icm_fixed_site_monitoring.chk_downloading,test.get_icm_fixed_site_monitoring.chk_vac_fridge, test.get_icm_fixed_site_monitoring.chk_ilr_logic, test.get_icm_fixed_site_monitoring.chk_ilr_logic_reason, test.get_icm_fixed_site_monitoring.is_archive, test.get_icm_fixed_site_monitoring.update_by, test.get_icm_fixed_site_monitoring.archive_reason,test.get_icm_fixed_site_monitoring.chk_epi_stock_leftover, test.get_icm_fixed_site_monitoring.date_updated,  test.xbi_campaign.campaign_ID, test.xbi_campaign.campaign_ActivityName, test.xbi_campaign.campaign_ActivityID_old, test.xbi_campaign.campaign_Yr, test.xbi_campaign.campaign_SubActivityName,  test.eoc_geolocation_tbl.name, test.eoc_geolocation_tbl.type, test.eoc_geolocation_tbl.code, test.eoc_geolocation_tbl.census_pop, test.eoc_geolocation_tbl.target, test.eoc_geolocation_tbl.status, test.eoc_geolocation_tbl.pname, test.eoc_geolocation_tbl.dname, test.eoc_geolocation_tbl.namedistrict, test.eoc_geolocation_tbl.codedistrict, test.eoc_geolocation_tbl.tname, test.eoc_geolocation_tbl.provincecode, test.eoc_geolocation_tbl.districtcode, test.eoc_geolocation_tbl.tehsilcode, test.eoc_geolocation_tbl.priority, test.eoc_geolocation_tbl.commnet, test.eoc_geolocation_tbl.hr, test.eoc_geolocation_tbl.fcm, test.eoc_geolocation_tbl.tier, test.eoc_geolocation_tbl.block, test.eoc_geolocation_tbl.division, test.eoc_geolocation_tbl.cordinates, test.eoc_geolocation_tbl.latitude, test.eoc_geolocation_tbl.longitude, test.eoc_geolocation_tbl.x, test.eoc_geolocation_tbl.y, test.eoc_geolocation_tbl.imagepath, test.eoc_geolocation_tbl.isccpv, test.eoc_geolocation_tbl.rank, test.eoc_geolocation_tbl.rank_score, test.eoc_geolocation_tbl.ishealthcamp, test.eoc_geolocation_tbl.isdsc, test.eoc_geolocation_tbl.ucorg, test.eoc_geolocation_tbl.organization, test.eoc_geolocation_tbl.tierfromaug161, test.eoc_geolocation_tbl.tierfromsep171, test.eoc_geolocation_tbl.tierfromdec181, test.eoc_geolocation_tbl.mtap, test.eoc_geolocation_tbl.rspuc, test.eoc_geolocation_tbl.issmt, test.eoc_geolocation_tbl.updateddatetime, test.eoc_geolocation_tbl.x_code, test.eoc_geolocation_tbl.draining_uc, test.eoc_geolocation_tbl.upap_districts, test.eoc_geolocation_tbl.shruc, test.eoc_geolocation_tbl.khidist_id"
            sql = "SELECT " + cols + "  FROM test.get_icm_fixed_site_monitoring eoc_1 left join test.eoc_geolocation_tbl eoc_2 ON eoc_1.fk_uc_id  = eoc_2.code left JOIN test.xbi_campaign eoc_3 ON eoc_1.campaign_id  = eoc_3.campaign_ID WHERE eoc_1.campaign_id ="+str(cid)
            data = client.execute(sql)

            apiDataFrame = pd.DataFrame(data)
            apiDataFrame = apiDataFrame.replace(r'^\s*$', np.nan, regex=True)
            all_columns = list(apiDataFrame)  # Creates list of all column headers
            cols = apiDataFrame.iloc[0]
            apiDataFrame[all_columns] = apiDataFrame[all_columns].astype(str)
            d = 'pk_icm_fixed_21_id', 'fk_form_id', 'fk_user_id', 'date_created', 'icm_campaign_id', 'surveyor_name', 'designation', 'surveyor_affliate', 'fk_prov_id', 'fk_dist_id', 'fk_tehsil_id', 'fk_uc_id', 'g1_aic_name', 'g1_team_no', 'g1_day', 'g1_site_name','g1_fsloc','g1_vac_chld','g1_doses','g1_site_fun', 'g1_vac_trained_government_accountable','g1_vac_trained_trained', 'g1_vac_trained_adult', 'g1_vac_trained_local', 'g1_opv_dry_valid_vvm', 'g1_opv_dry_cool', 'g1_opv_dry_dry', 'g1_FS_address', 'g1_Svisit_fixsite', 'g1_cat', 'g1_micro_avail', 'g1_vitaminA', 'g1_comments', 'epi_center_count', 'g2_temp_ilr', 'g2_r_imm_given', 'g2_reg_record_cvacc', 'g2_ipv_penta_chart', 'site_afp_surv_net', 'g3_zero_site','g3_afp_def_visible','g3_zero_report', 'g3_afp_reported', 'g3_Comments1', 'covid_brief_guidline', 'covid_disinfct_matrial', 'wear_ppe', 'covid_social_dist', 'covid_hold_child', 'unique_id', 'dev_remarks', 'app_version', 'g1_opv_dry_intact', 'g1_opv_dry_re_capped', 'match_admin_team','mp_sk_ucmo','mp_ask_ice_aic','chk_epi_stock','chk_downloading', 'chk_vac_fridge','chk_ilr_logic','chk_ilr_logic_reason', 'is_archive', 'update_by', 'archive_reason', 'chk_epi_stock_leftover', 'date_updated', 'campaign_ID', 'campaign_ActivityName', 'campaign_ActivityID_old', 'campaign_Yr', 'campaign_SubActivityName','geoLocation_name', 'geoLocation_type', 'geoLocation_code', 'geoLocation_census_pop', 'geoLocation_target', 'geoLocation_status', 'geoLocation_pname', 'geoLocation_dname', 'geoLocation_namedistrict', 'geoLocation_codedistrict', 'geoLocation_tname', 'geoLocation_provincecode', 'geoLocation_districtcode', 'geoLocation_tehsilcode', 'geoLocation_priority', 'geoLocation_commnet', 'geoLocation_hr', 'geoLocation_fcm', 'geoLocation_tier', 'geoLocation_block', 'geoLocation_division', 'geoLocation_cordinates', 'geoLocation_latitude', 'geoLocation_longitude', 'geoLocation_x', 'geoLocation_y', 'geoLocation_imagepath', 'geoLocation_isccpv', 'geoLocation_rank', 'geoLocation_rank_score', 'geoLocation_ishealthcamp', 'geoLocation_isdsc', 'geoLocation_ucorg', 'geoLocation_organization', 'geoLocation_tierfromaug161', 'geoLocation_tierfromsep171', 'geoLocation_tierfromdec181', 'geoLocation_mtap', 'geoLocation_rspuc', 'geoLocation_issmt', 'geoLocation_updateddatetime', 'geoLocation_x_code', 'geoLocation_draining_uc', 'geoLocation_upap_districts', 'geoLocation_shruc', 'geoLocation_khidist_id'
            dff = pd.DataFrame(columns=d)
            for index, item in enumerate(d):
                dff[item] = apiDataFrame[index].values
            
            df3 = client.query_dataframe("SELECT * FROM test.xbi_icm_fixed_site_monitoring WHERE icm_campaign_id ="+str(cid))
            print('\n\n Campaign \t\t--------\t', cid, '\t--------\tData Retrieved \t--------\t','Data Size: ',len(df3))
            if df3.empty:
                client.insert_dataframe('INSERT INTO test.xbi_icm_fixed_site_monitoring  VALUES', dff)
                logger.info('Data has been inserted into Table test.xbi_icm_fixed_site_monitoring for campaign '+str(cid))

                # sql = "DROP table if exists test.get_icm_fixed_site_monitoring"
                # client.execute(sql)
                print('\n\n Campaign \t\t--------\t', cid, '\t--------\tData Inserted \n\n')
            
            else:
                sql = "ALTER TABLE test.xbi_icm_fixed_site_monitoring DELETE WHERE icm_campaign_id ="+str(cid)
                client.execute(sql)
                #dff = dff.replace(r'^\s*$', np.nan, regex=True)
                client.insert_dataframe('INSERT INTO test.xbi_icm_fixed_site_monitoring  VALUES', dff)
                logger.info(' Data has been inserted into Table test.xbi_icm_fixed_site_monitoring for campaign '+str(cid))
                print('\n\n Campaign \t\t--------\t', cid, '\t--------\t Data Inserted \n\n')
                
        sql = "DROP table if exists test.get_icm_fixed_site_monitoring"
        client.execute(sql)


dag = DAG(
    'Prod_ICM_Fixed_Team_Monitoring_Automated',
    #schedule_interval='*/59 * * * *',  # will run every 10 min.
    schedule_interval='0 0 * * *',  # once a day at midnight.
    #schedule_interval='0 * * * *', # Run once an hour at the beginning of the hour 
    default_args=default_args,
    catchup=False)

with dag:
    GetAndInsertApiDataICMFixedSiteMonitoring = PythonOperator(
        task_id='GetAndInsertApiDataICMFixedSiteMonitoring',
        python_callable=GetAndInsertApiDataICMFixedSiteMonitoring,
    )
    CreateJoinTableOfICMFixedSiteMonitoring = PythonOperator(
        task_id='CreateJoinTableOfICMFixedSiteMonitoring',
        python_callable=CreateJoinTableOfICMFixedSiteMonitoring,
    )
GetAndInsertApiDataICMFixedSiteMonitoring >> CreateJoinTableOfICMFixedSiteMonitoring
