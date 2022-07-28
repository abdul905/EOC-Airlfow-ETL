from airflow import DAG
import pandas as pd
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import requests
import logging
import numpy as np
from clickhouse_driver import connect
from clickhouse_driver import Client
import sshtunnel as sshtunnel
import db_connection as dbConn

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

# def GetAndInsertApiAIC_CatchUp():
#     logger.info('Function \' GetAndInsertApiAIC_CatchUp \' Started Off')

#     li3 = [222,223,224,225,226,227,228,229,230,231,232,233,234,235,236,237,238,239,240,241,242,243,244,245,247,248,249,250,251,252,253,254,255,256,257,258,259]
    
#     client = dbConn.getConnectionDev()

#     get_q = "select campaign_ID FROM (select * from test.xbi_campaign xc  order by xc.campaign_ID desc limit 4) ORDER BY campaign_ID ASC "
#     df2 = client.query_dataframe(get_q)
#     print(df2)
#     df = pd.DataFrame(df2)

#     campID_list = df2.campaign_ID.tolist()
#     print(campID_list)   

#     merged_df = pd.DataFrame()
        
#     for cid in campID_list:
#         url = "https://idims.eoc.gov.pk/api_who/form_2b/form2b/5468XE2LN6CzR7qRG041/"+ str(cid)+"/"
#         print('\n\n\n Campaign-ID\t\t--------\t', cid)
#         logger.info('Requested Data From Api URL:'+url)
#         r = requests.get(url)
#         data = r.json()
#         if data['data'] == "No data found":
#             print("Not Data found for campaign: "+str(cid))
#         else:
#             print("Data found for campaign: "+str(cid))
#             df = pd.DataFrame()
#             print("Campaign_Total_Pages",data['total_page'])
#             i=1
#             while i <= data['total_page']:
#                 url_c = 'https://idims.eoc.gov.pk/api_who/form_2b/form2b/5468XE2LN6CzR7qRG041/'+str(cid)+'/'+str(i)
#                 r2 = requests.get(url_c)
#                 data_c = r2.json()
#                 if data_c['data'] == "No data found":
#                     print("Not Data found for campaign: "+url_c)
#                     break
#                 else:
#                     print("Data found for campaign: "+url_c)
#                     df = pd.DataFrame()
#                     rowsData = data_c["data"]["data"] 
#                     df_nested_list = pd.json_normalize(
#                         rowsData, 
#                         meta=[
#                                 'pk_icm_2b_id',
#                                 'date_created',
#                                 "fk_prov_id",
#                                 'fk_dist_id',
#                                 'fk_tehsil_id',
#                                 "fk_uc_id",
#                                 "campaign_id",
#                                 "cday",
#                                 "camp_type",
#                                 "aic_name",
#                                 "date1",
#                                 "total_vac_given_mt",
#                                 "total_vac_used_mt",
#                                 "total_vac_returned_mt",
#                                 "total_vac_used_fixed",
#                                 "total_vac_given_fixed",
#                                 "total_vac_returned_fixed",
#                                 "total_vac_used_transit",
#                                 "total_vac_given_transit",
#                                 "total_vac_returned_transit",
#                                 "unique_id",
#                                 "uc_incharge_name",
#                                 "aic_reported",
#                                 "form_2b_pic",
#                                 "fixed_sub",
#                                 "is_archive",
#                             ],
#                             record_path =['aic']
#                         )
#                     print(len(df_nested_list))
#                     df = df.append(df_nested_list, ignore_index=True)
#                     print("DF API Size",len(df))
#                     i+=1    
#             merged_df = merged_df.append(df, ignore_index=True)
#             print("Merged DF Size",len(merged_df))        
#             # df_nested_list.to_excel("Downloads/data.xlsx",sheet_name='Sheet_1', index=False)  
#             sql = "CREATE TABLE if not exists test.get_aic_catchup_data (pk_icm_mobile_2b_id Int32,fk_parent_id Int32,team_leader Int32,team_member Int32,child_mp_0_11 Int32,child_mp_12_59 Int32,child_telysheet_0_11 Int32,child_telysheet_12_59 Int32,child_vaccinated_0_11 Int32,child_vaccinated_12_59 Int32,recorded_missed_0_11 Int32,recorded_missed_12_59 Int32,covered_missed_0_11 Int32,covered_missed_12_59 Int32,school_covered Int32,guest_vaccinated Int32,hrmp_vaccinated Int32,vaccine_received Int32,vaccine_used Int32,vaccine_returned Int32,afp_cases Int32,zero_ri_recorded Int32,vitamin_a_red Int32,vitamin_a_blue Int32,area_aic String,area_aic_code Int32,recorded_missed_na_0_11 Int32,recorded_missed_na_12_59 Int32,recorded_missed_refusal_0_11 Int32,recorded_missed_refusal_12_59 Int32,covered_missed_na_0_11 Int32,covered_missed_na_12_59 Int32,covered_missed_refusal_0_11 Int32,covered_missed_refusal_12_59 Int32,guest_vaccinated_street Int32,mobile_teams Int32,fixed_sites Int32,transit_sites Int32,f_child_vac_total String,f_vaccine_received Int32,f_vaccine_returned Int32,f_afp_cases String,f_zero_ri_recorded Int32,f_vaccine_used Int32,t_child_vac_total String,t_vaccine_received Int32,t_vaccine_returned Int32,t_afp_cases Int32,t_zero_ri_recorded String,t_vaccine_used Int32,HHPlan Int32,HHvisit Int32,Covid19 Int32,fm_issued Int32,pk_icm_2b_id Int32,date_created Date,fk_prov_id Int32,fk_dist_id String,fk_tehsil_id Int32,fk_uc_id Int32,campaign_id Int32,cday Int32,camp_type Int32,aic_name String,date1 Date,total_vac_given_mt Int32,total_vac_used_mt Int32,total_vac_returned_mt Int32,total_vac_used_fixed Int32,total_vac_given_fixed Int32,total_vac_returned_fixed Int32,total_vac_used_transit Int32,total_vac_given_transit Int32,total_vac_returned_transit Int32,unique_id String,uc_incharge_name String,aic_reported Int32,form_2b_pic String,fixed_sub String,is_archive Int32)ENGINE = MergeTree PRIMARY KEY pk_icm_2b_id ORDER BY pk_icm_2b_id"
#             print(sql)
#             client.execute(sql)
#             df2 = client.query_dataframe(
#                 "SELECT * FROM test.get_aic_catchup_data")
#             if df2.empty:
#                 df['date_created'] = pd.to_datetime(df['date_created'],unit='s')
#                 df['date_created'] = df['date_created'].dt.date
#                 df = df.replace(r'^\s*$', np.nan, regex=True)
#                 client.insert_dataframe(
#                     'INSERT INTO test.get_aic_catchup_data VALUES', df)
#             else:
#                 df['date_created'] = pd.to_datetime(df['date_created'],unit='s')
#                 df['date_created'] = df['date_created'].dt.date
#                 df = df.replace(r'^\s*$', np.nan, regex=True)
#                 client.insert_dataframe(
#                     'INSERT INTO test.get_aic_catchup_data VALUES', df)

                
# def CreateJoinTableOfAIC_CatchUp():
#     logger.info(' Function  \' CreateJoinTableOfAIC_CatchUp \' has been Initiated')

#     client = dbConn.getConnectionDev()

#     get_q = "select campaign_ID FROM (select * from test.xbi_campaign xc  order by xc.campaign_ID desc limit 1) ORDER BY campaign_ID ASC "
#     df2 = client.query_dataframe(get_q)
#     print(df2)
#     df = pd.DataFrame(df2)

#     campID_list = df2.campaign_ID.tolist()
#     print(campID_list)

#     #li3 = [256,257,258,259]
#     for cid in campID_list:
#         sql = "CREATE TABLE if not exists test.xbi_aic_catchup (pk_icm_mobile_2b_id Int32,fk_parent_id Int32,team_leader Int32,team_member Int32,child_mp_0_11 Int32,child_mp_12_59 Int32,child_telysheet_0_11 Int32,child_telysheet_12_59 Int32,child_vaccinated_0_11 Int32,child_vaccinated_12_59 Int32,recorded_missed_0_11 Int32,recorded_missed_12_59 Int32,covered_missed_0_11 Int32,covered_missed_12_59 Int32,school_covered Int32,guest_vaccinated Int32,hrmp_vaccinated Int32,vaccine_received Int32,vaccine_used Int32,vaccine_returned Int32,afp_cases Int32,zero_ri_recorded Int32,vitamin_a_red Int32,vitamin_a_blue Int32,area_aic String,area_aic_code Int32,recorded_missed_na_0_11 Int32,recorded_missed_na_12_59 Int32,recorded_missed_refusal_0_11 Int32,recorded_missed_refusal_12_59 Int32,covered_missed_na_0_11 Int32,covered_missed_na_12_59 Int32,covered_missed_refusal_0_11 Int32,covered_missed_refusal_12_59 Int32,guest_vaccinated_street Int32,mobile_teams Int32,fixed_sites Int32,transit_sites Int32,f_child_vac_total String,f_vaccine_received Int32,f_vaccine_returned Int32,f_afp_cases String,f_zero_ri_recorded Int32,f_vaccine_used Int32,t_child_vac_total String,t_vaccine_received Int32,t_vaccine_returned Int32,t_afp_cases Int32,t_zero_ri_recorded String,t_vaccine_used Int32,HHPlan Int32,HHvisit Int32,Covid19 Int32,fm_issued Int32,pk_icm_2b_id Int32,date_created Date,fk_prov_id Int32,fk_dist_id String,fk_tehsil_id Int32,fk_uc_id Int32,campaign_id Int32,cday Int32,camp_type Int32,aic_name String,date1 Date,total_vac_given_mt Int32,total_vac_used_mt Int32,total_vac_returned_mt Int32,total_vac_used_fixed Int32,total_vac_given_fixed Int32,total_vac_returned_fixed Int32,total_vac_used_transit Int32,total_vac_given_transit Int32,total_vac_returned_transit Int32,unique_id String,uc_incharge_name String,aic_reported Int32,form_2b_pic String,fixed_sub String,is_archive Int32, campaign_ID Int32, campaign_ActivityName String, campaign_ActivityID_old Int32, campaign_Yr Int32, campaign_SubActivityName String, geoLocation_name String, geoLocation_type Int32, geoLocation_code Int32, geoLocation_census_pop Int32, geoLocation_target Int32, geoLocation_status Int32, geoLocation_pname String, geoLocation_dname String, geoLocation_namedistrict String, geoLocation_codedistrict String, geoLocation_tname String, geoLocation_provincecode Int32, geoLocation_districtcode Int32, geoLocation_tehsilcode Int32, geoLocation_priority Int32, geoLocation_commnet Int32, geoLocation_hr Int32, geoLocation_fcm Int8, geoLocation_tier Int32, geoLocation_block String, geoLocation_division String, geoLocation_cordinates String, geoLocation_latitude String, geoLocation_longitude String, geoLocation_x String, geoLocation_y String, geoLocation_imagepath String, geoLocation_isccpv Int32, geoLocation_rank Int32, geoLocation_rank_score Int32, geoLocation_ishealthcamp Int32, geoLocation_isdsc Int32, geoLocation_ucorg String, geoLocation_organization String, geoLocation_tierfromaug161 Int32, geoLocation_tierfromsep171 Int32, geoLocation_tierfromdec181 Int32, geoLocation_mtap Int32, geoLocation_rspuc Int32, geoLocation_issmt Int32, geoLocation_updateddatetime String, geoLocation_x_code Int32, geoLocation_draining_uc Int32, geoLocation_upap_districts Int32, geoLocation_shruc Int32, geoLocation_khidist_id String)ENGINE = MergeTree PRIMARY KEY pk_icm_2b_id ORDER BY pk_icm_2b_id"
#         #print(sql)
#         client.execute(sql)
            
#         cols = " test.get_aic_catchup_data.pk_icm_mobile_2b_id,	test.get_aic_catchup_data.fk_parent_id,	test.get_aic_catchup_data.team_leader,	test.get_aic_catchup_data.team_member,	test.get_aic_catchup_data.child_mp_0_11,	test.get_aic_catchup_data.child_mp_12_59,	test.get_aic_catchup_data.child_telysheet_0_11,	test.get_aic_catchup_data.child_telysheet_12_59,	test.get_aic_catchup_data.child_vaccinated_0_11,	test.get_aic_catchup_data.child_vaccinated_12_59,	test.get_aic_catchup_data.recorded_missed_0_11,	test.get_aic_catchup_data.recorded_missed_12_59,	test.get_aic_catchup_data.covered_missed_0_11,	test.get_aic_catchup_data.covered_missed_12_59,	test.get_aic_catchup_data.school_covered,	test.get_aic_catchup_data.guest_vaccinated,	test.get_aic_catchup_data.hrmp_vaccinated,	test.get_aic_catchup_data.vaccine_received,	test.get_aic_catchup_data.vaccine_used,	test.get_aic_catchup_data.vaccine_returned,	test.get_aic_catchup_data.afp_cases,	test.get_aic_catchup_data.zero_ri_recorded,	test.get_aic_catchup_data.vitamin_a_red,	test.get_aic_catchup_data.vitamin_a_blue,	test.get_aic_catchup_data.area_aic,	test.get_aic_catchup_data.area_aic_code,test.get_aic_catchup_data.recorded_missed_na_0_11,test.get_aic_catchup_data.recorded_missed_na_12_59,test.get_aic_catchup_data.recorded_missed_refusal_0_11,test.get_aic_catchup_data.recorded_missed_refusal_12_59,test.get_aic_catchup_data.covered_missed_na_0_11,test.get_aic_catchup_data.covered_missed_na_12_59,	test.get_aic_catchup_data.covered_missed_refusal_0_11,	test.get_aic_catchup_data.covered_missed_refusal_12_59,	test.get_aic_catchup_data.guest_vaccinated_street,	test.get_aic_catchup_data.mobile_teams,	test.get_aic_catchup_data.fixed_sites,	test.get_aic_catchup_data.transit_sites,	test.get_aic_catchup_data.f_child_vac_total,	test.get_aic_catchup_data.f_vaccine_received,	test.get_aic_catchup_data.f_vaccine_returned,	test.get_aic_catchup_data.f_afp_cases,	test.get_aic_catchup_data.f_zero_ri_recorded,	test.get_aic_catchup_data.f_vaccine_used,	test.get_aic_catchup_data.t_child_vac_total,	test.get_aic_catchup_data.t_vaccine_received,	test.get_aic_catchup_data.t_vaccine_returned,	test.get_aic_catchup_data.t_afp_cases,	test.get_aic_catchup_data.t_zero_ri_recorded,	test.get_aic_catchup_data.t_vaccine_used,	test.get_aic_catchup_data.HHPlan,	test.get_aic_catchup_data.HHvisit,	test.get_aic_catchup_data.Covid19,	test.get_aic_catchup_data.fm_issued,	test.get_aic_catchup_data.pk_icm_2b_id,	test.get_aic_catchup_data.date_created,	test.get_aic_catchup_data.fk_prov_id,	test.get_aic_catchup_data.fk_dist_id,	test.get_aic_catchup_data.fk_tehsil_id,	test.get_aic_catchup_data.fk_uc_id,	test.get_aic_catchup_data.campaign_id,	test.get_aic_catchup_data.cday,	test.get_aic_catchup_data.camp_type,	test.get_aic_catchup_data.aic_name,	test.get_aic_catchup_data.date1,	test.get_aic_catchup_data.total_vac_given_mt,	test.get_aic_catchup_data.total_vac_used_mt,	test.get_aic_catchup_data.total_vac_returned_mt,	test.get_aic_catchup_data.total_vac_used_fixed,	test.get_aic_catchup_data.total_vac_given_fixed,	test.get_aic_catchup_data.total_vac_returned_fixed,	test.get_aic_catchup_data.total_vac_used_transit,	test.get_aic_catchup_data.total_vac_given_transit,	test.get_aic_catchup_data.total_vac_returned_transit,	test.get_aic_catchup_data.unique_id,	test.get_aic_catchup_data.uc_incharge_name,	test.get_aic_catchup_data.aic_reported,	test.get_aic_catchup_data.form_2b_pic,	test.get_aic_catchup_data.fixed_sub,	test.get_aic_catchup_data.is_archive ,test.xbi_campaign.campaign_ID, test.xbi_campaign.campaign_ActivityName, test.xbi_campaign.campaign_ActivityID_old, test.xbi_campaign.campaign_Yr, test.xbi_campaign.campaign_SubActivityName,  test.eoc_geolocation_tbl.name, test.eoc_geolocation_tbl.type, test.eoc_geolocation_tbl.code, test.eoc_geolocation_tbl.census_pop, test.eoc_geolocation_tbl.target, test.eoc_geolocation_tbl.status, test.eoc_geolocation_tbl.pname, test.eoc_geolocation_tbl.dname, test.eoc_geolocation_tbl.namedistrict, test.eoc_geolocation_tbl.codedistrict, test.eoc_geolocation_tbl.tname, test.eoc_geolocation_tbl.provincecode, test.eoc_geolocation_tbl.districtcode, test.eoc_geolocation_tbl.tehsilcode, test.eoc_geolocation_tbl.priority, test.eoc_geolocation_tbl.commnet, test.eoc_geolocation_tbl.hr, test.eoc_geolocation_tbl.fcm, test.eoc_geolocation_tbl.tier, test.eoc_geolocation_tbl.block, test.eoc_geolocation_tbl.division, test.eoc_geolocation_tbl.cordinates, test.eoc_geolocation_tbl.latitude, test.eoc_geolocation_tbl.longitude, test.eoc_geolocation_tbl.x, test.eoc_geolocation_tbl.y, test.eoc_geolocation_tbl.imagepath, test.eoc_geolocation_tbl.isccpv, test.eoc_geolocation_tbl.rank, test.eoc_geolocation_tbl.rank_score, test.eoc_geolocation_tbl.ishealthcamp, test.eoc_geolocation_tbl.isdsc, test.eoc_geolocation_tbl.ucorg, test.eoc_geolocation_tbl.organization, test.eoc_geolocation_tbl.tierfromaug161, test.eoc_geolocation_tbl.tierfromsep171, test.eoc_geolocation_tbl.tierfromdec181, test.eoc_geolocation_tbl.mtap, test.eoc_geolocation_tbl.rspuc, test.eoc_geolocation_tbl.issmt, test.eoc_geolocation_tbl.updateddatetime, test.eoc_geolocation_tbl.x_code, test.eoc_geolocation_tbl.draining_uc, test.eoc_geolocation_tbl.upap_districts, test.eoc_geolocation_tbl.shruc, test.eoc_geolocation_tbl.khidist_id"
                                
#         sql = "SELECT " + cols + "  FROM test.get_aic_catchup_data eoc_1 left JOIN test.eoc_geolocation_tbl eoc_2 ON eoc_1.fk_uc_id  = eoc_2.code left JOIN test.xbi_campaign eoc_3 ON eoc_1.campaign_id  = eoc_3.campaign_ID WHERE eoc_1.campaign_id ="+str(cid)
#         data = client.execute(sql)

#         apiDataFrame = pd.DataFrame(data)
            
#         if apiDataFrame.empty:
#             print("No Data found for campaign: "+str(f))
#             print("API DF Size: ",len(apiDataFrame))
#         else:
#             all_columns = list(apiDataFrame)  # Creates list of all column headers
#             cols = apiDataFrame.iloc[0]


#             apiDataFrame[all_columns] = apiDataFrame[all_columns].astype(str)
#             d ='pk_icm_mobile_2b_id','fk_parent_id','team_leader','team_member','child_mp_0_11','child_mp_12_59','child_telysheet_0_11','child_telysheet_12_59','child_vaccinated_0_11','child_vaccinated_12_59','recorded_missed_0_11','recorded_missed_12_59','covered_missed_0_11','covered_missed_12_59','school_covered','guest_vaccinated','hrmp_vaccinated','vaccine_received','vaccine_used','vaccine_returned','afp_cases','zero_ri_recorded','vitamin_a_red','vitamin_a_blue','area_aic','area_aic_code','recorded_missed_na_0_11','recorded_missed_na_12_59','recorded_missed_refusal_0_11','recorded_missed_refusal_12_59','covered_missed_na_0_11','covered_missed_na_12_59','covered_missed_refusal_0_11','covered_missed_refusal_12_59','guest_vaccinated_street','mobile_teams','fixed_sites','transit_sites','f_child_vac_total','f_vaccine_received','f_vaccine_returned','f_afp_cases','f_zero_ri_recorded','f_vaccine_used','t_child_vac_total','t_vaccine_received','t_vaccine_returned','t_afp_cases','t_zero_ri_recorded','t_vaccine_used','HHPlan','HHvisit','Covid19','fm_issued','pk_icm_2b_id','date_created','fk_prov_id','fk_dist_id','fk_tehsil_id','fk_uc_id','campaign_id','cday','camp_type','aic_name','date1','total_vac_given_mt','total_vac_used_mt','total_vac_returned_mt','total_vac_used_fixed','total_vac_given_fixed','total_vac_returned_fixed','total_vac_used_transit','total_vac_given_transit','total_vac_returned_transit','unique_id','uc_incharge_name','aic_reported','form_2b_pic','fixed_sub','is_archive','campaign_ID', 'campaign_ActivityName', 'campaign_ActivityID_old', 'campaign_Yr', 'campaign_SubActivityName','geoLocation_name', 'geoLocation_type', 'geoLocation_code', 'geoLocation_census_pop', 'geoLocation_target', 'geoLocation_status', 'geoLocation_pname', 'geoLocation_dname', 'geoLocation_namedistrict', 'geoLocation_codedistrict', 'geoLocation_tname', 'geoLocation_provincecode', 'geoLocation_districtcode', 'geoLocation_tehsilcode', 'geoLocation_priority', 'geoLocation_commnet', 'geoLocation_hr', 'geoLocation_fcm', 'geoLocation_tier', 'geoLocation_block', 'geoLocation_division', 'geoLocation_cordinates', 'geoLocation_latitude', 'geoLocation_longitude', 'geoLocation_x', 'geoLocation_y', 'geoLocation_imagepath', 'geoLocation_isccpv', 'geoLocation_rank', 'geoLocation_rank_score', 'geoLocation_ishealthcamp', 'geoLocation_isdsc', 'geoLocation_ucorg', 'geoLocation_organization', 'geoLocation_tierfromaug161', 'geoLocation_tierfromsep171', 'geoLocation_tierfromdec181', 'geoLocation_mtap', 'geoLocation_rspuc', 'geoLocation_issmt', 'geoLocation_updateddatetime', 'geoLocation_x_code', 'geoLocation_draining_uc', 'geoLocation_upap_districts', 'geoLocation_shruc', 'geoLocation_khidist_id' 
                            
#             dff = pd.DataFrame(columns=d)
#             for index, item in enumerate(d):
#                 dff[item] = apiDataFrame[index].values
#             df2 = client.query_dataframe(
#                 "SELECT * FROM test.xbi_aic_catchup WHERE campaign_id=" +str(cid)) #IN (256,257,258,259)
        
#             if df2.empty:
#                 dff[["date_created"]] = dff[["date_created"]].apply(pd.to_datetime)
#                 client.insert_dataframe(
#                         'INSERT INTO test.xbi_aic_catchup  VALUES', dff)

#                         # sql = "DROP table if exists test.get_aic_catchup_data"
#                         # client.execute(sql)
#                         #print('\n\n\n CID\t\t--------\t', cid)

#             else:
#                 sql = "ALTER TABLE test.xbi_aic_catchup DELETE WHERE campaign_id=" +str(cid) #IN (256,257,258,259) 
#                 client.execute(sql)
#                 dff[["date_created"]] = dff[["date_created"]].apply(pd.to_datetime)
#                 client.insert_dataframe(
#                     'INSERT INTO test.xbi_aic_catchup  VALUES', dff)
#                 #print('\n\n\n CID\t\t--------\t', cid)
                        
#         sql = "DROP table if exists test.get_aic_catchup_data"
#         client.execute(sql)        

# dag = DAG(
#     'AIC_CatchUp',
#     schedule_interval='0 0 * * *',  # once a day at midnight.
#     #schedule_interval='0 * * * *', # Run once an hour at the beginning of the hour 
#     #schedule_interval='*/59 * * * *',  # will run every 10 min.
#     default_args=default_args,
#     catchup=False)
# with dag:
#     GetAndInsertApiAIC_CatchUp = PythonOperator(
#         task_id='GetAndInsertApiAIC_CatchUp',
#         python_callable=GetAndInsertApiAIC_CatchUp,
#     )
#     CreateJoinTableOfAIC_CatchUp = PythonOperator(
#         task_id='CreateJoinTableOfAIC_CatchUp',
#         python_callable=CreateJoinTableOfAIC_CatchUp,
#     )
# GetAndInsertApiAIC_CatchUp >> CreateJoinTableOfAIC_CatchUp


#---------------------------------------------------------------------------------#
def GetAndInsertApiAIC_CatchUp():
    logger.info('Function \' GetAndInsertApiAIC_CatchUp \' Started Off')
    li3 = [222,223,224,225,226,227,228,229,230,231,232,233,234,235,236,237,238,239,240,241,242,243,244,245,247,248,249,250,251,252,253,254,255,256,257,258,259]
    merged_df = pd.DataFrame()
    
    client = dbConn.getConnectionDev()
        
        #cid=256 # Campaign Id for NID MAY 2022
    merged_df = pd.DataFrame()
    for cid in li3:
        url = "https://idims.eoc.gov.pk/api_who/form_2b/form2b/5468XE2LN6CzR7qRG041/"+ str(cid)+"/"
        print('\n\n\n Campaign-ID\t\t--------\t', cid)
        logger.info('Requested Data From Api URL:'+url)
        r = requests.get(url)
        data = r.json()
        if data['data'] == "No data found":
            print("Not Data found for campaign: "+str(cid))
        else:
            print("Data found for campaign: "+str(cid))
            df = pd.DataFrame()
            print("Campaign_Total_Pages",data['total_page'])
            i=1
            while i <= data['total_page']:
                url_c = 'https://idims.eoc.gov.pk/api_who/form_2b/form2b/5468XE2LN6CzR7qRG041/'+str(cid)+'/'+str(i)
                r2 = requests.get(url_c)
                data_c = r2.json()
                if data_c['data'] == "No data found":
                    print("Not Data found for campaign: "+url_c)
                    break
                else:
                    print("Data found for campaign: "+url_c)
                    df = pd.DataFrame()
                    rowsData = data_c["data"]["data"] 
                    df_nested_list = pd.json_normalize(
                        rowsData, 
                        meta=[
                                'pk_icm_2b_id',
                                'date_created',
                                "fk_prov_id",
                                'fk_dist_id',
                                'fk_tehsil_id',
                                "fk_uc_id",
                                "campaign_id",
                                "cday",
                                "camp_type",
                                "aic_name",
                                "date1",
                                "total_vac_given_mt",
                                "total_vac_used_mt",
                                "total_vac_returned_mt",
                                "total_vac_used_fixed",
                                "total_vac_given_fixed",
                                "total_vac_returned_fixed",
                                "total_vac_used_transit",
                                "total_vac_given_transit",
                                "total_vac_returned_transit",
                                "unique_id",
                                "uc_incharge_name",
                                "aic_reported",
                                "form_2b_pic",
                                "fixed_sub",
                                "is_archive",
                            ],
                            record_path =['aic']
                    )
                    print(len(df_nested_list))
                    df = df.append(df_nested_list, ignore_index=True)
                    print("DF API Size",len(df))
                    i+=1    
            merged_df = merged_df.append(df, ignore_index=True)
            print("Merged DF Size",len(merged_df))        
                # df_nested_list.to_excel("Downloads/data.xlsx",sheet_name='Sheet_1', index=False)  
            sql = "CREATE TABLE if not exists test.get_aic_catchup_data (pk_icm_mobile_2b_id Int32,fk_parent_id Int32,team_leader Int32,team_member Int32,child_mp_0_11 Int32,child_mp_12_59 Int32,child_telysheet_0_11 Int32,child_telysheet_12_59 Int32,child_vaccinated_0_11 Int32,child_vaccinated_12_59 Int32,recorded_missed_0_11 Int32,recorded_missed_12_59 Int32,covered_missed_0_11 Int32,covered_missed_12_59 Int32,school_covered Int32,guest_vaccinated Int32,hrmp_vaccinated Int32,vaccine_received Int32,vaccine_used Int32,vaccine_returned Int32,afp_cases Int32,zero_ri_recorded Int32,vitamin_a_red Int32,vitamin_a_blue Int32,area_aic String,area_aic_code Int32,recorded_missed_na_0_11 Int32,recorded_missed_na_12_59 Int32,recorded_missed_refusal_0_11 Int32,recorded_missed_refusal_12_59 Int32,covered_missed_na_0_11 Int32,covered_missed_na_12_59 Int32,covered_missed_refusal_0_11 Int32,covered_missed_refusal_12_59 Int32,guest_vaccinated_street Int32,mobile_teams Int32,fixed_sites Int32,transit_sites Int32,f_child_vac_total String,f_vaccine_received Int32,f_vaccine_returned Int32,f_afp_cases String,f_zero_ri_recorded Int32,f_vaccine_used Int32,t_child_vac_total String,t_vaccine_received Int32,t_vaccine_returned Int32,t_afp_cases Int32,t_zero_ri_recorded String,t_vaccine_used Int32,HHPlan Int32,HHvisit Int32,Covid19 Int32,fm_issued Int32,pk_icm_2b_id Int32,date_created Date,fk_prov_id Int32,fk_dist_id String,fk_tehsil_id Int32,fk_uc_id Int32,campaign_id Int32,cday Int32,camp_type Int32,aic_name String,date1 Date,total_vac_given_mt Int32,total_vac_used_mt Int32,total_vac_returned_mt Int32,total_vac_used_fixed Int32,total_vac_given_fixed Int32,total_vac_returned_fixed Int32,total_vac_used_transit Int32,total_vac_given_transit Int32,total_vac_returned_transit Int32,unique_id String,uc_incharge_name String,aic_reported Int32,form_2b_pic String,fixed_sub String,is_archive Int32)ENGINE = MergeTree PRIMARY KEY pk_icm_2b_id ORDER BY pk_icm_2b_id"
            print(sql)
            client.execute(sql)
            df2 = client.query_dataframe(
                "SELECT * FROM test.get_aic_catchup_data")
            if df2.empty:
                df['date_created'] = pd.to_datetime(df['date_created'],unit='s')
                df['date_created'] = df['date_created'].dt.date
                #df['date1'] = df['date1'].dt.date
                df = df.replace(r'^\s*$', np.nan, regex=True)
                client.insert_dataframe(
                    'INSERT INTO test.get_aic_catchup_data VALUES', df)
                            #  sql = "DROP table if exists test.get_test_ai"
                            #  client.execute(sql)    
            sql = "CREATE TABLE if not exists test.xbi_aic_catchup (pk_icm_mobile_2b_id Int32,fk_parent_id Int32,team_leader Int32,team_member Int32,child_mp_0_11 Int32,child_mp_12_59 Int32,child_telysheet_0_11 Int32,child_telysheet_12_59 Int32,child_vaccinated_0_11 Int32,child_vaccinated_12_59 Int32,recorded_missed_0_11 Int32,recorded_missed_12_59 Int32,covered_missed_0_11 Int32,covered_missed_12_59 Int32,school_covered Int32,guest_vaccinated Int32,hrmp_vaccinated Int32,vaccine_received Int32,vaccine_used Int32,vaccine_returned Int32,afp_cases Int32,zero_ri_recorded Int32,vitamin_a_red Int32,vitamin_a_blue Int32,area_aic String,area_aic_code Int32,recorded_missed_na_0_11 Int32,recorded_missed_na_12_59 Int32,recorded_missed_refusal_0_11 Int32,recorded_missed_refusal_12_59 Int32,covered_missed_na_0_11 Int32,covered_missed_na_12_59 Int32,covered_missed_refusal_0_11 Int32,covered_missed_refusal_12_59 Int32,guest_vaccinated_street Int32,mobile_teams Int32,fixed_sites Int32,transit_sites Int32,f_child_vac_total String,f_vaccine_received Int32,f_vaccine_returned Int32,f_afp_cases String,f_zero_ri_recorded Int32,f_vaccine_used Int32,t_child_vac_total String,t_vaccine_received Int32,t_vaccine_returned Int32,t_afp_cases Int32,t_zero_ri_recorded String,t_vaccine_used Int32,HHPlan Int32,HHvisit Int32,Covid19 Int32,fm_issued Int32,pk_icm_2b_id Int32,date_created Date,fk_prov_id Int32,fk_dist_id String,fk_tehsil_id Int32,fk_uc_id Int32,campaign_id Int32,cday Int32,camp_type Int32,aic_name String,date1 Date,total_vac_given_mt Int32,total_vac_used_mt Int32,total_vac_returned_mt Int32,total_vac_used_fixed Int32,total_vac_given_fixed Int32,total_vac_returned_fixed Int32,total_vac_used_transit Int32,total_vac_given_transit Int32,total_vac_returned_transit Int32,unique_id String,uc_incharge_name String,aic_reported Int32,form_2b_pic String,fixed_sub String,is_archive Int32, campaign_ID Int32, campaign_ActivityName String, campaign_ActivityID_old Int32, campaign_Yr Int32, campaign_SubActivityName String, geoLocation_name String, geoLocation_type Int32, geoLocation_code Int32, geoLocation_census_pop Int32, geoLocation_target Int32, geoLocation_status Int32, geoLocation_pname String, geoLocation_dname String, geoLocation_namedistrict String, geoLocation_codedistrict String, geoLocation_tname String, geoLocation_provincecode Int32, geoLocation_districtcode Int32, geoLocation_tehsilcode Int32, geoLocation_priority Int32, geoLocation_commnet Int32, geoLocation_hr Int32, geoLocation_fcm Int8, geoLocation_tier Int32, geoLocation_block String, geoLocation_division String, geoLocation_cordinates String, geoLocation_latitude String, geoLocation_longitude String, geoLocation_x String, geoLocation_y String, geoLocation_imagepath String, geoLocation_isccpv Int32, geoLocation_rank Int32, geoLocation_rank_score Int32, geoLocation_ishealthcamp Int32, geoLocation_isdsc Int32, geoLocation_ucorg String, geoLocation_organization String, geoLocation_tierfromaug161 Int32, geoLocation_tierfromsep171 Int32, geoLocation_tierfromdec181 Int32, geoLocation_mtap Int32, geoLocation_rspuc Int32, geoLocation_issmt Int32, geoLocation_updateddatetime String, geoLocation_x_code Int32, geoLocation_draining_uc Int32, geoLocation_upap_districts Int32, geoLocation_shruc Int32, geoLocation_khidist_id String)ENGINE = MergeTree PRIMARY KEY pk_icm_2b_id ORDER BY pk_icm_2b_id"
            print(sql)
            client.execute(sql)

            cols = " test.get_aic_catchup_data.pk_icm_mobile_2b_id,	test.get_aic_catchup_data.fk_parent_id,	test.get_aic_catchup_data.team_leader,	test.get_aic_catchup_data.team_member,	test.get_aic_catchup_data.child_mp_0_11,	test.get_aic_catchup_data.child_mp_12_59,	test.get_aic_catchup_data.child_telysheet_0_11,	test.get_aic_catchup_data.child_telysheet_12_59,	test.get_aic_catchup_data.child_vaccinated_0_11,	test.get_aic_catchup_data.child_vaccinated_12_59,	test.get_aic_catchup_data.recorded_missed_0_11,	test.get_aic_catchup_data.recorded_missed_12_59,	test.get_aic_catchup_data.covered_missed_0_11,	test.get_aic_catchup_data.covered_missed_12_59,	test.get_aic_catchup_data.school_covered,	test.get_aic_catchup_data.guest_vaccinated,	test.get_aic_catchup_data.hrmp_vaccinated,	test.get_aic_catchup_data.vaccine_received,	test.get_aic_catchup_data.vaccine_used,	test.get_aic_catchup_data.vaccine_returned,	test.get_aic_catchup_data.afp_cases,	test.get_aic_catchup_data.zero_ri_recorded,	test.get_aic_catchup_data.vitamin_a_red,	test.get_aic_catchup_data.vitamin_a_blue,	test.get_aic_catchup_data.area_aic,	test.get_aic_catchup_data.area_aic_code,test.get_aic_catchup_data.recorded_missed_na_0_11,test.get_aic_catchup_data.recorded_missed_na_12_59,test.get_aic_catchup_data.recorded_missed_refusal_0_11,test.get_aic_catchup_data.recorded_missed_refusal_12_59,test.get_aic_catchup_data.covered_missed_na_0_11,test.get_aic_catchup_data.covered_missed_na_12_59,	test.get_aic_catchup_data.covered_missed_refusal_0_11,	test.get_aic_catchup_data.covered_missed_refusal_12_59,	test.get_aic_catchup_data.guest_vaccinated_street,	test.get_aic_catchup_data.mobile_teams,	test.get_aic_catchup_data.fixed_sites,	test.get_aic_catchup_data.transit_sites,	test.get_aic_catchup_data.f_child_vac_total,	test.get_aic_catchup_data.f_vaccine_received,	test.get_aic_catchup_data.f_vaccine_returned,	test.get_aic_catchup_data.f_afp_cases,	test.get_aic_catchup_data.f_zero_ri_recorded,	test.get_aic_catchup_data.f_vaccine_used,	test.get_aic_catchup_data.t_child_vac_total,	test.get_aic_catchup_data.t_vaccine_received,	test.get_aic_catchup_data.t_vaccine_returned,	test.get_aic_catchup_data.t_afp_cases,	test.get_aic_catchup_data.t_zero_ri_recorded,	test.get_aic_catchup_data.t_vaccine_used,	test.get_aic_catchup_data.HHPlan,	test.get_aic_catchup_data.HHvisit,	test.get_aic_catchup_data.Covid19,	test.get_aic_catchup_data.fm_issued,	test.get_aic_catchup_data.pk_icm_2b_id,	test.get_aic_catchup_data.date_created,	test.get_aic_catchup_data.fk_prov_id,	test.get_aic_catchup_data.fk_dist_id,	test.get_aic_catchup_data.fk_tehsil_id,	test.get_aic_catchup_data.fk_uc_id,	test.get_aic_catchup_data.campaign_id,	test.get_aic_catchup_data.cday,	test.get_aic_catchup_data.camp_type,	test.get_aic_catchup_data.aic_name,	test.get_aic_catchup_data.date1,	test.get_aic_catchup_data.total_vac_given_mt,	test.get_aic_catchup_data.total_vac_used_mt,	test.get_aic_catchup_data.total_vac_returned_mt,	test.get_aic_catchup_data.total_vac_used_fixed,	test.get_aic_catchup_data.total_vac_given_fixed,	test.get_aic_catchup_data.total_vac_returned_fixed,	test.get_aic_catchup_data.total_vac_used_transit,	test.get_aic_catchup_data.total_vac_given_transit,	test.get_aic_catchup_data.total_vac_returned_transit,	test.get_aic_catchup_data.unique_id,	test.get_aic_catchup_data.uc_incharge_name,	test.get_aic_catchup_data.aic_reported,	test.get_aic_catchup_data.form_2b_pic,	test.get_aic_catchup_data.fixed_sub,	test.get_aic_catchup_data.is_archive ,test.xbi_campaign.campaign_ID, test.xbi_campaign.campaign_ActivityName, test.xbi_campaign.campaign_ActivityID_old, test.xbi_campaign.campaign_Yr, test.xbi_campaign.campaign_SubActivityName,  test.eoc_geolocation_tbl.name, test.eoc_geolocation_tbl.type, test.eoc_geolocation_tbl.code, test.eoc_geolocation_tbl.census_pop, test.eoc_geolocation_tbl.target, test.eoc_geolocation_tbl.status, test.eoc_geolocation_tbl.pname, test.eoc_geolocation_tbl.dname, test.eoc_geolocation_tbl.namedistrict, test.eoc_geolocation_tbl.codedistrict, test.eoc_geolocation_tbl.tname, test.eoc_geolocation_tbl.provincecode, test.eoc_geolocation_tbl.districtcode, test.eoc_geolocation_tbl.tehsilcode, test.eoc_geolocation_tbl.priority, test.eoc_geolocation_tbl.commnet, test.eoc_geolocation_tbl.hr, test.eoc_geolocation_tbl.fcm, test.eoc_geolocation_tbl.tier, test.eoc_geolocation_tbl.block, test.eoc_geolocation_tbl.division, test.eoc_geolocation_tbl.cordinates, test.eoc_geolocation_tbl.latitude, test.eoc_geolocation_tbl.longitude, test.eoc_geolocation_tbl.x, test.eoc_geolocation_tbl.y, test.eoc_geolocation_tbl.imagepath, test.eoc_geolocation_tbl.isccpv, test.eoc_geolocation_tbl.rank, test.eoc_geolocation_tbl.rank_score, test.eoc_geolocation_tbl.ishealthcamp, test.eoc_geolocation_tbl.isdsc, test.eoc_geolocation_tbl.ucorg, test.eoc_geolocation_tbl.organization, test.eoc_geolocation_tbl.tierfromaug161, test.eoc_geolocation_tbl.tierfromsep171, test.eoc_geolocation_tbl.tierfromdec181, test.eoc_geolocation_tbl.mtap, test.eoc_geolocation_tbl.rspuc, test.eoc_geolocation_tbl.issmt, test.eoc_geolocation_tbl.updateddatetime, test.eoc_geolocation_tbl.x_code, test.eoc_geolocation_tbl.draining_uc, test.eoc_geolocation_tbl.upap_districts, test.eoc_geolocation_tbl.shruc, test.eoc_geolocation_tbl.khidist_id"
                            
            sql = "SELECT " + cols + "  FROM test.get_aic_catchup_data eoc_1 left JOIN test.eoc_geolocation_tbl eoc_2 ON eoc_1.fk_uc_id  = eoc_2.code left JOIN test.xbi_campaign eoc_3 ON eoc_1.campaign_id  = eoc_3.campaign_ID "
            data = client.execute(sql)

            apiDataFrame = pd.DataFrame(data)
            all_columns = list(apiDataFrame)  # Creates list of all column headers
            cols = apiDataFrame.iloc[0]

            apiDataFrame[all_columns] = apiDataFrame[all_columns].astype(str)
            d ='pk_icm_mobile_2b_id','fk_parent_id','team_leader','team_member','child_mp_0_11','child_mp_12_59','child_telysheet_0_11','child_telysheet_12_59','child_vaccinated_0_11','child_vaccinated_12_59','recorded_missed_0_11','recorded_missed_12_59','covered_missed_0_11','covered_missed_12_59','school_covered','guest_vaccinated','hrmp_vaccinated','vaccine_received','vaccine_used','vaccine_returned','afp_cases','zero_ri_recorded','vitamin_a_red','vitamin_a_blue','area_aic','area_aic_code','recorded_missed_na_0_11','recorded_missed_na_12_59','recorded_missed_refusal_0_11','recorded_missed_refusal_12_59','covered_missed_na_0_11','covered_missed_na_12_59','covered_missed_refusal_0_11','covered_missed_refusal_12_59','guest_vaccinated_street','mobile_teams','fixed_sites','transit_sites','f_child_vac_total','f_vaccine_received','f_vaccine_returned','f_afp_cases','f_zero_ri_recorded','f_vaccine_used','t_child_vac_total','t_vaccine_received','t_vaccine_returned','t_afp_cases','t_zero_ri_recorded','t_vaccine_used','HHPlan','HHvisit','Covid19','fm_issued','pk_icm_2b_id','date_created','fk_prov_id','fk_dist_id','fk_tehsil_id','fk_uc_id','campaign_id','cday','camp_type','aic_name','date1','total_vac_given_mt','total_vac_used_mt','total_vac_returned_mt','total_vac_used_fixed','total_vac_given_fixed','total_vac_returned_fixed','total_vac_used_transit','total_vac_given_transit','total_vac_returned_transit','unique_id','uc_incharge_name','aic_reported','form_2b_pic','fixed_sub','is_archive','campaign_ID', 'campaign_ActivityName', 'campaign_ActivityID_old', 'campaign_Yr', 'campaign_SubActivityName','geoLocation_name', 'geoLocation_type', 'geoLocation_code', 'geoLocation_census_pop', 'geoLocation_target', 'geoLocation_status', 'geoLocation_pname', 'geoLocation_dname', 'geoLocation_namedistrict', 'geoLocation_codedistrict', 'geoLocation_tname', 'geoLocation_provincecode', 'geoLocation_districtcode', 'geoLocation_tehsilcode', 'geoLocation_priority', 'geoLocation_commnet', 'geoLocation_hr', 'geoLocation_fcm', 'geoLocation_tier', 'geoLocation_block', 'geoLocation_division', 'geoLocation_cordinates', 'geoLocation_latitude', 'geoLocation_longitude', 'geoLocation_x', 'geoLocation_y', 'geoLocation_imagepath', 'geoLocation_isccpv', 'geoLocation_rank', 'geoLocation_rank_score', 'geoLocation_ishealthcamp', 'geoLocation_isdsc', 'geoLocation_ucorg', 'geoLocation_organization', 'geoLocation_tierfromaug161', 'geoLocation_tierfromsep171', 'geoLocation_tierfromdec181', 'geoLocation_mtap', 'geoLocation_rspuc', 'geoLocation_issmt', 'geoLocation_updateddatetime', 'geoLocation_x_code', 'geoLocation_draining_uc', 'geoLocation_upap_districts', 'geoLocation_shruc', 'geoLocation_khidist_id' 
                    
            dff = pd.DataFrame(columns=d)
            for index, item in enumerate(d):
                dff[item] = apiDataFrame[index].values
            df2 = client.query_dataframe(
                "SELECT * FROM test.xbi_aic_catchup")
            if df2.empty:
                dff[["date_created"]] = dff[["date_created"]].apply(pd.to_datetime)
                client.insert_dataframe(
                    'INSERT INTO test.xbi_aic_catchup  VALUES', dff)

                sql = "DROP table if exists test.get_aic_catchup_data"
                client.execute(sql)
                print('\n\n\n CID\t\t--------\t', cid)

            else:
                dff[["date_created"]] = dff[["date_created"]].apply(pd.to_datetime)
                client.insert_dataframe(
                    'INSERT INTO test.xbi_aic_catchup  VALUES', dff)
                print('\n\n\n CID\t\t--------\t', cid)
                 
                sql = "DROP table if exists test.get_aic_catchup_data"
                client.execute(sql)        

dag = DAG(
    'AIC_CatchUp',
    schedule_interval='0 0 * * *',  # once a day at midnight.
    #schedule_interval='0 * * * *', # Run once an hour at the beginning of the hour 
    #schedule_interval='*/59 * * * *',  # will run every 10 min.
    default_args=default_args,
    catchup=False)

with dag:
    GetAndInsertApiAIC_CatchUp = PythonOperator(
        task_id='GetAndInsertApiAIC_CatchUp',
        python_callable=GetAndInsertApiAIC_CatchUp,
    )

GetAndInsertApiAIC_CatchUp 