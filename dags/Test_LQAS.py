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

def GetAndInsertApiDataLQAS():
    logger.info('Function \' GetAndInsertApiDataLQAS \' Started Off')
    client = Client(host='161.97.136.95',
                    user='default',
                    password='pakistan',
                    port='9000', settings={"use_numpy": True})
    # clist = ["Outbreak Response 1 Jan 20 (PU) Phase-1","Outbreak Response 1 Jan 20 (KP-PU) Phase-2","CR Jan bOPV 20 (PU-BH-SD)","Outbreak Response 1 Jan 20 (KP-PU) Phase-3","NID Feb 20","IPV Feb 20","Outbreak Response 2 Mar 20 (KP) Phase-1","SNID March 20","Outbreak Response 2 Mar 20 (RWP-ISB) Phase-3","Outbreak Response 2 Mar 20 (KP) Phase-2","Outreach Mar 2020","CR mOPV2 Jul 20","SNID Aug 20","NID Sep 20","Outbreak Response-7(FSD) Oct 20","SNID Oct 20","NID Nov 20","IPV Dec 20","NID Jan 21","IPV Mar - Round2 21","NID Mar 21","SNID Jun 21","SNID_MAY_TEST","SNID Aug 21","NID Sep 21","IPV Aug 21","OBR DIK Sep21","OBR KP Oct 21","NID DEC 2021","SNID JAN 2022","NID MAR 2022","OBR Mar 2022","CR Wazir-N APR 2022","NID MAY 2022","fIPV/OPV JUN 2022","EOA JUN 2022","SNID JUN 2022"] -> ye campaign/Activity Names hain
    clist=[256] # Campaign Id for NID MAY 2022
    merged_df = pd.DataFrame()
    
    for cid in clist:
        url = "https://idims.eoc.gov.pk/api_who/api/get_lqas_22/Al68XE2L3N6CzR7qRR1/1/"+ str(cid)+"/"
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
                url_c = "https://idims.eoc.gov.pk/api_who/api/get_lqas_22/Al68XE2L3N6CzR7qRR1/"+ str(i)+"/"+ str(cid)
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
                    rowsData = data_c["data"]["data"]
                    print(rowsData)
                    df_nested_list = pd.json_normalize(
                        rowsData, 
                        meta=[
                            "pk_lqas_22_id",
                            "fk_user_id",
                            "date_created",
                            "fk_activity_id",
                            "col_designation",
                            "fk_prov_id",
                            "fk_dist_id",
                            "fk_tehsil_id",
                            "fk_uc_id",
                            "start_village_name",
                            "lot_number",
                            "cluster_number",
                            "team_code",
                            "supervisor_name",
                            "first_surveyor_male",
                            "first_surveyor_gender",
                            "second_surveyor_female",
                            "second_surveyor_gender",
                            "q11",
                            "q12",
                            "end_village_name",
                            "not_participated",
                            "final_remarks",
                            "lot_code",
                            "lot_no",
                            "total"
                        ],
                        record_path =['houses']
                    )
                    print(df_nested_list)
            merged_df = merged_df.append(df_nested_list, ignore_index=True)
            print("Merged DF Size",len(merged_df))
    
            # sql = "CREATE TABLE if not exists test.get_LQASTest (pk_lqas_22_id Int32,fk_user_id Int32,date_created  DateTime, fk_activity_id Int32, col_designation String, fk_prov_id Int32, fk_dist_id Int32, fk_tehsil_id Int32, fk_uc_id Int32,start_village_name String,lot_number String, cluster_number Int32,team_code String, supervisor_name String,first_surveyor_male String, first_surveyor_gender String,second_surveyor_female String,second_surveyor_gender String, q11 Int32, q12 Int32, end_village_name String,not_participated String,final_remarks String,lot_code String,lot_no Int32,total Int32, date_updated DateTime('Asia/Karachi')))ENGINE = MergeTree PRIMARY KEY pk_lqas_22_id ORDER BY pk_lqas_22_id"
            # client.execute(sql)

            # df2 = client.query_dataframe(
            #     "SELECT * FROM test.get_LQASTest")
            # if df2.empty:
            #     df['date_created'] = pd.to_datetime(df['date_created'],unit='s')
            #     df['date_created'] = df['date_created'].dt.strftime('%d-%m-%Y  %H:%M:%S')
            #     df['date_updated'] = pd.to_datetime(df['date_updated'],unit='s')
            #     df['date_updated'] = df['date_updated'].dt.strftime('%d-%m-%Y %H:%M:%S')
            #     df = df.replace(r'^\s*$', np.nan, regex=True)
            #     client.insert_dataframe(
            #         'INSERT INTO test.get_LQASTest VALUES', df)
            #     logger.info(
            #             ' Data has been inserted into Table test.get_LQASTest for campaign'+str(cid))   
            
def CreateJoinTableOfICMLQAS():
    cid = 256
    logger.info(' Function  \' CreateJoinTableOfICMLQAS \' has been Initiated')
    client = Client(host='161.97.136.95',
                    user='default',
                    password='pakistan',
                    port='9000', settings={"use_numpy": True})

    # sql = "CREATE TABLE if not exists test.xbi_icm_cluster (pk_icm_hh_cluster_21_id Int32, fk_form_id Int32, fk_user_id Int32, date_created String, fk_activity_id Int32, col_surveyor_name  String, partner String, col_designation String, fk_prov_id Int32, fk_dist_id Int32, fk_tehsil_id Int32, is_cbv String, fk_uc_id Int32, g1_Village_name_begin String, g1_popu_type String, g1_aic_name String, g1_team_no String, g1_dayofwork String, final_remarks String, unique_id String, cluseter_info String, dev_remarks String, old_info String, s_rutine_epi Float32, s_no_vacc_monitor Float32, s_child_0_11 Float32, s_child_12_59 Float32, s_child_0_11_vac Float32, s_child_12_59_vac Float32, s_team_miss_house Float32, s_child_away Float32, s_refusal Float32, s_team_miss_child Float32, s_guest Float32, s_guest_vac Float32, s_total_seen Float32, s_total_finger_mark Float32, s_doormarked Float32, is_archive Int32, update_by Int32, archive_reason String, app_version String,date_updated Int32, campaign_ID Int32, campaign_ActivityName String, campaign_ActivityID_old Int32, campaign_Yr Int32, campaign_SubActivityName String, geoLocation_name String, geoLocation_type String, geoLocation_code String, geoLocation_census_pop String, geoLocation_target String, geoLocation_status String, geoLocation_pname String, geoLocation_dname String, geoLocation_namedistrict String, geoLocation_codedistrict String, geoLocation_tname String, geoLocation_provincecode String, geoLocation_districtcode String, geoLocation_tehsilcode String, geoLocation_priority String, geoLocation_commnet String, geoLocation_hr String, geoLocation_fcm String, geoLocation_tier String, geoLocation_block String, geoLocation_division String, geoLocation_cordinates String, geoLocation_latitude String, geoLocation_longitude String, geoLocation_x String, geoLocation_y String, geoLocation_imagepath String, geoLocation_isccpv String, geoLocation_rank String, geoLocation_rank_score String, geoLocation_ishealthcamp String, geoLocation_isdsc String, geoLocation_ucorg String, geoLocation_organization String, geoLocation_tierfromaug161 String, geoLocation_tierfromsep171 String, geoLocation_tierfromdec181 String, geoLocation_mtap String, geoLocation_rspuc String, geoLocation_issmt String, geoLocation_updateddatetime String, geoLocation_x_code String, geoLocation_draining_uc String, geoLocation_upap_districts String, geoLocation_shruc String, geoLocation_khidist_id String)ENGINE = MergeTree PRIMARY KEY pk_icm_hh_cluster_21_id ORDER BY pk_icm_hh_cluster_21_id"
    # client.execute(sql)
            
    # logger.info('Get Data from get_icm_cluster for campaign'+str(cid))
    # cols = " test.get_icm_cluster.pk_icm_hh_cluster_21_id, test.get_icm_cluster.fk_form_id, test.get_icm_cluster.fk_user_id, test.get_icm_cluster.date_created, test.get_icm_cluster.fk_activity_id, test.get_icm_cluster.col_surveyor_name ,test.get_icm_cluster.partner, test.get_icm_cluster.col_designation,  test.get_icm_cluster.fk_prov_id, test.get_icm_cluster.fk_dist_id, test.get_icm_cluster.fk_tehsil_id, test.get_icm_cluster.is_cbv, test.get_icm_cluster.fk_uc_id,  test.get_icm_cluster.g1_Village_name_begin, test.get_icm_cluster.g1_popu_type, test.get_icm_cluster.g1_aic_name, test.get_icm_cluster.g1_team_no, test.get_icm_cluster.g1_dayofwork, test.get_icm_cluster.final_remarks, test.get_icm_cluster.unique_id, test.get_icm_cluster.cluseter_info, test.get_icm_cluster.dev_remarks, test.get_icm_cluster.old_info, test.get_icm_cluster.s_rutine_epi, test.get_icm_cluster.s_no_vacc_monitor, test.get_icm_cluster.s_child_0_11, test.get_icm_cluster.s_child_12_59, test.get_icm_cluster.s_child_0_11_vac, test.get_icm_cluster.s_child_12_59_vac, test.get_icm_cluster.s_team_miss_house, test.get_icm_cluster.s_child_away, test.get_icm_cluster.s_refusal, test.get_icm_cluster.s_team_miss_child, test.get_icm_cluster.s_guest, test.get_icm_cluster.s_guest_vac, test.get_icm_cluster.s_total_seen, test.get_icm_cluster.s_total_finger_mark, test.get_icm_cluster.s_doormarked, test.get_icm_cluster.is_archive, test.get_icm_cluster.update_by, test.get_icm_cluster.archive_reason, test.get_icm_cluster.app_version,test.get_icm_cluster.date_updated,test.xbi_campaign.campaign_ID, test.xbi_campaign.campaign_ActivityName, test.xbi_campaign.campaign_ActivityID_old, test.xbi_campaign.campaign_Yr, test.xbi_campaign.campaign_SubActivityName,  test.eoc_geolocation_t.name, test.eoc_geolocation_t.type, test.eoc_geolocation_t.code, test.eoc_geolocation_t.census_pop, test.eoc_geolocation_t.target, test.eoc_geolocation_t.status, test.eoc_geolocation_t.pname, test.eoc_geolocation_t.dname, test.eoc_geolocation_t.namedistrict, test.eoc_geolocation_t.codedistrict, test.eoc_geolocation_t.tname, test.eoc_geolocation_t.provincecode, test.eoc_geolocation_t.districtcode, test.eoc_geolocation_t.tehsilcode, test.eoc_geolocation_t.priority, test.eoc_geolocation_t.commnet, test.eoc_geolocation_t.hr, test.eoc_geolocation_t.fcm, test.eoc_geolocation_t.tier, test.eoc_geolocation_t.block, test.eoc_geolocation_t.division, test.eoc_geolocation_t.cordinates, test.eoc_geolocation_t.latitude, test.eoc_geolocation_t.longitude, test.eoc_geolocation_t.x, test.eoc_geolocation_t.y, test.eoc_geolocation_t.imagepath, test.eoc_geolocation_t.isccpv, test.eoc_geolocation_t.rank, test.eoc_geolocation_t.rank_score, test.eoc_geolocation_t.ishealthcamp, test.eoc_geolocation_t.isdsc, test.eoc_geolocation_t.ucorg, test.eoc_geolocation_t.organization, test.eoc_geolocation_t.tierfromaug161, test.eoc_geolocation_t.tierfromsep171, test.eoc_geolocation_t.tierfromdec181, test.eoc_geolocation_t.mtap, test.eoc_geolocation_t.rspuc, test.eoc_geolocation_t.issmt, test.eoc_geolocation_t.updateddatetime, test.eoc_geolocation_t.x_code, test.eoc_geolocation_t.draining_uc, test.eoc_geolocation_t.upap_districts, test.eoc_geolocation_t.shruc, test.eoc_geolocation_t.khidist_id"
    # sql = "SELECT " + cols + "  FROM test.get_icm_cluster eoc_1 left join test.eoc_geolocation_t eoc_2 ON eoc_1.fk_uc_id  = eoc_2.code left JOIN test.xbi_campaign eoc_3 ON eoc_1.fk_activity_id  = eoc_3.campaign_ID "
    # data = client.execute(sql)

    # apiDataFrame = pd.DataFrame(data)
    # apiDataFrame = apiDataFrame.replace(r'^\s*$', np.nan, regex=True)
    # all_columns = list(apiDataFrame)  # Creates list of all column headers
    # cols = apiDataFrame.iloc[0]
    # apiDataFrame[all_columns] = apiDataFrame[all_columns].astype(str)
    # d = 'pk_icm_hh_cluster_21_id', 'fk_form_id', 'fk_user_id', 'date_created', 'fk_activity_id', 'col_surveyor_name', 'partner','col_designation', 'fk_prov_id', 'fk_dist_id', 'fk_tehsil_id', 'is_cbv', 'fk_uc_id',  'g1_Village_name_begin', 'g1_popu_type', 'g1_aic_name', 'g1_team_no', 'g1_dayofwork', 'final_remarks', 'unique_id', 'cluseter_info', 'dev_remarks', 'old_info', 's_rutine_epi', 's_no_vacc_monitor', 's_child_0_11', 's_child_12_59', 's_child_0_11_vac', 's_child_12_59_vac', 's_team_miss_house', 's_child_away', 's_refusal', 's_team_miss_child', 's_guest', 's_guest_vac', 's_total_seen', 's_total_finger_mark', 's_doormarked', 'is_archive', 'update_by', 'archive_reason', 'app_version','date_updated', 'campaign_ID', 'campaign_ActivityName', 'campaign_ActivityID_old', 'campaign_Yr', 'campaign_SubActivityName','geoLocation_name', 'geoLocation_type', 'geoLocation_code', 'geoLocation_census_pop', 'geoLocation_target', 'geoLocation_status', 'geoLocation_pname', 'geoLocation_dname', 'geoLocation_namedistrict', 'geoLocation_codedistrict', 'geoLocation_tname', 'geoLocation_provincecode', 'geoLocation_districtcode', 'geoLocation_tehsilcode', 'geoLocation_priority', 'geoLocation_commnet', 'geoLocation_hr', 'geoLocation_fcm', 'geoLocation_tier', 'geoLocation_block', 'geoLocation_division', 'geoLocation_cordinates', 'geoLocation_latitude', 'geoLocation_longitude', 'geoLocation_x', 'geoLocation_y', 'geoLocation_imagepath', 'geoLocation_isccpv', 'geoLocation_rank', 'geoLocation_rank_score', 'geoLocation_ishealthcamp', 'geoLocation_isdsc', 'geoLocation_ucorg', 'geoLocation_organization', 'geoLocation_tierfromaug161', 'geoLocation_tierfromsep171', 'geoLocation_tierfromdec181', 'geoLocation_mtap', 'geoLocation_rspuc', 'geoLocation_issmt', 'geoLocation_updateddatetime', 'geoLocation_x_code', 'geoLocation_draining_uc', 'geoLocation_upap_districts', 'geoLocation_shruc', 'geoLocation_khidist_id' 
    # dff = pd.DataFrame(columns=d)
    # for index, item in enumerate(d):
    #     dff[item] = apiDataFrame[index].values
               
    # df3 = client.query_dataframe(
    #         "SELECT * FROM test.xbi_icm_cluster WHERE fk_activity_id = 256")
    # if df3.empty:
    #     client.insert_dataframe(
    #         'INSERT INTO test.xbi_icm_cluster  VALUES', dff)
    #     logger.info(
    #         'Data has been inserted into Table test.xbi_icm_cluster for campaign '+str(cid))

    #     sql = "DROP table if exists test.get_icm_cluster"
    #     client.execute(sql)
    #     print('\n\n Campaign \t\t--------\t', cid, '\t--------\tData Inserted \n\n')
           

    # else:
    #     sql = "ALTER TABLE test.xbi_icm_cluster DELETE WHERE fk_activity_id = 256"
    #     client.execute(sql)

    #     dff = dff.replace(r'^\s*$', np.nan, regex=True)
    #     client.insert_dataframe(
    #         'INSERT INTO test.xbi_icm_cluster  VALUES', dff)
    #     logger.info(
    #             ' Data has been inserted into Table test.xbi_icm_cluster for campaign '+str(cid))    
    #     print('\n\n Campaign \t\t--------\t', cid, '\t--------\tData Inserted \n\n')
        
    #     sql = "DROP table if exists test.get_icm_cluster"
    #     client.execute(sql)
dag = DAG(
    'LQAS_Cluster_Automated',
    schedule_interval='0 0 * * *',  # once a day at midnight.
    #schedule_interval='0 * * * *', # Run once an hour at the beginning of the hour 
    #schedule_interval='*/59 * * * *',  # will run every 10 min.
    default_args=default_args,
    catchup=False)

with dag:
    GetAndInsertApiDataLQAS = PythonOperator(
        task_id='GetAndInsertApiDataLQAS',
        python_callable=GetAndInsertApiDataLQAS,
    )
    CreateJoinTableOfICMLQAS = PythonOperator(
        task_id='CreateJoinTableOfICMLQAS',
        python_callable=CreateJoinTableOfICMLQAS,
    )
GetAndInsertApiDataLQAS >> CreateJoinTableOfICMLQAS