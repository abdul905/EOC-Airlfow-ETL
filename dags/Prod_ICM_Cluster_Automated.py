
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd
import numpy as np
from datetime import datetime
import requests
import logging
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
#-------------------------------------------- INSERT LATEST CAMPAIGN DATA ICM_Cluster ------------------------------------------#
#------------------------------------------------ Author: Abdul Bari Malik -----------------------------------------------------#
#-------------------------------------------------------------------------------------------------------------------------------#
def GetAndInsertApiDataICMCluster():
    
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
        #li3 = [256,257,258,259]
        merged_df = pd.DataFrame()

        get_q = "select campaign_ID FROM (select * from test.xbi_campaign xc  order by xc.campaign_ID desc limit 4) ORDER BY campaign_ID ASC "
        df2 = client.query_dataframe(get_q)
        print(df2)
        df = pd.DataFrame(df2)

        campID_list = df2.campaign_ID.tolist()
        print(campID_list)

        for cid in campID_list:
            logger.info('Function \' GetAndInsertApiDataICMCluster \' Started Off')
                                            
            url = "http://idims.eoc.gov.pk/api_who/api/get_icm_cluster/Al68XE2L3N6CzR7qRR1/"+ str(cid)+"/"
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
                    url_c = "http://idims.eoc.gov.pk/api_who/api/get_icm_cluster/Al68XE2L3N6CzR7qRR1/"+str(cid)+'/'+str(i)
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
                sql = "CREATE TABLE if not exists test.get_icm_cluster ( pk_icm_hh_cluster_21_id Int32, fk_form_id Int32, fk_user_id Int32, date_created Date, fk_activity_id Int32, col_surveyor_name  String, partner String, col_designation String, fk_prov_id Int32, fk_dist_id Int32, fk_tehsil_id Int32, is_cbv String, fk_uc_id Int32, g1_Village_name_begin String, g1_popu_type String, g1_aic_name String, g1_team_no String, g1_dayofwork String, final_remarks String, unique_id String, cluseter_info String, dev_remarks String, old_info String, s_rutine_epi Float32, s_no_vacc_monitor Float32, s_child_0_11 Float32, s_child_12_59 Float32, s_child_0_11_vac Float32, s_child_12_59_vac Float32, s_team_miss_house Float32, s_child_away Float32, s_refusal Float32, s_team_miss_child Float32, s_guest Float32, s_guest_vac Float32, s_total_seen Float32, s_total_finger_mark Float32, s_doormarked Float32, is_archive Int32, update_by Int32, archive_reason String, app_version String,date_updated Date )ENGINE = MergeTree PRIMARY KEY pk_icm_hh_cluster_21_id ORDER BY pk_icm_hh_cluster_21_id"
                client.execute(sql)
                df2 = client.query_dataframe("SELECT * FROM test.get_icm_cluster")
                if df2.empty:
                    df['date_created'] = pd.to_datetime(df['date_created'],unit='s')
                    df['date_created'] = df['date_created'].dt.date
                    df['date_updated'] = pd.to_datetime(df['date_updated'],unit='s')
                    df['date_updated'] = df['date_updated'].dt.date
                    df = df.replace(r'^\s*$', np.nan, regex=True)
                    client.insert_dataframe(
                        'INSERT INTO test.get_icm_cluster VALUES', df)
                    print(
                            ' Data has been inserted into Table get_icm_cluster for campaign'+str(cid))
                else:
                    df['date_created'] = pd.to_datetime(df['date_created'],unit='s')
                    df['date_created'] = df['date_created'].dt.date
                    df['date_updated'] = pd.to_datetime(df['date_updated'],unit='s')
                    df['date_updated'] = df['date_updated'].dt.date
                    df = df.replace(r'^\s*$', np.nan, regex=True)
                    client.insert_dataframe(
                        'INSERT INTO test.get_icm_cluster VALUES', df)
                    print(
                            ' Data has been inserted into Table get_icm_cluster for campaign'+str(cid))


def CreateJoinTableOfICMCluster():
    # clist = [256, 257, 258, 259]
    logger.info(' Function  \' CreateJoinTableOfICMCluster \' has been Initiated')

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
            sql = "CREATE TABLE if not exists test.xbi_icm_cluster (pk_icm_hh_cluster_21_id Int32, fk_form_id Int32, fk_user_id Int32, date_created Date, fk_activity_id Int32, col_surveyor_name  String, partner String, col_designation String, fk_prov_id Int32, fk_dist_id Int32, fk_tehsil_id Int32, is_cbv String, fk_uc_id Int32, g1_Village_name_begin String, g1_popu_type String, g1_aic_name String, g1_team_no String, g1_dayofwork String, final_remarks String, unique_id String, cluseter_info String, dev_remarks String, old_info String, s_rutine_epi Float32, s_no_vacc_monitor Float32, s_child_0_11 Float32, s_child_12_59 Float32, s_child_0_11_vac Float32, s_child_12_59_vac Float32, s_team_miss_house Float32, s_child_away Float32, s_refusal Float32, s_team_miss_child Float32, s_guest Float32, s_guest_vac Float32, s_total_seen Float32, s_total_finger_mark Float32, s_doormarked Float32, is_archive Int32, update_by Int32, archive_reason String, app_version String,date_updated Date, campaign_ID Int32, campaign_ActivityName String, campaign_ActivityID_old Int32, campaign_Yr Int32, campaign_SubActivityName String, geoLocation_name String, geoLocation_type Int32, geoLocation_code Int32, geoLocation_census_pop Int32, geoLocation_target Int32, geoLocation_status Int32, geoLocation_pname String, geoLocation_dname String, geoLocation_namedistrict String, geoLocation_codedistrict String, geoLocation_tname String, geoLocation_provincecode Int32, geoLocation_districtcode Int32, geoLocation_tehsilcode Int32, geoLocation_priority Int32, geoLocation_commnet Int32, geoLocation_hr Int32, geoLocation_fcm Int8, geoLocation_tier Int32, geoLocation_block String, geoLocation_division String, geoLocation_cordinates String, geoLocation_latitude String, geoLocation_longitude String, geoLocation_x String, geoLocation_y String, geoLocation_imagepath String, geoLocation_isccpv Int32, geoLocation_rank Int32, geoLocation_rank_score Int32, geoLocation_ishealthcamp Int32, geoLocation_isdsc Int32, geoLocation_ucorg String, geoLocation_organization String, geoLocation_tierfromaug161 Int32, geoLocation_tierfromsep171 Int32, geoLocation_tierfromdec181 Int32, geoLocation_mtap Int32, geoLocation_rspuc Int32, geoLocation_issmt Int32, geoLocation_updateddatetime String, geoLocation_x_code Int32, geoLocation_draining_uc Int32, geoLocation_upap_districts Int32, geoLocation_shruc Int32, geoLocation_khidist_id String)ENGINE = MergeTree PRIMARY KEY pk_icm_hh_cluster_21_id ORDER BY pk_icm_hh_cluster_21_id"
            client.execute(sql)

            logger.info('Get Data from get_icm_cluster for campaign'+str(cid))
            cols = " test.get_icm_cluster.pk_icm_hh_cluster_21_id, test.get_icm_cluster.fk_form_id, test.get_icm_cluster.fk_user_id, test.get_icm_cluster.date_created, test.get_icm_cluster.fk_activity_id, test.get_icm_cluster.col_surveyor_name ,test.get_icm_cluster.partner, test.get_icm_cluster.col_designation,  test.get_icm_cluster.fk_prov_id, test.get_icm_cluster.fk_dist_id, test.get_icm_cluster.fk_tehsil_id, test.get_icm_cluster.is_cbv, test.get_icm_cluster.fk_uc_id,  test.get_icm_cluster.g1_Village_name_begin, test.get_icm_cluster.g1_popu_type, test.get_icm_cluster.g1_aic_name, test.get_icm_cluster.g1_team_no, test.get_icm_cluster.g1_dayofwork, test.get_icm_cluster.final_remarks, test.get_icm_cluster.unique_id, test.get_icm_cluster.cluseter_info, test.get_icm_cluster.dev_remarks, test.get_icm_cluster.old_info, test.get_icm_cluster.s_rutine_epi, test.get_icm_cluster.s_no_vacc_monitor, test.get_icm_cluster.s_child_0_11, test.get_icm_cluster.s_child_12_59, test.get_icm_cluster.s_child_0_11_vac, test.get_icm_cluster.s_child_12_59_vac, test.get_icm_cluster.s_team_miss_house, test.get_icm_cluster.s_child_away, test.get_icm_cluster.s_refusal, test.get_icm_cluster.s_team_miss_child, test.get_icm_cluster.s_guest, test.get_icm_cluster.s_guest_vac, test.get_icm_cluster.s_total_seen, test.get_icm_cluster.s_total_finger_mark, test.get_icm_cluster.s_doormarked, test.get_icm_cluster.is_archive, test.get_icm_cluster.update_by, test.get_icm_cluster.archive_reason, test.get_icm_cluster.app_version,test.get_icm_cluster.date_updated,test.xbi_campaign.campaign_ID, test.xbi_campaign.campaign_ActivityName, test.xbi_campaign.campaign_ActivityID_old, test.xbi_campaign.campaign_Yr, test.xbi_campaign.campaign_SubActivityName,  test.eoc_geolocation_tbl.name, test.eoc_geolocation_tbl.type, test.eoc_geolocation_tbl.code, test.eoc_geolocation_tbl.census_pop, test.eoc_geolocation_tbl.target, test.eoc_geolocation_tbl.status, test.eoc_geolocation_tbl.pname, test.eoc_geolocation_tbl.dname, test.eoc_geolocation_tbl.namedistrict, test.eoc_geolocation_tbl.codedistrict, test.eoc_geolocation_tbl.tname, test.eoc_geolocation_tbl.provincecode, test.eoc_geolocation_tbl.districtcode, test.eoc_geolocation_tbl.tehsilcode, test.eoc_geolocation_tbl.priority, test.eoc_geolocation_tbl.commnet, test.eoc_geolocation_tbl.hr, test.eoc_geolocation_tbl.fcm, test.eoc_geolocation_tbl.tier, test.eoc_geolocation_tbl.block, test.eoc_geolocation_tbl.division, test.eoc_geolocation_tbl.cordinates, test.eoc_geolocation_tbl.latitude, test.eoc_geolocation_tbl.longitude, test.eoc_geolocation_tbl.x, test.eoc_geolocation_tbl.y, test.eoc_geolocation_tbl.imagepath, test.eoc_geolocation_tbl.isccpv, test.eoc_geolocation_tbl.rank, test.eoc_geolocation_tbl.rank_score, test.eoc_geolocation_tbl.ishealthcamp, test.eoc_geolocation_tbl.isdsc, test.eoc_geolocation_tbl.ucorg, test.eoc_geolocation_tbl.organization, test.eoc_geolocation_tbl.tierfromaug161, test.eoc_geolocation_tbl.tierfromsep171, test.eoc_geolocation_tbl.tierfromdec181, test.eoc_geolocation_tbl.mtap, test.eoc_geolocation_tbl.rspuc, test.eoc_geolocation_tbl.issmt, test.eoc_geolocation_tbl.updateddatetime, test.eoc_geolocation_tbl.x_code, test.eoc_geolocation_tbl.draining_uc, test.eoc_geolocation_tbl.upap_districts, test.eoc_geolocation_tbl.shruc, test.eoc_geolocation_tbl.khidist_id"
            sql = "SELECT " + cols + "  FROM test.get_icm_cluster eoc_1 left join test.eoc_geolocation_tbl eoc_2 ON eoc_1.fk_uc_id  = eoc_2.code left JOIN test.xbi_campaign eoc_3 ON eoc_1.fk_activity_id  = eoc_3.campaign_ID WHERE eoc_1.fk_activity_id ="+str(cid)
            data = client.execute(sql)

            apiDataFrame = pd.DataFrame(data)
            apiDataFrame = apiDataFrame.replace(r'^\s*$', np.nan, regex=True)
            all_columns = list(apiDataFrame)  # Creates list of all column headers
            cols = apiDataFrame.iloc[0]
            apiDataFrame[all_columns] = apiDataFrame[all_columns].astype(str)
            d = 'pk_icm_hh_cluster_21_id', 'fk_form_id', 'fk_user_id', 'date_created', 'fk_activity_id', 'col_surveyor_name', 'partner','col_designation', 'fk_prov_id', 'fk_dist_id', 'fk_tehsil_id', 'is_cbv', 'fk_uc_id',  'g1_Village_name_begin', 'g1_popu_type', 'g1_aic_name', 'g1_team_no', 'g1_dayofwork', 'final_remarks', 'unique_id', 'cluseter_info', 'dev_remarks', 'old_info', 's_rutine_epi', 's_no_vacc_monitor', 's_child_0_11', 's_child_12_59', 's_child_0_11_vac', 's_child_12_59_vac', 's_team_miss_house', 's_child_away', 's_refusal', 's_team_miss_child', 's_guest', 's_guest_vac', 's_total_seen', 's_total_finger_mark', 's_doormarked', 'is_archive', 'update_by', 'archive_reason', 'app_version','date_updated', 'campaign_ID', 'campaign_ActivityName', 'campaign_ActivityID_old', 'campaign_Yr', 'campaign_SubActivityName','geoLocation_name', 'geoLocation_type', 'geoLocation_code', 'geoLocation_census_pop', 'geoLocation_target', 'geoLocation_status', 'geoLocation_pname', 'geoLocation_dname', 'geoLocation_namedistrict', 'geoLocation_codedistrict', 'geoLocation_tname', 'geoLocation_provincecode', 'geoLocation_districtcode', 'geoLocation_tehsilcode', 'geoLocation_priority', 'geoLocation_commnet', 'geoLocation_hr', 'geoLocation_fcm', 'geoLocation_tier', 'geoLocation_block', 'geoLocation_division', 'geoLocation_cordinates', 'geoLocation_latitude', 'geoLocation_longitude', 'geoLocation_x', 'geoLocation_y', 'geoLocation_imagepath', 'geoLocation_isccpv', 'geoLocation_rank', 'geoLocation_rank_score', 'geoLocation_ishealthcamp', 'geoLocation_isdsc', 'geoLocation_ucorg', 'geoLocation_organization', 'geoLocation_tierfromaug161', 'geoLocation_tierfromsep171', 'geoLocation_tierfromdec181', 'geoLocation_mtap', 'geoLocation_rspuc', 'geoLocation_issmt', 'geoLocation_updateddatetime', 'geoLocation_x_code', 'geoLocation_draining_uc', 'geoLocation_upap_districts', 'geoLocation_shruc', 'geoLocation_khidist_id' 
            dff = pd.DataFrame(columns=d)
            for index, item in enumerate(d):
                dff[item] = apiDataFrame[index].values
                    
            df3 = client.query_dataframe(
                    "SELECT * FROM test.xbi_icm_cluster WHERE fk_activity_id  ="+str(cid))
            print('\n\n Campaign \t\t--------\t', cid, '\t--------\tData Retrieved \t--------\t','Data Size: ',len(df3))    
                    
            if df3.empty:
                client.insert_dataframe(
                    'INSERT INTO test.xbi_icm_cluster  VALUES', dff)
                logger.info(
                    'Data has been inserted into Table test.xbi_icm_cluster for campaign '+str(cid))

                # sql = "DROP table if exists test.get_icm_cluster"
                # client.execute(sql)
                print('\n\n Campaign \t\t--------\t', cid, '\t--------\tData Inserted \n\n')
                

            else:
                sql = "ALTER TABLE test.xbi_icm_cluster DELETE WHERE fk_activity_id ="+str(cid)
                client.execute(sql)

                client.insert_dataframe(
                    'INSERT INTO test.xbi_icm_cluster  VALUES', dff)
                logger.info(
                        ' Data has been inserted into Table test.xbi_icm_cluster for campaign '+str(cid)) 

                print('\n\n Campaign \t\t--------\t', cid, '\t--------\tData Inserted \n\n')
                
        sql = "DROP table if exists test.get_icm_cluster"
        client.execute(sql)


dag = DAG(
    'Prod_ICM_Cluster_Automated',
    schedule_interval='0 0 * * *',  # once a day at midnight.
    #schedule_interval='0 * * * *', # Run once an hour at the beginning of the hour 
    #schedule_interval='*/59 * * * *',  # will run every 10 min.
    default_args=default_args,
    catchup=False)

with dag:
    GetAndInsertApiDataICMCluster = PythonOperator(
        task_id='GetAndInsertApiDataICMCluster',
        python_callable=GetAndInsertApiDataICMCluster,
    )
    CreateJoinTableOfICMCluster = PythonOperator(
        task_id='CreateJoinTableOfICMCluster',
        python_callable=CreateJoinTableOfICMCluster,
    )
GetAndInsertApiDataICMCluster >> CreateJoinTableOfICMCluster

#-------------------------------------------------------------------------------------------------------------------------------#
#------------------------------------------- INSERT PAST DATA ICM_Cluster ------------------------------------------------------#
#-------------------------------------------- Author: Abdul Bari Malik ---------------------------------------------------------#
#-------------------------------------------------------------------------------------------------------------------------------#
# def GetAndInsertApiDataICMCluster():
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
#             logger.info('Function \' GetAndInsertApiDataICMCluster \' Started Off')
                                            
#             url = "http://idims.eoc.gov.pk/api_who/api/get_icm_cluster/Al68XE2L3N6CzR7qRR1/"+ str(cid)+"/"
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
#                     url_c = "http://idims.eoc.gov.pk/api_who/api/get_icm_cluster/Al68XE2L3N6CzR7qRR1/"+str(cid)+'/'+str(i)
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
#                 sql = "CREATE TABLE if not exists test.get_icm_cluster ( pk_icm_hh_cluster_21_id Int32, fk_form_id Int32, fk_user_id Int32, date_created Date, fk_activity_id Int32, col_surveyor_name  String, partner String, col_designation String, fk_prov_id Int32, fk_dist_id Int32, fk_tehsil_id Int32, is_cbv String, fk_uc_id Int32, g1_Village_name_begin String, g1_popu_type String, g1_aic_name String, g1_team_no String, g1_dayofwork String, final_remarks String, unique_id String, cluseter_info String, dev_remarks String, old_info String, s_rutine_epi Float32, s_no_vacc_monitor Float32, s_child_0_11 Float32, s_child_12_59 Float32, s_child_0_11_vac Float32, s_child_12_59_vac Float32, s_team_miss_house Float32, s_child_away Float32, s_refusal Float32, s_team_miss_child Float32, s_guest Float32, s_guest_vac Float32, s_total_seen Float32, s_total_finger_mark Float32, s_doormarked Float32, is_archive Int32, update_by Int32, archive_reason String, app_version String,date_updated Date )ENGINE = MergeTree PRIMARY KEY pk_icm_hh_cluster_21_id ORDER BY pk_icm_hh_cluster_21_id"
#                 client.execute(sql)
#                 df2 = client.query_dataframe("SELECT * FROM test.get_icm_cluster")
#                 if df2.empty:
#                     df['date_created'] = pd.to_datetime(df['date_created'],unit='s')
#                     df['date_created'] = df['date_created'].dt.date
#                     #df['date_created'] = df['date_created'].dt.strftime('%d-%m-%Y')
#                     df['date_updated'] = pd.to_datetime(df['date_updated'],unit='s')
#                     df['date_updated'] = df['date_updated'].dt.date
#                     df = df.replace(r'^\s*$', np.nan, regex=True)
#                     client.insert_dataframe(
#                         'INSERT INTO test.get_icm_cluster VALUES', df)
#                     logger.info(
#                             ' Data has been inserted into Table get_icm_cluster for campaign'+str(cid))    
#                 sql = "CREATE TABLE if not exists test.xbi_icm_cluster (pk_icm_hh_cluster_21_id Int32, fk_form_id Int32, fk_user_id Int32, date_created Date, fk_activity_id Int32, col_surveyor_name  String, partner String, col_designation String, fk_prov_id Int32, fk_dist_id Int32, fk_tehsil_id Int32, is_cbv String, fk_uc_id Int32, g1_Village_name_begin String, g1_popu_type String, g1_aic_name String, g1_team_no String, g1_dayofwork String, final_remarks String, unique_id String, cluseter_info String, dev_remarks String, old_info String, s_rutine_epi Float32, s_no_vacc_monitor Float32, s_child_0_11 Float32, s_child_12_59 Float32, s_child_0_11_vac Float32, s_child_12_59_vac Float32, s_team_miss_house Float32, s_child_away Float32, s_refusal Float32, s_team_miss_child Float32, s_guest Float32, s_guest_vac Float32, s_total_seen Float32, s_total_finger_mark Float32, s_doormarked Float32, is_archive Int32, update_by Int32, archive_reason String, app_version String,date_updated Date, campaign_ID Int32, campaign_ActivityName String, campaign_ActivityID_old Int32, campaign_Yr Int32, campaign_SubActivityName String, geoLocation_name String, geoLocation_type Int32, geoLocation_code Int32, geoLocation_census_pop Int32, geoLocation_target Int32, geoLocation_status Int32, geoLocation_pname String, geoLocation_dname String, geoLocation_namedistrict String, geoLocation_codedistrict String, geoLocation_tname String, geoLocation_provincecode Int32, geoLocation_districtcode Int32, geoLocation_tehsilcode Int32, geoLocation_priority Int32, geoLocation_commnet Int32, geoLocation_hr Int32, geoLocation_fcm Int8, geoLocation_tier Int32, geoLocation_block String, geoLocation_division String, geoLocation_cordinates String, geoLocation_latitude String, geoLocation_longitude String, geoLocation_x String, geoLocation_y String, geoLocation_imagepath String, geoLocation_isccpv Int32, geoLocation_rank Int32, geoLocation_rank_score Int32, geoLocation_ishealthcamp Int32, geoLocation_isdsc Int32, geoLocation_ucorg String, geoLocation_organization String, geoLocation_tierfromaug161 Int32, geoLocation_tierfromsep171 Int32, geoLocation_tierfromdec181 Int32, geoLocation_mtap Int32, geoLocation_rspuc Int32, geoLocation_issmt Int32, geoLocation_updateddatetime String, geoLocation_x_code Int32, geoLocation_draining_uc Int32, geoLocation_upap_districts Int32, geoLocation_shruc Int32, geoLocation_khidist_id String)ENGINE = MergeTree PRIMARY KEY pk_icm_hh_cluster_21_id ORDER BY pk_icm_hh_cluster_21_id"
#                 client.execute(sql)
                
#                 logger.info('Get Data from get_icm_cluster for campaign'+str(cid))
#                 cols = " test.get_icm_cluster.pk_icm_hh_cluster_21_id, test.get_icm_cluster.fk_form_id, test.get_icm_cluster.fk_user_id, test.get_icm_cluster.date_created, test.get_icm_cluster.fk_activity_id, test.get_icm_cluster.col_surveyor_name ,test.get_icm_cluster.partner, test.get_icm_cluster.col_designation,  test.get_icm_cluster.fk_prov_id, test.get_icm_cluster.fk_dist_id, test.get_icm_cluster.fk_tehsil_id, test.get_icm_cluster.is_cbv, test.get_icm_cluster.fk_uc_id,  test.get_icm_cluster.g1_Village_name_begin, test.get_icm_cluster.g1_popu_type, test.get_icm_cluster.g1_aic_name, test.get_icm_cluster.g1_team_no, test.get_icm_cluster.g1_dayofwork, test.get_icm_cluster.final_remarks, test.get_icm_cluster.unique_id, test.get_icm_cluster.cluseter_info, test.get_icm_cluster.dev_remarks, test.get_icm_cluster.old_info, test.get_icm_cluster.s_rutine_epi, test.get_icm_cluster.s_no_vacc_monitor, test.get_icm_cluster.s_child_0_11, test.get_icm_cluster.s_child_12_59, test.get_icm_cluster.s_child_0_11_vac, test.get_icm_cluster.s_child_12_59_vac, test.get_icm_cluster.s_team_miss_house, test.get_icm_cluster.s_child_away, test.get_icm_cluster.s_refusal, test.get_icm_cluster.s_team_miss_child, test.get_icm_cluster.s_guest, test.get_icm_cluster.s_guest_vac, test.get_icm_cluster.s_total_seen, test.get_icm_cluster.s_total_finger_mark, test.get_icm_cluster.s_doormarked, test.get_icm_cluster.is_archive, test.get_icm_cluster.update_by, test.get_icm_cluster.archive_reason, test.get_icm_cluster.app_version,test.get_icm_cluster.date_updated,test.xbi_campaign.campaign_ID, test.xbi_campaign.campaign_ActivityName, test.xbi_campaign.campaign_ActivityID_old, test.xbi_campaign.campaign_Yr, test.xbi_campaign.campaign_SubActivityName,  test.eoc_geolocation_tbl.name, test.eoc_geolocation_tbl.type, test.eoc_geolocation_tbl.code, test.eoc_geolocation_tbl.census_pop, test.eoc_geolocation_tbl.target, test.eoc_geolocation_tbl.status, test.eoc_geolocation_tbl.pname, test.eoc_geolocation_tbl.dname, test.eoc_geolocation_tbl.namedistrict, test.eoc_geolocation_tbl.codedistrict, test.eoc_geolocation_tbl.tname, test.eoc_geolocation_tbl.provincecode, test.eoc_geolocation_tbl.districtcode, test.eoc_geolocation_tbl.tehsilcode, test.eoc_geolocation_tbl.priority, test.eoc_geolocation_tbl.commnet, test.eoc_geolocation_tbl.hr, test.eoc_geolocation_tbl.fcm, test.eoc_geolocation_tbl.tier, test.eoc_geolocation_tbl.block, test.eoc_geolocation_tbl.division, test.eoc_geolocation_tbl.cordinates, test.eoc_geolocation_tbl.latitude, test.eoc_geolocation_tbl.longitude, test.eoc_geolocation_tbl.x, test.eoc_geolocation_tbl.y, test.eoc_geolocation_tbl.imagepath, test.eoc_geolocation_tbl.isccpv, test.eoc_geolocation_tbl.rank, test.eoc_geolocation_tbl.rank_score, test.eoc_geolocation_tbl.ishealthcamp, test.eoc_geolocation_tbl.isdsc, test.eoc_geolocation_tbl.ucorg, test.eoc_geolocation_tbl.organization, test.eoc_geolocation_tbl.tierfromaug161, test.eoc_geolocation_tbl.tierfromsep171, test.eoc_geolocation_tbl.tierfromdec181, test.eoc_geolocation_tbl.mtap, test.eoc_geolocation_tbl.rspuc, test.eoc_geolocation_tbl.issmt, test.eoc_geolocation_tbl.updateddatetime, test.eoc_geolocation_tbl.x_code, test.eoc_geolocation_tbl.draining_uc, test.eoc_geolocation_tbl.upap_districts, test.eoc_geolocation_tbl.shruc, test.eoc_geolocation_tbl.khidist_id"
#                 sql = "SELECT " + cols + "  FROM test.get_icm_cluster eoc_1 left join test.eoc_geolocation_tbl eoc_2 ON eoc_1.fk_uc_id  = eoc_2.code left JOIN test.xbi_campaign eoc_3 ON eoc_1.fk_activity_id  = eoc_3.campaign_ID "
#                 data = client.execute(sql)

#                 apiDataFrame = pd.DataFrame(data)
#                 apiDataFrame = apiDataFrame.replace(r'^\s*$', np.nan, regex=True)
#                 all_columns = list(apiDataFrame)  # Creates list of all column headers
#                 cols = apiDataFrame.iloc[0]
#                 apiDataFrame[all_columns] = apiDataFrame[all_columns].astype(str)
#                 d = 'pk_icm_hh_cluster_21_id', 'fk_form_id', 'fk_user_id', 'date_created', 'fk_activity_id', 'col_surveyor_name', 'partner','col_designation', 'fk_prov_id', 'fk_dist_id', 'fk_tehsil_id', 'is_cbv', 'fk_uc_id',  'g1_Village_name_begin', 'g1_popu_type', 'g1_aic_name', 'g1_team_no', 'g1_dayofwork', 'final_remarks', 'unique_id', 'cluseter_info', 'dev_remarks', 'old_info', 's_rutine_epi', 's_no_vacc_monitor', 's_child_0_11', 's_child_12_59', 's_child_0_11_vac', 's_child_12_59_vac', 's_team_miss_house', 's_child_away', 's_refusal', 's_team_miss_child', 's_guest', 's_guest_vac', 's_total_seen', 's_total_finger_mark', 's_doormarked', 'is_archive', 'update_by', 'archive_reason', 'app_version','date_updated', 'campaign_ID', 'campaign_ActivityName', 'campaign_ActivityID_old', 'campaign_Yr', 'campaign_SubActivityName','geoLocation_name', 'geoLocation_type', 'geoLocation_code', 'geoLocation_census_pop', 'geoLocation_target', 'geoLocation_status', 'geoLocation_pname', 'geoLocation_dname', 'geoLocation_namedistrict', 'geoLocation_codedistrict', 'geoLocation_tname', 'geoLocation_provincecode', 'geoLocation_districtcode', 'geoLocation_tehsilcode', 'geoLocation_priority', 'geoLocation_commnet', 'geoLocation_hr', 'geoLocation_fcm', 'geoLocation_tier', 'geoLocation_block', 'geoLocation_division', 'geoLocation_cordinates', 'geoLocation_latitude', 'geoLocation_longitude', 'geoLocation_x', 'geoLocation_y', 'geoLocation_imagepath', 'geoLocation_isccpv', 'geoLocation_rank', 'geoLocation_rank_score', 'geoLocation_ishealthcamp', 'geoLocation_isdsc', 'geoLocation_ucorg', 'geoLocation_organization', 'geoLocation_tierfromaug161', 'geoLocation_tierfromsep171', 'geoLocation_tierfromdec181', 'geoLocation_mtap', 'geoLocation_rspuc', 'geoLocation_issmt', 'geoLocation_updateddatetime', 'geoLocation_x_code', 'geoLocation_draining_uc', 'geoLocation_upap_districts', 'geoLocation_shruc', 'geoLocation_khidist_id' 
#                 dff = pd.DataFrame(columns=d)
#                 for index, item in enumerate(d):
#                     dff[item] = apiDataFrame[index].values
#                     #dff[item]= dff[item].replace(r'^\s*$', np.nan, regex=True)
#                 df3 = client.query_dataframe(
#                         "SELECT * FROM test.xbi_icm_cluster")
#                 if df3.empty:
#                     dff[["date_created"]] = dff[["date_created"]].apply(pd.to_datetime)
#                     dff[["date_updated"]] = dff[["date_updated"]].apply(pd.to_datetime)
#                     client.insert_dataframe(
#                             'INSERT INTO test.xbi_icm_cluster  VALUES', dff)
#                     logger.info(
#                             'Data has been inserted into Table test.xbi_icm_cluster for campaign '+str(cid))

#                     sql = "DROP table if exists test.get_icm_cluster"
#                     client.execute(sql)
#                     print('\n\n\n CampaignID\t\t--------\t', cid)
#                     print('\n\n\n Campaign Record Page:\t\t--------\t', i)    
#                 else:
#                     dff[["date_created"]] = dff[["date_created"]].apply(pd.to_datetime)
#                     dff[["date_updated"]] = dff[["date_updated"]].apply(pd.to_datetime)
#                     client.insert_dataframe(
#                         'INSERT INTO test.xbi_icm_cluster  VALUES', dff)
#                     logger.info(
#                             ' Data has been inserted into Table test.xbi_icm_cluster for campaign '+str(cid))    
#                     print('\n\n\n CampaignID\t\t--------\t', cid)
#                     print('\n\n\n Campaign Page\t\t--------\t', i)
#                     sql = "DROP table if exists test.get_icm_cluster"
#                     client.execute(sql)
                       
                        
 
# dag = DAG(
#     'Prod_ICM_Cluster_Automated',
#     #schedule_interval='*/10 * * * *',# will run every 10 min.
#     schedule_interval='0 0 * * *',  # will run every mid-night
#     default_args=default_args,
#     catchup=False)

# with dag:
#     GetAndInsertApiDataICMCluster = PythonOperator(
#         task_id='GetAndInsertApiDataICMCluster',
#         python_callable=GetAndInsertApiDataICMCluster,
#     )
# GetAndInsertApiDataICMCluster
