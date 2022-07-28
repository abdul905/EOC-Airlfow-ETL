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
#----------------------------------------------- INSERT CAMPAIGN DATA UC NEAP --------------------------------------------------#
#-------------------------------------------- Author: Abdul Bari Malik ---------------------------------------------------------#
#-------------------------------------------------------------------------------------------------------------------------------#


def GetAndInsertApiDataUCNeap():
    logger.info('Function \' GetAndInsertApiDataUCNeap \' Started Off')
    #cid = 2
    #y = 2022

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
            url = "http://idims.eoc.gov.pk/api_who/api/get_allneap/5468XE2LN6CzR7qRG041/"+ str(cid)+"/"+ str(y)
            logger.info('Requested Data From Api URL: '+url)

            r = requests.get(url)
            data = r.json()
            
            if data['data'] != "No data found":
                logger.info('Received Data  From Api URL: '+url)

                rowsData = data["data"]["data"]
                apiDataFrame = pd.DataFrame(rowsData)
                sql = "CREATE TABLE if not exists test.get_uc_neap (  ID Int32, UserName String, IDcampCat Int32, ActivityID Int32, Yr Int32, TimeStamp Date, DateAssessment Date, TehsilID Int32, UCID Int32, DistID Int32, DivID Int32, ProvID Int32, targeted Int32, DaysBeforeActivity Int32, UCMORounds Int32, inaccCh Int32, areasInacc Int32, mobileTeams Int32, fixedTeams Int32, TransitPoints Int32, HH011_MP Int32, HH1259_MP Int32, School_MP Int32, Fixed_MP Int32, Transit_MP Int32, areaIcs Int32, aicFemale Int32, aicGovt Int32, aicRounds Int32, aicTrained Int32, AICSelfA Int32, stall_method_trg Int32, mov_stall_method_trg Int32, MPDR_Passed Int32, MPDR_Int Int32, MPDR_Failed Int32, MPFV_Passed Int32, MPFV_Int Int32, MPFV_Failed Int32, MPDRD_Passed Int32, MPDRD_Int Int32, MPDRD_Failed Int32, MPFVD_Passed Int32, MPFVD_Int Int32, MPFVD_Failed Int32, zonalSupervisor String, alltm18yr Int32, govtWorker Int32, mtLocalmem Int32, mtFemale Int32, HRMP String, HRMP_status String, NomadsChild Int32, NomadicSett Int32, BriklinsChild Int32, BriklinsSett Int32, SeasonalMigChild Int32, SeasonalMigSett Int32, AgrWorkersChild Int32, AgrWorkersSett Int32, Other Int32, IDPChild Int32, IDPSett Int32, RefugeesChild Int32, RefugeesSett Int32, alltmTrg Int32, ddmCard Int32, UpecHeld Int32, UpecDate DateTime('Asia/Karachi'), ucmo Int32, ucSecratry Int32, shoUpec Int32, scVerified Int32, lastCampDose0 Int32, Dose0Cov Int32, remarks String, mov_mp_uc_staf Int32, mov_aic_train Int32, mov_mp_target Int32, mov_hr Int32, mov_upec Int32, mov_zero_dose Int32, mov_team_train Int32,vacc_carr_avbl Int32, vacc_carr_req Int32, mov_mp_dist_staf Int32, upecSign Int32, status Int32, trash Int32, isSync Int32, location_code  Int64)ENGINE = MergeTree PRIMARY KEY ID ORDER BY ID"
                client.execute(sql)

                df2 = client.query_dataframe(
                    "SELECT * FROM test.get_uc_neap")
                if df2.empty:
                    apiDataFrame = apiDataFrame.replace(r'^\s*$', np.nan, regex=True)
                    apiDataFrame['TimeStamp'] = pd.to_datetime(apiDataFrame['TimeStamp'], errors='coerce')
                    apiDataFrame['DateAssessment'] = pd.to_datetime(apiDataFrame['DateAssessment'], errors='coerce')
                    apiDataFrame['UpecDate'] = pd.to_datetime(apiDataFrame['UpecDate'], errors='coerce')    
                    apiDataFrame['alltmTrg'] = pd.to_numeric(apiDataFrame['alltmTrg'], errors='coerce').fillna(0)
                    apiDataFrame['alltm18yr'] = pd.to_numeric(apiDataFrame['alltm18yr'], errors='coerce').fillna(0)
                    apiDataFrame['location_code'] = pd.to_numeric(apiDataFrame['location_code'], errors='coerce').fillna(0)
                    apiDataFrame['mtLocalmem'] = pd.to_numeric(apiDataFrame['mtLocalmem'], errors='coerce').fillna(0)
                    apiDataFrame['govtWorker'] = pd.to_numeric(apiDataFrame['govtWorker'], errors='coerce').fillna(0)
                    apiDataFrame['mtFemale'] = pd.to_numeric(apiDataFrame['mtFemale'], errors='coerce').fillna(0)
                    apiDataFrame['aicTrained'] = pd.to_numeric(apiDataFrame['aicTrained'], errors='coerce').fillna(0)
                    apiDataFrame['areaIcs'] = pd.to_numeric(apiDataFrame['areaIcs'], errors='coerce').fillna(0)
                    apiDataFrame['TransitPoints'] = pd.to_numeric(apiDataFrame['TransitPoints'], errors='coerce').fillna(0)
                    apiDataFrame['fixedTeams'] = pd.to_numeric(apiDataFrame['fixedTeams'], errors='coerce').fillna(0)
                    apiDataFrame['UCMORounds'] = pd.to_numeric(apiDataFrame['UCMORounds'], errors='coerce').fillna(0)
                    apiDataFrame['ucmo'] = pd.to_numeric(apiDataFrame['ucmo'], errors='coerce').fillna(0)
                    apiDataFrame['ucSecratry'] = pd.to_numeric(apiDataFrame['ucSecratry'], errors='coerce').fillna(0)
                    print('cid --------------------------->\t', cid)
                    print('year--------------------------->\t', y)

                    client.insert_dataframe(
                    'INSERT INTO test.get_uc_neap VALUES', apiDataFrame)
                    logger.info(' Function  \' CreateJoinTableOfNeap \' has been Initiated')
                else:
                    apiDataFrame = apiDataFrame.replace(r'^\s*$', np.nan, regex=True)
                    apiDataFrame['TimeStamp'] = pd.to_datetime(apiDataFrame['TimeStamp'], errors='coerce')
                    apiDataFrame['DateAssessment'] = pd.to_datetime(apiDataFrame['DateAssessment'], errors='coerce')
                    apiDataFrame['UpecDate'] = pd.to_datetime(apiDataFrame['UpecDate'], errors='coerce')    
                    apiDataFrame['alltmTrg'] = pd.to_numeric(apiDataFrame['alltmTrg'], errors='coerce').fillna(0)
                    apiDataFrame['alltm18yr'] = pd.to_numeric(apiDataFrame['alltm18yr'], errors='coerce').fillna(0)
                    apiDataFrame['location_code'] = pd.to_numeric(apiDataFrame['location_code'], errors='coerce').fillna(0)
                    apiDataFrame['mtLocalmem'] = pd.to_numeric(apiDataFrame['mtLocalmem'], errors='coerce').fillna(0)
                    apiDataFrame['govtWorker'] = pd.to_numeric(apiDataFrame['govtWorker'], errors='coerce').fillna(0)
                    apiDataFrame['mtFemale'] = pd.to_numeric(apiDataFrame['mtFemale'], errors='coerce').fillna(0)
                    apiDataFrame['aicTrained'] = pd.to_numeric(apiDataFrame['aicTrained'], errors='coerce').fillna(0)
                    apiDataFrame['areaIcs'] = pd.to_numeric(apiDataFrame['areaIcs'], errors='coerce').fillna(0)
                    apiDataFrame['TransitPoints'] = pd.to_numeric(apiDataFrame['TransitPoints'], errors='coerce').fillna(0)
                    apiDataFrame['fixedTeams'] = pd.to_numeric(apiDataFrame['fixedTeams'], errors='coerce').fillna(0)
                    apiDataFrame['UCMORounds'] = pd.to_numeric(apiDataFrame['UCMORounds'], errors='coerce').fillna(0)
                    apiDataFrame['ucmo'] = pd.to_numeric(apiDataFrame['ucmo'], errors='coerce').fillna(0)
                    apiDataFrame['ucSecratry'] = pd.to_numeric(apiDataFrame['ucSecratry'], errors='coerce').fillna(0)
                    print('cid --------------------------->\t', cid)
                    print('year--------------------------->\t', y)


                
def CreateJoinTableOfUCNEAP(): 
    logger.info('Function \' CreateJoinTableOfUCNEAP \' Started Off')

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
            sql = "CREATE TABLE if not exists test.xbi_neapUcPlan ( ID Int32, UserName String, IDcampCat Int32, ActivityID Int32, Yr Int32, TimeStamp Date, DateAssessment Date, TehsilID Int32, UCID Int32, DistID Int32, DivID Int32, ProvID Int32, targeted Int32, DaysBeforeActivity Int32, UCMORounds Int32, inaccCh Int32, areasInacc Int32, mobileTeams Int32, fixedTeams Int32, TransitPoints Int32, HH011_MP Int32, HH1259_MP Int32, School_MP Int32, Fixed_MP Int32, Transit_MP Int32, areaIcs Int32, aicFemale Int32, aicGovt Int32, aicRounds Int32, aicTrained Int32, AICSelfA Int32, stall_method_trg Int32, mov_stall_method_trg Int32, MPDR_Passed Int32, MPDR_Int Int32, MPDR_Failed Int32, MPFV_Passed Int32, MPFV_Int Int32, MPFV_Failed Int32, MPDRD_Passed Int32, MPDRD_Int Int32, MPDRD_Failed Int32, MPFVD_Passed Int32, MPFVD_Int Int32, MPFVD_Failed Int32, zonalSupervisor String, alltm18yr Int32, govtWorker Int32, mtLocalmem Int32, mtFemale Int32, HRMP String, HRMP_status String, NomadsChild Int32, NomadicSett Int32, BriklinsChild Int32, BriklinsSett Int32, SeasonalMigChild Int32, SeasonalMigSett Int32, AgrWorkersChild Int32, AgrWorkersSett Int32, Other Int32, IDPChild Int32, IDPSett Int32, RefugeesChild Int32, RefugeesSett Int32, alltmTrg Int32, ddmCard Int32, UpecHeld Int32, UpecDate DateTime('Asia/Karachi'), ucmo Int32, ucSecratry Int32, shoUpec Int32, scVerified Int32, lastCampDose0 Int32, Dose0Cov Int32, remarks String, mov_mp_uc_staf Int32, mov_aic_train Int32, mov_mp_target Int32, mov_hr Int32, mov_upec Int32, mov_zero_dose Int32, mov_team_train Int32,vacc_carr_avbl Int32, vacc_carr_req Int32, mov_mp_dist_staf Int32, upecSign Int32, status Int32, trash Int32, isSync Int32, location_code  Int64, campaign_ID Int32, campaign_ActivityName String, campaign_ActivityID_old Int32, campaign_Yr Int32, campaign_SubActivityName String, geoLocation_name String, geoLocation_type Int32, geoLocation_code Int32, geoLocation_census_pop Int32, geoLocation_target Int32, geoLocation_status Int32, geoLocation_pname String, geoLocation_dname String, geoLocation_namedistrict String, geoLocation_codedistrict String, geoLocation_tname String, geoLocation_provincecode Int32, geoLocation_districtcode Int32, geoLocation_tehsilcode Int32, geoLocation_priority Int32, geoLocation_commnet Int32, geoLocation_hr Int32, geoLocation_fcm Int32, geoLocation_tier Int32, geoLocation_block String, geoLocation_division String, geoLocation_cordinates String, geoLocation_latitude String, geoLocation_longitude String, geoLocation_x String, geoLocation_y String, geoLocation_imagepath String, geoLocation_isccpv Int32, geoLocation_rank Int32, geoLocation_rank_score String, geoLocation_ishealthcamp Int32, geoLocation_isdsc Int32, geoLocation_ucorg String, geoLocation_organization String, geoLocation_tierfromaug161 Int32, geoLocation_tierfromsep171 Int32, geoLocation_tierfromdec181 Int32, geoLocation_mtap Int32, geoLocation_rspuc Int32, geoLocation_issmt Int32, geoLocation_updateddatetime String, geoLocation_x_code Int32, geoLocation_draining_uc Int32, geoLocation_upap_districts Int32, geoLocation_shruc Int32, geoLocation_khidist_id String  )ENGINE = MergeTree PRIMARY KEY ID ORDER BY ID"
            client.execute(sql)
            cols = "test.get_uc_neap.ID, test.get_uc_neap.UserName, test.get_uc_neap.IDcampCat, test.get_uc_neap.ActivityID, test.get_uc_neap.Yr, test.get_uc_neap.TimeStamp, test.get_uc_neap.DateAssessment, test.get_uc_neap.TehsilID, test.get_uc_neap.UCID, test.get_uc_neap.DistID, test.get_uc_neap.DivID, test.get_uc_neap.ProvID, test.get_uc_neap.targeted, test.get_uc_neap.DaysBeforeActivity, test.get_uc_neap.UCMORounds, test.get_uc_neap.inaccCh, test.get_uc_neap.areasInacc, test.get_uc_neap.mobileTeams, test.get_uc_neap.fixedTeams, test.get_uc_neap.TransitPoints, test.get_uc_neap.HH011_MP, test.get_uc_neap.HH1259_MP, test.get_uc_neap.School_MP, test.get_uc_neap.Fixed_MP, test.get_uc_neap.Transit_MP, test.get_uc_neap.areaIcs, test.get_uc_neap.aicFemale, test.get_uc_neap.aicGovt, test.get_uc_neap.aicRounds, test.get_uc_neap.aicTrained, test.get_uc_neap.AICSelfA, test.get_uc_neap.stall_method_trg, test.get_uc_neap.mov_stall_method_trg, test.get_uc_neap.MPDR_Passed, test.get_uc_neap.MPDR_Int, test.get_uc_neap.MPDR_Failed, test.get_uc_neap.MPFV_Passed, test.get_uc_neap.MPFV_Int, test.get_uc_neap.MPFV_Failed, test.get_uc_neap.MPDRD_Passed, test.get_uc_neap.MPDRD_Int, test.get_uc_neap.MPDRD_Failed, test.get_uc_neap.MPFVD_Passed, test.get_uc_neap.MPFVD_Int, test.get_uc_neap.MPFVD_Failed, test.get_uc_neap.zonalSupervisor, test.get_uc_neap.alltm18yr, test.get_uc_neap.govtWorker, test.get_uc_neap.mtLocalmem, test.get_uc_neap.mtFemale, test.get_uc_neap.HRMP, test.get_uc_neap.HRMP_status, test.get_uc_neap.NomadsChild, test.get_uc_neap.NomadicSett, test.get_uc_neap.BriklinsChild, test.get_uc_neap.BriklinsSett, test.get_uc_neap.SeasonalMigChild, test.get_uc_neap.SeasonalMigSett, test.get_uc_neap.AgrWorkersChild, test.get_uc_neap.AgrWorkersSett, test.get_uc_neap.Other, test.get_uc_neap.IDPChild, test.get_uc_neap.IDPSett, test.get_uc_neap.RefugeesChild, test.get_uc_neap.RefugeesSett, test.get_uc_neap.alltmTrg, test.get_uc_neap.ddmCard, test.get_uc_neap.UpecHeld, test.get_uc_neap.UpecDate, test.get_uc_neap.ucmo, test.get_uc_neap.ucSecratry, test.get_uc_neap.shoUpec, test.get_uc_neap.scVerified, test.get_uc_neap.lastCampDose0, test.get_uc_neap.Dose0Cov, test.get_uc_neap.remarks, test.get_uc_neap.mov_mp_uc_staf, test.get_uc_neap.mov_aic_train, test.get_uc_neap.mov_mp_target, test.get_uc_neap.mov_hr, test.get_uc_neap.mov_upec, test.get_uc_neap.mov_zero_dose, test.get_uc_neap.mov_team_train, test.get_uc_neap.vacc_carr_avbl, test.get_uc_neap.vacc_carr_req, test.get_uc_neap.mov_mp_dist_staf, test.get_uc_neap.upecSign, test.get_uc_neap.status, test.get_uc_neap.trash, test.get_uc_neap.isSync, test.get_uc_neap.location_code , test.xbi_campaign.campaign_ID, test.xbi_campaign.campaign_ActivityName, test.xbi_campaign.campaign_ActivityID_old, test.xbi_campaign.campaign_Yr, test.xbi_campaign.campaign_SubActivityName,  test.eoc_geolocation_tbl.name, test.eoc_geolocation_tbl.type, test.eoc_geolocation_tbl.code, test.eoc_geolocation_tbl.census_pop, test.eoc_geolocation_tbl.target, test.eoc_geolocation_tbl.status, test.eoc_geolocation_tbl.pname, test.eoc_geolocation_tbl.dname, test.eoc_geolocation_tbl.namedistrict, test.eoc_geolocation_tbl.codedistrict, test.eoc_geolocation_tbl.tname, test.eoc_geolocation_tbl.provincecode, test.eoc_geolocation_tbl.districtcode, test.eoc_geolocation_tbl.tehsilcode, test.eoc_geolocation_tbl.priority, test.eoc_geolocation_tbl.commnet, test.eoc_geolocation_tbl.hr, test.eoc_geolocation_tbl.fcm, test.eoc_geolocation_tbl.tier, test.eoc_geolocation_tbl.block, test.eoc_geolocation_tbl.division, test.eoc_geolocation_tbl.cordinates, test.eoc_geolocation_tbl.latitude, test.eoc_geolocation_tbl.longitude, test.eoc_geolocation_tbl.x, test.eoc_geolocation_tbl.y, test.eoc_geolocation_tbl.imagepath, test.eoc_geolocation_tbl.isccpv, test.eoc_geolocation_tbl.rank, test.eoc_geolocation_tbl.rank_score, test.eoc_geolocation_tbl.ishealthcamp, test.eoc_geolocation_tbl.isdsc, test.eoc_geolocation_tbl.ucorg, test.eoc_geolocation_tbl.organization, test.eoc_geolocation_tbl.tierfromaug161, test.eoc_geolocation_tbl.tierfromsep171, test.eoc_geolocation_tbl.tierfromdec181, test.eoc_geolocation_tbl.mtap, test.eoc_geolocation_tbl.rspuc, test.eoc_geolocation_tbl.issmt, test.eoc_geolocation_tbl.updateddatetime, test.eoc_geolocation_tbl.x_code, test.eoc_geolocation_tbl.draining_uc, test.eoc_geolocation_tbl.upap_districts, test.eoc_geolocation_tbl.shruc, test.eoc_geolocation_tbl.khidist_id "
            sql = "SELECT " + cols + "  FROM test.get_uc_neap tsm left JOIN test.eoc_geolocation_tbl eoc_2 ON tsm.location_code  = eoc_2.code left JOIN test.xbi_campaign eoc_3 ON (tsm.ActivityID  = eoc_3.campaign_ActivityID_old And tsm.Yr = eoc_3.campaign_Yr) WHERE tsm.ActivityID ="+str(f)+" AND tsm.Yr="+str(b)

            data = client.execute(sql)
            apiDataFrame = pd.DataFrame(data)

            if apiDataFrame.empty:
                print("No Data found for campaign: "+str(f))
                print("API DF Size: ",len(apiDataFrame))
            else:
                all_columns = list(apiDataFrame)  # Creates list of all column headers
                cols = apiDataFrame.iloc[0]
                apiDataFrame[all_columns] = apiDataFrame[all_columns].astype(str)
                d =  'ID', 'UserName', 'IDcampCat', 'ActivityID', 'Yr', 'TimeStamp', 'DateAssessment', 'TehsilID', 'UCID', 'DistID', 'DivID', 'ProvID', 'targeted', 'DaysBeforeActivity', 'UCMORounds', 'inaccCh', 'areasInacc', 'mobileTeams', 'fixedTeams', 'TransitPoints', 'HH011_MP', 'HH1259_MP', 'School_MP', 'Fixed_MP', 'Transit_MP', 'areaIcs', 'aicFemale', 'aicGovt', 'aicRounds', 'aicTrained', 'AICSelfA', 'stall_method_trg', 'mov_stall_method_trg', 'MPDR_Passed', 'MPDR_Int', 'MPDR_Failed', 'MPFV_Passed', 'MPFV_Int', 'MPFV_Failed', 'MPDRD_Passed', 'MPDRD_Int', 'MPDRD_Failed', 'MPFVD_Passed', 'MPFVD_Int', 'MPFVD_Failed', 'zonalSupervisor', 'alltm18yr', 'govtWorker', 'mtLocalmem', 'mtFemale', 'HRMP', 'HRMP_status', 'NomadsChild', 'NomadicSett', 'BriklinsChild', 'BriklinsSett', 'SeasonalMigChild', 'SeasonalMigSett', 'AgrWorkersChild', 'AgrWorkersSett', 'Other', 'IDPChild', 'IDPSett', 'RefugeesChild', 'RefugeesSett', 'alltmTrg', 'ddmCard', 'UpecHeld', 'UpecDate', 'ucmo', 'ucSecratry', 'shoUpec', 'scVerified', 'lastCampDose0', 'Dose0Cov', 'remarks', 'mov_mp_uc_staf', 'mov_aic_train', 'mov_mp_target', 'mov_hr', 'mov_upec', 'mov_zero_dose', 'mov_team_train', 'vacc_carr_avbl', 'vacc_carr_req', 'mov_mp_dist_staf', 'upecSign', 'status', 'trash', 'isSync', 'location_code', 'campaign_ID', 'campaign_ActivityName', 'campaign_ActivityID_old', 'campaign_Yr', 'campaign_SubActivityName', 'geoLocation_name', 'geoLocation_type', 'geoLocation_code', 'geoLocation_census_pop', 'geoLocation_target', 'geoLocation_status', 'geoLocation_pname', 'geoLocation_dname', 'geoLocation_namedistrict', 'geoLocation_codedistrict', 'geoLocation_tname', 'geoLocation_provincecode', 'geoLocation_districtcode', 'geoLocation_tehsilcode', 'geoLocation_priority', 'geoLocation_commnet', 'geoLocation_hr', 'geoLocation_fcm', 'geoLocation_tier', 'geoLocation_block', 'geoLocation_division', 'geoLocation_cordinates', 'geoLocation_latitude', 'geoLocation_longitude', 'geoLocation_x', 'geoLocation_y', 'geoLocation_imagepath', 'geoLocation_isccpv', 'geoLocation_rank', 'geoLocation_rank_score', 'geoLocation_ishealthcamp', 'geoLocation_isdsc', 'geoLocation_ucorg', 'geoLocation_organization', 'geoLocation_tierfromaug161', 'geoLocation_tierfromsep171', 'geoLocation_tierfromdec181', 'geoLocation_mtap', 'geoLocation_rspuc', 'geoLocation_issmt', 'geoLocation_updateddatetime', 'geoLocation_x_code', 'geoLocation_draining_uc', 'geoLocation_upap_districts', 'geoLocation_shruc', 'geoLocation_khidist_id' 
                
                dff = pd.DataFrame(columns=d)
                for index, item in enumerate(d):
                    dff[item] = apiDataFrame[index].values
                
                data_q = "SELECT * FROM test.xbi_neapUcPlan WHERE Yr = "+str(b)+" AND ActivityID = "+str(f)
                df2 = client.query_dataframe(data_q)  
                #df2 = client.query_dataframe("SELECT * FROM  test.xbi_neapUcPlan WHERE Yr = 2022 and ActivityID = 2")
                if df2.empty:
                    dff[["TimeStamp"]] = dff[["TimeStamp"]].apply(pd.to_datetime)
                    dff[["DateAssessment"]] = dff[["DateAssessment"]].apply(pd.to_datetime)
                    dff[["UpecDate"]] = dff[["UpecDate"]].apply(pd.to_datetime)        
                    dff[["alltm18yr"]] = dff[["alltm18yr"]].apply(pd.to_numeric )
                    dff[["alltmTrg"]] = dff[["alltmTrg"]].apply(pd.to_numeric )
                    dff[["location_code"]] = dff[["location_code"]].apply(pd.to_numeric )
                    dff[["mtLocalmem"]] = dff[["mtLocalmem"]].apply(pd.to_numeric )
                    dff[["govtWorker"]] = dff[["govtWorker"]].apply(pd.to_numeric )
                    dff[["mtFemale"]] = dff[["mtFemale"]].apply(pd.to_numeric )
                    dff[["aicTrained"]] = dff[["aicTrained"]].apply(pd.to_numeric )
                    dff[["areaIcs"]] = dff[["areaIcs"]].apply(pd.to_numeric )
                    dff[["TransitPoints"]] = dff[["TransitPoints"]].apply(pd.to_numeric )
                    dff[["fixedTeams"]] = dff[["fixedTeams"]].apply(pd.to_numeric )
                    dff[["UCMORounds"]] = dff[["UCMORounds"]].apply(pd.to_numeric )
                    dff[["ucmo"]] = dff[["ucmo"]].apply(pd.to_numeric )
                    dff[["ucSecratry"]] = dff[["ucSecratry"]].apply(pd.to_numeric )

                    client.insert_dataframe(
                        'INSERT INTO  test.xbi_neapUcPlan  VALUES', dff)
                    logger.info(
                        ' Data has been inserted into Table\' INSERT INTO  test.xbi_neapUcPlan  VALUES \' ')
                    print('\n\n\n Data Row Count\t\t--------\t', dff.shape[0])    
                    # sql = "DROP table if exists  test.get_uc_neap"
                    # client.execute(sql)

                else:
                    sql = "ALTER TABLE test.xbi_neapUcPlan DELETE WHERE Yr = "+ str(b) + " AND ActivityID = "+str(f)
                    client.execute(sql)
                    dff[["TimeStamp"]] = dff[["TimeStamp"]].apply(pd.to_datetime)
                    dff[["DateAssessment"]] = dff[["DateAssessment"]].apply(pd.to_datetime)
                    dff[["UpecDate"]] = dff[["UpecDate"]].apply(pd.to_datetime)
                    dff[["alltmTrg"]] = dff[["alltmTrg"]].apply(pd.to_numeric )
                    dff[["alltm18yr"]] = dff[["alltm18yr"]].apply(pd.to_numeric )
                    dff[["location_code"]] = dff[["location_code"]].apply(pd.to_numeric )
                    dff[["mtLocalmem"]] = dff[["mtLocalmem"]].apply(pd.to_numeric )
                    dff[["govtWorker"]] = dff[["govtWorker"]].apply(pd.to_numeric )
                    dff[["mtFemale"]] = dff[["mtFemale"]].apply(pd.to_numeric )
                    dff[["aicTrained"]] = dff[["aicTrained"]].apply(pd.to_numeric )
                    dff[["areaIcs"]] = dff[["areaIcs"]].apply(pd.to_numeric )
                    dff[["TransitPoints"]] = dff[["TransitPoints"]].apply(pd.to_numeric )
                    dff[["fixedTeams"]] = dff[["fixedTeams"]].apply(pd.to_numeric )
                    dff[["UCMORounds"]] = dff[["UCMORounds"]].apply(pd.to_numeric )
                    dff[["ucmo"]] = dff[["ucmo"]].apply(pd.to_numeric )
                    dff[["ucSecratry"]] = dff[["ucSecratry"]].apply(pd.to_numeric )
                    
                    client.insert_dataframe(
                        'INSERT INTO  test.xbi_neapUcPlan  VALUES', dff)
                    print('\n\n\n Data Row Count\t\t--------\t', dff.shape[0])  
        sql = "DROP table if exists  test.get_uc_neap"
        client.execute(sql)


dag = DAG(
    'Prod_Neap_UC_Automated',
    schedule_interval='0 0 * * *',  # will run every mid-night.
    default_args=default_args,
    catchup=False)

with dag:
    GetAndInsertApiDataUCNeap = PythonOperator(
        task_id='GetAndInsertApiDataUCNeap',
        python_callable=GetAndInsertApiDataUCNeap,
    )
    CreateJoinTableOfUCNEAP = PythonOperator(
        task_id='CreateJoinTableOfUCNEAP',
        python_callable=CreateJoinTableOfUCNEAP,
    )
GetAndInsertApiDataUCNeap >> CreateJoinTableOfUCNEAP

#-------------------------------------------------------------------------------------------------------------------------------#
#----------------------------------------------- INSERT CAMPAIGN DATA UC NEAP   ------------------------------------------------#
#-------------------------------------------- Author: Abdul Bari Malik ---------------------------------------------------------#
#-------------------------------------------------------------------------------------------------------------------------------#

# def GetAndInsertApiDataUCNeap():
#     li1 = [28, 29, 31, 30, 1, 106, 34, 9, 37, 40,  43, 10, 2, 46, 11, 3, 107, 1, 106, 2, 9, 8, 10, 3, 107, 28, 31, 4, 9, 1, 28, 31, 2, 257, 258, 10]
#     li2 = [2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2022, 2022, 2022, 2022, 2022, 2022, 2022, 2022]
    
#     logger.info('Function \' GetAndInsertApiDataUCNeap \' Started Off')

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
    
#         for f, b in zip(li1, li2):
#             url = "http://idims.eoc.gov.pk/api_who/api/get_allneap/5468XE2LN6CzR7qRG041/"+ str(f)+"/"+ str(b)
#             logger.info('Requested Data From Api URL: '+url)

#             r = requests.get(url)
#             data = r.json()
#             if data['data'] != "No data found":
#                 logger.info('Received Data  From Api URL: '+url)

#                 rowsData = data["data"]["data"]
#                 apiDataFrame = pd.DataFrame(rowsData)
#                 sql = "CREATE TABLE if not exists test.get_uc_neap (  ID Int32, UserName String, IDcampCat Int32, ActivityID Int32, Yr Int32, TimeStamp Date, DateAssessment Date, TehsilID Int32, UCID Int32, DistID Int32, DivID Int32, ProvID Int32, targeted Int32, DaysBeforeActivity Int32, UCMORounds Int32, inaccCh Int32, areasInacc Int32, mobileTeams Int32, fixedTeams Int32, TransitPoints Int32, HH011_MP Int32, HH1259_MP Int32, School_MP Int32, Fixed_MP Int32, Transit_MP Int32, areaIcs Int32, aicFemale Int32, aicGovt Int32, aicRounds Int32, aicTrained Int32, AICSelfA Int32, stall_method_trg Int32, mov_stall_method_trg Int32, MPDR_Passed Int32, MPDR_Int Int32, MPDR_Failed Int32, MPFV_Passed Int32, MPFV_Int Int32, MPFV_Failed Int32, MPDRD_Passed Int32, MPDRD_Int Int32, MPDRD_Failed Int32, MPFVD_Passed Int32, MPFVD_Int Int32, MPFVD_Failed Int32, zonalSupervisor String, alltm18yr Int32, govtWorker Int32, mtLocalmem Int32, mtFemale Int32, HRMP String, HRMP_status String, NomadsChild Int32, NomadicSett Int32, BriklinsChild Int32, BriklinsSett Int32, SeasonalMigChild Int32, SeasonalMigSett Int32, AgrWorkersChild Int32, AgrWorkersSett Int32, Other Int32, IDPChild Int32, IDPSett Int32, RefugeesChild Int32, RefugeesSett Int32, alltmTrg Int32, ddmCard Int32, UpecHeld Int32, UpecDate DateTime('Asia/Karachi'), ucmo Int32, ucSecratry Int32, shoUpec Int32, scVerified Int32, lastCampDose0 Int32, Dose0Cov Int32, remarks String, mov_mp_uc_staf Int32, mov_aic_train Int32, mov_mp_target Int32, mov_hr Int32, mov_upec Int32, mov_zero_dose Int32, mov_team_train Int32, vacc_carr_avbl Int32, vacc_carr_req Int32, mov_mp_dist_staf Int32, upecSign Int32, status Int32, trash Int32, isSync Int32, location_code  Int64)ENGINE = MergeTree PRIMARY KEY ID ORDER BY ID"
#                 client.execute(sql)

#                 df2 = client.query_dataframe(
#                     "SELECT * FROM test.get_uc_neap")
#                 if df2.empty:
#                     apiDataFrame = apiDataFrame.replace(r'^\s*$', np.nan, regex=True)
#                     apiDataFrame['TimeStamp'] = pd.to_datetime(apiDataFrame['TimeStamp'], errors='coerce')
#                     apiDataFrame['DateAssessment'] = pd.to_datetime(apiDataFrame['DateAssessment'], errors='coerce')
#                     apiDataFrame['UpecDate'] = pd.to_datetime(apiDataFrame['UpecDate'], errors='coerce')
#                     apiDataFrame['alltmTrg'] = pd.to_numeric(apiDataFrame['alltmTrg'], errors='coerce').fillna(0)
#                     apiDataFrame['alltm18yr'] = pd.to_numeric(apiDataFrame['alltm18yr'], errors='coerce').fillna(0)
#                     apiDataFrame['location_code'] = pd.to_numeric(apiDataFrame['location_code'], errors='coerce').fillna(0)
#                     apiDataFrame['mtLocalmem'] = pd.to_numeric(apiDataFrame['mtLocalmem'], errors='coerce').fillna(0)
#                     apiDataFrame['govtWorker'] = pd.to_numeric(apiDataFrame['govtWorker'], errors='coerce').fillna(0)
#                     apiDataFrame['mtFemale'] = pd.to_numeric(apiDataFrame['mtFemale'], errors='coerce').fillna(0)
#                     apiDataFrame['aicTrained'] = pd.to_numeric(apiDataFrame['aicTrained'], errors='coerce').fillna(0)
#                     apiDataFrame['areaIcs'] = pd.to_numeric(apiDataFrame['areaIcs'], errors='coerce').fillna(0)
#                     apiDataFrame['TransitPoints'] = pd.to_numeric(apiDataFrame['TransitPoints'], errors='coerce').fillna(0)
#                     apiDataFrame['fixedTeams'] = pd.to_numeric(apiDataFrame['fixedTeams'], errors='coerce').fillna(0)
#                     apiDataFrame['UCMORounds'] = pd.to_numeric(apiDataFrame['UCMORounds'], errors='coerce').fillna(0)
#                     apiDataFrame['ucmo'] = pd.to_numeric(apiDataFrame['ucmo'], errors='coerce').fillna(0)
#                     apiDataFrame['ucSecratry'] = pd.to_numeric(apiDataFrame['ucSecratry'], errors='coerce').fillna(0)
#                     print('cid --------------------------->\t', f)
#                     print('year--------------------------->\t', b)

#                     client.insert_dataframe(
#                         'INSERT INTO test.get_uc_neap VALUES', apiDataFrame)

#                 sql = "CREATE TABLE if not exists test.xbi_neapUcPlan ( ID Int32, UserName String, IDcampCat Int32, ActivityID Int32, Yr Int32, TimeStamp Date, DateAssessment Date, TehsilID Int32, UCID Int32, DistID Int32, DivID Int32, ProvID Int32, targeted Int32, DaysBeforeActivity Int32, UCMORounds Int32, inaccCh Int32, areasInacc Int32, mobileTeams Int32, fixedTeams Int32, TransitPoints Int32, HH011_MP Int32, HH1259_MP Int32, School_MP Int32, Fixed_MP Int32, Transit_MP Int32, areaIcs Int32, aicFemale Int32, aicGovt Int32, aicRounds Int32, aicTrained Int32, AICSelfA Int32, stall_method_trg Int32, mov_stall_method_trg Int32, MPDR_Passed Int32, MPDR_Int Int32, MPDR_Failed Int32, MPFV_Passed Int32, MPFV_Int Int32, MPFV_Failed Int32, MPDRD_Passed Int32, MPDRD_Int Int32, MPDRD_Failed Int32, MPFVD_Passed Int32, MPFVD_Int Int32, MPFVD_Failed Int32, zonalSupervisor String, alltm18yr Int32, govtWorker Int32, mtLocalmem Int32, mtFemale Int32, HRMP String, HRMP_status String, NomadsChild Int32, NomadicSett Int32, BriklinsChild Int32, BriklinsSett Int32, SeasonalMigChild Int32, SeasonalMigSett Int32, AgrWorkersChild Int32, AgrWorkersSett Int32, Other Int32, IDPChild Int32, IDPSett Int32, RefugeesChild Int32, RefugeesSett Int32, alltmTrg Int32, ddmCard Int32, UpecHeld Int32, UpecDate DateTime('Asia/Karachi'), ucmo Int32, ucSecratry Int32, shoUpec Int32, scVerified Int32, lastCampDose0 Int32, Dose0Cov Int32, remarks String, mov_mp_uc_staf Int32, mov_aic_train Int32, mov_mp_target Int32, mov_hr Int32, mov_upec Int32, mov_zero_dose Int32, mov_team_train Int32, vacc_carr_avbl Int32, vacc_carr_req Int32, mov_mp_dist_staf Int32, upecSign Int32, status Int32, trash Int32, isSync Int32, location_code  Int64, campaign_ID Int32, campaign_ActivityName String, campaign_ActivityID_old Int32, campaign_Yr Int32, campaign_SubActivityName String, geoLocation_name String, geoLocation_type Int32, geoLocation_code Int32, geoLocation_census_pop Int32, geoLocation_target Int32, geoLocation_status Int32, geoLocation_pname String, geoLocation_dname String, geoLocation_namedistrict String, geoLocation_codedistrict String, geoLocation_tname String, geoLocation_provincecode Int32, geoLocation_districtcode Int32, geoLocation_tehsilcode Int32, geoLocation_priority Int32, geoLocation_commnet Int32, geoLocation_hr Int32, geoLocation_fcm Int32, geoLocation_tier Int32, geoLocation_block String, geoLocation_division String, geoLocation_cordinates String, geoLocation_latitude String, geoLocation_longitude String, geoLocation_x String, geoLocation_y String, geoLocation_imagepath String, geoLocation_isccpv Int32, geoLocation_rank Int32, geoLocation_rank_score String, geoLocation_ishealthcamp Int32, geoLocation_isdsc Int32, geoLocation_ucorg String, geoLocation_organization String, geoLocation_tierfromaug161 Int32, geoLocation_tierfromsep171 Int32, geoLocation_tierfromdec181 Int32, geoLocation_mtap Int32, geoLocation_rspuc Int32, geoLocation_issmt Int32, geoLocation_updateddatetime String, geoLocation_x_code Int32, geoLocation_draining_uc Int32, geoLocation_upap_districts Int32, geoLocation_shruc Int32, geoLocation_khidist_id String  )ENGINE = MergeTree PRIMARY KEY ID ORDER BY ID"
#                 client.execute(sql)
#                 cols = "test.get_uc_neap.ID, test.get_uc_neap.UserName, test.get_uc_neap.IDcampCat, test.get_uc_neap.ActivityID, test.get_uc_neap.Yr, test.get_uc_neap.TimeStamp, test.get_uc_neap.DateAssessment, test.get_uc_neap.TehsilID, test.get_uc_neap.UCID, test.get_uc_neap.DistID, test.get_uc_neap.DivID, test.get_uc_neap.ProvID, test.get_uc_neap.targeted, test.get_uc_neap.DaysBeforeActivity, test.get_uc_neap.UCMORounds, test.get_uc_neap.inaccCh, test.get_uc_neap.areasInacc, test.get_uc_neap.mobileTeams, test.get_uc_neap.fixedTeams, test.get_uc_neap.TransitPoints, test.get_uc_neap.HH011_MP, test.get_uc_neap.HH1259_MP, test.get_uc_neap.School_MP, test.get_uc_neap.Fixed_MP, test.get_uc_neap.Transit_MP, test.get_uc_neap.areaIcs, test.get_uc_neap.aicFemale, test.get_uc_neap.aicGovt, test.get_uc_neap.aicRounds, test.get_uc_neap.aicTrained, test.get_uc_neap.AICSelfA, test.get_uc_neap.stall_method_trg, test.get_uc_neap.mov_stall_method_trg, test.get_uc_neap.MPDR_Passed, test.get_uc_neap.MPDR_Int, test.get_uc_neap.MPDR_Failed, test.get_uc_neap.MPFV_Passed, test.get_uc_neap.MPFV_Int, test.get_uc_neap.MPFV_Failed, test.get_uc_neap.MPDRD_Passed, test.get_uc_neap.MPDRD_Int, test.get_uc_neap.MPDRD_Failed, test.get_uc_neap.MPFVD_Passed, test.get_uc_neap.MPFVD_Int, test.get_uc_neap.MPFVD_Failed, test.get_uc_neap.zonalSupervisor, test.get_uc_neap.alltm18yr, test.get_uc_neap.govtWorker, test.get_uc_neap.mtLocalmem, test.get_uc_neap.mtFemale, test.get_uc_neap.HRMP, test.get_uc_neap.HRMP_status, test.get_uc_neap.NomadsChild, test.get_uc_neap.NomadicSett, test.get_uc_neap.BriklinsChild, test.get_uc_neap.BriklinsSett, test.get_uc_neap.SeasonalMigChild, test.get_uc_neap.SeasonalMigSett, test.get_uc_neap.AgrWorkersChild, test.get_uc_neap.AgrWorkersSett, test.get_uc_neap.Other, test.get_uc_neap.IDPChild, test.get_uc_neap.IDPSett, test.get_uc_neap.RefugeesChild, test.get_uc_neap.RefugeesSett, test.get_uc_neap.alltmTrg, test.get_uc_neap.ddmCard, test.get_uc_neap.UpecHeld, test.get_uc_neap.UpecDate, test.get_uc_neap.ucmo, test.get_uc_neap.ucSecratry, test.get_uc_neap.shoUpec, test.get_uc_neap.scVerified, test.get_uc_neap.lastCampDose0, test.get_uc_neap.Dose0Cov, test.get_uc_neap.remarks, test.get_uc_neap.mov_mp_uc_staf, test.get_uc_neap.mov_aic_train, test.get_uc_neap.mov_mp_target, test.get_uc_neap.mov_hr, test.get_uc_neap.mov_upec, test.get_uc_neap.mov_zero_dose, test.get_uc_neap.mov_team_train, test.get_uc_neap.vacc_carr_avbl, test.get_uc_neap.vacc_carr_req, test.get_uc_neap.mov_mp_dist_staf, test.get_uc_neap.upecSign, test.get_uc_neap.status, test.get_uc_neap.trash, test.get_uc_neap.isSync, test.get_uc_neap.location_code , test.xbi_campaign.campaign_ID, test.xbi_campaign.campaign_ActivityName, test.xbi_campaign.campaign_ActivityID_old, test.xbi_campaign.campaign_Yr, test.xbi_campaign.campaign_SubActivityName,  test.eoc_geolocation_tbl.name, test.eoc_geolocation_tbl.type, test.eoc_geolocation_tbl.code, test.eoc_geolocation_tbl.census_pop, test.eoc_geolocation_tbl.target, test.eoc_geolocation_tbl.status, test.eoc_geolocation_tbl.pname, test.eoc_geolocation_tbl.dname, test.eoc_geolocation_tbl.namedistrict, test.eoc_geolocation_tbl.codedistrict, test.eoc_geolocation_tbl.tname, test.eoc_geolocation_tbl.provincecode, test.eoc_geolocation_tbl.districtcode, test.eoc_geolocation_tbl.tehsilcode, test.eoc_geolocation_tbl.priority, test.eoc_geolocation_tbl.commnet, test.eoc_geolocation_tbl.hr, test.eoc_geolocation_tbl.fcm, test.eoc_geolocation_tbl.tier, test.eoc_geolocation_tbl.block, test.eoc_geolocation_tbl.division, test.eoc_geolocation_tbl.cordinates, test.eoc_geolocation_tbl.latitude, test.eoc_geolocation_tbl.longitude, test.eoc_geolocation_tbl.x, test.eoc_geolocation_tbl.y, test.eoc_geolocation_tbl.imagepath, test.eoc_geolocation_tbl.isccpv, test.eoc_geolocation_tbl.rank, test.eoc_geolocation_tbl.rank_score, test.eoc_geolocation_tbl.ishealthcamp, test.eoc_geolocation_tbl.isdsc, test.eoc_geolocation_tbl.ucorg, test.eoc_geolocation_tbl.organization, test.eoc_geolocation_tbl.tierfromaug161, test.eoc_geolocation_tbl.tierfromsep171, test.eoc_geolocation_tbl.tierfromdec181, test.eoc_geolocation_tbl.mtap, test.eoc_geolocation_tbl.rspuc, test.eoc_geolocation_tbl.issmt, test.eoc_geolocation_tbl.updateddatetime, test.eoc_geolocation_tbl.x_code, test.eoc_geolocation_tbl.draining_uc, test.eoc_geolocation_tbl.upap_districts, test.eoc_geolocation_tbl.shruc, test.eoc_geolocation_tbl.khidist_id "
#                 sql = "SELECT " + cols + "  FROM test.get_uc_neap tsm left JOIN test.eoc_geolocation_tbl eoc_2 ON tsm.location_code  = eoc_2.code left JOIN test.xbi_campaign eoc_3 ON (tsm.ActivityID  = eoc_3.campaign_ActivityID_old And tsm.Yr = eoc_3.campaign_Yr) "

#                 data = client.execute(sql)
#                 apiDataFrame = pd.DataFrame(data)
#                 all_columns = list(apiDataFrame)  # Creates list of all column headers
#                 cols = apiDataFrame.iloc[0]
#                 apiDataFrame[all_columns] = apiDataFrame[all_columns].astype(str)
#                 d =  'ID', 'UserName', 'IDcampCat', 'ActivityID', 'Yr', 'TimeStamp', 'DateAssessment', 'TehsilID', 'UCID', 'DistID', 'DivID', 'ProvID', 'targeted', 'DaysBeforeActivity', 'UCMORounds', 'inaccCh', 'areasInacc', 'mobileTeams', 'fixedTeams', 'TransitPoints', 'HH011_MP', 'HH1259_MP', 'School_MP', 'Fixed_MP', 'Transit_MP', 'areaIcs', 'aicFemale', 'aicGovt', 'aicRounds', 'aicTrained', 'AICSelfA', 'stall_method_trg', 'mov_stall_method_trg', 'MPDR_Passed', 'MPDR_Int', 'MPDR_Failed', 'MPFV_Passed', 'MPFV_Int', 'MPFV_Failed', 'MPDRD_Passed', 'MPDRD_Int', 'MPDRD_Failed', 'MPFVD_Passed', 'MPFVD_Int', 'MPFVD_Failed', 'zonalSupervisor', 'alltm18yr', 'govtWorker', 'mtLocalmem', 'mtFemale', 'HRMP', 'HRMP_status', 'NomadsChild', 'NomadicSett', 'BriklinsChild', 'BriklinsSett', 'SeasonalMigChild', 'SeasonalMigSett', 'AgrWorkersChild', 'AgrWorkersSett', 'Other', 'IDPChild', 'IDPSett', 'RefugeesChild', 'RefugeesSett', 'alltmTrg', 'ddmCard', 'UpecHeld', 'UpecDate', 'ucmo', 'ucSecratry', 'shoUpec', 'scVerified', 'lastCampDose0', 'Dose0Cov', 'remarks', 'mov_mp_uc_staf', 'mov_aic_train', 'mov_mp_target', 'mov_hr', 'mov_upec', 'mov_zero_dose', 'mov_team_train', 'vacc_carr_avbl', 'vacc_carr_req', 'mov_mp_dist_staf', 'upecSign', 'status', 'trash', 'isSync', 'location_code', 'campaign_ID', 'campaign_ActivityName', 'campaign_ActivityID_old', 'campaign_Yr', 'campaign_SubActivityName', 'geoLocation_name', 'geoLocation_type', 'geoLocation_code', 'geoLocation_census_pop', 'geoLocation_target', 'geoLocation_status', 'geoLocation_pname', 'geoLocation_dname', 'geoLocation_namedistrict', 'geoLocation_codedistrict', 'geoLocation_tname', 'geoLocation_provincecode', 'geoLocation_districtcode', 'geoLocation_tehsilcode', 'geoLocation_priority', 'geoLocation_commnet', 'geoLocation_hr', 'geoLocation_fcm', 'geoLocation_tier', 'geoLocation_block', 'geoLocation_division', 'geoLocation_cordinates', 'geoLocation_latitude', 'geoLocation_longitude', 'geoLocation_x', 'geoLocation_y', 'geoLocation_imagepath', 'geoLocation_isccpv', 'geoLocation_rank', 'geoLocation_rank_score', 'geoLocation_ishealthcamp', 'geoLocation_isdsc', 'geoLocation_ucorg', 'geoLocation_organization', 'geoLocation_tierfromaug161', 'geoLocation_tierfromsep171', 'geoLocation_tierfromdec181', 'geoLocation_mtap', 'geoLocation_rspuc', 'geoLocation_issmt', 'geoLocation_updateddatetime', 'geoLocation_x_code', 'geoLocation_draining_uc', 'geoLocation_upap_districts', 'geoLocation_shruc', 'geoLocation_khidist_id' 
#                 dff = pd.DataFrame(columns=d)
#                 for index, item in enumerate(d):
#                     dff[item] = apiDataFrame[index].values
#                 df2 = client.query_dataframe(
#                     "SELECT * FROM  test.xbi_neapUcPlan")
#                 if df2.empty:
#                     #apiDataFrame = apiDataFrame.replace(r'^\s*$', np.nan, regex=True)
#                     dff[["TimeStamp"]] = dff[["TimeStamp"]].apply(pd.to_datetime)
#                     dff[["DateAssessment"]] = dff[["DateAssessment"]].apply(pd.to_datetime)
#                     dff[["UpecDate"]] = dff[["UpecDate"]].apply(pd.to_datetime)
#                     dff[["alltm18yr"]] = dff[["alltm18yr"]].apply(pd.to_numeric )
#                     dff[["alltmTrg"]] = dff[["alltmTrg"]].apply(pd.to_numeric )
#                     dff[["location_code"]] = dff[["location_code"]].apply(pd.to_numeric )
#                     dff[["mtLocalmem"]] = dff[["mtLocalmem"]].apply(pd.to_numeric )
#                     dff[["govtWorker"]] = dff[["govtWorker"]].apply(pd.to_numeric )
#                     dff[["mtFemale"]] = dff[["mtFemale"]].apply(pd.to_numeric )
#                     dff[["aicTrained"]] = dff[["aicTrained"]].apply(pd.to_numeric )
#                     dff[["areaIcs"]] = dff[["areaIcs"]].apply(pd.to_numeric )
#                     dff[["TransitPoints"]] = dff[["TransitPoints"]].apply(pd.to_numeric )
#                     dff[["fixedTeams"]] = dff[["fixedTeams"]].apply(pd.to_numeric )
#                     dff[["UCMORounds"]] = dff[["UCMORounds"]].apply(pd.to_numeric )
#                     dff[["ucmo"]] = dff[["ucmo"]].apply(pd.to_numeric )
#                     dff[["ucSecratry"]] = dff[["ucSecratry"]].apply(pd.to_numeric )
#                     client.insert_dataframe(
#                         'INSERT INTO  test.xbi_neapUcPlan  VALUES', dff)
#                     logger.info(
#                         ' Data has been inserted into Table\' INSERT INTO  test.xbi_neapUcPlan  VALUES \' ')

#                     sql = "DROP table if exists  test.get_uc_neap"
#                     client.execute(sql)
#                 else:
#                     #dff = dff.replace(r'^\s*$', np.nan, regex=True)
#                     dff[["TimeStamp"]] = dff[["TimeStamp"]].apply(pd.to_datetime)
#                     dff[["DateAssessment"]] = dff[["DateAssessment"]].apply(pd.to_datetime)
#                     dff[["UpecDate"]] = dff[["UpecDate"]].apply(pd.to_datetime)
#                     dff[["alltmTrg"]] = dff[["alltmTrg"]].apply(pd.to_numeric )
#                     dff[["alltm18yr"]] = dff[["alltm18yr"]].apply(pd.to_numeric )
#                     dff[["location_code"]] = dff[["location_code"]].apply(pd.to_numeric )
#                     dff[["mtLocalmem"]] = dff[["mtLocalmem"]].apply(pd.to_numeric )
#                     dff[["govtWorker"]] = dff[["govtWorker"]].apply(pd.to_numeric )
#                     dff[["mtFemale"]] = dff[["mtFemale"]].apply(pd.to_numeric )
#                     dff[["aicTrained"]] = dff[["aicTrained"]].apply(pd.to_numeric )
#                     dff[["areaIcs"]] = dff[["areaIcs"]].apply(pd.to_numeric )
#                     dff[["TransitPoints"]] = dff[["TransitPoints"]].apply(pd.to_numeric )
#                     dff[["fixedTeams"]] = dff[["fixedTeams"]].apply(pd.to_numeric )
#                     dff[["UCMORounds"]] = dff[["UCMORounds"]].apply(pd.to_numeric )
#                     dff[["ucmo"]] = dff[["ucmo"]].apply(pd.to_numeric )
#                     dff[["ucSecratry"]] = dff[["ucSecratry"]].apply(pd.to_numeric )

#                     client.insert_dataframe(
#                         'INSERT INTO  test.xbi_neapUcPlan  VALUES', dff)

#                     sql = "DROP table if exists  test.get_uc_neap"
#                     client.execute(sql)


# dag = DAG(
#     'Prod_Neap_UC_Automated',
#     schedule_interval='0 0 * * *',  # will run every mid-night.
#     default_args=default_args,
#     catchup=False)

# with dag:
#     GetAndInsertApiDataUCNeap = PythonOperator(
#         task_id='GetAndInsertApiDataUCNeap',
#         python_callable=GetAndInsertApiDataUCNeap,
#     )
# GetAndInsertApiDataUCNeap
