from airflow import DAG
import pandas as pd
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import requests
from clickhouse_driver import Client
import logging
import numpy as np
from datetime import datetime

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
#---------------------------------------- INSERT CAMPAIGN DATA PROCESS CUBE  ---------------------------------------------------#
#-------------------------------------------- Author: Abdul Bari Malik ---------------------------------------------------------#
#-------------------------------------------------------------------------------------------------------------------------------#

def GetAndInsertDataProcessCube():
    logger.info('Function \' GetAndInsertDataProcessCube \' Started Off')
    client = Client(host='161.97.136.95',
                        user='default',
                        password='pakistan',
                        port='9000', settings={"use_numpy": True})

    sql = """CREATE TABLE if not exists test.get_process_cube ( ID Int32, IDcampCat Int32,ActivityID Int32,Yr Int32,TimeStamp DateTime('Asia/Karachi'), Cday Int32, TehsilID Int32, UCID Int32 
            ,ccpvTargets Int32,TeamsRpt Int32,HH011_MP Int32,HH1259_MP Int32,HH011_TS Int32,HH1259_TS Int32,HH011 Int32,HH1259 Int32,OutofH011 Int32,OutofH1259 Int32, RecNA011 Int32,RecNA1259 Int32,RecRef011 Int32,RecRef1259 Int32,CovNA011 Int32,CovNA1259 Int32,CovRef011 Int32,CovRef1259 Int32,Guests Int32,VaccinatedSchool Int32,NewBorn Int32,AlreadyVaccinated Int32,FixSite011 Int32,FixSite1259 Int32,Transit011 Int32,Transit1259 Int32,CovMob Int32,HHvisit Int32,HHPlan Int32,MultipleFamily String,Weakness Int32,ZeroRt Int32,Stage34 Int32,Inaccessible Int32,V_Age611 Int32,v_Age1259 Int32,V_capGiven Int32,V_capUsed Int32,V_capRet Int32,trigers String,Covid19 Int32,campcat_name String
            ,UserName String,DistID Int32,DivID Int32,ProvID Int32
            ,CCovNA011 Int32,CCovNA1259 Int32,CCovRef011 Int32,CCovRef1259 Int32
            ,COutofH011 Int32,COutofH1259 Int32,CGuests Int32,CCovMob Int32,CNewBorn Int32,CAlreadyVaccinated Int32,CFixSite011 Int32,CFixSite1259 Int32,CTransit011 Int32,CTransit1259 Int32,UnRecorded_Cov Int32,AFP_CaseRpt Int32,ZeroDos1 Int32,V_Age611_CP Int32,v_Age1259_CP Int32,missing_vacc_carr Int32,missing_vacc_carr_reason String
            ,ACCovNA011 Int32,ACCovNA1259 Int32,ACCovRef011 Int32,ACCovRef1259 Int32,ACOutofH011 Int32,ACOutofH1259 Int32,ACGuests Int32,ACCovMob Int32,ACNewBorn Int32,ACAlreadyVaccinated Int32,UnRecCov Int32
            ,PersistentlyMC Int32,PersistentlyMCHRMP Int32, fm_issued Int32,fm_retrieved Int32
            ,VaccinationDate DateTime('Asia/Karachi'),OPVGiven Int32,OPVUsed Int32,OPVReturned Int32,status Int32,trash Int32,isSync Int32
            ,UserID Int32,CategoryID Int32,Proposed_HH_Targets Int32,Final_HH_Targets Int32 ,Seasonal String,Nomads String,Agriculture String,Beggers String,IDPs String,Family String,Others String,Status String,Trash Int32
            ,Remarks String,location_code Int32
            ,campaign_ID Int32, campaign_ActivityName String, campaign_ActivityID_old Int32, campaign_Yr Int32, campaign_SubActivityName String
            ,geoLocation_name String, geoLocation_type Int32, geoLocation_code Int32, geoLocation_census_pop Int32, geoLocation_target Int32, geoLocation_status Int32, geoLocation_pname String, geoLocation_dname String, geoLocation_namedistrict String, geoLocation_codedistrict String, geoLocation_tname String, geoLocation_provincecode Int32, geoLocation_districtcode Int32, geoLocation_tehsilcode Int32, geoLocation_priority Int32, geoLocation_commnet Int32, geoLocation_hr Int32, geoLocation_fcm Int32, geoLocation_tier Int32, geoLocation_block String, geoLocation_division String, geoLocation_cordinates String, geoLocation_latitude String, geoLocation_longitude String, geoLocation_x String, geoLocation_y String, geoLocation_imagepath String, geoLocation_isccpv Int32, geoLocation_rank Int32, geoLocation_rank_score String, geoLocation_ishealthcamp Int32, geoLocation_isdsc Int32, geoLocation_ucorg String, geoLocation_organization String, geoLocation_tierfromaug161 Int32, geoLocation_tierfromsep171 Int32, geoLocation_tierfromdec181 Int32, geoLocation_mtap Int32, geoLocation_rspuc Int32, geoLocation_issmt Int32, geoLocation_updateddatetime String, geoLocation_x_code Int32, geoLocation_draining_uc Int32, geoLocation_upap_districts Int32, geoLocation_shruc Int32, geoLocation_khidist_id String
    
            )ENGINE = MergeTree PRIMARY KEY ID ORDER BY ID"""
    client.execute(sql)

    df = client.query_dataframe(
                """
                    SELECT ID,	IDcampCat,	ActivityID,	Yr,	TimeStamp,	Cday,	TehsilID,	UCID, 
                    -- Control Room --
                    ccpvTargets,TeamsRpt,HH011_MP,HH1259_MP,HH011_TS,HH1259_TS,HH011,HH1259,OutofH011,OutofH1259,
                    RecNA011,RecNA1259,RecRef011,RecRef1259,CovNA011,CovNA1259,CovRef011,CovRef1259,Guests, 
                    VaccinatedSchool,NewBorn,AlreadyVaccinated,FixSite011,FixSite1259,Transit011,Transit1259,CovMob,HHvisit,HHPlan,MultipleFamily,Weakness,ZeroRt,Stage34,
                    Inaccessible,V_Age611,v_Age1259,V_capGiven,V_capUsed,V_capRet,trigers,Covid19,campcat_name
                    -- CatchUp --
                    ,NULL AS UserName,NULL AS DistID,NULL AS DivID,NULL AS ProvID
                    ,NULL AS CCovNA011, NULL AS CCovNA1259,NULL AS CCovRef011,NULL AS CCovRef1259
                    ,NULL AS COutofH011,NULL AS COutofH1259,NULL AS CGuests,NULL AS CCovMob,NULL AS CNewBorn,NULL AS CAlreadyVaccinated,NULL AS CFixSite011,NULL AS CFixSite1259,NULL AS CTransit011,NULL AS CTransit1259,NULL AS UnRecorded_Cov,
                    NULL AS AFP_CaseRpt,NULL AS ZeroDos1,NULL AS V_Age611_CP,NULL AS v_Age1259_CP,NULL AS missing_vacc_carr,NULL AS missing_vacc_carr_reason
                    -- ACU --
                    ,NULL AS ACCovNA011,NULL AS ACCovNA1259,NULL AS ACCovRef011,NULL AS ACCovRef1259,NULL AS ACOutofH011,NULL AS ACOutofH1259,NULL AS ACGuests,NULL AS ACCovMob,NULL AS ACNewBorn,NULL AS ACAlreadyVaccinated,NULL AS UnRecCov
                    -- CU = ACU --
                    ,NULL AS PersistentlyMC,NULL AS PersistentlyMCHRMP, NULL AS fm_issued,NULL AS fm_retrieved
                    -- CR = CU = ACU --
                    ,VaccinationDate,OPVGiven,OPVUsed,OPVReturned,status,trash,isSync,
                    -- HH Target --
                    NULL AS UserID,NULL AS CategoryID, NULL AS Proposed_HH_Targets, NULL AS Final_HH_Targets , NULL AS Seasonal, NULL AS Nomads, NULL AS Agriculture, NULL AS Beggers, NULL AS IDPs, NULL AS Family, NULL AS Others 
                    ,NULL AS Status,NULL AS Trash 
                    --- Common --
                    ,Remarks,location_code
                    ,campaign_ID, campaign_ActivityName, campaign_ActivityID_old, campaign_Yr, campaign_SubActivityName
                    ,geoLocation_name, geoLocation_type, geoLocation_code, geoLocation_census_pop, geoLocation_target, geoLocation_status, geoLocation_pname, geoLocation_dname, geoLocation_namedistrict, geoLocation_codedistrict, geoLocation_tname, geoLocation_provincecode, geoLocation_districtcode, geoLocation_tehsilcode, geoLocation_priority, geoLocation_commnet, geoLocation_hr, geoLocation_fcm, geoLocation_tier, geoLocation_block, geoLocation_division, geoLocation_cordinates, geoLocation_latitude, geoLocation_longitude, geoLocation_x, geoLocation_y, geoLocation_imagepath, geoLocation_isccpv, geoLocation_rank, geoLocation_rank_score, geoLocation_ishealthcamp, geoLocation_isdsc, geoLocation_ucorg, geoLocation_organization, geoLocation_tierfromaug161, geoLocation_tierfromsep171, geoLocation_tierfromdec181, geoLocation_mtap, geoLocation_rspuc, geoLocation_issmt, geoLocation_updateddatetime, geoLocation_x_code, geoLocation_draining_uc, geoLocation_upap_districts, geoLocation_shruc, geoLocation_khidist_id
                                
                    FROM test.xbi_controlroom
                    where ActivityID = 2 and Yr = 2022
                    ----------
                    UNION ALL
                    ----------
                    SELECT ID,IDcampCat,ActivityID,Yr,TimeStamp,Cday,TehsilID,UCID, 
                    -- Control Room --
                    NULL AS ccpvTargets,NULL AS TeamsRpt,NULL AS HH011_MP,NULL AS HH1259_MP,NULL AS HH011_TS,NULL AS HH1259_TS,NULL AS HH011,NULL AS HH1259,NULL AS OutofH011,NULL AS OutofH1259,
                    NULL AS RecNA011,NULL AS RecNA1259,NULL AS RecRef011,NULL AS RecRef1259,NULL AS CovNA011,NULL AS CovNA1259,NULL AS CovRef011,NULL AS CovRef1259,NULL AS Guests,
                    NULL AS VaccinatedSchool,NULL AS NewBorn,NULL AS AlreadyVaccinated,NULL AS FixSite011,NULL AS FixSite1259,NULL AS Transit011,NULL AS Transit1259,NULL AS CovMob,NULL AS HHvisit,NULL AS HHPlan,NULL AS MultipleFamily,NULL AS Weakness,NULL AS ZeroRt,NULL AS Stage34,
                    NULL AS Inaccessible,NULL AS V_Age611,NULL AS v_Age1259,NULL AS V_capGiven,NULL AS V_capUsed,NULL AS V_capRet,NULL AS trigers,NULL AS Covid19,NULL AS campcat_name
                    -- CatchUp --
                    ,UserName,DistID,DivID,ProvID
                    ,CCovNA011,CCovNA1259,CCovRef011,CCovRef1259
                    ,COutofH011,COutofH1259,CGuests,CCovMob,CNewBorn,CAlreadyVaccinated,CFixSite011,CFixSite1259,CTransit011,CTransit1259,UnRecorded_Cov,
                    AFP_CaseRpt,ZeroDos1,V_Age611_CP,v_Age1259_CP,missing_vacc_carr,missing_vacc_carr_reason
                    -- ACU --
                    ,NULL AS ACCovNA011,NULL AS ACCovNA1259,NULL AS ACCovRef011,NULL AS ACCovRef1259,NULL AS ACOutofH011,NULL AS ACOutofH1259,NULL AS ACGuests,NULL AS ACCovMob,NULL AS ACNewBorn,NULL AS ACAlreadyVaccinated,NULL AS UnRecCov
                    -- CU = ACU --
                    ,PersistentlyMC,PersistentlyMCHRMP,fm_issued,fm_retrieved
                    -- CR = CU = ACU --
                    ,NULL AS VaccinationDate,OPVGiven,OPVUsed,CAST(OPVReturned AS INT)OPVReturned ,status,trash,isSync,
                    -- HH Target --
                    NULL AS UserID,NULL AS CategoryID, NULL AS Proposed_HH_Targets, NULL AS Final_HH_Targets , NULL AS Seasonal, NULL AS Nomads, NULL AS Agriculture, NULL AS Beggers, NULL AS IDPs, NULL AS Family, NULL AS Others 
                    ,NULL AS Status,NULL AS Trash 
                    --- Common --
                    ,Remarks,location_code
                    ,campaign_ID, campaign_ActivityName, campaign_ActivityID_old, campaign_Yr, campaign_SubActivityName
                    ,geoLocation_name, geoLocation_type, geoLocation_code, geoLocation_census_pop, geoLocation_target, geoLocation_status, geoLocation_pname, geoLocation_dname, geoLocation_namedistrict, geoLocation_codedistrict, geoLocation_tname, geoLocation_provincecode, geoLocation_districtcode, geoLocation_tehsilcode, geoLocation_priority, geoLocation_commnet, geoLocation_hr, geoLocation_fcm, geoLocation_tier, geoLocation_block, CAST(geoLocation_division AS String)geoLocation_division, geoLocation_cordinates, geoLocation_latitude, geoLocation_longitude, geoLocation_x, geoLocation_y, geoLocation_imagepath, geoLocation_isccpv, geoLocation_rank, geoLocation_rank_score, geoLocation_ishealthcamp, geoLocation_isdsc, geoLocation_ucorg, geoLocation_organization, geoLocation_tierfromaug161, geoLocation_tierfromsep171, geoLocation_tierfromdec181, geoLocation_mtap, geoLocation_rspuc, geoLocation_issmt, geoLocation_updateddatetime, geoLocation_x_code, geoLocation_draining_uc, geoLocation_upap_districts, geoLocation_shruc, geoLocation_khidist_id

                    FROM test.xbi_catchUp
                    where ActivityID = 2 and Yr = 2022
                    -----------
                    UNION ALL
                    ----------
                    SELECT ID,IDcampCat,ActivityID,Yr,TimeStamp,cday AS Cday,TehsilID,UCID,
                    -- Control Room --
                    NULL AS ccpvTargets,NULL AS TeamsRpt,NULL AS HH011_MP,NULL AS HH1259_MP,NULL AS HH011_TS,NULL AS HH1259_TS,NULL AS HH011,NULL AS HH1259,NULL AS OutofH011,NULL AS OutofH1259,
                    NULL AS RecNA011,NULL AS RecNA1259,NULL AS RecRef011,NULL AS RecRef1259,NULL AS CovNA011,NULL AS CovNA1259,NULL AS CovRef011,NULL AS CovRef1259,NULL AS Guests,
                    NULL AS VaccinatedSchool,NULL AS NewBorn,NULL AS AlreadyVaccinated,NULL AS FixSite011,NULL AS FixSite1259,NULL AS Transit011,NULL AS Transit1259,NULL AS CovMob,NULL AS HHvisit,NULL AS HHPlan,NULL AS MultipleFamily,NULL AS Weakness,NULL AS ZeroRt,NULL AS Stage34,
                    NULL AS Inaccessible,NULL AS V_Age611,NULL AS v_Age1259,NULL AS V_capGiven,NULL AS V_capUsed,NULL AS V_capRet,NULL AS trigers,NULL AS Covid19,NULL AS campcat_name
                    -- CatchUp=afterCatchUp --
                    ,UserName,DistID,DivID,ProvID
                    -- CatchUp --
                    ,NULL AS CCovNA011,NULL AS CCovNA1259,NULL AS CCovRef011,NULL AS CCovRef1259
                    ,NULL AS COutofH011,NULL AS COutofH1259,NULL AS CGuests,NULL AS CCovMob,NULL AS CNewBorn,NULL AS CAlreadyVaccinated,NULL AS CFixSite011,NULL AS CFixSite1259,NULL AS CTransit011,NULL AS CTransit1259,NULL AS UnRecorded_Cov,
                    NULL AS AFP_CaseRpt,NULL AS ZeroDos1,NULL AS V_Age611_CP,NULL AS v_Age1259_CP,NULL AS missing_vacc_carr,NULL AS missing_vacc_carr_reason
                    -- ACU --
                    ,ACCovNA011,ACCovNA1259,ACCovRef011,ACCovRef1259,ACOutofH011,ACOutofH1259,ACGuests,ACCovMob,ACNewBorn,ACAlreadyVaccinated,UnRecCov
                    -- CU = ACU --
                    ,PersistentlyMC,PersistentlyMCHRMP,fm_issued,fm_retrieved
                    -- CR = CU = ACU --
                    ,VaccinationDate,OPVGiven,OPVUsed,CAST(OPVReturned AS INT)OPVReturned,status,trash,isSync,
                    -- HH Target --
                    NULL AS UserID,NULL AS CategoryID, NULL AS Proposed_HH_Targets, NULL AS Final_HH_Targets , NULL AS Seasonal, NULL AS Nomads, NULL AS Agriculture, NULL AS Beggers, NULL AS IDPs, NULL AS Family, NULL AS Others 
                    ,NULL AS Status,NULL AS Trash 
                    --- Common --
                    ,Remarks,location_code
                    ,campaign_ID, campaign_ActivityName, campaign_ActivityID_old, campaign_Yr, campaign_SubActivityName
                    ,geoLocation_name, geoLocation_type, geoLocation_code, geoLocation_census_pop, geoLocation_target, geoLocation_status, geoLocation_pname, geoLocation_dname, geoLocation_namedistrict, geoLocation_codedistrict, geoLocation_tname, geoLocation_provincecode, geoLocation_districtcode, geoLocation_tehsilcode, geoLocation_priority, geoLocation_commnet, geoLocation_hr, geoLocation_fcm, geoLocation_tier, geoLocation_block, geoLocation_division, geoLocation_cordinates, geoLocation_latitude, geoLocation_longitude, geoLocation_x, geoLocation_y, geoLocation_imagepath, geoLocation_isccpv, geoLocation_rank, geoLocation_rank_score, geoLocation_ishealthcamp, geoLocation_isdsc, geoLocation_ucorg, geoLocation_organization, geoLocation_tierfromaug161, geoLocation_tierfromsep171, geoLocation_tierfromdec181, geoLocation_mtap, geoLocation_rspuc, geoLocation_issmt, geoLocation_updateddatetime, geoLocation_x_code, geoLocation_draining_uc, geoLocation_upap_districts, geoLocation_shruc, geoLocation_khidist_id

                    FROM test.xbi_after_catchUp
                    where ActivityID = 2 and Yr = 2022
                    ----------
                    UNION ALL
                    ----------
                    SELECT ID,NULL AS IDcampCat,ActivityID_fk AS ActivityID,Yr,CAST (TimeStamp AS String)TimeStamp, NULL AS Cday,TehsilID,UCID,
                    -- Control Room --
                    NULL AS ccpvTargets,NULL AS TeamsRpt,NULL AS HH011_MP,NULL AS HH1259_MP,NULL AS HH011_TS,NULL AS HH1259_TS,NULL AS HH011,NULL AS HH1259,NULL AS OutofH011,NULL AS OutofH1259,
                    NULL AS RecNA011,NULL AS RecNA1259,NULL AS RecRef011,NULL AS RecRef1259,NULL AS CovNA011,NULL AS CovNA1259,NULL AS CovRef011,NULL AS CovRef1259,NULL AS Guests,
                    NULL AS VaccinatedSchool,NULL AS NewBorn,NULL AS AlreadyVaccinated,NULL AS FixSite011,NULL AS FixSite1259,NULL AS Transit011,NULL AS Transit1259,NULL AS CovMob,NULL AS HHvisit,NULL AS HHPlan,NULL AS MultipleFamily,NULL AS Weakness,NULL AS ZeroRt,NULL AS Stage34,
                    NULL AS Inaccessible,NULL AS V_Age611,NULL AS v_Age1259,NULL AS V_capGiven,NULL AS V_capUsed,NULL AS V_capRet,NULL AS trigers,NULL AS Covid19,NULL AS campcat_name
                    -- CatchUp=afterCatchUp --
                    ,NULL AS UserName,DistID,DivID,ProvID
                    -- CatchUp --
                    ,NULL AS CCovNA011,NULL AS CCovNA1259,NULL AS CCovRef011,NULL AS CCovRef1259
                    ,NULL AS COutofH011,NULL AS COutofH1259,NULL AS CGuests,NULL AS CCovMob,NULL AS CNewBorn,NULL AS CAlreadyVaccinated,NULL AS CFixSite011,NULL AS CFixSite1259,NULL AS CTransit011,NULL AS CTransit1259,NULL AS UnRecorded_Cov,
                    NULL AS AFP_CaseRpt,NULL AS ZeroDos1,NULL AS V_Age611_CP,NULL AS v_Age1259_CP,NULL AS missing_vacc_carr,NULL AS missing_vacc_carr_reason
                    -- ACU --
                    ,NULL AS ACCovNA011,NULL AS ACCovNA1259,NULL AS ACCovRef011,NULL AS ACCovRef1259,NULL AS ACOutofH011,NULL AS ACOutofH1259,NULL AS ACGuests,NULL AS ACCovMob,NULL AS ACNewBorn,NULL AS ACAlreadyVaccinated,NULL AS UnRecCov
                    -- CU = ACU --
                    ,NULL AS PersistentlyMC,NULL AS PersistentlyMCHRMP,NULL AS fm_issued,NULL AS fm_retrieved
                    -- CR = CU = ACU --
                    ,NULL AS VaccinationDate,NULL AS OPVGiven,NULL AS OPVUsed,NULL AS OPVReturned,NULL AS status,NULL AS trash,NULL AS isSync,
                    -- HH Target --
                    UserID,CategoryID,Proposed_HH_Targets,Final_HH_Targets ,Seasonal,Nomads,Agriculture,Beggers,IDPs,Family,Others,Status,Trash 
                    --- Common --
                    ,Remarks,Ucode AS location_code
                    ,campaign_ID, campaign_ActivityName, campaign_ActivityID_old, campaign_Yr, campaign_SubActivityName
                    ,geoLocation_name, geoLocation_type, geoLocation_code, geoLocation_census_pop, geoLocation_target, geoLocation_status, geoLocation_pname, geoLocation_dname, geoLocation_namedistrict, geoLocation_codedistrict, geoLocation_tname, geoLocation_provincecode, geoLocation_districtcode, geoLocation_tehsilcode, geoLocation_priority, geoLocation_commnet, geoLocation_hr, geoLocation_fcm, geoLocation_tier, geoLocation_block, geoLocation_division, geoLocation_cordinates, geoLocation_latitude, geoLocation_longitude, geoLocation_x, geoLocation_y, geoLocation_imagepath, geoLocation_isccpv, geoLocation_rank, geoLocation_rank_score, geoLocation_ishealthcamp, geoLocation_isdsc, geoLocation_ucorg, geoLocation_organization, geoLocation_tierfromaug161, geoLocation_tierfromsep171, geoLocation_tierfromdec181, geoLocation_mtap, geoLocation_rspuc, geoLocation_issmt, geoLocation_updateddatetime, geoLocation_x_code, geoLocation_draining_uc, geoLocation_upap_districts, geoLocation_shruc, geoLocation_khidist_id

                    FROM test.xbi_hhtarget
                    where ActivityID = 2 and Yr = 2022
                """)
    #df['TimeStamp'] = datetime.strptime(df['TimeStamp'], '%d/%m/%y %H:%M:%S')
    df['TimeStamp'] = pd.to_datetime(df['TimeStamp'], errors='coerce')
    df['VaccinationDate'] = pd.to_datetime(df['VaccinationDate'], errors='coerce')
    df[["TimeStamp"]] = df[["TimeStamp"]].apply(pd.to_datetime)
    df[["VaccinationDate"]] = df[["VaccinationDate"]].apply(pd.to_datetime)         
    client.insert_dataframe(
        'INSERT INTO test.get_process_cube  VALUES', df)
    logger.info(
            ' Data has been inserted into Table\' INSERT INTO test.get_process_cube VALUES \' ')            
    
    #---------------- xbi_table --------------------#
def CreateJoinTableOfProcessCube():
    logger.info('Function \' CreateJoinTableOfProcessCube \' Started Off')
    client = Client(host='161.97.136.95',
                    user='default',
                    password='pakistan',
                    port='9000', settings={"use_numpy": True})   

    cid = 2
    y = 2022

    sql = """CREATE TABLE if not exists test.xbi_process_cube ( ID Int32, IDcampCat Int32,ActivityID Int32,Yr Int32,TimeStamp DateTime('Asia/Karachi'), Cday Int32, TehsilID Int32, UCID Int32 
            ,ccpvTargets Int32,TeamsRpt Int32,HH011_MP Int32,HH1259_MP Int32,HH011_TS Int32,HH1259_TS Int32,HH011 Int32,HH1259 Int32,OutofH011 Int32,OutofH1259 Int32, RecNA011 Int32,RecNA1259 Int32,RecRef011 Int32,RecRef1259 Int32,CovNA011 Int32,CovNA1259 Int32,CovRef011 Int32,CovRef1259 Int32,Guests Int32,VaccinatedSchool Int32,NewBorn Int32,AlreadyVaccinated Int32,FixSite011 Int32,FixSite1259 Int32,Transit011 Int32,Transit1259 Int32,CovMob Int32,HHvisit Int32,HHPlan Int32,MultipleFamily String,Weakness Int32,ZeroRt Int32,Stage34 Int32,Inaccessible Int32,V_Age611 Int32,v_Age1259 Int32,V_capGiven Int32,V_capUsed Int32,V_capRet Int32,trigers String,Covid19 Int32,campcat_name String
            ,UserName String,DistID Int32,DivID Int32,ProvID Int32
            ,CCovNA011 Int32,CCovNA1259 Int32,CCovRef011 Int32,CCovRef1259 Int32,COutofH011 Int32,COutofH1259 Int32,CGuests Int32,CCovMob Int32,CNewBorn Int32,CAlreadyVaccinated Int32,CFixSite011 Int32,CFixSite1259 Int32,CTransit011 Int32,CTransit1259 Int32,UnRecorded_Cov Int32,AFP_CaseRpt Int32,ZeroDos1 Int32,V_Age611_CP Int32,v_Age1259_CP Int32,missing_vacc_carr Int32,missing_vacc_carr_reason String
            ,ACCovNA011 Int32,ACCovNA1259 Int32,ACCovRef011 Int32,ACCovRef1259 Int32,ACOutofH011 Int32,ACOutofH1259 Int32,ACGuests Int32,ACCovMob Int32,ACNewBorn Int32,ACAlreadyVaccinated Int32,UnRecCov Int32
            ,PersistentlyMC Int32,PersistentlyMCHRMP Int32, fm_issued Int32,fm_retrieved Int32
            ,VaccinationDate DateTime('Asia/Karachi'),OPVGiven Int32,OPVUsed Int32,OPVReturned Int32,status Int32,trash Int32,isSync Int32
            
            ,UserID Int32,CategoryID Int32,Proposed_HH_Targets Int32,Final_HH_Targets Int32 ,Seasonal String,Nomads String,Agriculture String,Beggers String,IDPs String,Family String,Others String,Status String,Trash Int32
            
            ,Remarks String,location_code Int32
            ,campaign_ID Int32, campaign_ActivityName String, campaign_ActivityID_old Int32, campaign_Yr Int32, campaign_SubActivityName String
            ,geoLocation_name String, geoLocation_type Int32, geoLocation_code Int32, geoLocation_census_pop Int32, geoLocation_target Int32, geoLocation_status Int32, geoLocation_pname String, geoLocation_dname String, geoLocation_namedistrict String, geoLocation_codedistrict String, geoLocation_tname String, geoLocation_provincecode Int32, geoLocation_districtcode Int32, geoLocation_tehsilcode Int32, geoLocation_priority Int32, geoLocation_commnet Int32, geoLocation_hr Int32, geoLocation_fcm Int32, geoLocation_tier Int32, geoLocation_block String, geoLocation_division String, geoLocation_cordinates String, geoLocation_latitude String, geoLocation_longitude String, geoLocation_x String, geoLocation_y String, geoLocation_imagepath String, geoLocation_isccpv Int32, geoLocation_rank Int32, geoLocation_rank_score String, geoLocation_ishealthcamp Int32, geoLocation_isdsc Int32, geoLocation_ucorg String, geoLocation_organization String, geoLocation_tierfromaug161 Int32, geoLocation_tierfromsep171 Int32, geoLocation_tierfromdec181 Int32, geoLocation_mtap Int32, geoLocation_rspuc Int32, geoLocation_issmt Int32, geoLocation_updateddatetime String, geoLocation_x_code Int32, geoLocation_draining_uc Int32, geoLocation_upap_districts Int32, geoLocation_shruc Int32, geoLocation_khidist_id String
            
            )ENGINE = MergeTree PRIMARY KEY ID ORDER BY ID"""
    client.execute(sql)

    cols = """
            eoc_1.ID,	eoc_1.IDcampCat,	eoc_1.ActivityID,	eoc_1.Yr,	eoc_1.TimeStamp,	eoc_1.Cday,	eoc_1.TehsilID,	eoc_1.UCID, 
            eoc_1.ccpvTargets,eoc_1.TeamsRpt,eoc_1.HH011_MP,eoc_1.HH1259_MP,eoc_1.HH011_TS,eoc_1.HH1259_TS,eoc_1.HH011,eoc_1.HH1259,eoc_1.OutofH011,eoc_1.OutofH1259,
            eoc_1.RecNA011,eoc_1.RecNA1259,eoc_1.RecRef011,eoc_1.RecRef1259,eoc_1.CovNA011,eoc_1.CovNA1259,eoc_1.CovRef011,eoc_1.CovRef1259,eoc_1.Guests, 
            eoc_1.VaccinatedSchool,eoc_1.NewBorn,eoc_1.AlreadyVaccinated,eoc_1.FixSite011,eoc_1.FixSite1259,eoc_1.Transit011,eoc_1.Transit1259,eoc_1.CovMob,eoc_1.HHvisit,eoc_1.HHPlan,eoc_1.MultipleFamily,eoc_1.Weakness,eoc_1.ZeroRt,eoc_1.Stage34,
            eoc_1.Inaccessible,eoc_1.V_Age611,eoc_1.v_Age1259,eoc_1.V_capGiven,eoc_1.V_capUsed,eoc_1.V_capRet,eoc_1.trigers,eoc_1.Covid19,eoc_1.campcat_name

            ,eoc_1.UserName,eoc_1.DistID,eoc_1.DivID,eoc_1.ProvID
            ,eoc_1.CCovNA011, eoc_1.CCovNA1259,eoc_1.CCovRef011,eoc_1.CCovRef1259
            ,eoc_1.COutofH011,eoc_1.COutofH1259,eoc_1.CGuests,eoc_1.CCovMob,eoc_1.CNewBorn,eoc_1.CAlreadyVaccinated,eoc_1.CFixSite011,eoc_1.CFixSite1259,eoc_1.CTransit011,eoc_1.CTransit1259,eoc_1.UnRecorded_Cov
            ,eoc_1.AFP_CaseRpt,eoc_1.ZeroDos1,eoc_1.V_Age611_CP,eoc_1.v_Age1259_CP,eoc_1.missing_vacc_carr,eoc_1.missing_vacc_carr_reason

            ,eoc_1.ACCovNA011,eoc_1.ACCovNA1259,eoc_1.ACCovRef011,eoc_1.ACCovRef1259,eoc_1.ACOutofH011,eoc_1.ACOutofH1259,eoc_1.ACGuests,eoc_1.ACCovMob,eoc_1.ACNewBorn,eoc_1.ACAlreadyVaccinated,eoc_1.UnRecCov

            ,eoc_1.PersistentlyMC,eoc_1.PersistentlyMCHRMP, eoc_1.fm_issued,eoc_1.fm_retrieved

            ,eoc_1.VaccinationDate,eoc_1.OPVGiven,eoc_1.OPVUsed,eoc_1.OPVReturned,eoc_1.status,eoc_1.trash,eoc_1.isSync

            ,eoc_1.UserID,CategoryID,eoc_1.Proposed_HH_Targets,eoc_1.Final_HH_Targets ,eoc_1.Seasonal,eoc_1.Nomads,eoc_1.Agriculture,eoc_1.Beggers,eoc_1.IDPs,eoc_1.Family,eoc_1.Others,eoc_1.Status,eoc_1.Trash 
            ,eoc_1.Remarks,eoc_1.location_code 
            ,eoc_1.campaign_ID, eoc_1.campaign_ActivityName, eoc_1.campaign_ActivityID_old, eoc_1.campaign_Yr, eoc_1.campaign_SubActivityName,  
            
            eoc_1.geoLocation_name, eoc_1.geoLocation_type, eoc_1.geoLocation_code, eoc_1.geoLocation_census_pop, eoc_1.geoLocation_target, eoc_1.geoLocation_status, eoc_1.geoLocation_pname, eoc_1.geoLocation_dname, eoc_1.geoLocation_namedistrict, eoc_1.geoLocation_codedistrict, eoc_1.geoLocation_tname, eoc_1.geoLocation_provincecode, eoc_1.geoLocation_districtcode, eoc_1.geoLocation_tehsilcode, eoc_1.geoLocation_priority, eoc_1.geoLocation_commnet, eoc_1.geoLocation_hr, eoc_1.geoLocation_fcm, eoc_1.geoLocation_tier, eoc_1.geoLocation_block, eoc_1.geoLocation_division, eoc_1.geoLocation_cordinates, eoc_1.geoLocation_latitude, eoc_1.geoLocation_longitude, eoc_1.geoLocation_x, eoc_1.geoLocation_y, eoc_1.geoLocation_imagepath, eoc_1.geoLocation_isccpv, eoc_1.geoLocation_rank, eoc_1.geoLocation_rank_score, eoc_1.geoLocation_ishealthcamp, eoc_1.geoLocation_isdsc, eoc_1.geoLocation_ucorg, eoc_1.geoLocation_organization, eoc_1.geoLocation_tierfromaug161, eoc_1.geoLocation_tierfromsep171, eoc_1.geoLocation_tierfromdec181, eoc_1.geoLocation_mtap, eoc_1.geoLocation_rspuc, eoc_1.geoLocation_issmt, eoc_1.geoLocation_updateddatetime, eoc_1.geoLocation_x_code, eoc_1.geoLocation_draining_uc, eoc_1.geoLocation_upap_districts, eoc_1.geoLocation_shruc, eoc_1.geoLocation_khidist_id
            """
    sql = "SELECT " + cols + "  FROM test.get_process_cube  eoc_1"# left JOIN test.xbi_hhtarget hht on (hht.Yr = eoc_1.Yr and hht.ActivityID_fk = eoc_1.ActivityID and hht.Ucode = eoc_1.location_code) left JOIN test.eoc_geolocation_t eoc_2 ON eoc_1.location_code  = eoc_1.code left JOIN test.xbi_campaign eoc_3 ON (eoc_1.ActivityID  = eoc_3.campaign_ActivityID_old And eoc_1.Yr = eoc_3.campaign_Yr) "
    data = client.execute(sql)

    xbiDataFrame = pd.DataFrame(data)
    all_columns = list(xbiDataFrame)  # Creates list of all column headers
    cols = xbiDataFrame.iloc[0]

    xbiDataFrame[all_columns] = xbiDataFrame[all_columns].astype(str)

    d = ['ID', 'IDcampCat',	'ActivityID','Yr','TimeStamp','Cday', 'TehsilID', 'UCID', 
        'ccpvTargets','TeamsRpt','HH011_MP','HH1259_MP','HH011_TS','HH1259_TS','HH011','HH1259','OutofH011','OutofH1259',
        'RecNA011','RecNA1259','RecRef011','RecRef1259','CovNA011','CovNA1259','CovRef011','CovRef1259','Guests', 
        'VaccinatedSchool','NewBorn','AlreadyVaccinated','FixSite011','FixSite1259','Transit011','Transit1259','CovMob','HHvisit','HHPlan','MultipleFamily','Weakness','ZeroRt','Stage34',
        'Inaccessible','V_Age611','v_Age1259','V_capGiven','V_capUsed','V_capRet','trigers','Covid19','campcat_name'
        ,'UserName','DistID','DivID','ProvID'
        ,'CCovNA011', 'CCovNA1259','CCovRef011','CCovRef1259'
        ,'COutofH011','COutofH1259','CGuests','CCovMob','CNewBorn','CAlreadyVaccinated','CFixSite011','CFixSite1259','CTransit011','CTransit1259','UnRecorded_Cov'
        ,'AFP_CaseRpt','ZeroDos1','V_Age611_CP','v_Age1259_CP','missing_vacc_carr','missing_vacc_carr_reason'
        ,'ACCovNA011','ACCovNA1259','ACCovRef011','ACCovRef1259','ACOutofH011','ACOutofH1259','ACGuests','ACCovMob','ACNewBorn','ACAlreadyVaccinated','UnRecCov'
        ,'PersistentlyMC','PersistentlyMCHRMP', 'fm_issued','fm_retrieved'
        ,'VaccinationDate','OPVGiven','OPVUsed','OPVReturned','status','trash','isSync'
        
        ,'UserID','CategoryID','Proposed_HH_Targets','Final_HH_Targets','Seasonal','Nomads','Agriculture','Beggers','IDPs','Family','Others','Status','Trash'
            
        ,'Remarks','location_code'
        ,'campaign_ID', 'campaign_ActivityName', 'campaign_ActivityID_old', 'campaign_Yr', 'campaign_SubActivityName'
        ,'geoLocation_name', 'geoLocation_type', 'geoLocation_code', 'geoLocation_census_pop', 'geoLocation_target', 'geoLocation_status', 'geoLocation_pname', 'geoLocation_dname', 'geoLocation_namedistrict', 'geoLocation_codedistrict', 'geoLocation_tname', 'geoLocation_provincecode', 'geoLocation_districtcode', 'geoLocation_tehsilcode', 'geoLocation_priority', 'geoLocation_commnet', 'geoLocation_hr', 'geoLocation_fcm', 'geoLocation_tier', 'geoLocation_block', 'geoLocation_division', 'geoLocation_cordinates', 'geoLocation_latitude', 'geoLocation_longitude', 'geoLocation_x', 'geoLocation_y', 'geoLocation_imagepath', 'geoLocation_isccpv', 'geoLocation_rank', 'geoLocation_rank_score', 'geoLocation_ishealthcamp', 'geoLocation_isdsc', 'geoLocation_ucorg', 'geoLocation_organization', 'geoLocation_tierfromaug161', 'geoLocation_tierfromsep171', 'geoLocation_tierfromdec181', 'geoLocation_mtap', 'geoLocation_rspuc', 'geoLocation_issmt', 'geoLocation_updateddatetime', 'geoLocation_x_code', 'geoLocation_draining_uc', 'geoLocation_upap_districts', 'geoLocation_shruc', 'geoLocation_khidist_id' 
        ]
    dff = pd.DataFrame(columns=d)
    for index, item in enumerate(d):
        dff[item] = xbiDataFrame[index].values
    logger.info(
            'Get Data from test.get_process_cube for campaign')#+str(cid))

    df3 = client.query_dataframe(
                    "SELECT * FROM test.xbi_process_cube WHERE Yr = 2022 and ActivityID = 2")
    if df3.empty:
        client.insert_dataframe(
            'INSERT INTO test.xbi_process_cube  VALUES', dff)
        logger.info(
            'Data has been inserted into Table test.xbi_process_cube for campaign ')#+str(cid))

        sql = "DROP table if exists test.get_process_cube"
        client.execute(sql)
    
    else:
        sql = "ALTER TABLE test.xbi_process_cube DELETE WHERE Yr = 2022 and ActivityID = 2"
        client.execute(sql)

        client.insert_dataframe(
            'INSERT INTO test.xbi_process_cube VALUES', dff)
        logger.info(
            ' Data has been inserted into Table test.xbi_process_cube for campaign')#+str(cid))    

        sql = "DROP table if exists test.get_process_cube"
        client.execute(sql)

dag = DAG(
    'ProcessCube_Automated',
    #schedule_interval='*/10 * * * *',# will run every 10 min.
    schedule_interval='0 0 * * *',  # will run every midnight
    default_args=default_args,
    catchup=False)

with dag:
    GetAndInsertDataProcessCube = PythonOperator(
        task_id='GetAndInsertDataProcessCube',
        python_callable=GetAndInsertDataProcessCube,
    )
    CreateJoinTableOfProcessCube = PythonOperator(
        task_id='CreateJoinTableOfProcessCube',
        python_callable=CreateJoinTableOfProcessCube,
    )
GetAndInsertDataProcessCube >> CreateJoinTableOfProcessCube

#-------------------------------------------------------------------------------------------------------------------------------#
#---------------------------------------- INSERT CAMPAIGN DATA PROCESS CUBE  ---------------------------------------------------#
#-------------------------------------------- Author: Abdul Bari Malik ---------------------------------------------------------#
#-------------------------------------------------------------------------------------------------------------------------------#
# def GetAndInsertDataProcessCube():
    
#     logger.info('Function \' GetAndInsertDataProcessCube \' Started Off')
#     client = Client(host='161.97.136.95',
#                         user='default',
#                         password='pakistan',
#                         port='9000', settings={"use_numpy": True})

#     sql = """CREATE TABLE if not exists test.get_process_cube ( ID Int32, IDcampCat Int32,ActivityID Int32,Yr Int32,TimeStamp DateTime('Asia/Karachi'), Cday Int32, TehsilID Int32, UCID Int32 
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
#                     -- where ActivityID = 2 and Yr = 2022 --
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
#                     -- where ActivityID = 2 and Yr = 2022 --
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
#                     -- where ActivityID = 2 and Yr = 2022--
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
#                     -- where ActivityID = 2 and Yr = 2022--
#                 """)
#     #df['TimeStamp'] = datetime.strptime(df['TimeStamp'], '%d/%m/%y %H:%M:%S')
#     df['TimeStamp'] = pd.to_datetime(df['TimeStamp'], errors='coerce')
#     df['VaccinationDate'] = pd.to_datetime(df['VaccinationDate'], errors='coerce')
#     df[["TimeStamp"]] = df[["TimeStamp"]].apply(pd.to_datetime)
#     df[["VaccinationDate"]] = df[["VaccinationDate"]].apply(pd.to_datetime)         
#     client.insert_dataframe(
#         'INSERT INTO test.get_process_cube  VALUES', df)
#     logger.info(
#             ' Data has been inserted into Table\' INSERT INTO test.get_process_cube VALUES \' ')            
    
#     #---------------- xbi_table --------------------#
#     sql = """CREATE TABLE if not exists test.xbi_process_cube ( ID Int32, IDcampCat Int32,ActivityID Int32,Yr Int32,TimeStamp DateTime('Asia/Karachi'), Cday Int32, TehsilID Int32, UCID Int32 
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
#     sql = "SELECT " + cols + "  FROM test.get_process_cube  eoc_1"# left JOIN test.xbi_hhtarget hht on (hht.Yr = eoc_1.Yr and hht.ActivityID_fk = eoc_1.ActivityID and hht.Ucode = eoc_1.location_code) left JOIN test.eoc_geolocation_t eoc_2 ON eoc_1.location_code  = eoc_1.code left JOIN test.xbi_campaign eoc_3 ON (eoc_1.ActivityID  = eoc_3.campaign_ActivityID_old And eoc_1.Yr = eoc_3.campaign_Yr) "
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
#             'Get Data from test.get_process_cube for campaign')#+str(cid))

#     df3 = client.query_dataframe(
#                     "SELECT * FROM test.xbi_process_cube")
#     if df3.empty:
#         client.insert_dataframe(
#             'INSERT INTO test.xbi_process_cube  VALUES', dff)
#         logger.info(
#             'Data has been inserted into Table test.xbi_process_cube for campaign ')#+str(cid))

#         sql = "DROP table if exists test.get_process_cube"
#         client.execute(sql)
    
#     else:
#         client.insert_dataframe(
#             'INSERT INTO test.xbi_process_cube VALUES', dff)
#         logger.info(
#             ' Data has been inserted into Table test.xbi_process_cube for campaign ')#+str(cid))    

#         sql = "DROP table if exists test.get_process_cube"
#         client.execute(sql)
                       
           
                        
 
# dag = DAG(
#     'ProcessCube_Automated',
#     #schedule_interval='*/10 * * * *',# will run every 10 min.
#     schedule_interval='0 0 * * *',  
#     default_args=default_args,
#     catchup=False)

# with dag:
#     GetAndInsertDataProcessCube = PythonOperator(
#         task_id='GetAndInsertDataProcessCube',
#         python_callable=GetAndInsertDataProcessCube,
#     )
# GetAndInsertDataProcessCube
