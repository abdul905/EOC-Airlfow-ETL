
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
#-------------------------------------------------- GENERATE DAG FILES  --------------------------------------------------------#
#------------------------------------------------ Author: AB Malik -------------------------------------------------------------#
#-------------------------------------------------------------------------------------------------------------------------------#

# def GetAndInsertDataAllNeap():
    
#     logger.info('Function \' GetAndInsertDataAllNeap \' Started Off')
#     client = Client(host='161.97.136.95',
#                         user='default',
#                         password='pakistan',
#                         port='9000', settings={"use_numpy": True})

#     sql = """CREATE TABLE if not exists test.get_all_neap ( 
#                     ID Int32,UserName String,ActivityID Int32,TimeStamp DateTime('Asia/Karachi'),Yr Int32,ProvID Int32,DivID Int32,DistID Int32,
#                     TehsilID Int32,	UCID Int32,	IDcampCat Int32,	DateAssessment DateTime('Asia/Karachi'),	targeted Int32,	DaysBeforeActivity Int32,	UCMORounds Int32,	inaccCh Int32,	areasInacc Int32,	mobileTeams Int32,	fixedTeams Int32,	TransitPoints Int32,
                        
#                     HH011_MP Int32,	HH1259_MP Int32,	School_MP Int32,	Fixed_MP Int32,	Transit_MP Int32,	areaIcs Int32,	aicFemale Int32,	aicGovt Int32,	aicRounds Int32,	aicTrained Int32,	AICSelfA Int32,	stall_method_trg Int32,

#                     mov_stall_method_trg Int32,	MPDR_Passed Int32,	MPDR_Int Int32,	MPDR_Failed Int32,	MPFV_Passed Int32,	MPFV_Int Int32,	MPFV_Failed Int32,	MPDRD_Passed Int32,	MPDRD_Int Int32,	MPDRD_Failed Int32,	MPFVD_Passed Int32,	MPFVD_Int Int32,	MPFVD_Failed Int32,

#                     zonalSupervisor String,	alltm18yr Int32,	govtWorker Int32,	mtLocalmem Int32,	mtFemale Int32,	HRMP Int32,	HRMP_status String,	NomadsChild Int32,	NomadicSett Int32,	BriklinsChild Int32,	BriklinsSett Int32,	SeasonalMigChild Int32,

#                     SeasonalMigSett Int32,	AgrWorkersChild Int32,	AgrWorkersSett Int32,	Other Int32,	IDPChild Int32,	IDPSett Int32,	RefugeesChild Int32,	RefugeesSett Int32,	alltmTrg Int32,	ddmCard Int32,
                        
#                     UpecHeld Int32,	UpecDate DateTime('Asia/Karachi'),	ucmo Int32,	ucSecratry Int32,	shoUpec Int32,	scVerified Int32,	lastCampDose0 Int32,	Dose0Cov Int32,	mov_mp_uc_staf Int32,	mov_aic_train Int32,	mov_mp_target Int32,	mov_hr Int32,	mov_upec Int32,

#                     mov_zero_dose Int32,	mov_team_train Int32,	vacc_carr_avbl Int32,	vacc_carr_req Int32,	mov_mp_dist_staf Int32,	upecSign Int32,	

#                     day1 DateTime('Asia/Karachi'),	dpec DateTime('Asia/Karachi'),	dco Int32,	edo Int32,	dpo_dpec Int32,	allMember Int32,	rmDate DateTime('Asia/Karachi'),	rmDC Int32,	rmDHO Int32,	rmSecurity Int32,	rmAllMembers Int32,
                        
#                     districtReady Int32,	resheduleDate DateTime('Asia/Karachi'),	actionLPUC Int32,	actionType Int32,	reviewSIA Int32,
                        
#                     inaugrated_by String,	OPVVilesReceived Int32,	VaccArival DateTime('Asia/Karachi'),	TeleSheets Int32,	FingerMarker_before_sia Int32,	FingerMarker Int32,	SmMatrialReceived DateTime('Asia/Karachi'),
                        
#                     avbl_vacc_carrier Int32,	mov_dpec Int32,	mov_redines Int32,	VaccineType String,

#                     Remarks String,	status Int32,	trash Int32,	isSync Int32,

#                     campaign_ID Int32, campaign_ActivityName String, campaign_ActivityID_old Int32, campaign_Yr Int32, campaign_SubActivityName String,
#                     location_code Int32
#                     ,geoLocation_name String, geoLocation_type Int32, geoLocation_code Int32, geoLocation_census_pop Int32, geoLocation_target Int32, geoLocation_status Int32, geoLocation_pname String, geoLocation_dname String, geoLocation_namedistrict String, geoLocation_codedistrict String, geoLocation_tname String, geoLocation_provincecode Int32, geoLocation_districtcode Int32, geoLocation_tehsilcode Int32, geoLocation_priority Int32, geoLocation_commnet Int32, geoLocation_hr Int32, geoLocation_fcm Int32, geoLocation_tier Int32, geoLocation_block String, geoLocation_division String, geoLocation_cordinates String, geoLocation_latitude String, geoLocation_longitude String, geoLocation_x String, geoLocation_y String, geoLocation_imagepath String, geoLocation_isccpv Int32, geoLocation_rank Int32, geoLocation_rank_score String, geoLocation_ishealthcamp Int32, geoLocation_isdsc Int32, geoLocation_ucorg String, geoLocation_organization String, geoLocation_tierfromaug161 Int32, geoLocation_tierfromsep171 Int32, geoLocation_tierfromdec181 Int32, geoLocation_mtap Int32, geoLocation_rspuc Int32, geoLocation_issmt Int32, geoLocation_updateddatetime String, geoLocation_x_code Int32, geoLocation_draining_uc Int32, geoLocation_upap_districts Int32, geoLocation_shruc Int32, geoLocation_khidist_id String
#                     ,location_id Int32, location_name String, location_type String, location_status Int32

#                     )ENGINE = MergeTree PRIMARY KEY ID ORDER BY ID"""
#     client.execute(sql)

#     df = client.query_dataframe(
#                 """
#                     SELECT ID,UserName,ActivityID,TimeStamp,Yr,ProvID,DivID,DistID,
                    
#                     TehsilID,UCID,	IDcampCat,DateAssessment,targeted,DaysBeforeActivity,UCMORounds,inaccCh,areasInacc,	mobileTeams,fixedTeams,TransitPoints--,	
#                     ,HH011_MP,HH1259_MP,School_MP,Fixed_MP,Transit_MP,areaIcs,aicFemale,	aicGovt,aicRounds,aicTrained,AICSelfA,stall_method_trg	
#                     ,mov_stall_method_trg,MPDR_Passed,MPDR_Int,MPDR_Failed,MPFV_Passed,MPFV_Int,MPFV_Failed,MPDRD_Passed,MPDRD_Int,MPDRD_Failed,MPFVD_Passed,MPFVD_Int,MPFVD_Failed	
#                     ,zonalSupervisor,alltm18yr,govtWorker,mtLocalmem,mtFemale,HRMP,HRMP_status,NomadsChild,NomadicSett,BriklinsChild,BriklinsSett,SeasonalMigChild	
#                     ,SeasonalMigSett,AgrWorkersChild,AgrWorkersSett,Other,IDPChild,IDPSett,RefugeesChild,RefugeesSett,alltmTrg,ddmCard	
#                     ,UpecHeld,UpecDate,ucmo,ucSecratry,shoUpec,scVerified,lastCampDose0,Dose0Cov,mov_mp_uc_staf,mov_aic_train,mov_mp_target,mov_hr,mov_upec	
#                     ,mov_zero_dose,mov_team_train,vacc_carr_avbl,vacc_carr_req,mov_mp_dist_staf,upecSign
                    
#                     ,NULL AS day1,	NULL AS dpec,	NULL AS dco,	NULL AS edo,	NULL AS dpo_dpec,	NULL AS allMember,	NULL AS rmDate,	NULL AS rmDC,	NULL AS rmDHO,	NULL AS rmSecurity,	NULL AS rmAllMembers,	
#                     NULL AS districtReady,	NULL AS resheduleDate,	NULL AS actionLPUC,	NULL AS actionType,	NULL AS reviewSIA,	
#                     NULL AS inaugrated_by,	NULL AS OPVVilesReceived,	NULL AS VaccArival,	NULL AS TeleSheets,	NULL AS FingerMarker_before_sia,	NULL AS FingerMarker,	NULL AS SmMatrialReceived,	
#                     NULL AS avbl_vacc_carrier, NULL AS mov_dpec,	NULL AS mov_redines,	
#                     NULL AS VaccineType,
                    
#                     remarks As Remarks,status,	trash,	isSync	
                    
#                     ,campaign_ID, campaign_ActivityName, campaign_ActivityID_old, campaign_Yr, campaign_SubActivityName
                   
#                     ,location_code
#                     ,geoLocation_name, geoLocation_type, geoLocation_code, geoLocation_census_pop, geoLocation_target, geoLocation_status, geoLocation_pname, geoLocation_dname, geoLocation_namedistrict, geoLocation_codedistrict, geoLocation_tname, geoLocation_provincecode, geoLocation_districtcode, geoLocation_tehsilcode, geoLocation_priority, geoLocation_commnet, geoLocation_hr, geoLocation_fcm, geoLocation_tier, geoLocation_block, geoLocation_division, geoLocation_cordinates, geoLocation_latitude, geoLocation_longitude, geoLocation_x, geoLocation_y, geoLocation_imagepath, geoLocation_isccpv, geoLocation_rank, geoLocation_rank_score, geoLocation_ishealthcamp, geoLocation_isdsc, geoLocation_ucorg, geoLocation_organization, geoLocation_tierfromaug161, geoLocation_tierfromsep171, geoLocation_tierfromdec181, geoLocation_mtap, geoLocation_rspuc, geoLocation_issmt, geoLocation_updateddatetime, geoLocation_x_code, geoLocation_draining_uc, geoLocation_upap_districts, geoLocation_shruc, geoLocation_khidist_id
#                     ,NULL AS location_id, NULL AS location_name, NULL AS location_type, NULL AS location_status
                    
#                     FROM test.xbi_neapUcPlan 
                    
#                     UNION ALL

#                     SELECT ID,UserName,ActivityID,TimeStamp,Yr,prov_id AS ProvID,div_id AS DivID,dist_id AS DistID
                   
#                     ,NULL AS TehsilID, NULL AS UCID,	NULL AS IDcampCat,NULL AS DateAssessment,NULL AS targeted,NULL AS DaysBeforeActivity,NULL AS UCMORounds,NULL AS inaccCh,NULL AS areasInacc,	NULL AS mobileTeams,NULL AS fixedTeams,NULL AS TransitPoints--,	
#                     ,NULL AS HH011_MP,NULL AS HH1259_MP,NULL AS School_MP,NULL AS Fixed_MP,NULL AS Transit_MP,NULL AS areaIcs,NULL AS aicFemale,NULL AS aicGovt,NULL AS aicRounds,NULL AS aicTrained,NULL AS AICSelfA,NULL AS stall_method_trg	
#                     ,NULL AS mov_stall_method_trg,NULL AS MPDR_Passed,NULL AS MPDR_Int,NULL AS MPDR_Failed,NULL AS MPFV_Passed,NULL AS MPFV_Int,NULL AS MPFV_Failed,NULL AS MPDRD_Passed,NULL AS MPDRD_Int,NULL AS MPDRD_Failed,NULL AS MPFVD_Passed,NULL AS MPFVD_Int,NULL AS MPFVD_Failed	
#                     ,NULL AS zonalSupervisor,NULL AS alltm18yr,NULL AS govtWorker,NULL AS mtLocalmem,NULL AS mtFemale,NULL AS HRMP,NULL AS HRMP_status,NULL AS NomadsChild,NULL AS NomadicSett,NULL AS BriklinsChild,NULL AS BriklinsSett,NULL AS SeasonalMigChild	
#                     ,NULL AS SeasonalMigSett,NULL AS AgrWorkersChild,NULL AS AgrWorkersSett,NULL AS Other,NULL AS IDPChild,NULL AS IDPSett,NULL AS RefugeesChild,NULL AS RefugeesSett,NULL AS alltmTrg,NULL AS ddmCard	
#                     ,NULL AS UpecHeld,NULL AS UpecDate,NULL AS ucmo,NULL AS ucSecratry,NULL AS shoUpec,NULL AS scVerified,NULL AS lastCampDose0,NULL AS Dose0Cov,NULL AS mov_mp_uc_staf,NULL AS mov_aic_train,NULL AS mov_mp_target,NULL AS mov_hr,NULL AS mov_upec	
#                     ,NULL AS mov_zero_dose,NULL AS mov_team_train,NULL AS vacc_carr_avbl,NULL AS vacc_carr_req,NULL AS mov_mp_dist_staf,NULL AS upecSign
                    
#                     , day1,	dpec,dco,edo,dpo_dpec,allMember,rmDate,rmDC,rmDHO,rmSecurity,rmAllMembers,	
#                     districtReady,	resheduleDate,actionLPUC,actionType,reviewSIA,	
#                     inaugrated_by, OPVVilesReceived,VaccArival,TeleSheets,	FingerMarker_before_sia,FingerMarker,SmMatrialReceived,	
#                     avbl_vacc_carrier,mov_dpec,mov_redines,VaccineType	
                    
#                     ,Remarks,status,trash,isSync
                    
#                     ,campaign_ID, campaign_ActivityName, campaign_ActivityID_old, campaign_Yr, campaign_SubActivityName
                    
#                     ,location_code
#                     ,NULL AS geoLocation_name, NULL AS geoLocation_type, NULL AS geoLocation_code, NULL AS geoLocation_census_pop, NULL AS geoLocation_target, NULL AS geoLocation_status, NULL AS geoLocation_pname, NULL AS geoLocation_dname, NULL AS geoLocation_namedistrict, NULL AS geoLocation_codedistrict, NULL AS geoLocation_tname, NULL AS geoLocation_provincecode, NULL AS geoLocation_districtcode, NULL AS geoLocation_tehsilcode, NULL AS geoLocation_priority, NULL AS geoLocation_commnet, NULL AS geoLocation_hr, NULL AS geoLocation_fcm, NULL AS geoLocation_tier, NULL AS geoLocation_block, NULL AS geoLocation_division, NULL AS geoLocation_cordinates, NULL AS geoLocation_latitude, NULL AS geoLocation_longitude, NULL AS geoLocation_x, NULL AS geoLocation_y, NULL AS geoLocation_imagepath, NULL AS geoLocation_isccpv, NULL AS geoLocation_rank, NULL AS geoLocation_rank_score, NULL AS geoLocation_ishealthcamp, NULL AS geoLocation_isdsc, NULL AS geoLocation_ucorg, NULL AS geoLocation_organization, NULL AS geoLocation_tierfromaug161, NULL AS geoLocation_tierfromsep171, NULL AS geoLocation_tierfromdec181, NULL AS geoLocation_mtap, NULL AS geoLocation_rspuc, NULL AS geoLocation_issmt, NULL AS geoLocation_updateddatetime, NULL AS geoLocation_x_code, NULL AS geoLocation_draining_uc, NULL AS geoLocation_upap_districts, NULL AS geoLocation_shruc, NULL AS geoLocation_khidist_id
#                     ,location_id, location_name, location_type, location_status

#                     FROM test.xbi_neapDistrictPlan 
                    
#                 """)
#     #df['TimeStamp'] = datetime.strptime(df['TimeStamp'], '%d/%m/%y %H:%M:%S')
#     df = df.replace(r'^\s*$', np.nan, regex=True)
#     df['TimeStamp'] = pd.to_datetime(df['TimeStamp'], errors='coerce')
#     df['DateAssessment'] = pd.to_datetime(df['DateAssessment'], errors='coerce')
#     df[["TimeStamp"]] = df[["TimeStamp"]].apply(pd.to_datetime)
#     df[["DateAssessment"]] = df[["DateAssessment"]].apply(pd.to_datetime)         
#     client.insert_dataframe(
#         'INSERT INTO test.get_all_neap  VALUES', df)
#     logger.info(
#             ' Data has been inserted into Table\' INSERT INTO test.get_all_neap VALUES \' ')            
   
#     #---------------- xbi_table --------------------#
#     sql = """CREATE TABLE if not exists test.xbi_neap( 
#                     ID Int32,UserName String,ActivityID Int32,TimeStamp DateTime('Asia/Karachi'),Yr Int32,ProvID Int32,DivID Int32,DistID Int32,
#                     TehsilID Int32,	UCID Int32,	IDcampCat Int32,	DateAssessment DateTime('Asia/Karachi'),	targeted Int32,	DaysBeforeActivity Int32,	UCMORounds Int32,	inaccCh Int32,	areasInacc Int32,	mobileTeams Int32,	fixedTeams Int32,	TransitPoints Int32,
                        
#                     HH011_MP Int32,	HH1259_MP Int32,	School_MP Int32,	Fixed_MP Int32,	Transit_MP Int32,	areaIcs Int32,	aicFemale Int32,	aicGovt Int32,	aicRounds Int32,	aicTrained Int32,	AICSelfA Int32,	stall_method_trg Int32,

#                     mov_stall_method_trg Int32,	MPDR_Passed Int32,	MPDR_Int Int32,	MPDR_Failed Int32,	MPFV_Passed Int32,	MPFV_Int Int32,	MPFV_Failed Int32,	MPDRD_Passed Int32,	MPDRD_Int Int32,	MPDRD_Failed Int32,	MPFVD_Passed Int32,	MPFVD_Int Int32,	MPFVD_Failed Int32,

#                     zonalSupervisor String,	alltm18yr Int32,	govtWorker Int32,	mtLocalmem Int32,	mtFemale Int32,	HRMP Int32,	HRMP_status String,	NomadsChild Int32,	NomadicSett Int32,	BriklinsChild Int32,	BriklinsSett Int32,	SeasonalMigChild Int32,

#                     SeasonalMigSett Int32,	AgrWorkersChild Int32,	AgrWorkersSett Int32,	Other Int32,	IDPChild Int32,	IDPSett Int32,	RefugeesChild Int32,	RefugeesSett Int32,	alltmTrg Int32,	ddmCard Int32,
                        
#                     UpecHeld Int32,	UpecDate DateTime('Asia/Karachi'),	ucmo Int32,	ucSecratry Int32,	shoUpec Int32,	scVerified Int32,	lastCampDose0 Int32,	Dose0Cov Int32,	mov_mp_uc_staf Int32,	mov_aic_train Int32,	mov_mp_target Int32,	mov_hr Int32,	mov_upec Int32,

#                     mov_zero_dose Int32,	mov_team_train Int32,	vacc_carr_avbl Int32,	vacc_carr_req Int32,	mov_mp_dist_staf Int32,	upecSign Int32,	

#                     day1 DateTime('Asia/Karachi'),	dpec DateTime('Asia/Karachi'),	dco Int32,	edo Int32,	dpo_dpec Int32,	allMember Int32,	rmDate DateTime('Asia/Karachi'),	rmDC Int32,	rmDHO Int32,	rmSecurity Int32,	rmAllMembers Int32,
                        
#                     districtReady Int32,	resheduleDate DateTime('Asia/Karachi'),	actionLPUC Int32,	actionType Int32,	reviewSIA Int32,
                        
#                     inaugrated_by String,	OPVVilesReceived Int32,	VaccArival DateTime('Asia/Karachi'),	TeleSheets Int32,	FingerMarker_before_sia Int32,	FingerMarker Int32,	SmMatrialReceived DateTime('Asia/Karachi'),
                        
#                     avbl_vacc_carrier Int32,	mov_dpec Int32,	mov_redines Int32,	VaccineType String,

#                     Remarks String,	status Int32,	trash Int32,	isSync Int32

#                     ,campaign_ID Int32, campaign_ActivityName String, campaign_ActivityID_old Int32, campaign_Yr Int32, campaign_SubActivityName String
                    
#                     ,location_code Int32
#                     ,geoLocation_name String, geoLocation_type Int32, geoLocation_code Int32, geoLocation_census_pop Int32, geoLocation_target Int32, geoLocation_status Int32, geoLocation_pname String, geoLocation_dname String, geoLocation_namedistrict String, geoLocation_codedistrict String, geoLocation_tname String, geoLocation_provincecode Int32, geoLocation_districtcode Int32, geoLocation_tehsilcode Int32, geoLocation_priority Int32, geoLocation_commnet Int32, geoLocation_hr Int32, geoLocation_fcm Int32, geoLocation_tier Int32, geoLocation_block String, geoLocation_division String, geoLocation_cordinates String, geoLocation_latitude String, geoLocation_longitude String, geoLocation_x String, geoLocation_y String, geoLocation_imagepath String, geoLocation_isccpv Int32, geoLocation_rank Int32, geoLocation_rank_score String, geoLocation_ishealthcamp Int32, geoLocation_isdsc Int32, geoLocation_ucorg String, geoLocation_organization String, geoLocation_tierfromaug161 Int32, geoLocation_tierfromsep171 Int32, geoLocation_tierfromdec181 Int32, geoLocation_mtap Int32, geoLocation_rspuc Int32, geoLocation_issmt Int32, geoLocation_updateddatetime String, geoLocation_x_code Int32, geoLocation_draining_uc Int32, geoLocation_upap_districts Int32, geoLocation_shruc Int32, geoLocation_khidist_id String
#                     ,location_id Int32, location_name String, location_type String, location_status Int32
#             )ENGINE = MergeTree PRIMARY KEY ID ORDER BY ID"""
#     client.execute(sql)

#     cols = """
#             eoc_1.ID ,eoc_1.UserName ,eoc_1.ActivityID ,eoc_1.TimeStamp ,eoc_1.Yr ,eoc_1.ProvID ,eoc_1.DivID ,eoc_1.DistID ,
#             eoc_1.TehsilID ,	eoc_1.UCID ,	eoc_1.IDcampCat ,	eoc_1.DateAssessment ,	eoc_1.targeted ,	eoc_1.DaysBeforeActivity ,	eoc_1.UCMORounds ,	eoc_1.inaccCh ,	eoc_1.areasInacc ,	eoc_1.mobileTeams ,	eoc_1.fixedTeams ,	eoc_1.TransitPoints ,
                
#             eoc_1.HH011_MP ,	eoc_1.HH1259_MP ,	eoc_1.School_MP ,	eoc_1.Fixed_MP ,	eoc_1.Transit_MP ,	eoc_1.areaIcs ,	eoc_1.aicFemale ,	eoc_1.aicGovt ,	eoc_1.aicRounds ,	eoc_1.aicTrained ,	eoc_1.AICSelfA ,	eoc_1.stall_method_trg ,

#             eoc_1.mov_stall_method_trg ,	eoc_1.MPDR_Passed ,	eoc_1.MPDR_Int ,	eoc_1.MPDR_Failed ,	eoc_1.MPFV_Passed ,	eoc_1.MPFV_Int ,	eoc_1.MPFV_Failed ,	eoc_1.MPDRD_Passed ,	eoc_1.MPDRD_Int ,	eoc_1.MPDRD_Failed ,	eoc_1.MPFVD_Passed ,	eoc_1.MPFVD_Int ,	eoc_1.MPFVD_Failed ,

#             eoc_1.zonalSupervisor ,	eoc_1.alltm18yr ,	eoc_1.govtWorker ,	eoc_1.mtLocalmem ,	eoc_1.mtFemale ,	eoc_1.HRMP ,	eoc_1.HRMP_status ,	eoc_1.NomadsChild ,	eoc_1.NomadicSett ,	eoc_1.BriklinsChild ,	eoc_1.BriklinsSett ,	eoc_1.SeasonalMigChild ,

#             eoc_1.SeasonalMigSett ,	eoc_1.AgrWorkersChild ,	eoc_1.AgrWorkersSett ,	eoc_1.Other ,	eoc_1.IDPChild ,	eoc_1.IDPSett ,	eoc_1.RefugeesChild ,	eoc_1.RefugeesSett ,	eoc_1.alltmTrg ,	eoc_1.ddmCard ,
                
#             eoc_1.UpecHeld ,	eoc_1.UpecDate ,	eoc_1.ucmo ,	eoc_1.ucSecratry ,	eoc_1.shoUpec ,	eoc_1.scVerified ,	eoc_1.lastCampDose0 ,	eoc_1.Dose0Cov ,	eoc_1.mov_mp_uc_staf ,	eoc_1.mov_aic_train ,	eoc_1.mov_mp_target ,	eoc_1.mov_hr ,	eoc_1.mov_upec ,

#             eoc_1.mov_zero_dose ,	eoc_1.mov_team_train ,	eoc_1.vacc_carr_avbl ,	eoc_1.vacc_carr_req ,	eoc_1.mov_mp_dist_staf ,	eoc_1.upecSign ,	

#             eoc_1.day1 ,	eoc_1.dpec ,	eoc_1.dco ,	eoc_1.edo ,	eoc_1.dpo_dpec ,	eoc_1.allMember ,	eoc_1.rmDate ,	eoc_1.rmDC ,	eoc_1.rmDHO ,	eoc_1.rmSecurity ,	eoc_1.rmAllMembers ,
                
#             eoc_1.districtReady ,	eoc_1.resheduleDate ,	eoc_1.actionLPUC ,	eoc_1.actionType ,	eoc_1.reviewSIA ,
                
#             eoc_1.inaugrated_by ,	eoc_1.OPVVilesReceived ,	eoc_1.VaccArival ,	eoc_1.TeleSheets ,	eoc_1.FingerMarker_before_sia ,	eoc_1.FingerMarker ,	eoc_1.SmMatrialReceived ,
                
#             eoc_1.avbl_vacc_carrier ,	eoc_1.mov_dpec ,	eoc_1.mov_redines ,	eoc_1.VaccineType ,

#             eoc_1.Remarks ,	eoc_1.status ,	eoc_1.trash ,	eoc_1.isSync 

#             ,eoc_1.campaign_ID, eoc_1.campaign_ActivityName, eoc_1.campaign_ActivityID_old, eoc_1.campaign_Yr, eoc_1.campaign_SubActivityName,  
            
#             eoc_1.location_code ,
            
#             eoc_1.geoLocation_name, eoc_1.geoLocation_type, eoc_1.geoLocation_code, eoc_1.geoLocation_census_pop, eoc_1.geoLocation_target, eoc_1.geoLocation_status, eoc_1.geoLocation_pname, eoc_1.geoLocation_dname, eoc_1.geoLocation_namedistrict, eoc_1.geoLocation_codedistrict, eoc_1.geoLocation_tname, geoLocation_provincecode, eoc_1.geoLocation_districtcode, eoc_1.geoLocation_tehsilcode, eoc_1.geoLocation_priority, eoc_1.geoLocation_commnet, eoc_1.geoLocation_hr, eoc_1.geoLocation_fcm, eoc_1.geoLocation_tier, eoc_1.geoLocation_block, eoc_1.geoLocation_division, eoc_1.geoLocation_cordinates, eoc_1.geoLocation_latitude, eoc_1.geoLocation_longitude, eoc_1.geoLocation_x, eoc_1.geoLocation_y, eoc_1.geoLocation_imagepath, eoc_1.geoLocation_isccpv, eoc_1.geoLocation_rank, eoc_1.geoLocation_rank_score, eoc_1.geoLocation_ishealthcamp, eoc_1.geoLocation_isdsc, eoc_1.geoLocation_ucorg, eoc_1.geoLocation_organization, eoc_1.geoLocation_tierfromaug161, eoc_1.geoLocation_tierfromsep171, eoc_1.geoLocation_tierfromdec181, eoc_1.geoLocation_mtap, eoc_1.geoLocation_rspuc, eoc_1.geoLocation_issmt, eoc_1.geoLocation_updateddatetime, eoc_1.geoLocation_x_code, eoc_1.geoLocation_draining_uc, eoc_1.geoLocation_upap_districts, eoc_1.geoLocation_shruc, eoc_1.geoLocation_khidist_id,
        
#             eoc_1.location_id, eoc_1.location_name, eoc_1.location_type, eoc_1.location_status
#             """
#     sql = "SELECT " + cols + "  FROM test.get_all_neap  eoc_1" #left JOIN test.eoc_geolocation_t eoc_1 ON eoc_1.location_code  = eoc_1.code left JOIN test.xbi_geolocation gl1 ON (eoc_1.DistID = gl1.ID) left JOIN test.xbi_campaign eoc_1 ON (eoc_1.ActivityID  = eoc_1.campaign_ActivityID_old And eoc_1.Yr = eoc_1.campaign_Yr) "
#     data = client.execute(sql)

#     xbiDataFrame = pd.DataFrame(data)
#     all_columns = list(xbiDataFrame)  # Creates list of all column headers
#     cols = xbiDataFrame.iloc[0]

#     xbiDataFrame[all_columns] = xbiDataFrame[all_columns].astype(str)

#     d = [
#         'ID' ,'UserName' ,'ActivityID' ,'TimeStamp' ,'Yr' ,'ProvID' ,'DivID' ,'DistID' ,
#         'TehsilID' ,	'UCID' ,	'IDcampCat' ,	'DateAssessment' ,	'targeted' ,	'DaysBeforeActivity' ,	'UCMORounds' ,	'inaccCh' ,	'areasInacc' ,	'mobileTeams' ,	'fixedTeams' ,	'TransitPoints' ,
            
#         'HH011_MP' ,	'HH1259_MP' ,	'School_MP' ,	'Fixed_MP' ,	'Transit_MP' ,	'areaIcs' ,	'aicFemale' ,	'aicGovt' ,	'aicRounds' ,	'aicTrained',	'AICSelfA' ,	'stall_method_trg' ,

#         'mov_stall_method_trg' ,	'MPDR_Passed' ,	'MPDR_Int' ,	'MPDR_Failed' ,	'MPFV_Passed' ,	'MPFV_Int' ,	'MPFV_Failed' ,	'MPDRD_Passed' ,	'MPDRD_Int' ,	'MPDRD_Failed' , 'MPFVD_Passed' ,	'MPFVD_Int' ,	'MPFVD_Failed' ,

#         'zonalSupervisor' ,	'alltm18yr' ,	'govtWorker' ,	'mtLocalmem' ,	'mtFemale' ,	'HRMP' ,	'HRMP_status' ,	'NomadsChild' ,	'NomadicSett' ,	'BriklinsChild' ,	'BriklinsSett' ,	'SeasonalMigChild' ,

#         'SeasonalMigSett' ,	'AgrWorkersChild' ,	'AgrWorkersSett' ,	'Other' ,	'IDPChild' ,	'IDPSett' ,	'RefugeesChild' ,	'RefugeesSett' ,	'alltmTrg' ,	'ddmCard' ,
            
#         'UpecHeld' ,	'UpecDate' ,	'ucmo' ,	'ucSecratry' ,	'shoUpec' ,	'scVerified' ,	'lastCampDose0' ,	'Dose0Cov' ,	'mov_mp_uc_staf' ,	'mov_aic_train' ,	'mov_mp_target' ,	'mov_hr' ,	'mov_upec' ,

#         'mov_zero_dose' ,	'mov_team_train' ,	'vacc_carr_avbl' ,	'vacc_carr_req' ,	'mov_mp_dist_staf' ,	'upecSign' ,	

#         'day1' ,	'dpec' ,	'dco' ,	'edo' ,	'dpo_dpec' ,	'allMember' ,	'rmDate' ,	'rmDC' ,	'rmDHO' ,	'rmSecurity' ,	'rmAllMembers' ,
            
#         'districtReady' ,	'resheduleDate' ,	'actionLPUC' ,	'actionType' ,	'reviewSIA' ,
            
#         'inaugrated_by' ,	'OPVVilesReceived' ,	'VaccArival' ,	'TeleSheets' ,	'FingerMarker_before_sia' ,	'FingerMarker' ,	'SmMatrialReceived' ,
            
#         'avbl_vacc_carrier' ,	'mov_dpec' ,	'mov_redines',	'VaccineType' ,

#         'Remarks' ,	'status' ,	'trash' ,	'isSync' 

#         ,'campaign_ID', 'campaign_ActivityName', 'campaign_ActivityID_old', 'campaign_Yr', 'campaign_SubActivityName'
#         ,'location_code' 
#         ,'geoLocation_name', 'geoLocation_type', 'geoLocation_code', 'geoLocation_census_pop', 'geoLocation_target', 'geoLocation_status', 'geoLocation_pname', 'geoLocation_dname', 'geoLocation_namedistrict', 'geoLocation_codedistrict', 'geoLocation_tname', 'geoLocation_provincecode', 'geoLocation_districtcode', 'geoLocation_tehsilcode', 'geoLocation_priority', 'geoLocation_commnet', 'geoLocation_hr', 'geoLocation_fcm', 'geoLocation_tier', 'geoLocation_block', 'geoLocation_division', 'geoLocation_cordinates', 'geoLocation_latitude', 'geoLocation_longitude', 'geoLocation_x', 'geoLocation_y', 'geoLocation_imagepath', 'geoLocation_isccpv', 'geoLocation_rank', 'geoLocation_rank_score', 'geoLocation_ishealthcamp', 'geoLocation_isdsc', 'geoLocation_ucorg', 'geoLocation_organization', 'geoLocation_tierfromaug161', 'geoLocation_tierfromsep171', 'geoLocation_tierfromdec181', 'geoLocation_mtap', 'geoLocation_rspuc', 'geoLocation_issmt', 'geoLocation_updateddatetime', 'geoLocation_x_code', 'geoLocation_draining_uc', 'geoLocation_upap_districts', 'geoLocation_shruc', 'geoLocation_khidist_id' 
#         ,'location_id',  'location_name','location_type','location_status'
#         ]
#     dff = pd.DataFrame(columns=d)
#     for index, item in enumerate(d):
#         dff[item] = xbiDataFrame[index].values
#     logger.info(
#             'Get Data from test.get_all_neap for campaign')#+str(cid))

#     df3 = client.query_dataframe(
#                     "SELECT * FROM test.xbi_neap")
#     if df3.empty:
#         client.insert_dataframe(
#             'INSERT INTO test.xbi_neap VALUES', dff)
#         logger.info(
#             'Data has been inserted into Table test.xbi_neap for campaign ')#+str(cid))

#         sql = "DROP table if exists test.get_all_neap"
#         client.execute(sql)
    
#     else:
#         client.insert_dataframe(
#             'INSERT INTO test.xbi_neapVALUES', dff)
#         logger.info(
#             ' Data has been inserted into Table test.xbi_neap for campaign ')#+str(cid))    

#         sql = "DROP table if exists test.get_all_neap"
#         client.execute(sql)
                       
# dag = DAG(
#     'All-Neap_Automated',
#     schedule_interval=None, 
#     #schedule_interval='0 0 * * *', 
#     #schedule_interval='*/10 * * * *',# will run every 10 min.
#     default_args=default_args,
#     catchup=False)

# with dag:
#     GetAndInsertDataAllNeap = PythonOperator(
#         task_id='GetAndInsertDataAllNeap',
#         python_callable=GetAndInsertDataAllNeap,
#     )
# GetAndInsertDataAllNeap



#-------------------------------------------------------------------------------------------------------------------------------#
#-------------------------------------------------- GENERATE DAG FILES  --------------------------------------------------------#
#------------------------------------------------ Author: BABAR ALI SHAH -------------------------------------------------------#
#-------------------------------------------------------------------------------------------------------------------------------#

def GetAndInsertApiDataAllNeap():
    logger.info('Function \' GetAndInsertApiDataAllNeap \' Started Off')
    client = Client(host='161.97.136.95',
                    user='default',
                    password='pakistan',
                    port='9000', settings={"use_numpy": True})

    sql = """CREATE TABLE if not exists test.get_all_neap ( 
                    ID Int32,UserName String,ActivityID Int32,TimeStamp DateTime('Asia/Karachi'),Yr Int32,ProvID Int32,DivID Int32,DistID Int32,
                    TehsilID Int32,	UCID Int32,	IDcampCat Int32,	DateAssessment DateTime('Asia/Karachi'),	targeted Int32,	DaysBeforeActivity Int32,	UCMORounds Int32,	inaccCh Int32,	areasInacc Int32,	mobileTeams Int32,	fixedTeams Int32,	TransitPoints Int32,
                        
                    HH011_MP Int32,	HH1259_MP Int32,	School_MP Int32,	Fixed_MP Int32,	Transit_MP Int32,	areaIcs Int32,	aicFemale Int32,	aicGovt Int32,	aicRounds Int32,	aicTrained Int32,	AICSelfA Int32,	stall_method_trg Int32,

                    mov_stall_method_trg Int32,	MPDR_Passed Int32,	MPDR_Int Int32,	MPDR_Failed Int32,	MPFV_Passed Int32,	MPFV_Int Int32,	MPFV_Failed Int32,	MPDRD_Passed Int32,	MPDRD_Int Int32,	MPDRD_Failed Int32,	MPFVD_Passed Int32,	MPFVD_Int Int32,	MPFVD_Failed Int32,

                    zonalSupervisor String,	alltm18yr Int32,	govtWorker Int32,	mtLocalmem Int32,	mtFemale Int32,	HRMP Int32,	HRMP_status String,	NomadsChild Int32,	NomadicSett Int32,	BriklinsChild Int32,	BriklinsSett Int32,	SeasonalMigChild Int32,

                    SeasonalMigSett Int32,	AgrWorkersChild Int32,	AgrWorkersSett Int32,	Other Int32,	IDPChild Int32,	IDPSett Int32,	RefugeesChild Int32,	RefugeesSett Int32,	alltmTrg Int32,	ddmCard Int32,
                        
                    UpecHeld Int32,	UpecDate DateTime('Asia/Karachi'),	ucmo Int32,	ucSecratry Int32,	shoUpec Int32,	scVerified Int32,	lastCampDose0 Int32,	Dose0Cov Int32,	mov_mp_uc_staf Int32,	mov_aic_train Int32,	mov_mp_target Int32,	mov_hr Int32,	mov_upec Int32,

                    mov_zero_dose Int32,	mov_team_train Int32,	vacc_carr_avbl Int32,	vacc_carr_req Int32,	mov_mp_dist_staf Int32,	upecSign Int32,	

                    day1 DateTime('Asia/Karachi'),	dpec DateTime('Asia/Karachi'),	dco Int32,	edo Int32,	dpo_dpec Int32,	allMember Int32,	rmDate DateTime('Asia/Karachi'),	rmDC Int32,	rmDHO Int32,	rmSecurity Int32,	rmAllMembers Int32,
                        
                    districtReady Int32,	resheduleDate DateTime('Asia/Karachi'),	actionLPUC Int32,	actionType Int32,	reviewSIA Int32,
                        
                    inaugrated_by String,	OPVVilesReceived Int32,	VaccArival DateTime('Asia/Karachi'),	TeleSheets Int32,	FingerMarker_before_sia Int32,	FingerMarker Int32,	SmMatrialReceived DateTime('Asia/Karachi'),
                        
                    avbl_vacc_carrier Int32,	mov_dpec Int32,	mov_redines Int32,	VaccineType String,

                    Remarks String,	status Int32,	trash Int32,	isSync Int32,

                    campaign_ID Int32, campaign_ActivityName String, campaign_ActivityID_old Int32, campaign_Yr Int32, campaign_SubActivityName String,
                    location_code Int32
                    ,geoLocation_name String, geoLocation_type Int32, geoLocation_code Int32, geoLocation_census_pop Int32, geoLocation_target Int32, geoLocation_status Int32, geoLocation_pname String, geoLocation_dname String, geoLocation_namedistrict String, geoLocation_codedistrict String, geoLocation_tname String, geoLocation_provincecode Int32, geoLocation_districtcode Int32, geoLocation_tehsilcode Int32, geoLocation_priority Int32, geoLocation_commnet Int32, geoLocation_hr Int32, geoLocation_fcm Int32, geoLocation_tier Int32, geoLocation_block String, geoLocation_division String, geoLocation_cordinates String, geoLocation_latitude String, geoLocation_longitude String, geoLocation_x String, geoLocation_y String, geoLocation_imagepath String, geoLocation_isccpv Int32, geoLocation_rank Int32, geoLocation_rank_score String, geoLocation_ishealthcamp Int32, geoLocation_isdsc Int32, geoLocation_ucorg String, geoLocation_organization String, geoLocation_tierfromaug161 Int32, geoLocation_tierfromsep171 Int32, geoLocation_tierfromdec181 Int32, geoLocation_mtap Int32, geoLocation_rspuc Int32, geoLocation_issmt Int32, geoLocation_updateddatetime String, geoLocation_x_code Int32, geoLocation_draining_uc Int32, geoLocation_upap_districts Int32, geoLocation_shruc Int32, geoLocation_khidist_id String
                    ,location_id Int32, location_name String, location_type String, location_status Int32

                    )ENGINE = MergeTree PRIMARY KEY ID ORDER BY ID"""
    client.execute(sql)

    df = client.query_dataframe(
                """
                    SELECT ID,UserName,ActivityID,TimeStamp,Yr,ProvID,DivID,DistID,
                    
                    TehsilID,UCID,	IDcampCat,DateAssessment,targeted,DaysBeforeActivity,UCMORounds,inaccCh,areasInacc,	mobileTeams,fixedTeams,TransitPoints--,	
                    ,HH011_MP,HH1259_MP,School_MP,Fixed_MP,Transit_MP,areaIcs,aicFemale,	aicGovt,aicRounds,aicTrained,AICSelfA,stall_method_trg	
                    ,mov_stall_method_trg,MPDR_Passed,MPDR_Int,MPDR_Failed,MPFV_Passed,MPFV_Int,MPFV_Failed,MPDRD_Passed,MPDRD_Int,MPDRD_Failed,MPFVD_Passed,MPFVD_Int,MPFVD_Failed	
                    ,zonalSupervisor,alltm18yr,govtWorker,mtLocalmem,mtFemale,HRMP,HRMP_status,NomadsChild,NomadicSett,BriklinsChild,BriklinsSett,SeasonalMigChild	
                    ,SeasonalMigSett,AgrWorkersChild,AgrWorkersSett,Other,IDPChild,IDPSett,RefugeesChild,RefugeesSett,alltmTrg,ddmCard	
                    ,UpecHeld,UpecDate,ucmo,ucSecratry,shoUpec,scVerified,lastCampDose0,Dose0Cov,mov_mp_uc_staf,mov_aic_train,mov_mp_target,mov_hr,mov_upec	
                    ,mov_zero_dose,mov_team_train,vacc_carr_avbl,vacc_carr_req,mov_mp_dist_staf,upecSign
                    
                    ,NULL AS day1,	NULL AS dpec,	NULL AS dco,	NULL AS edo,	NULL AS dpo_dpec,	NULL AS allMember,	NULL AS rmDate,	NULL AS rmDC,	NULL AS rmDHO,	NULL AS rmSecurity,	NULL AS rmAllMembers,	
                    NULL AS districtReady,	NULL AS resheduleDate,	NULL AS actionLPUC,	NULL AS actionType,	NULL AS reviewSIA,	
                    NULL AS inaugrated_by,	NULL AS OPVVilesReceived,	NULL AS VaccArival,	NULL AS TeleSheets,	NULL AS FingerMarker_before_sia,	NULL AS FingerMarker,	NULL AS SmMatrialReceived,	
                    NULL AS avbl_vacc_carrier, NULL AS mov_dpec,	NULL AS mov_redines,	
                    NULL AS VaccineType,
                    
                    remarks As Remarks,status,	trash,	isSync	
                    
                    ,campaign_ID, campaign_ActivityName, campaign_ActivityID_old, campaign_Yr, campaign_SubActivityName
                   
                    ,location_code
                    ,geoLocation_name, geoLocation_type, geoLocation_code, geoLocation_census_pop, geoLocation_target, geoLocation_status, geoLocation_pname, geoLocation_dname, geoLocation_namedistrict, geoLocation_codedistrict, geoLocation_tname, geoLocation_provincecode, geoLocation_districtcode, geoLocation_tehsilcode, geoLocation_priority, geoLocation_commnet, geoLocation_hr, geoLocation_fcm, geoLocation_tier, geoLocation_block, geoLocation_division, geoLocation_cordinates, geoLocation_latitude, geoLocation_longitude, geoLocation_x, geoLocation_y, geoLocation_imagepath, geoLocation_isccpv, geoLocation_rank, geoLocation_rank_score, geoLocation_ishealthcamp, geoLocation_isdsc, geoLocation_ucorg, geoLocation_organization, geoLocation_tierfromaug161, geoLocation_tierfromsep171, geoLocation_tierfromdec181, geoLocation_mtap, geoLocation_rspuc, geoLocation_issmt, geoLocation_updateddatetime, geoLocation_x_code, geoLocation_draining_uc, geoLocation_upap_districts, geoLocation_shruc, geoLocation_khidist_id
                    ,NULL AS location_id, NULL AS location_name, NULL AS location_type, NULL AS location_status
                    
                    FROM test.xbi_neapUcPlan 
                    WHERE ActivityID = 2 and Yr = 2022
                    UNION ALL

                    SELECT ID,UserName,ActivityID,TimeStamp,Yr,prov_id AS ProvID,div_id AS DivID,dist_id AS DistID
                   
                    ,NULL AS TehsilID, NULL AS UCID,	NULL AS IDcampCat,NULL AS DateAssessment,NULL AS targeted,NULL AS DaysBeforeActivity,NULL AS UCMORounds,NULL AS inaccCh,NULL AS areasInacc,	NULL AS mobileTeams,NULL AS fixedTeams,NULL AS TransitPoints--,	
                    ,NULL AS HH011_MP,NULL AS HH1259_MP,NULL AS School_MP,NULL AS Fixed_MP,NULL AS Transit_MP,NULL AS areaIcs,NULL AS aicFemale,NULL AS aicGovt,NULL AS aicRounds,NULL AS aicTrained,NULL AS AICSelfA,NULL AS stall_method_trg	
                    ,NULL AS mov_stall_method_trg,NULL AS MPDR_Passed,NULL AS MPDR_Int,NULL AS MPDR_Failed,NULL AS MPFV_Passed,NULL AS MPFV_Int,NULL AS MPFV_Failed,NULL AS MPDRD_Passed,NULL AS MPDRD_Int,NULL AS MPDRD_Failed,NULL AS MPFVD_Passed,NULL AS MPFVD_Int,NULL AS MPFVD_Failed	
                    ,NULL AS zonalSupervisor,NULL AS alltm18yr,NULL AS govtWorker,NULL AS mtLocalmem,NULL AS mtFemale,NULL AS HRMP,NULL AS HRMP_status,NULL AS NomadsChild,NULL AS NomadicSett,NULL AS BriklinsChild,NULL AS BriklinsSett,NULL AS SeasonalMigChild	
                    ,NULL AS SeasonalMigSett,NULL AS AgrWorkersChild,NULL AS AgrWorkersSett,NULL AS Other,NULL AS IDPChild,NULL AS IDPSett,NULL AS RefugeesChild,NULL AS RefugeesSett,NULL AS alltmTrg,NULL AS ddmCard	
                    ,NULL AS UpecHeld,NULL AS UpecDate,NULL AS ucmo,NULL AS ucSecratry,NULL AS shoUpec,NULL AS scVerified,NULL AS lastCampDose0,NULL AS Dose0Cov,NULL AS mov_mp_uc_staf,NULL AS mov_aic_train,NULL AS mov_mp_target,NULL AS mov_hr,NULL AS mov_upec	
                    ,NULL AS mov_zero_dose,NULL AS mov_team_train,NULL AS vacc_carr_avbl,NULL AS vacc_carr_req,NULL AS mov_mp_dist_staf,NULL AS upecSign
                    
                    , day1,	dpec,dco,edo,dpo_dpec,allMember,rmDate,rmDC,rmDHO,rmSecurity,rmAllMembers,	
                    districtReady,	resheduleDate,actionLPUC,actionType,reviewSIA,	
                    inaugrated_by, OPVVilesReceived,VaccArival,TeleSheets,	FingerMarker_before_sia,FingerMarker,SmMatrialReceived,	
                    avbl_vacc_carrier,mov_dpec,mov_redines,VaccineType	
                    
                    ,Remarks,status,trash,isSync
                    
                    ,campaign_ID, campaign_ActivityName, campaign_ActivityID_old, campaign_Yr, campaign_SubActivityName
                    
                    ,location_code
                    ,NULL AS geoLocation_name, NULL AS geoLocation_type, NULL AS geoLocation_code, NULL AS geoLocation_census_pop, NULL AS geoLocation_target, NULL AS geoLocation_status, NULL AS geoLocation_pname, NULL AS geoLocation_dname, NULL AS geoLocation_namedistrict, NULL AS geoLocation_codedistrict, NULL AS geoLocation_tname, NULL AS geoLocation_provincecode, NULL AS geoLocation_districtcode, NULL AS geoLocation_tehsilcode, NULL AS geoLocation_priority, NULL AS geoLocation_commnet, NULL AS geoLocation_hr, NULL AS geoLocation_fcm, NULL AS geoLocation_tier, NULL AS geoLocation_block, NULL AS geoLocation_division, NULL AS geoLocation_cordinates, NULL AS geoLocation_latitude, NULL AS geoLocation_longitude, NULL AS geoLocation_x, NULL AS geoLocation_y, NULL AS geoLocation_imagepath, NULL AS geoLocation_isccpv, NULL AS geoLocation_rank, NULL AS geoLocation_rank_score, NULL AS geoLocation_ishealthcamp, NULL AS geoLocation_isdsc, NULL AS geoLocation_ucorg, NULL AS geoLocation_organization, NULL AS geoLocation_tierfromaug161, NULL AS geoLocation_tierfromsep171, NULL AS geoLocation_tierfromdec181, NULL AS geoLocation_mtap, NULL AS geoLocation_rspuc, NULL AS geoLocation_issmt, NULL AS geoLocation_updateddatetime, NULL AS geoLocation_x_code, NULL AS geoLocation_draining_uc, NULL AS geoLocation_upap_districts, NULL AS geoLocation_shruc, NULL AS geoLocation_khidist_id
                    ,location_id, location_name, location_type, location_status

                    FROM test.xbi_neapDistrictPlan
                    WHERE ActivityID = 2 and Yr = 2022 
                    
                """)
    #df['TimeStamp'] = datetime.strptime(df['TimeStamp'], '%d/%m/%y %H:%M:%S')
    df = df.replace(r'^\s*$', np.nan, regex=True)
    df['TimeStamp'] = pd.to_datetime(df['TimeStamp'], errors='coerce')
    df['DateAssessment'] = pd.to_datetime(df['DateAssessment'], errors='coerce')
    df[["TimeStamp"]] = df[["TimeStamp"]].apply(pd.to_datetime)
    df[["DateAssessment"]] = df[["DateAssessment"]].apply(pd.to_datetime)         
    client.insert_dataframe(
        'INSERT INTO test.get_all_neap  VALUES', df)
    logger.info(
            ' Data has been inserted into Table\' INSERT INTO test.get_all_neap VALUES \' ')            
   
def CreateJoinTableOfNeap():
    logger.info('Function \' GetAndInsertApiDataAllNeap \' Started Off')
    client = Client(host='161.97.136.95',
                    user='default',
                    password='pakistan',
                    port='9000', settings={"use_numpy": True})

    #---------------- xbi_table --------------------#
    sql = """CREATE TABLE if not exists test.xbi_neap( 
                    ID Int32,UserName String,ActivityID Int32,TimeStamp DateTime('Asia/Karachi'),Yr Int32,ProvID Int32,DivID Int32,DistID Int32,
                    TehsilID Int32,	UCID Int32,	IDcampCat Int32,	DateAssessment DateTime('Asia/Karachi'),	targeted Int32,	DaysBeforeActivity Int32,	UCMORounds Int32,	inaccCh Int32,	areasInacc Int32,	mobileTeams Int32,	fixedTeams Int32,	TransitPoints Int32,
                        
                    HH011_MP Int32,	HH1259_MP Int32,	School_MP Int32,	Fixed_MP Int32,	Transit_MP Int32,	areaIcs Int32,	aicFemale Int32,	aicGovt Int32,	aicRounds Int32,	aicTrained Int32,	AICSelfA Int32,	stall_method_trg Int32,

                    mov_stall_method_trg Int32,	MPDR_Passed Int32,	MPDR_Int Int32,	MPDR_Failed Int32,	MPFV_Passed Int32,	MPFV_Int Int32,	MPFV_Failed Int32,	MPDRD_Passed Int32,	MPDRD_Int Int32,	MPDRD_Failed Int32,	MPFVD_Passed Int32,	MPFVD_Int Int32,	MPFVD_Failed Int32,

                    zonalSupervisor String,	alltm18yr Int32,	govtWorker Int32,	mtLocalmem Int32,	mtFemale Int32,	HRMP Int32,	HRMP_status String,	NomadsChild Int32,	NomadicSett Int32,	BriklinsChild Int32,	BriklinsSett Int32,	SeasonalMigChild Int32,

                    SeasonalMigSett Int32,	AgrWorkersChild Int32,	AgrWorkersSett Int32,	Other Int32,	IDPChild Int32,	IDPSett Int32,	RefugeesChild Int32,	RefugeesSett Int32,	alltmTrg Int32,	ddmCard Int32,
                        
                    UpecHeld Int32,	UpecDate DateTime('Asia/Karachi'),	ucmo Int32,	ucSecratry Int32,	shoUpec Int32,	scVerified Int32,	lastCampDose0 Int32,	Dose0Cov Int32,	mov_mp_uc_staf Int32,	mov_aic_train Int32,	mov_mp_target Int32,	mov_hr Int32,	mov_upec Int32,

                    mov_zero_dose Int32,	mov_team_train Int32,	vacc_carr_avbl Int32,	vacc_carr_req Int32,	mov_mp_dist_staf Int32,	upecSign Int32,	

                    day1 DateTime('Asia/Karachi'),	dpec DateTime('Asia/Karachi'),	dco Int32,	edo Int32,	dpo_dpec Int32,	allMember Int32,	rmDate DateTime('Asia/Karachi'),	rmDC Int32,	rmDHO Int32,	rmSecurity Int32,	rmAllMembers Int32,
                        
                    districtReady Int32,	resheduleDate DateTime('Asia/Karachi'),	actionLPUC Int32,	actionType Int32,	reviewSIA Int32,
                        
                    inaugrated_by String,	OPVVilesReceived Int32,	VaccArival DateTime('Asia/Karachi'),	TeleSheets Int32,	FingerMarker_before_sia Int32,	FingerMarker Int32,	SmMatrialReceived DateTime('Asia/Karachi'),
                        
                    avbl_vacc_carrier Int32,	mov_dpec Int32,	mov_redines Int32,	VaccineType String,

                    Remarks String,	status Int32,	trash Int32,	isSync Int32

                    ,campaign_ID Int32, campaign_ActivityName String, campaign_ActivityID_old Int32, campaign_Yr Int32, campaign_SubActivityName String
                    
                    ,location_code Int32
                    ,geoLocation_name String, geoLocation_type Int32, geoLocation_code Int32, geoLocation_census_pop Int32, geoLocation_target Int32, geoLocation_status Int32, geoLocation_pname String, geoLocation_dname String, geoLocation_namedistrict String, geoLocation_codedistrict String, geoLocation_tname String, geoLocation_provincecode Int32, geoLocation_districtcode Int32, geoLocation_tehsilcode Int32, geoLocation_priority Int32, geoLocation_commnet Int32, geoLocation_hr Int32, geoLocation_fcm Int32, geoLocation_tier Int32, geoLocation_block String, geoLocation_division String, geoLocation_cordinates String, geoLocation_latitude String, geoLocation_longitude String, geoLocation_x String, geoLocation_y String, geoLocation_imagepath String, geoLocation_isccpv Int32, geoLocation_rank Int32, geoLocation_rank_score String, geoLocation_ishealthcamp Int32, geoLocation_isdsc Int32, geoLocation_ucorg String, geoLocation_organization String, geoLocation_tierfromaug161 Int32, geoLocation_tierfromsep171 Int32, geoLocation_tierfromdec181 Int32, geoLocation_mtap Int32, geoLocation_rspuc Int32, geoLocation_issmt Int32, geoLocation_updateddatetime String, geoLocation_x_code Int32, geoLocation_draining_uc Int32, geoLocation_upap_districts Int32, geoLocation_shruc Int32, geoLocation_khidist_id String
                    ,location_id Int32, location_name String, location_type String, location_status Int32
            )ENGINE = MergeTree PRIMARY KEY ID ORDER BY ID"""
    client.execute(sql)

    cols = """
            eoc_1.ID ,eoc_1.UserName ,eoc_1.ActivityID ,eoc_1.TimeStamp ,eoc_1.Yr ,eoc_1.ProvID ,eoc_1.DivID ,eoc_1.DistID ,
            eoc_1.TehsilID ,	eoc_1.UCID ,	eoc_1.IDcampCat ,	eoc_1.DateAssessment ,	eoc_1.targeted ,	eoc_1.DaysBeforeActivity ,	eoc_1.UCMORounds ,	eoc_1.inaccCh ,	eoc_1.areasInacc ,	eoc_1.mobileTeams ,	eoc_1.fixedTeams ,	eoc_1.TransitPoints ,
                
            eoc_1.HH011_MP ,	eoc_1.HH1259_MP ,	eoc_1.School_MP ,	eoc_1.Fixed_MP ,	eoc_1.Transit_MP ,	eoc_1.areaIcs ,	eoc_1.aicFemale ,	eoc_1.aicGovt ,	eoc_1.aicRounds ,	eoc_1.aicTrained ,	eoc_1.AICSelfA ,	eoc_1.stall_method_trg ,

            eoc_1.mov_stall_method_trg ,	eoc_1.MPDR_Passed ,	eoc_1.MPDR_Int ,	eoc_1.MPDR_Failed ,	eoc_1.MPFV_Passed ,	eoc_1.MPFV_Int ,	eoc_1.MPFV_Failed ,	eoc_1.MPDRD_Passed ,	eoc_1.MPDRD_Int ,	eoc_1.MPDRD_Failed ,	eoc_1.MPFVD_Passed ,	eoc_1.MPFVD_Int ,	eoc_1.MPFVD_Failed ,

            eoc_1.zonalSupervisor ,	eoc_1.alltm18yr ,	eoc_1.govtWorker ,	eoc_1.mtLocalmem ,	eoc_1.mtFemale ,	eoc_1.HRMP ,	eoc_1.HRMP_status ,	eoc_1.NomadsChild ,	eoc_1.NomadicSett ,	eoc_1.BriklinsChild ,	eoc_1.BriklinsSett ,	eoc_1.SeasonalMigChild ,

            eoc_1.SeasonalMigSett ,	eoc_1.AgrWorkersChild ,	eoc_1.AgrWorkersSett ,	eoc_1.Other ,	eoc_1.IDPChild ,	eoc_1.IDPSett ,	eoc_1.RefugeesChild ,	eoc_1.RefugeesSett ,	eoc_1.alltmTrg ,	eoc_1.ddmCard ,
                
            eoc_1.UpecHeld ,	eoc_1.UpecDate ,	eoc_1.ucmo ,	eoc_1.ucSecratry ,	eoc_1.shoUpec ,	eoc_1.scVerified ,	eoc_1.lastCampDose0 ,	eoc_1.Dose0Cov ,	eoc_1.mov_mp_uc_staf ,	eoc_1.mov_aic_train ,	eoc_1.mov_mp_target ,	eoc_1.mov_hr ,	eoc_1.mov_upec ,

            eoc_1.mov_zero_dose ,	eoc_1.mov_team_train ,	eoc_1.vacc_carr_avbl ,	eoc_1.vacc_carr_req ,	eoc_1.mov_mp_dist_staf ,	eoc_1.upecSign ,	

            eoc_1.day1 ,	eoc_1.dpec ,	eoc_1.dco ,	eoc_1.edo ,	eoc_1.dpo_dpec ,	eoc_1.allMember ,	eoc_1.rmDate ,	eoc_1.rmDC ,	eoc_1.rmDHO ,	eoc_1.rmSecurity ,	eoc_1.rmAllMembers ,
                
            eoc_1.districtReady ,	eoc_1.resheduleDate ,	eoc_1.actionLPUC ,	eoc_1.actionType ,	eoc_1.reviewSIA ,
                
            eoc_1.inaugrated_by ,	eoc_1.OPVVilesReceived ,	eoc_1.VaccArival ,	eoc_1.TeleSheets ,	eoc_1.FingerMarker_before_sia ,	eoc_1.FingerMarker ,	eoc_1.SmMatrialReceived ,
                
            eoc_1.avbl_vacc_carrier ,	eoc_1.mov_dpec ,	eoc_1.mov_redines ,	eoc_1.VaccineType ,

            eoc_1.Remarks ,	eoc_1.status ,	eoc_1.trash ,	eoc_1.isSync 

            ,eoc_1.campaign_ID, eoc_1.campaign_ActivityName, eoc_1.campaign_ActivityID_old, eoc_1.campaign_Yr, eoc_1.campaign_SubActivityName,  
            
            eoc_1.location_code ,
            
            eoc_1.geoLocation_name, eoc_1.geoLocation_type, eoc_1.geoLocation_code, eoc_1.geoLocation_census_pop, eoc_1.geoLocation_target, eoc_1.geoLocation_status, eoc_1.geoLocation_pname, eoc_1.geoLocation_dname, eoc_1.geoLocation_namedistrict, eoc_1.geoLocation_codedistrict, eoc_1.geoLocation_tname, geoLocation_provincecode, eoc_1.geoLocation_districtcode, eoc_1.geoLocation_tehsilcode, eoc_1.geoLocation_priority, eoc_1.geoLocation_commnet, eoc_1.geoLocation_hr, eoc_1.geoLocation_fcm, eoc_1.geoLocation_tier, eoc_1.geoLocation_block, eoc_1.geoLocation_division, eoc_1.geoLocation_cordinates, eoc_1.geoLocation_latitude, eoc_1.geoLocation_longitude, eoc_1.geoLocation_x, eoc_1.geoLocation_y, eoc_1.geoLocation_imagepath, eoc_1.geoLocation_isccpv, eoc_1.geoLocation_rank, eoc_1.geoLocation_rank_score, eoc_1.geoLocation_ishealthcamp, eoc_1.geoLocation_isdsc, eoc_1.geoLocation_ucorg, eoc_1.geoLocation_organization, eoc_1.geoLocation_tierfromaug161, eoc_1.geoLocation_tierfromsep171, eoc_1.geoLocation_tierfromdec181, eoc_1.geoLocation_mtap, eoc_1.geoLocation_rspuc, eoc_1.geoLocation_issmt, eoc_1.geoLocation_updateddatetime, eoc_1.geoLocation_x_code, eoc_1.geoLocation_draining_uc, eoc_1.geoLocation_upap_districts, eoc_1.geoLocation_shruc, eoc_1.geoLocation_khidist_id,
        
            eoc_1.location_id, eoc_1.location_name, eoc_1.location_type, eoc_1.location_status
            """
    sql = "SELECT " + cols + "  FROM test.get_all_neap  eoc_1" #left JOIN test.eoc_geolocation_t eoc_1 ON eoc_1.location_code  = eoc_1.code left JOIN test.xbi_geolocation gl1 ON (eoc_1.DistID = gl1.ID) left JOIN test.xbi_campaign eoc_1 ON (eoc_1.ActivityID  = eoc_1.campaign_ActivityID_old And eoc_1.Yr = eoc_1.campaign_Yr) "
    data = client.execute(sql)

    xbiDataFrame = pd.DataFrame(data)
    all_columns = list(xbiDataFrame)  # Creates list of all column headers
    cols = xbiDataFrame.iloc[0]

    xbiDataFrame[all_columns] = xbiDataFrame[all_columns].astype(str)

    d = [
        'ID' ,'UserName' ,'ActivityID' ,'TimeStamp' ,'Yr' ,'ProvID' ,'DivID' ,'DistID' ,
        'TehsilID' ,	'UCID' ,	'IDcampCat' ,	'DateAssessment' ,	'targeted' ,	'DaysBeforeActivity' ,	'UCMORounds' ,	'inaccCh' ,	'areasInacc' ,	'mobileTeams' ,	'fixedTeams' ,	'TransitPoints' ,
            
        'HH011_MP' ,	'HH1259_MP' ,	'School_MP' ,	'Fixed_MP' ,	'Transit_MP' ,	'areaIcs' ,	'aicFemale' ,	'aicGovt' ,	'aicRounds' ,	'aicTrained',	'AICSelfA' ,	'stall_method_trg' ,

        'mov_stall_method_trg' ,	'MPDR_Passed' ,	'MPDR_Int' ,	'MPDR_Failed' ,	'MPFV_Passed' ,	'MPFV_Int' ,	'MPFV_Failed' ,	'MPDRD_Passed' ,	'MPDRD_Int' ,	'MPDRD_Failed' , 'MPFVD_Passed' ,	'MPFVD_Int' ,	'MPFVD_Failed' ,

        'zonalSupervisor' ,	'alltm18yr' ,	'govtWorker' ,	'mtLocalmem' ,	'mtFemale' ,	'HRMP' ,	'HRMP_status' ,	'NomadsChild' ,	'NomadicSett' ,	'BriklinsChild' ,	'BriklinsSett' ,	'SeasonalMigChild' ,

        'SeasonalMigSett' ,	'AgrWorkersChild' ,	'AgrWorkersSett' ,	'Other' ,	'IDPChild' ,	'IDPSett' ,	'RefugeesChild' ,	'RefugeesSett' ,	'alltmTrg' ,	'ddmCard' ,
            
        'UpecHeld' ,	'UpecDate' ,	'ucmo' ,	'ucSecratry' ,	'shoUpec' ,	'scVerified' ,	'lastCampDose0' ,	'Dose0Cov' ,	'mov_mp_uc_staf' ,	'mov_aic_train' ,	'mov_mp_target' ,	'mov_hr' ,	'mov_upec' ,

        'mov_zero_dose' ,	'mov_team_train' ,	'vacc_carr_avbl' ,	'vacc_carr_req' ,	'mov_mp_dist_staf' ,	'upecSign' ,	

        'day1' ,	'dpec' ,	'dco' ,	'edo' ,	'dpo_dpec' ,	'allMember' ,	'rmDate' ,	'rmDC' ,	'rmDHO' ,	'rmSecurity' ,	'rmAllMembers' ,
            
        'districtReady' ,	'resheduleDate' ,	'actionLPUC' ,	'actionType' ,	'reviewSIA' ,
            
        'inaugrated_by' ,	'OPVVilesReceived' ,	'VaccArival' ,	'TeleSheets' ,	'FingerMarker_before_sia' ,	'FingerMarker' ,	'SmMatrialReceived' ,
            
        'avbl_vacc_carrier' ,	'mov_dpec' ,	'mov_redines',	'VaccineType' ,

        'Remarks' ,	'status' ,	'trash' ,	'isSync' 

        ,'campaign_ID', 'campaign_ActivityName', 'campaign_ActivityID_old', 'campaign_Yr', 'campaign_SubActivityName'
        ,'location_code' 
        ,'geoLocation_name', 'geoLocation_type', 'geoLocation_code', 'geoLocation_census_pop', 'geoLocation_target', 'geoLocation_status', 'geoLocation_pname', 'geoLocation_dname', 'geoLocation_namedistrict', 'geoLocation_codedistrict', 'geoLocation_tname', 'geoLocation_provincecode', 'geoLocation_districtcode', 'geoLocation_tehsilcode', 'geoLocation_priority', 'geoLocation_commnet', 'geoLocation_hr', 'geoLocation_fcm', 'geoLocation_tier', 'geoLocation_block', 'geoLocation_division', 'geoLocation_cordinates', 'geoLocation_latitude', 'geoLocation_longitude', 'geoLocation_x', 'geoLocation_y', 'geoLocation_imagepath', 'geoLocation_isccpv', 'geoLocation_rank', 'geoLocation_rank_score', 'geoLocation_ishealthcamp', 'geoLocation_isdsc', 'geoLocation_ucorg', 'geoLocation_organization', 'geoLocation_tierfromaug161', 'geoLocation_tierfromsep171', 'geoLocation_tierfromdec181', 'geoLocation_mtap', 'geoLocation_rspuc', 'geoLocation_issmt', 'geoLocation_updateddatetime', 'geoLocation_x_code', 'geoLocation_draining_uc', 'geoLocation_upap_districts', 'geoLocation_shruc', 'geoLocation_khidist_id' 
        ,'location_id',  'location_name','location_type','location_status'
        ]
    dff = pd.DataFrame(columns=d)
    for index, item in enumerate(d):
        dff[item] = xbiDataFrame[index].values
        logger.info(
            'Get Data from test.get_all_neap for campaign')#+str(cid))

    df3 = client.query_dataframe(
                    "SELECT * FROM test.xbi_neap WHERE Yr = 2022 and ActivityID = 2")
    if df3.empty:
        client.insert_dataframe(
            'INSERT INTO test.xbi_neap VALUES', dff)
        logger.info(
            'Data has been inserted into Table test.xbi_neap for campaign ')#+str(cid))

        sql = "DROP table if exists test.get_all_neap"
        client.execute(sql)
    
    else:
        sql = "ALTER TABLE test.xbi_neap DELETE WHERE Yr = 2022 and ActivityID = 2"
        client.execute(sql)
        client.insert_dataframe(
            'INSERT INTO test.xbi_neap VALUES', dff)
        logger.info(
            ' Data has been inserted into Table test.xbi_neap for campaign ')#+str(cid))    

        sql = "DROP table if exists test.get_all_neap"
        client.execute(sql)
    
dag = DAG(
    'All-Neap_Automated',
    schedule_interval='0 0 * * *',  # will run every mid-night.
    #schedule_interval=None,
    #schedule_interval='*/59 * * * *',  # will run every 60 min.
    default_args=default_args,
    catchup=False)

with dag:
    GetAndInsertApiDataAllNeap = PythonOperator(
        task_id='GetAndInsertApiDataAllNeap',
        python_callable=GetAndInsertApiDataAllNeap,
    )
    CreateJoinTableOfNeap = PythonOperator(
        task_id='CreateJoinTableOfNeap',
        python_callable=CreateJoinTableOfNeap,
    )
GetAndInsertApiDataAllNeap >> CreateJoinTableOfNeap
    
               

    # url = 'http://idims.eoc.gov.pk/api_who/api/get_allneap/5468XE2LN6CzR7qRG041/1/2022'
    # logger.info('Requested Data From Api URL:  \' http://idims.eoc.gov.pk/api_who/api/get_allneap/5468XE2LN6CzR7qRG041/1/2022 \' ')

    # r = requests.get(url)
    # data = r.json()
    # logger.info('Received Data  From Api URL:  \' http://idims.eoc.gov.pk/api_who/api/get_allneap/5468XE2LN6CzR7qRG041/1/2022 \' ')

    # rowsData = data["data"]["data"]
    # apiDataFrame = pd.DataFrame(rowsData)
    # sql = "CREATE TABLE if not exists test.get_all_neap (  ID Int32, UserName String, IDcampCat Int32, ActivityID Int32, Yr Int32, TimeStamp String, DateAssessment String, TehsilID Int32, UCID Int32, DistID Int32, DivID Int32, ProvID Int32, targeted Int32, DaysBeforeActivity Int32, UCMORounds Int32, inaccCh String, areasInacc String, mobileTeams Int32, fixedTeams Int32, TransitPoints Int32, HH011_MP Int32, HH1259_MP Int32, School_MP Int32, Fixed_MP Int32, Transit_MP Int32, areaIcs Int32, aicFemale String, aicGovt String, aicRounds String, aicTrained Int32, AICSelfA String, stall_method_trg Int32, mov_stall_method_trg Int32, MPDR_Passed String, MPDR_Int String, MPDR_Failed String, MPFV_Passed String, MPFV_Int String, MPFV_Failed String, MPDRD_Passed String, MPDRD_Int String, MPDRD_Failed String, MPFVD_Passed String, MPFVD_Int String, MPFVD_Failed String, zonalSupervisor String, alltm18yr Int32, govtWorker Int32, mtLocalmem Int32, mtFemale Int32, HRMP String, HRMP_status String, NomadsChild String, NomadicSett String, BriklinsChild String, BriklinsSett String, SeasonalMigChild String, SeasonalMigSett String, AgrWorkersChild String, AgrWorkersSett String, Other String, IDPChild String, IDPSett String, RefugeesChild String, RefugeesSett String, alltmTrg Int32, ddmCard String, UpecHeld String, UpecDate String, ucmo Int32, ucSecratry Int32, shoUpec Int32, scVerified String, lastCampDose0 String, Dose0Cov String, remarks String, mov_mp_uc_staf Int32, mov_aic_train Int32, mov_mp_target Int32, mov_hr Int32, mov_upec Int32, mov_zero_dose Int32, mov_team_train Int32, mov_mp_dist_staf Int32, upecSign Int32, status Int32, trash Int32, isSync Int32, location_code  Int64)ENGINE = MergeTree PRIMARY KEY ID ORDER BY ID"
    # client.execute(sql)

    # df2 = client.query_dataframe(
    #     "SELECT * FROM test.get_all_neap")
    # if df2.empty:
    #     client.insert_dataframe(
    #         'INSERT INTO test.get_all_neap VALUES', apiDataFrame)


# def CreateJoinTableOfNeap():
#     logger.info(' Function  \' CreateJoinTableOfNeap \' has been Initiated')
#     client = Client(host='161.97.136.95',
#                     user='default',
#                     password='pakistan',
#                     port='9000', settings={"use_numpy": True})
#     sql = "CREATE TABLE if not exists test.xbi_neap ( ID Int32, UserName String, IDcampCat Int32, ActivityID Int32, Yr Int32, TimeStamp String, DateAssessment String, TehsilID Int32, UCID Int32, DistID Int32, DivID Int32, ProvID Int32, targeted Int32, DaysBeforeActivity Int32, UCMORounds Int32, inaccCh String, areasInacc String, mobileTeams Int32, fixedTeams Int32, TransitPoints Int32, HH011_MP Int32, HH1259_MP Int32, School_MP Int32, Fixed_MP Int32, Transit_MP Int32, areaIcs Int32, aicFemale String, aicGovt String, aicRounds String, aicTrained Int32, AICSelfA String, stall_method_trg Int32, mov_stall_method_trg Int32, MPDR_Passed String, MPDR_Int String, MPDR_Failed String, MPFV_Passed String, MPFV_Int String, MPFV_Failed String, MPDRD_Passed String, MPDRD_Int String, MPDRD_Failed String, MPFVD_Passed String, MPFVD_Int String, MPFVD_Failed String, zonalSupervisor String, alltm18yr Int32, govtWorker Int32, mtLocalmem Int32, mtFemale Int32, HRMP String, HRMP_status String, NomadsChild String, NomadicSett String, BriklinsChild String, BriklinsSett String, SeasonalMigChild String, SeasonalMigSett String, AgrWorkersChild String, AgrWorkersSett String, Other String, IDPChild String, IDPSett String, RefugeesChild String, RefugeesSett String, alltmTrg Int32, ddmCard String, UpecHeld String, UpecDate String, ucmo Int32, ucSecratry Int32, shoUpec Int32, scVerified String, lastCampDose0 String, Dose0Cov String, remarks String, mov_mp_uc_staf Int32, mov_aic_train Int32, mov_mp_target Int32, mov_hr Int32, mov_upec Int32, mov_zero_dose Int32, mov_team_train Int32, mov_mp_dist_staf Int32, upecSign Int32, status Int32, trash Int32, isSync Int32, location_code  Int64, campaign_ID Int32, campaign_ActivityName String, campaign_ActivityID_old Int32, campaign_Yr Int32, campaign_SubActivityName String, geolocation_ID Int32, geolocation_code Int32, geolocation_name String, location_type String, location_target Int32, location_status Int32, location_priority String, hr_status String  )ENGINE = MergeTree PRIMARY KEY ID ORDER BY ID"
#     client.execute(sql)
#     cols = "test.get_all_neap.ID, test.get_all_neap.UserName, test.get_all_neap.IDcampCat, test.get_all_neap.ActivityID, test.get_all_neap.Yr, test.get_all_neap.TimeStamp, test.get_all_neap.DateAssessment, test.get_all_neap.TehsilID, test.get_all_neap.UCID, test.get_all_neap.DistID, test.get_all_neap.DivID, test.get_all_neap.ProvID, test.get_all_neap.targeted, test.get_all_neap.DaysBeforeActivity, test.get_all_neap.UCMORounds, test.get_all_neap.inaccCh, test.get_all_neap.areasInacc, test.get_all_neap.mobileTeams, test.get_all_neap.fixedTeams, test.get_all_neap.TransitPoints, test.get_all_neap.HH011_MP, test.get_all_neap.HH1259_MP, test.get_all_neap.School_MP, test.get_all_neap.Fixed_MP, test.get_all_neap.Transit_MP, test.get_all_neap.areaIcs, test.get_all_neap.aicFemale, test.get_all_neap.aicGovt, test.get_all_neap.aicRounds, test.get_all_neap.aicTrained, test.get_all_neap.AICSelfA, test.get_all_neap.stall_method_trg, test.get_all_neap.mov_stall_method_trg, test.get_all_neap.MPDR_Passed, test.get_all_neap.MPDR_Int, test.get_all_neap.MPDR_Failed, test.get_all_neap.MPFV_Passed, test.get_all_neap.MPFV_Int, test.get_all_neap.MPFV_Failed, test.get_all_neap.MPDRD_Passed, test.get_all_neap.MPDRD_Int, test.get_all_neap.MPDRD_Failed, test.get_all_neap.MPFVD_Passed, test.get_all_neap.MPFVD_Int, test.get_all_neap.MPFVD_Failed, test.get_all_neap.zonalSupervisor, test.get_all_neap.alltm18yr, test.get_all_neap.govtWorker, test.get_all_neap.mtLocalmem, test.get_all_neap.mtFemale, test.get_all_neap.HRMP, test.get_all_neap.HRMP_status, test.get_all_neap.NomadsChild, test.get_all_neap.NomadicSett, test.get_all_neap.BriklinsChild, test.get_all_neap.BriklinsSett, test.get_all_neap.SeasonalMigChild, test.get_all_neap.SeasonalMigSett, test.get_all_neap.AgrWorkersChild, test.get_all_neap.AgrWorkersSett, test.get_all_neap.Other, test.get_all_neap.IDPChild, test.get_all_neap.IDPSett, test.get_all_neap.RefugeesChild, test.get_all_neap.RefugeesSett, test.get_all_neap.alltmTrg, test.get_all_neap.ddmCard, test.get_all_neap.UpecHeld, test.get_all_neap.UpecDate, test.get_all_neap.ucmo, test.get_all_neap.ucSecratry, test.get_all_neap.shoUpec, test.get_all_neap.scVerified, test.get_all_neap.lastCampDose0, test.get_all_neap.Dose0Cov, test.get_all_neap.remarks, test.get_all_neap.mov_mp_uc_staf, test.get_all_neap.mov_aic_train, test.get_all_neap.mov_mp_target, test.get_all_neap.mov_hr, test.get_all_neap.mov_upec, test.get_all_neap.mov_zero_dose, test.get_all_neap.mov_team_train, test.get_all_neap.mov_mp_dist_staf, test.get_all_neap.upecSign, test.get_all_neap.status, test.get_all_neap.trash, test.get_all_neap.isSync, test.get_all_neap.location_code , test.xbi_campaign.campaign_ID, test.xbi_campaign.campaign_ActivityName, test.xbi_campaign.campaign_ActivityID_old, test.xbi_campaign.campaign_Yr, test.xbi_campaign.campaign_SubActivityName,  test.xbi_geolocation.ID, test.xbi_geolocation.code, test.xbi_geolocation.name, test.xbi_geolocation.type, test.xbi_geolocation.location_target, test.xbi_geolocation.location_status, test.xbi_geolocation.location_priority, test.xbi_geolocation.hr_status "
#     sql = "SELECT " + cols + "  FROM test.get_all_neap tsm left JOIN test.xbi_geolocation eoc_1 ON tsm.location_code  = eoc_1.code left JOIN test.xbi_campaign eoc_1 ON (tsm.ActivityID  = eoc_1.campaign_ActivityID_old And tsm.Yr = eoc_1.campaign_Yr) "

#     data = client.execute(sql)
#     apiDataFrame = pd.DataFrame(data)
#     all_columns = list(apiDataFrame)  # Creates list of all column headers
#     cols = apiDataFrame.iloc[0]
#     apiDataFrame[all_columns] = apiDataFrame[all_columns].astype(str)
#     d =  'ID', 'UserName', 'IDcampCat', 'ActivityID', 'Yr', 'TimeStamp', 'DateAssessment', 'TehsilID', 'UCID', 'DistID', 'DivID', 'ProvID', 'targeted', 'DaysBeforeActivity', 'UCMORounds', 'inaccCh', 'areasInacc', 'mobileTeams', 'fixedTeams', 'TransitPoints', 'HH011_MP', 'HH1259_MP', 'School_MP', 'Fixed_MP', 'Transit_MP', 'areaIcs', 'aicFemale', 'aicGovt', 'aicRounds', 'aicTrained', 'AICSelfA', 'stall_method_trg', 'mov_stall_method_trg', 'MPDR_Passed', 'MPDR_Int', 'MPDR_Failed', 'MPFV_Passed', 'MPFV_Int', 'MPFV_Failed', 'MPDRD_Passed', 'MPDRD_Int', 'MPDRD_Failed', 'MPFVD_Passed', 'MPFVD_Int', 'MPFVD_Failed', 'zonalSupervisor', 'alltm18yr', 'govtWorker', 'mtLocalmem', 'mtFemale', 'HRMP', 'HRMP_status', 'NomadsChild', 'NomadicSett', 'BriklinsChild', 'BriklinsSett', 'SeasonalMigChild', 'SeasonalMigSett', 'AgrWorkersChild', 'AgrWorkersSett', 'Other', 'IDPChild', 'IDPSett', 'RefugeesChild', 'RefugeesSett', 'alltmTrg', 'ddmCard', 'UpecHeld', 'UpecDate', 'ucmo', 'ucSecratry', 'shoUpec', 'scVerified', 'lastCampDose0', 'Dose0Cov', 'remarks', 'mov_mp_uc_staf', 'mov_aic_train', 'mov_mp_target', 'mov_hr', 'mov_upec', 'mov_zero_dose', 'mov_team_train', 'mov_mp_dist_staf', 'upecSign', 'status', 'trash', 'isSync', 'location_code', 'campaign_ID', 'campaign_ActivityName', 'campaign_ActivityID_old', 'campaign_Yr', 'campaign_SubActivityName', 'geolocation_ID', 'geolocation_code', 'geolocation_name', 'location_type', 'location_target', 'location_status', 'location_priority', 'hr_status' 
#     dff = pd.DataFrame(columns=d)
#     for index, item in enumerate(d):
#         dff[item] = apiDataFrame[index].values
#     df2 = client.query_dataframe(
#         "SELECT * FROM  test.xbi_neap")
#     if df2.empty:
#         client.insert_dataframe(
#             'INSERT INTO  test.xbi_neap  VALUES', dff)
#         logger.info(
#             ' Data has been inserted into Table\' INSERT INTO  test.xbi_neap  VALUES \' ')

#         sql = "DROP table if exists  test.get_all_neap"
#         client.execute(sql)

#     else:
#         df = pd.concat([dff, df2])
#         df = df.astype('str')
#         df = df.drop_duplicates(subset='ID',
#                                 keep="first", inplace=False)
#         sql = "DROP TABLE if exists  test.xbi_neap;"
#         client.execute(sql)
#         sql = "CREATE TABLE if not exists test.xbi_neap ( ID Int32, UserName String, IDcampCat Int32, ActivityID Int32, Yr Int32, TimeStamp String, DateAssessment String, TehsilID Int32, UCID Int32, DistID Int32, DivID Int32, ProvID Int32, targeted Int32, DaysBeforeActivity Int32, UCMORounds Int32, inaccCh String, areasInacc String, mobileTeams Int32, fixedTeams Int32, TransitPoints Int32, HH011_MP Int32, HH1259_MP Int32, School_MP Int32, Fixed_MP Int32, Transit_MP Int32, areaIcs Int32, aicFemale String, aicGovt String, aicRounds String, aicTrained Int32, AICSelfA String, stall_method_trg Int32, mov_stall_method_trg Int32, MPDR_Passed String, MPDR_Int String, MPDR_Failed String, MPFV_Passed String, MPFV_Int String, MPFV_Failed String, MPDRD_Passed String, MPDRD_Int String, MPDRD_Failed String, MPFVD_Passed String, MPFVD_Int String, MPFVD_Failed String, zonalSupervisor String, alltm18yr Int32, govtWorker Int32, mtLocalmem Int32, mtFemale Int32, HRMP String, HRMP_status String, NomadsChild String, NomadicSett String, BriklinsChild String, BriklinsSett String, SeasonalMigChild String, SeasonalMigSett String, AgrWorkersChild String, AgrWorkersSett String, Other String, IDPChild String, IDPSett String, RefugeesChild String, RefugeesSett String, alltmTrg Int32, ddmCard String, UpecHeld String, UpecDate String, ucmo Int32, ucSecratry Int32, shoUpec Int32, scVerified String, lastCampDose0 String, Dose0Cov String, remarks String, mov_mp_uc_staf Int32, mov_aic_train Int32, mov_mp_target Int32, mov_hr Int32, mov_upec Int32, mov_zero_dose Int32, mov_team_train Int32, mov_mp_dist_staf Int32, upecSign Int32, status Int32, trash Int32, isSync Int32, location_code  Int64, campaign_ID Int32, campaign_ActivityName String, campaign_ActivityID_old Int32, campaign_Yr Int32, campaign_SubActivityName String, geolocation_ID Int32, geolocation_code Int32, geolocation_name String, location_type String, location_target Int32, location_status Int32, location_priority String, hr_status String  )ENGINE = MergeTree PRIMARY KEY ID ORDER BY ID"
#         client.execute(sql)
#         client.insert_dataframe(
#             'INSERT INTO  test.xbi_neap  VALUES', df)

#         sql = "DROP table if exists  test.get_all_neap"
#         client.execute(sql)


# dag = DAG(
#     'All-Neap_Automated',
#     #schedule_interval='0 0 * * *',  # will run every mid-night.
#     schedule_interval=None,
#     #schedule_interval='*/59 * * * *',  # will run every 60 min.
#     default_args=default_args,
#     catchup=False)

# with dag:
#     GetAndInsertApiDataAllNeap = PythonOperator(
#         task_id='GetAndInsertApiDataAllNeap',
#         python_callable=GetAndInsertApiDataAllNeap,
#     )
#     CreateJoinTableOfNeap = PythonOperator(
#         task_id='CreateJoinTableOfNeap',
#         python_callable=CreateJoinTableOfNeap,
#     )
# GetAndInsertApiDataAllNeap >> CreateJoinTableOfNeap
