
from airflow import DAG
import pandas as pd
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import requests
from clickhouse_driver import Client
import logging

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
#------------------------------------------------ Author: BABAR ALI SHAH -------------------------------------------------------#
#-------------------------------------------------------------------------------------------------------------------------------#

def GetAndInsertApiDataControlRoom():
    logger.info('Function \' GetAndInsertApiDataControlRoom \' Started Off')
    client = Client(host='161.97.136.95',
                    user='default',
                    password='pakistan',
                    port='9000', settings={"use_numpy": True})
    
    url = 'http://idims.eoc.gov.pk/api_who/api/get_allcontrollroom_combined/5468XE2LN6CzR7qRG041/1/2022'
    logger.info('Requested Data From Api URL:  \' http://idims.eoc.gov.pk/api_who/api/get_allcontrollroom_combined/5468XE2LN6CzR7qRG041/1/2022 \' ')

    r = requests.get(url)
    data = r.json()
    logger.info('Received Data  From Api URL:  \' http://idims.eoc.gov.pk/api_who/api/get_allcontrollroom_combined/5468XE2LN6CzR7qRG041/1/2022 \' ')

    rowsData = data["data"]["data"]
    apiDataFrame = pd.DataFrame(rowsData)
    sql = "CREATE TABLE if not exists test.get_combined_coverage  (ID Int32, IDcampCat Int32, ActivityID Int32, Yr Int32, TimeStamp varchar(10), Cday Int32, TehsilID Int32, UCID Int32, ccpvTargets Int32, VaccinationDate varchar(50), TeamsRpt Int32, HH011_MP Int32, HH1259_MP Int32, HH011_TS Int32, HH1259_TS Int32, HH011 Int32, HH1259 Int32, OutofH011 Int32, OutofH1259 Int32, RecNA011 Int32, RecNA1259 Int32, RecRef011 Int32, RecRef1259 Int32, CovNA011 Int32, CovNA1259 Int32, CovRef011 Int32, CovRef1259 Int32, Guests Int32, VaccinatedSchool Int32, NewBorn Int32, AlreadyVaccinated Int32, FixSite011 Int32, FixSite1259 Int32, Transit011 Int32, Transit1259 Int32, CovMob Int32, HHvisit Int32, HHPlan Int32, MultipleFamily String, Weakness Int32, ZeroRt Int32, OPVGiven Int32, OPVUsed Int32, OPVReturned Int32, Stage34 Int32, Inaccessible Int32, Remarks String, V_Age611 Int32, v_Age1259 Int32, V_capGiven Int32, V_capUsed Int32, V_capRet Int32, status Int32, trash Int32, isSync Int32, trigers String, Covid19 Int32, location_code Int32, campcat_name String, PRIMARY KEY(ID ))ENGINE = MergeTree"
    client.execute(sql)

    df2 = client.query_dataframe(
        "SELECT * FROM test.get_combined_coverage")
    if df2.empty:
        client.insert_dataframe(
            'INSERT INTO test.get_combined_coverage VALUES', apiDataFrame)


def CreateJoinTableOfCampaignCoverage():
    logger.info(' Function  \' CreateJoinTableOfCampaignCoverage \' has been Initiated')
    client = Client(host='161.97.136.95',
                    user='default',
                    password='pakistan',
                    port='9000', settings={"use_numpy": True})
    sql = "CREATE TABLE if not exists test.xbi_campaigncoverage (controlroom_ID Int32, ActivityID Int32,Yr Int32,TehsilID Int32,UCID Int32,HH011_MP Int32,HH1259_MP Int32,HH011_TS Int32,HH1259_TS Int32,HH011 Int32,HH1259 Int32,OutofH011 Int32,OutofH1259 Int32,RecNA011 Int32,RecNA1259 Int32,RecRef011 Int32,RecRef1259 Int32,CovNA011 Int32,CovNA1259 Int32,CovRef011 Int32,CovRef1259 Int32,Guests Int32,VaccinatedSchool Int32,NewBorn Int32,AlreadyVaccinated Int32,FixSite011 Int32,FixSite1259 Int32,Transit011 Int32,Transit1259 Int32,CovMob Int32,HHvisit Int32,HHPlan Int32,Weakness Int32,ZeroRt Int32,location_code Int32, catchup_CCovNA011 Int32, catchup_CCovNA1259 Int32, catchup_CCovRef011 Int32, catchup_CCovRef1259 Int32, catchup_COutofH011 Int32, catchup_COutofH1259 Int32, catchup_CGuests Int32, catchup_CCovMob Int32, catchup_CNewBorn Int32, catchup_CAlreadyVaccinated Int32, catchup_CFixSite011 Int32, catchup_CFixSite1259 Int32, catchup_CTransit011 Int32, catchup_CTransit1259 Int32, catchup_UnRecorded_Cov Int32,ACCovNA011 Int32, ACCovNA1259 Int32, ACCovRef011 Int32, ACCovRef1259 Int32, ACOutofH011 Int32, ACOutofH1259 Int32, ACGuests Int32, ACCovMob Int32, ACNewBorn Int32,ACAlreadyVaccinated Int32, UnRecCov Int32, campaign_ID Int32, campaign_ActivityName String, campaign_ActivityID_old Int32, campaign_Yr Int32, campaign_SubActivityName String, location_ID Int32, geo_location_code Int32, location_name String, location_type String, location_target Int32, location_status Int32, location_priority String, hr_status String)ENGINE = MergeTree PRIMARY KEY controlroom_ID ORDER BY controlroom_ID"
    client.execute(sql)
    cols = "test.get_combined_coverage.ID,test.get_combined_coverage.ActivityID,test.get_combined_coverage.Yr,test.get_combined_coverage.TehsilID,test.get_combined_coverage.UCID,test.get_combined_coverage.HH011_MP,test.get_combined_coverage.HH1259_MP,test.get_combined_coverage.HH011_TS,test.get_combined_coverage.HH1259_TS,test.get_combined_coverage.HH011,test.get_combined_coverage.HH1259,test.get_combined_coverage.OutofH011,test.get_combined_coverage.OutofH1259,test.get_combined_coverage.RecNA011,test.get_combined_coverage.RecNA1259,test.get_combined_coverage.RecRef011,test.get_combined_coverage.RecRef1259,test.get_combined_coverage.CovNA011,test.get_combined_coverage.CovNA1259,test.get_combined_coverage.CovRef011,test.get_combined_coverage.CovRef1259,test.get_combined_coverage.Guests,test.get_combined_coverage.VaccinatedSchool,test.get_combined_coverage.NewBorn,test.get_combined_coverage.AlreadyVaccinated,test.get_combined_coverage.FixSite011,test.get_combined_coverage.FixSite1259,test.get_combined_coverage.Transit011,test.get_combined_coverage.Transit1259,test.get_combined_coverage.CovMob,test.get_combined_coverage.HHvisit,test.get_combined_coverage.HHPlan,test.get_combined_coverage.Weakness,test.get_combined_coverage.ZeroRt,test.get_combined_coverage.location_code,test.xbi_catchUp.catchup_CCovNA011,test.xbi_catchUp.catchup_CCovNA1259,test.xbi_catchUp.catchup_CCovRef011,test.xbi_catchUp.catchup_CCovRef1259,test.xbi_catchUp.catchup_COutofH011,test.xbi_catchUp.catchup_COutofH1259,test.xbi_catchUp.catchup_CGuests,test.xbi_catchUp.catchup_CCovMob,test.xbi_catchUp.catchup_CNewBorn,test.xbi_catchUp.catchup_CAlreadyVaccinated,test.xbi_catchUp.catchup_CFixSite011,test.xbi_catchUp.catchup_CFixSite1259,test.xbi_catchUp.catchup_CTransit011,test.xbi_catchUp.catchup_CTransit1259,test.xbi_catchUp.catchup_UnRecorded_Cov,test.xbi_after_catchUp.ACCovNA011,test.xbi_after_catchUp.ACCovNA1259,test.xbi_after_catchUp.ACCovRef011,test.xbi_after_catchUp.ACCovRef1259,test.xbi_after_catchUp.ACOutofH011,test.xbi_after_catchUp.ACOutofH1259,test.xbi_after_catchUp.ACGuests,test.xbi_after_catchUp.ACCovMob,test.xbi_after_catchUp.ACNewBorn,test.xbi_after_catchUp.ACAlreadyVaccinated,test.xbi_after_catchUp.UnRecCov, test.xbi_campaign.campaign_ID, test.xbi_campaign.campaign_ActivityName, test.xbi_campaign.campaign_ActivityID_old, test.xbi_campaign.campaign_Yr, test.xbi_campaign.campaign_SubActivityName,  test.xbi_geolocation.ID, test.xbi_geolocation.code, test.xbi_geolocation.name, test.xbi_geolocation.type,  test.xbi_geolocation.location_target, test.xbi_geolocation.location_status, test.xbi_geolocation.location_priority, test.xbi_geolocation.hr_status"
    sql = "SELECT " + cols + "  FROM test.get_combined_coverage eoc_1 left JOIN test.xbi_geolocation eoc_2 ON eoc_1.location_code  = eoc_2.code left JOIN test.xbi_campaign eoc_3 ON (eoc_1.ActivityID  = eoc_3.campaign_ActivityID_old And eoc_1.Yr = eoc_3.campaign_Yr) left JOIN test.xbi_catchUp eoc_4 ON (eoc_4.catchup_Yr = eoc_1.Yr and eoc_4.catchup_ActivityID  = eoc_1.ActivityID and eoc_4.catchup_location_code = eoc_1.location_code) left JOIN test.xbi_after_catchUp eoc_5 ON (eoc_5.Yr  = eoc_1.Yr and eoc_5.ActivityID  = eoc_1.ActivityID and eoc_5.location_code = eoc_1.location_code) "

    data = client.execute(sql)
    apiDataFrame = pd.DataFrame(data)
    all_columns = list(apiDataFrame)  # Creates list of all column headers
    cols = apiDataFrame.iloc[0]
    apiDataFrame[all_columns] = apiDataFrame[all_columns].astype(str)
    d = 'controlroom_ID','ActivityID','Yr','TehsilID','UCID','HH011_MP','HH1259_MP','HH011_TS','HH1259_TS','HH011','HH1259','OutofH011','OutofH1259','RecNA011','RecNA1259','RecRef011','RecRef1259','CovNA011','CovNA1259','CovRef011','CovRef1259','Guests','VaccinatedSchool','NewBorn','AlreadyVaccinated','FixSite011','FixSite1259','Transit011','Transit1259','CovMob','HHvisit','HHPlan','Weakness','ZeroRt','location_code','catchup_CCovNA011','catchup_CCovNA1259','catchup_CCovRef011','catchup_CCovRef1259','catchup_COutofH011','catchup_COutofH1259','catchup_CGuests','catchup_CCovMob','catchup_CNewBorn','catchup_CAlreadyVaccinated','catchup_CFixSite011','catchup_CFixSite1259','catchup_CTransit011','catchup_CTransit1259','catchup_UnRecorded_Cov','ACCovNA011','ACCovNA1259','ACCovRef011','ACCovRef1259','ACOutofH011','ACOutofH1259','ACGuests','ACCovMob','ACNewBorn','ACAlreadyVaccinated','UnRecCov','campaign_ID','campaign_ActivityName','campaign_ActivityID_old','campaign_Yr','campaign_SubActivityName','location_ID','geo_location_code','location_name','location_type','location_target','location_status','location_priority','hr_status'
    dff = pd.DataFrame(columns=d)
    for index, item in enumerate(d):
        dff[item] = apiDataFrame[index].values
    df2 = client.query_dataframe(
        "SELECT * FROM test.xbi_campaigncoverage")
    if df2.empty:
        client.insert_dataframe(
            'INSERT INTO test.xbi_campaigncoverage  VALUES', dff)
        logger.info(
            ' Data has been inserted into Table\' INSERT INTO test.xbi_campaigncoverage  VALUES \' ')

        sql = "DROP table if exists test.get_combined_coverage"
        client.execute(sql)

    else:
        df = pd.concat([dff, df2])
        df = df.astype('str')
        df = df.drop_duplicates(subset='controlroom_ID',
                                keep="first", inplace=False)
        sql = "DROP TABLE if exists test.xbi_campaigncoverage;"
        client.execute(sql)
        sql = "CREATE TABLE if not exists test.xbi_campaigncoverage (controlroom_ID Int32, ActivityID Int32,Yr Int32,TehsilID Int32,UCID Int32,HH011_MP Int32,HH1259_MP Int32,HH011_TS Int32,HH1259_TS Int32,HH011 Int32,HH1259 Int32,OutofH011 Int32,OutofH1259 Int32,RecNA011 Int32,RecNA1259 Int32,RecRef011 Int32,RecRef1259 Int32,CovNA011 Int32,CovNA1259 Int32,CovRef011 Int32,CovRef1259 Int32,Guests Int32,VaccinatedSchool Int32,NewBorn Int32,AlreadyVaccinated Int32,FixSite011 Int32,FixSite1259 Int32,Transit011 Int32,Transit1259 Int32,CovMob Int32,HHvisit Int32,HHPlan Int32,Weakness Int32,ZeroRt Int32,location_code Int32, catchup_CCovNA011 Int32, catchup_CCovNA1259 Int32, catchup_CCovRef011 Int32, catchup_CCovRef1259 Int32, catchup_COutofH011 Int32, catchup_COutofH1259 Int32, catchup_CGuests Int32, catchup_CCovMob Int32, catchup_CNewBorn Int32, catchup_CAlreadyVaccinated Int32, catchup_CFixSite011 Int32, catchup_CFixSite1259 Int32, catchup_CTransit011 Int32, catchup_CTransit1259 Int32, catchup_UnRecorded_Cov Int32,ACCovNA011 Int32, ACCovNA1259 Int32, ACCovRef011 Int32, ACCovRef1259 Int32, ACOutofH011 Int32, ACOutofH1259 Int32, ACGuests Int32, ACCovMob Int32, ACNewBorn Int32,ACAlreadyVaccinated Int32, UnRecCov Int32, campaign_ID Int32, campaign_ActivityName String, campaign_ActivityID_old Int32, campaign_Yr Int32, campaign_SubActivityName String, location_ID Int32, geo_location_code Int32, location_name String, location_type String, location_target Int32, location_status Int32, location_priority String, hr_status String)ENGINE = MergeTree PRIMARY KEY controlroom_ID ORDER BY controlroom_ID"
        client.execute(sql)
        client.insert_dataframe(
            'INSERT INTO test.xbi_campaigncoverage  VALUES', df)

        sql = "DROP table if exists test.get_combined_coverage"
        client.execute(sql)


dag = DAG(
    'Campaign-Coverage_Automated',
    schedule_interval='*/59 * * * *',  # will run every 10 min.
    default_args=default_args,
    catchup=False)

with dag:
    GetAndInsertApiDataControlRoom = PythonOperator(
        task_id='GetAndInsertApiDataControlRoom',
        python_callable=GetAndInsertApiDataControlRoom,
    )
    CreateJoinTableOfCampaignCoverage = PythonOperator(
        task_id='CreateJoinTableOfCampaignCoverage',
        python_callable=CreateJoinTableOfCampaignCoverage,
    )
GetAndInsertApiDataControlRoom >> CreateJoinTableOfCampaignCoverage
