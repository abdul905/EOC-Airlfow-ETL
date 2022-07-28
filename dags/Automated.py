
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
    sql = "CREATE TABLE if not exists test.get_allcontrollroom_combined  (ID Int32, IDcampCat Int32, ActivityID Int32, Yr Int32, TimeStamp varchar(10), Cday Int32, TehsilID Int32, UCID Int32, ccpvTargets Int32, VaccinationDate varchar(50), TeamsRpt Int32, HH011_MP Int32, HH1259_MP Int32, HH011_TS Int32, HH1259_TS Int32, HH011 Int32, HH1259 Int32, OutofH011 Int32, OutofH1259 Int32, RecNA011 Int32, RecNA1259 Int32, RecRef011 Int32, RecRef1259 Int32, CovNA011 Int32, CovNA1259 Int32, CovRef011 Int32, CovRef1259 Int32, Guests Int32, VaccinatedSchool Int32, NewBorn Int32, AlreadyVaccinated Int32, FixSite011 Int32, FixSite1259 Int32, Transit011 Int32, Transit1259 Int32, CovMob Int32, HHvisit Int32, HHPlan Int32, MultipleFamily String, Weakness Int32, ZeroRt Int32, OPVGiven Int32, OPVUsed Int32, OPVReturned Int32, Stage34 Int32, Inaccessible Int32, Remarks String, V_Age611 Int32, v_Age1259 Int32, V_capGiven Int32, V_capUsed Int32, V_capRet Int32, status Int32, trash Int32, isSync Int32, trigers String, Covid19 Int32, location_code Int32, campcat_name String, PRIMARY KEY(ID ))ENGINE = MergeTree"
    client.execute(sql)

    df2 = client.query_dataframe(
        "SELECT * FROM test.get_allcontrollroom_combined")
    if df2.empty:
        client.insert_dataframe(
            'INSERT INTO test.get_allcontrollroom_combined VALUES', apiDataFrame)


def CreateJoinTableOfControlRoom():
    logger.info(' Function  \' CreateJoinTableOfControlRoom \' has been Initiated')
    client = Client(host='161.97.136.95',
                    user='default',
                    password='pakistan',
                    port='9000', settings={"use_numpy": True})
    sql = "CREATE TABLE if not exists test.xbi_controlroom (ID Int32, IDcampCat Int32,ActivityID Int32,Yr Int32,TimeStamp String,Cday Int32,TehsilID Int32,UCID Int32,ccpvTargets Int32,VaccinationDate String,TeamsRpt Int32,HH011_MP Int32,HH1259_MP Int32,HH011_TS Int32,HH1259_TS Int32,HH011 Int32,HH1259 Int32,OutofH011 Int32,OutofH1259 Int32,RecNA011 Int32,RecNA1259 Int32,RecRef011 Int32,RecRef1259 Int32,CovNA011 Int32,CovNA1259 Int32,CovRef011 Int32,CovRef1259 Int32,Guests Int32,VaccinatedSchool Int32,NewBorn Int32,AlreadyVaccinated Int32,FixSite011 Int32,FixSite1259 Int32,Transit011 Int32,Transit1259 Int32,CovMob Int32,HHvisit Int32,HHPlan Int32,MultipleFamily String,Weakness Int32,ZeroRt Int32,OPVGiven Int32,OPVUsed Int32,OPVReturned Int32,Stage34 Int32,Inaccessible Int32,Remarks String,V_Age611 Int32,v_Age1259 Int32,V_capGiven Int32,V_capUsed Int32,V_capRet Int32,status Int32,trash Int32,isSync Int32,trigers String,Covid19 Int32,location_code Int32,campcat_name String, campaign_ID Int32, campaign_ActivityName String, campaign_ActivityID_old Int32, campaign_Yr Int32, campaign_SubActivityName String, location_ID Int32, location_code Int32, location_name String, location_type String, location_target Int32, location_status Int32, location_priority String, hr_status String)ENGINE = MergeTree PRIMARY KEY ID ORDER BY ID"
    client.execute(sql)
    cols = "test.get_allcontrollroom_combined.ID,test.get_allcontrollroom_combined.IDcampCat,test.get_allcontrollroom_combined.ActivityID,test.get_allcontrollroom_combined.Yr,test.get_allcontrollroom_combined.TimeStamp,test.get_allcontrollroom_combined.Cday,test.get_allcontrollroom_combined.TehsilID,test.get_allcontrollroom_combined.UCID,test.get_allcontrollroom_combined.ccpvTargets,test.get_allcontrollroom_combined.VaccinationDate,test.get_allcontrollroom_combined.TeamsRpt,test.get_allcontrollroom_combined.HH011_MP,test.get_allcontrollroom_combined.HH1259_MP,test.get_allcontrollroom_combined.HH011_TS,test.get_allcontrollroom_combined.HH1259_TS,test.get_allcontrollroom_combined.HH011,test.get_allcontrollroom_combined.HH1259,test.get_allcontrollroom_combined.OutofH011,test.get_allcontrollroom_combined.OutofH1259,test.get_allcontrollroom_combined.RecNA011,test.get_allcontrollroom_combined.RecNA1259,test.get_allcontrollroom_combined.RecRef011,test.get_allcontrollroom_combined.RecRef1259,test.get_allcontrollroom_combined.CovNA011,test.get_allcontrollroom_combined.CovNA1259,test.get_allcontrollroom_combined.CovRef011,test.get_allcontrollroom_combined.CovRef1259,test.get_allcontrollroom_combined.Guests,test.get_allcontrollroom_combined.VaccinatedSchool,test.get_allcontrollroom_combined.NewBorn,test.get_allcontrollroom_combined.AlreadyVaccinated,test.get_allcontrollroom_combined.FixSite011,test.get_allcontrollroom_combined.FixSite1259,test.get_allcontrollroom_combined.Transit011,test.get_allcontrollroom_combined.Transit1259,test.get_allcontrollroom_combined.CovMob,test.get_allcontrollroom_combined.HHvisit,test.get_allcontrollroom_combined.HHPlan,test.get_allcontrollroom_combined.MultipleFamily,test.get_allcontrollroom_combined.Weakness,test.get_allcontrollroom_combined.ZeroRt,test.get_allcontrollroom_combined.OPVGiven,test.get_allcontrollroom_combined.OPVUsed,test.get_allcontrollroom_combined.OPVReturned,test.get_allcontrollroom_combined.Stage34,test.get_allcontrollroom_combined.Inaccessible,test.get_allcontrollroom_combined.Remarks,test.get_allcontrollroom_combined.V_Age611,test.get_allcontrollroom_combined.v_Age1259,test.get_allcontrollroom_combined.V_capGiven,test.get_allcontrollroom_combined.V_capUsed,test.get_allcontrollroom_combined.V_capRet,test.get_allcontrollroom_combined.status,test.get_allcontrollroom_combined.trash,test.get_allcontrollroom_combined.isSync,test.get_allcontrollroom_combined.trigers,test.get_allcontrollroom_combined.Covid19,test.get_allcontrollroom_combined.location_code,test.get_allcontrollroom_combined.campcat_name, test.xbi_campaign.campaign_ID, test.xbi_campaign.campaign_ActivityName, test.xbi_campaign.campaign_ActivityID_old, test.xbi_campaign.campaign_Yr, test.xbi_campaign.campaign_SubActivityName,  test.xbi_geolocation.ID, test.xbi_geolocation.code, test.xbi_geolocation.name, test.xbi_geolocation.type,  test.xbi_geolocation.location_target, test.xbi_geolocation.location_status, test.xbi_geolocation.location_priority, test.xbi_geolocation.hr_status"
    sql = "SELECT " + cols + "  FROM test.get_allcontrollroom_combined eoc_1 left JOIN test.xbi_geolocation eoc_2 ON eoc_1.location_code  = eoc_2.code left JOIN test.xbi_campaign eoc_3 ON (eoc_1.ActivityID  = eoc_3.campaign_ActivityID_old And eoc_1.Yr = eoc_3.campaign_Yr) "

    data = client.execute(sql)
    apiDataFrame = pd.DataFrame(data)
    all_columns = list(apiDataFrame)  # Creates list of all column headers
    cols = apiDataFrame.iloc[0]
    apiDataFrame[all_columns] = apiDataFrame[all_columns].astype(str)
    d = 'ID', 'IDcampCat', 'ActivityID', 'Yr', 'TimeStamp', 'Cday', 'TehsilID', 'UCID', 'ccpvTargets', 'VaccinationDate', 'TeamsRpt', 'HH011_MP', 'HH1259_MP', 'HH011_TS', 'HH1259_TS', 'HH011', 'HH1259', 'OutofH011', 'OutofH1259', 'RecNA011', 'RecNA1259', 'RecRef011', 'RecRef1259', 'CovNA011', 'CovNA1259', 'CovRef011', 'CovRef1259', 'Guests', 'VaccinatedSchool', 'NewBorn', 'AlreadyVaccinated', 'FixSite011', 'FixSite1259', 'Transit011', 'Transit1259', 'CovMob', 'HHvisit', 'HHPlan', 'MultipleFamily', 'Weakness', 'ZeroRt', 'OPVGiven', 'OPVUsed', 'OPVReturned', 'Stage34', 'Inaccessible', 'Remarks', 'V_Age611', 'v_Age1259', 'V_capGiven', 'V_capUsed', 'V_capRet', 'status', 'trash', 'isSync', 'trigers', 'Covid19', 'location_code', 'campcat_name', 'campaign_ID', 'campaign_ActivityName', 'campaign_ActivityID_old', 'campaign_Yr', 'campaign_SubActivityName','location_ID', 'location_code', 'location_name', 'location_type', 'location_target', 'location_status', 'location_priority', 'hr_status' 
    dff = pd.DataFrame(columns=d)
    for index, item in enumerate(d):
        dff[item] = apiDataFrame[index].values
    df2 = client.query_dataframe(
        "SELECT * FROM test.xbi_controlroom")
    if df2.empty:
        client.insert_dataframe(
            'INSERT INTO test.xbi_controlroom  VALUES', dff)
        logger.info(
            ' Data has been inserted into Table\' INSERT INTO test.xbi_controlroom  VALUES \' ')

        sql = "DROP table if exists test.get_allcontrollroom_combined"
        client.execute(sql)

    else:
        df = pd.concat([dff, df2])
        df = df.astype('str')
        df = df.drop_duplicates(subset='ID',
                                keep="first", inplace=False)
        sql = "DROP TABLE if exists test.xbi_controlroom;"
        client.execute(sql)
        sql = "CREATE TABLE if not exists test.xbi_controlroom (ID Int32, IDcampCat Int32,ActivityID Int32,Yr Int32,TimeStamp String,Cday Int32,TehsilID Int32,UCID Int32,ccpvTargets Int32,VaccinationDate String,TeamsRpt Int32,HH011_MP Int32,HH1259_MP Int32,HH011_TS Int32,HH1259_TS Int32,HH011 Int32,HH1259 Int32,OutofH011 Int32,OutofH1259 Int32,RecNA011 Int32,RecNA1259 Int32,RecRef011 Int32,RecRef1259 Int32,CovNA011 Int32,CovNA1259 Int32,CovRef011 Int32,CovRef1259 Int32,Guests Int32,VaccinatedSchool Int32,NewBorn Int32,AlreadyVaccinated Int32,FixSite011 Int32,FixSite1259 Int32,Transit011 Int32,Transit1259 Int32,CovMob Int32,HHvisit Int32,HHPlan Int32,MultipleFamily String,Weakness Int32,ZeroRt Int32,OPVGiven Int32,OPVUsed Int32,OPVReturned Int32,Stage34 Int32,Inaccessible Int32,Remarks String,V_Age611 Int32,v_Age1259 Int32,V_capGiven Int32,V_capUsed Int32,V_capRet Int32,status Int32,trash Int32,isSync Int32,trigers String,Covid19 Int32,location_code Int32,campcat_name String, campaign_ID Int32, campaign_ActivityName String, campaign_ActivityID_old Int32, campaign_Yr Int32, campaign_SubActivityName String, location_ID Int32, location_code Int32, location_name String, location_type String, location_target Int32, location_status Int32, location_priority String, hr_status String)ENGINE = MergeTree PRIMARY KEY ID ORDER BY ID"
        client.execute(sql)
        client.insert_dataframe(
            'INSERT INTO test.xbi_controlroom  VALUES', df)

        sql = "DROP table if exists test.get_allcontrollroom_combined"
        client.execute(sql)


dag = DAG(
    'Automated',
    schedule_interval='*/59 * * * *',  # will run every 10 min.
    default_args=default_args,
    catchup=False)

with dag:
    GetAndInsertApiDataControlRoom = PythonOperator(
        task_id='GetAndInsertApiDataControlRoom',
        python_callable=GetAndInsertApiDataControlRoom,
    )
    CreateJoinTableOfControlRoom = PythonOperator(
        task_id='CreateJoinTableOfControlRoom',
        python_callable=CreateJoinTableOfControlRoom,
    )
GetAndInsertApiDataControlRoom >> CreateJoinTableOfControlRoom
