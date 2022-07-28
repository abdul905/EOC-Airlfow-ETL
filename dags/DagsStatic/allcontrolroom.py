from airflow import DAG
from airflow_clickhouse_plugin.hooks.clickhouse_hook import ClickHouseHook
from sqlalchemy import create_engine
import pandas as pd
from csv import reader
from types import new_class
from airflow.hooks.mysql_hook import MySqlHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mysql_operator import MySqlOperator
from datetime import datetime
import requests
import smtplib
import ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from clickhouse_driver import Client
import logging
from logging.config import dictConfig
logger = logging.getLogger(__name__)
# Create handlers
c_handler = logging.StreamHandler()
f_handler = logging.FileHandler('AllControlRoom.log')
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


def send_email_basic(sender, receiver, email_subject, **kwargs):
    port = 465  # For SSL
    smtp_server = "smtp.gmail.com"
    sender_email = 'syedbabar957@gmail.com'  # Enter your address
    receiver_email = 'b.alishah@micromerger.com'  # Enter receiver address
    password = '840s6989'  # Enter your gmail password
    email_html = """<html>
    <body>
        <p>Hello!</p>
        <h3>You are receiving Airflow notifications</h3>
        <p>Add any text you'd like to the body of the e-mail here!</p><br>
    </body>
    </html>"""

    message = MIMEMultipart("multipart")
    # Turn these into plain/html MIMEText objects
    part2 = MIMEText(email_html, "html")
    # Add HTML/plain-text parts to MIMEMultipart message
    # The email client will try to render the last part first
    message.attach(part2)
    message["Subject"] = email_subject
    message["From"] = sender_email

    for i, val in enumerate(receiver):
        message["To"] = val
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
        server.login(sender_email, password)
        server.sendmail(sender_email, receiver_email, message.as_string())


def init_email():
    sender = "syedbabar957@gmail.com"  # add the sender gmail address here
    # add your e-mail recipients here
    recipients = ["babar.bscs3261@iiu.edu.pk", "b.alishah@micromerger.com"]
    subject = "Subject Line"  # add the subject of the e-mail you'd like here
    send_email_basic(sender, recipients, subject)


def GetAndInsertApiDataControlRoom(**kwargs):
    logger.info('Function \'GetAndInsertApiDataControlRoom\' Started Off')
    client = Client(host='161.97.136.95',
                    user='default',
                    password='pakistan',
                    port='9000', settings={"use_numpy": True})
    url = 'http://idims.eoc.gov.pk/api_who/api/get_allcontrollroom_combined/5468XE2LN6CzR7qRG041/1/2020'
    logger.info('Requested Data From Api \'get_allcontrollroom_combined\' ')

    r = requests.get(url)
    data = r.json()
    logger.info('Received Data From Api \'get_allcontrollroom_combined\' ')

    rowsData = data["data"]["data"]
    columnNames = data["data"]["keys"]
    apiDataFrame = pd.DataFrame(rowsData)
    dataTypesArray = []
    for a in apiDataFrame.iloc[0]:
        dataTypesArray.append(a.isdecimal())
    arr = []

    for index, c in enumerate(columnNames):
        if c == 'TimeStamp':
            arr.append(c + ' varchar(10)')
        elif dataTypesArray[index]:
            arr.append(c + ' int')
        else:
            arr.append(c + ' varchar(50)')
    arr = '{}'.format(', '.join(arr))
    sql = "CREATE TABLE if not exists test.get_allcontrollroom_combined  ({}, PRIMARY KEY(ID ))ENGINE = MergeTree".format(
        arr)
    print('\n\n\n\nsql\n', sql)
    client.execute(sql)
    logger.info('Created Table \'test.get_allcontrollroom_combined\' ')

    df2 = client.query_dataframe(
        "SELECT * FROM test.get_allcontrollroom_combined")
    if df2.empty:
        logger.info(' Table \'test.get_allcontrollroom_combined\' is Empty')
        logger.info(
            'Inserting into Table \'test.get_allcontrollroom_combined\' ')
        client.insert_dataframe(
            'INSERT INTO test.get_allcontrollroom_combined VALUES', apiDataFrame)
        a = client.execute(
            "select count(*) from test.get_allcontrollroom_combined")
        logger.info(
            'Data inserted into Table \'test.get_allcontrollroom_combined\' ')
        logger.info(
            'Number of Rows Inserted in Table \'test.get_allcontrollroom_combined\'  :\t  %s', (a))
    else:
        logger.info(' Table \'test.get_allcontrollroom_combined\' is not Empty')


def CreateJoinTableOfControlRoom():
    logger.info(' Function  \'CreateJoinTableOfControlRoom\' Initiated')
    client = Client(host='161.97.136.95',
                    user='default',
                    password='pakistan',
                    port='9000', settings={"use_numpy": True})

    sql = "CREATE TABLE if not exists test.xbi_controlroom (controlroom_ID Int32, controlroom_IDcampCat Int32,controlroom_ActivityID Int32,controlroom_Yr Int32,controlroom_TimeStamp String,controlroom_Cday Int32,controlroom_TehsilID Int32,controlroom_UCID Int32,controlroom_ccpvTargets Int32,controlroom_VaccinationDate String,controlroom_TeamsRpt Int32,controlroom_HH011_MP Int32,controlroom_HH1259_MP Int32,controlroom_HH011_TS Int32,controlroom_HH1259_TS Int32,controlroom_HH011 Int32,controlroom_HH1259 Int32,controlroom_OutofH011 Int32,controlroom_OutofH1259 Int32,controlroom_RecNA011 Int32,controlroom_RecNA1259 Int32,controlroom_RecRef011 Int32,controlroom_RecRef1259 Int32,controlroom_CovNA011 Int32,controlroom_CovNA1259 Int32,controlroom_CovRef011 Int32,controlroom_CovRef1259 Int32,controlroom_Guests Int32,controlroom_VaccinatedSchool Int32,controlroom_NewBorn Int32,controlroom_AlreadyVaccinated Int32,controlroom_FixSite011 Int32,controlroom_FixSite1259 Int32,controlroom_Transit011 Int32,controlroom_Transit1259 Int32,controlroom_CovMob Int32,controlroom_HHvisit Int32,controlroom_HHPlan Int32,controlroom_MultipleFamily String,controlroom_Weakness Int32,controlroom_ZeroRt Int32,controlroom_OPVGiven Int32,controlroom_OPVUsed Int32,controlroom_OPVReturned Int32,controlroom_Stage34 Int32,controlroom_Inaccessible Int32,controlroom_Remarks String,controlroom_V_Age611 Int32,controlroom_v_Age1259 Int32,controlroom_V_capGiven Int32,controlroom_V_capUsed Int32,controlroom_V_capRet Int32,controlroom_status Int32,controlroom_trash Int32,controlroom_isSync Int32,controlroom_trigers String,controlroom_Covid19 Int32,controlroom_location_code Int32,controlroom_campcat_name String,compaign_id Int32,compaign_compaign_name String,compaign_type Int32,compaign_month_date Int32,compaign_comp_year Int32,compaign_start_date String,compaign_end_date String,compaign_code String,compaign_created_by Int32,compaign_create_date String,compaign_status Int8,compaign_is_deleted Int8,compaign_sortorder Int32,compaign_iscomnet Int32,compaign_api_id Int32,compaign_exports Int32,compaign_isipv Int32,geoLocation_name String,geoLocation_type Int32,geoLocation_code Int32,geoLocation_census_pop Int32,geoLocation_target Int32,geoLocation_status Int32,geoLocation_pname String,geoLocation_dname String,geoLocation_namedistrict String,geoLocation_codedistrict String,geoLocation_tname String,geoLocation_provincecode Int32,geoLocation_districtcode Int32,geoLocation_tehsilcode Int32,geoLocation_priority Int32,geoLocation_commnet Int32,geoLocation_hr Int32,geoLocation_fcm Int8,geoLocation_tier Int32,geoLocation_block String,geoLocation_division String,geoLocation_cordinates String,geoLocation_latitude String,geoLocation_longitude String,geoLocation_x String,geoLocation_y String,geoLocation_imagepath String,geoLocation_isccpv Int32,geoLocation_rank Int32,geoLocation_rank_score Float64,geoLocation_ishealthcamp Int32,geoLocation_isdsc Int32,geoLocation_ucorg String,geoLocation_organization String,geoLocation_tierfromaug161 Int32,geoLocation_tierfromsep171 Int32,geoLocation_tierfromdec181 Int32,geoLocation_mtap Int32,geoLocation_rspuc Int32,geoLocation_issmt Int32,geoLocation_updateddatetime String,geoLocation_x_code Int32,geoLocation_draining_uc Int32,geoLocation_upap_districts Int32,geoLocation_shruc Int32,geoLocation_khidist_id String)ENGINE = MergeTree PRIMARY KEY controlroom_ID ORDER BY controlroom_ID"
    client.execute(sql)
    logger.info(' Table  \'test.xbi_controlroom\' Created')
    logger.info(
        'Data Processing has been started for Joined Insertion in the Data Table')
    cols = "test.get_allcontrollroom_combined.ID,test.get_allcontrollroom_combined.IDcampCat,test.get_allcontrollroom_combined.ActivityID,test.get_allcontrollroom_combined.Yr,test.get_allcontrollroom_combined.TimeStamp,test.get_allcontrollroom_combined.Cday,test.get_allcontrollroom_combined.TehsilID,test.get_allcontrollroom_combined.UCID,test.get_allcontrollroom_combined.ccpvTargets,test.get_allcontrollroom_combined.VaccinationDate,test.get_allcontrollroom_combined.TeamsRpt,test.get_allcontrollroom_combined.HH011_MP,test.get_allcontrollroom_combined.HH1259_MP,test.get_allcontrollroom_combined.HH011_TS,test.get_allcontrollroom_combined.HH1259_TS,test.get_allcontrollroom_combined.HH011,test.get_allcontrollroom_combined.HH1259,test.get_allcontrollroom_combined.OutofH011,test.get_allcontrollroom_combined.OutofH1259,test.get_allcontrollroom_combined.RecNA011,test.get_allcontrollroom_combined.RecNA1259,test.get_allcontrollroom_combined.RecRef011,test.get_allcontrollroom_combined.RecRef1259,test.get_allcontrollroom_combined.CovNA011,test.get_allcontrollroom_combined.CovNA1259,test.get_allcontrollroom_combined.CovRef011,test.get_allcontrollroom_combined.CovRef1259,test.get_allcontrollroom_combined.Guests,test.get_allcontrollroom_combined.VaccinatedSchool,test.get_allcontrollroom_combined.NewBorn, test.eoc_geolocation_t.AlreadyVaccinated,test.get_allcontrollroom_combined.FixSite011,test.get_allcontrollroom_combined.FixSite1259,test.get_allcontrollroom_combined.Transit011,test.get_allcontrollroom_combined.Transit1259,test.get_allcontrollroom_combined.CovMob,test.get_allcontrollroom_combined.HHvisit,test.get_allcontrollroom_combined.HHPlan,test.get_allcontrollroom_combined.MultipleFamily,test.get_allcontrollroom_combined.Weakness,test.get_allcontrollroom_combined.ZeroRt,test.get_allcontrollroom_combined.OPVGiven,test.get_allcontrollroom_combined.OPVUsed,test.get_allcontrollroom_combined.OPVReturned,test.get_allcontrollroom_combined.Stage34,test.get_allcontrollroom_combined.Inaccessible,test.get_allcontrollroom_combined.Remarks,test.get_allcontrollroom_combined.V_Age611,test.get_allcontrollroom_combined.v_Age1259,test.get_allcontrollroom_combined.V_capGiven,test.get_allcontrollroom_combined.V_capUsed,test.get_allcontrollroom_combined.V_capRet,test.get_allcontrollroom_combined.status,test.get_allcontrollroom_combined.trash,test.get_allcontrollroom_combined.isSync,test.get_allcontrollroom_combined.trigers,test.get_allcontrollroom_combined.Covid19,test.get_allcontrollroom_combined.location_code,test.get_allcontrollroom_combined.campcat_name, test.eoc_compaign_tbl.id, test.eoc_compaign_tbl.compaign_name, test.eoc_compaign_tbl.type, test.eoc_compaign_tbl.month_date, test.eoc_compaign_tbl.comp_year, test.eoc_compaign_tbl.start_date, test.eoc_compaign_tbl.end_date, test.eoc_compaign_tbl.code, test.eoc_compaign_tbl.created_by, test.eoc_compaign_tbl.create_date, test.eoc_compaign_tbl.status, test.eoc_compaign_tbl.is_deleted, test.eoc_compaign_tbl.sortorder, test.eoc_compaign_tbl.iscomnet, test.eoc_compaign_tbl.api_id, test.eoc_compaign_tbl.exports, test.eoc_compaign_tbl.isipv, test.eoc_geolocation_t.name, test.eoc_geolocation_t.type, test.eoc_geolocation_t.code, test.eoc_geolocation_t.census_pop, test.eoc_geolocation_t.target, test.eoc_geolocation_t.status, test.eoc_geolocation_t.pname, test.eoc_geolocation_t.dname, test.eoc_geolocation_t.namedistrict, test.eoc_geolocation_t.codedistrict, test.eoc_geolocation_t.tname, test.eoc_geolocation_t.provincecode, test.eoc_geolocation_t.districtcode, test.eoc_geolocation_t.tehsilcode, test.eoc_geolocation_t.priority, test.eoc_geolocation_t.commnet, test.eoc_geolocation_t.hr, test.eoc_geolocation_t.fcm, test.eoc_geolocation_t.tier, test.eoc_geolocation_t.block, test.eoc_geolocation_t.division, test.eoc_geolocation_t.cordinates, test.eoc_geolocation_t.latitude, test.eoc_geolocation_t.longitude, test.eoc_geolocation_t.x, test.eoc_geolocation_t.y, test.eoc_geolocation_t.imagepath, test.eoc_geolocation_t.isccpv, test.eoc_geolocation_t.rank, test.eoc_geolocation_t.rank_score, test.eoc_geolocation_t.ishealthcamp, test.eoc_geolocation_t.isdsc, test.eoc_geolocation_t.ucorg, test.eoc_geolocation_t.organization, test.eoc_geolocation_t.tierfromaug161, test.eoc_geolocation_t.tierfromsep171, test.eoc_geolocation_t.tierfromdec181, test.eoc_geolocation_t.mtap, test.eoc_geolocation_t.rspuc, test.eoc_geolocation_t.issmt, test.eoc_geolocation_t.updateddatetime, test.eoc_geolocation_t.x_code, test.eoc_geolocation_t.draining_uc, test.eoc_geolocation_t.upap_districts, test.eoc_geolocation_t.shruc, test.eoc_geolocation_t.khidist_id"
    sql = "SELECT  " + cols + " FROM test.get_allcontrollroom_combined eoc_1 left JOIN test.eoc_geolocation_t eoc_2 ON eoc_1.location_code  = eoc_2.code left JOIN test.eoc_compaign_tbl eoc_3 ON eoc_1.IDcampCat  = eoc_3.id"
    data = client.execute(sql)
    apiDataFrame = pd.DataFrame(data)
    all_columns = list(apiDataFrame)  # Creates list of all column headers
    cols = apiDataFrame.iloc[0]
    apiDataFrame[all_columns] = apiDataFrame[all_columns].astype(str)
    d = ['controlroom_ID', 'controlroom_IDcampCat', 'controlroom_ActivityID', 'controlroom_Yr', 'controlroom_TimeStamp', 'controlroom_Cday', 'controlroom_TehsilID', 'controlroom_UCID', 'controlroom_ccpvTargets', 'controlroom_VaccinationDate', 'controlroom_TeamsRpt', 'controlroom_HH011_MP', 'controlroom_HH1259_MP', 'controlroom_HH011_TS', 'controlroom_HH1259_TS', 'controlroom_HH011', 'controlroom_HH1259', 'controlroom_OutofH011', 'controlroom_OutofH1259', 'controlroom_RecNA011', 'controlroom_RecNA1259', 'controlroom_RecRef011', 'controlroom_RecRef1259', 'controlroom_CovNA011', 'controlroom_CovNA1259', 'controlroom_CovRef011', 'controlroom_CovRef1259', 'controlroom_Guests', 'controlroom_VaccinatedSchool', 'controlroom_NewBorn', 'controlroom_AlreadyVaccinated', 'controlroom_FixSite011', 'controlroom_FixSite1259', 'controlroom_Transit011', 'controlroom_Transit1259', 'controlroom_CovMob', 'controlroom_HHvisit', 'controlroom_HHPlan', 'controlroom_MultipleFamily', 'controlroom_Weakness', 'controlroom_ZeroRt', 'controlroom_OPVGiven', 'controlroom_OPVUsed', 'controlroom_OPVReturned', 'controlroom_Stage34', 'controlroom_Inaccessible', 'controlroom_Remarks', 'controlroom_V_Age611', 'controlroom_v_Age1259', 'controlroom_V_capGiven', 'controlroom_V_capUsed', 'controlroom_V_capRet', 'controlroom_status', 'controlroom_trash', 'controlroom_isSync', 'controlroom_trigers', 'controlroom_Covid19', 'controlroom_location_code', 'controlroom_campcat_name',
         'compaign_id', 'compaign_compaign_name', 'compaign_type', 'compaign_month_date', 'compaign_comp_year', 'compaign_start_date', 'compaign_end_date', 'compaign_code', 'compaign_created_by', 'compaign_create_date', 'compaign_status', 'compaign_is_deleted', 'compaign_sortorder', 'compaign_iscomnet', 'compaign_api_id', 'compaign_exports', 'compaign_isipv', 'geoLocation_name', 'geoLocation_type', 'geoLocation_code', 'geoLocation_census_pop', 'geoLocation_target', 'geoLocation_status', 'geoLocation_pname', 'geoLocation_dname', 'geoLocation_namedistrict', 'geoLocation_codedistrict', 'geoLocation_tname', 'geoLocation_provincecode', 'geoLocation_districtcode', 'geoLocation_tehsilcode', 'geoLocation_priority', 'geoLocation_commnet', 'geoLocation_hr', 'geoLocation_fcm', 'geoLocation_tier', 'geoLocation_block', 'geoLocation_division', 'geoLocation_cordinates', 'geoLocation_latitude', 'geoLocation_longitude', 'geoLocation_x', 'geoLocation_y', 'geoLocation_imagepath', 'geoLocation_isccpv', 'geoLocation_rank', 'geoLocation_rank_score', 'geoLocation_ishealthcamp', 'geoLocation_isdsc', 'geoLocation_ucorg', 'geoLocation_organization', 'geoLocation_tierfromaug161', 'geoLocation_tierfromsep171', 'geoLocation_tierfromdec181', 'geoLocation_mtap', 'geoLocation_rspuc', 'geoLocation_issmt', 'geoLocation_updateddatetime', 'geoLocation_x_code', 'geoLocation_draining_uc', 'geoLocation_upap_districts', 'geoLocation_shruc', 'geoLocation_khidist_id']
    dff = pd.DataFrame(columns=d)
    for index, item in enumerate(d):
        dff[item] = apiDataFrame[index].values
    df2 = client.query_dataframe(
        "SELECT * FROM test.xbi_controlroom")
    if df2.empty:
        logger.info(' Table  \'test.xbi_controlroom\' is Empty')
        # print('DataFrame is empty!')
        logger.info('Start Inserting into Table  \'test.xbi_controlroom\'')

        client.insert_dataframe(
            'INSERT INTO test.xbi_controlroom  VALUES', dff)
        logger.info(
            ' Data has been inserted into Table\'test.xbi_controlroom\' ')

        a = client.execute(
            "select count(*) from test.xbi_controlroom")
        logger.info(
            'Number of Rows Inserted in Table \'test.xbi_controlroom\'  :\t  %s', (a))

        sql = "DROP table if exists test.get_allcontrollroom_combined"
        client.execute(sql)
        logger.info(
            ' Table  \'test.get_allcontrollroom_combined\' has been Dropped')

    else:
        logger.info(' Table  \'test.xbi_controlroom\' is not Empty')
        # print('not empty')
        # df2.update(dff)
        df = pd.concat([dff, df2])
        df = df.astype('str')
        df = df.drop_duplicates(subset='controlroom_ID',
                                keep="first", inplace=False)
        sql = "DROP TABLE test.xbi_controlroom;"
        client.execute(sql)
        sql = "CREATE TABLE if not exists test.xbi_controlroom (controlroom_ID Int32, controlroom_IDcampCat Int32,controlroom_ActivityID Int32,controlroom_Yr Int32,controlroom_TimeStamp String,controlroom_Cday Int32,controlroom_TehsilID Int32,controlroom_UCID Int32,controlroom_ccpvTargets Int32,controlroom_VaccinationDate String,controlroom_TeamsRpt Int32,controlroom_HH011_MP Int32,controlroom_HH1259_MP Int32,controlroom_HH011_TS Int32,controlroom_HH1259_TS Int32,controlroom_HH011 Int32,controlroom_HH1259 Int32,controlroom_OutofH011 Int32,controlroom_OutofH1259 Int32,controlroom_RecNA011 Int32,controlroom_RecNA1259 Int32,controlroom_RecRef011 Int32,controlroom_RecRef1259 Int32,controlroom_CovNA011 Int32,controlroom_CovNA1259 Int32,controlroom_CovRef011 Int32,controlroom_CovRef1259 Int32,controlroom_Guests Int32,controlroom_VaccinatedSchool Int32,controlroom_NewBorn Int32,controlroom_AlreadyVaccinated Int32,controlroom_FixSite011 Int32,controlroom_FixSite1259 Int32,controlroom_Transit011 Int32,controlroom_Transit1259 Int32,controlroom_CovMob Int32,controlroom_HHvisit Int32,controlroom_HHPlan Int32,controlroom_MultipleFamily String,controlroom_Weakness Int32,controlroom_ZeroRt Int32,controlroom_OPVGiven Int32,controlroom_OPVUsed Int32,controlroom_OPVReturned Int32,controlroom_Stage34 Int32,controlroom_Inaccessible Int32,controlroom_Remarks String,controlroom_V_Age611 Int32,controlroom_v_Age1259 Int32,controlroom_V_capGiven Int32,controlroom_V_capUsed Int32,controlroom_V_capRet Int32,controlroom_status Int32,controlroom_trash Int32,controlroom_isSync Int32,controlroom_trigers String,controlroom_Covid19 Int32,controlroom_location_code Int32,controlroom_campcat_name String,compaign_id Int32,compaign_compaign_name String,compaign_type Int32,compaign_month_date Int32,compaign_comp_year Int32,compaign_start_date String,compaign_end_date String,compaign_code String,compaign_created_by Int32,compaign_create_date String,compaign_status Int8,compaign_is_deleted Int8,compaign_sortorder Int32,compaign_iscomnet Int32,compaign_api_id Int32,compaign_exports Int32,compaign_isipv Int32,geoLocation_name String,geoLocation_type Int32,geoLocation_code Int32,geoLocation_census_pop Int32,geoLocation_target Int32,geoLocation_status Int32,geoLocation_pname String,geoLocation_dname String,geoLocation_namedistrict String,geoLocation_codedistrict String,geoLocation_tname String,geoLocation_provincecode Int32,geoLocation_districtcode Int32,geoLocation_tehsilcode Int32,geoLocation_priority Int32,geoLocation_commnet Int32,geoLocation_hr Int32,geoLocation_fcm Int8,geoLocation_tier Int32,geoLocation_block String,geoLocation_division String,geoLocation_cordinates String,geoLocation_latitude String,geoLocation_longitude String,geoLocation_x String,geoLocation_y String,geoLocation_imagepath String,geoLocation_isccpv Int32,geoLocation_rank Int32,geoLocation_rank_score Float64,geoLocation_ishealthcamp Int32,geoLocation_isdsc Int32,geoLocation_ucorg String,geoLocation_organization String,geoLocation_tierfromaug161 Int32,geoLocation_tierfromsep171 Int32,geoLocation_tierfromdec181 Int32,geoLocation_mtap Int32,geoLocation_rspuc Int32,geoLocation_issmt Int32,geoLocation_updateddatetime String,geoLocation_x_code Int32,geoLocation_draining_uc Int32,geoLocation_upap_districts Int32,geoLocation_shruc Int32,geoLocation_khidist_id String)ENGINE = MergeTree PRIMARY KEY controlroom_ID ORDER BY controlroom_ID"
        client.execute(sql)
        logger.info(' Table  \'test.xbi_controlroom\' has been Created')
        logger.info('Start Inserting into Table  \'test.xbi_controlroom\'')
        client.insert_dataframe(
            'INSERT INTO test.xbi_controlroom  VALUES', df)
        logger.info(
            ' Data has been inserted into Table\'test.xbi_controlroom\' ')

        a = client.execute(
            "select count(*) from test.xbi_controlroom")
        logger.info(
            'Number of Rows Inserted in Table \'test.xbi_controlroom\'  :\t  %s', (a))

        sql = "DROP table if exists test.get_allcontrollroom_combined"
        client.execute(sql)
        logger.info(
            ' Table  \'test.get_allcontrollroom_combined\' has been Dropped')


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'email': ['syedbabar957@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'email_on_success': True,
    'retries': 0
}
with DAG(
    "ControlRoom",
    start_date=datetime.now(),
    max_active_runs=1,
    schedule_interval='0 */1 * * * ',  # will run every 1 hour
    # schedule_interval=timedelta(minutes=2),
    catchup=False  # enable if you don't want historical dag runs to run

) as dag:
    GetAndInsertApiDataControlRoom = PythonOperator(
        task_id='GetAndInsertApiDataControlRoom',
        python_callable=GetAndInsertApiDataControlRoom,
    )
    CreateJoinTableOfControlRoom = PythonOperator(
        task_id='CreateJoinTableOfControlRoom',
        python_callable=CreateJoinTableOfControlRoom,
    )
GetAndInsertApiDataControlRoom >> CreateJoinTableOfControlRoom
