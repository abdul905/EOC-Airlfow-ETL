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
f_handler = logging.FileHandler('CatchUp.log')
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
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


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


def GetAndInsertApiDataCatchUp():
    logger.info('Function \'GetAndInsertApiDataCatchUp\' Started Off')
    client = Client(host='161.97.136.95',
                    user='default',
                    password='pakistan',
                    port='9000', settings={"use_numpy": True})

    url = 'http://idims.eoc.gov.pk/api_who/api/get_allcatchup/5468XE2LN6CzR7qRG041/1/2021'
    logger.info('Requested Data From Api \'get_catch_up\' ')

    r = requests.get(url)
    data = r.json()
    logger.info('Received Data From Api \'get_catch_up\' ')

    rowsData = data["data"]["data"]
    columnNames = data["data"]["keys"]
    apiDataFrame = pd.DataFrame(rowsData)
    dataTypesArray = []
    for a in apiDataFrame.iloc[0]:
        dataTypesArray.append(a.isdecimal())
    arr = []
    for index, c in enumerate(columnNames):
        if c == 'TimeStamp':
            arr.append(c + ' varchar(30)')
        elif dataTypesArray[index]:
            arr.append(c + ' int')
        else:
            arr.append(c + ' varchar(50)')
    arr = '{}'.format(', '.join(arr))
    sql = " Create Table if not exists test.get_catch_up (ID Int32, UserName String, IDcampCat Int32, ActivityID Int32, TimeStamp String, Yr Int32, TehsilID Int32, UCID Int32, DistID Int32, DivID Int32, ProvID Int32, Cday Int32, CCovNA011 Int32, CCovNA1259 Int32, CCovRef011 Int32, CCovRef1259 Int32, COutofH011 Int32, COutofH1259 Int32, CGuests Int32, CCovMob Int32, CNewBorn Int32, PersistentlyMC Int32, PersistentlyMCHRMP Int32, CAlreadyVaccinated Int32, CFixSite011 Int32, CFixSite1259 Int32, CTransit011 Int32, CTransit1259 Int32, UnRecorded_Cov Int32, OPVGiven Int32, OPVUsed Int32, OPVReturned Int32, AFP_CaseRpt Int32, ZeroDos1 Int32, V_Age611_CP Int32, v_Age1259_CP Int32, status Int32, trash Int32, isSync Int32, Remarks String, location_code String)ENGINE = MergeTree PRIMARY KEY ID ORDER BY ID"
    # sql = "CREATE TABLE if not exists test.get_catch_up  ({}, PRIMARY KEY({}))ENGINE = MergeTree".format(
    #     arr, columnNames[0])
    client.execute(sql)
    logger.info('Created Table \'test.get_catch_up\' ')

    df2 = client.query_dataframe(
        "SELECT * FROM test.get_catch_up")
    if df2.empty:
        print('\n\nApi DataFrame\n', apiDataFrame)
        logger.info(' Table \'test.get_catch_up\' is Empty')
        logger.info(
            'Inserting into Table \'test.get_catch_up\' ')
        # client.insert_dataframe(
        #     'INSERT INTO test.TableApiCatchup VALUES', apiDataFrame)
        client.insert_dataframe(
            'INSERT INTO test.get_catch_up VALUES', apiDataFrame)
        a = client.execute(
            "select count(*) from test.get_catch_up")
        logger.info(
            'Data inserted into Table \'test.get_catch_up\' ')
        logger.info(
            'Number of Rows Inserted in Table \'test.get_catch_up\'  :\t  %s', (a))
    else:
        logger.info(' Table \'test.get_catch_up\' is not Empty')


def CreateJoinTableOfCatchUp():
    logger.info(' Function  \'CreateJoinTableOfCatchUp\' Initiated')
    client = Client(host='161.97.136.95',
                    user='default',
                    password='pakistan',
                    port='9000', settings={"use_numpy": True})
    sql = "CREATE TABLE if not exists test.xbi_catchUp (catchup_ID Int32, catchup_UserName String, catchup_IDcampCat  Int32, catchup_ActivityID String, catchup_TimeStamp String, catchup_Yr String, catchup_TehsilID String, catchup_UCID String, catchup_DistID String, catchup_DivID String, catchup_ProvID String, catchup_Cday String, catchup_CCovNA011 String, catchup_CCovNA1259 String, catchup_CCovRef011 String, catchup_CCovRef1259 String, catchup_COutofH011 String, catchup_COutofH1259 String, catchup_CGuests String, catchup_CCovMob String, catchup_CNewBorn String, catchup_PersistentlyMC String, catchup_PersistentlyMCHRMP String, catchup_CAlreadyVaccinated String, catchup_CFixSite011 String, catchup_CFixSite1259 String, catchup_CTransit011 String, catchup_CTransit1259 String, catchup_UnRecorded_Cov String, catchup_OPVGiven String, catchup_OPVUsed String, catchup_OPVReturned String, catchup_AFP_CaseRpt String, catchup_ZeroDos1 String, catchup_V_Age611_CP String, catchup_v_Age1259_CP String, catchup_status String, catchup_trash String, catchup_isSync String, catchup_Remarks String, catchup_location_code String, compaign_id	Int32, compaign_compaign_name	String, compaign_type	Int32, compaign_month_date	Int32, compaign_comp_year	Int32, compaign_start_date	String, compaign_end_date	String, compaign_code	String, compaign_created_by	Int32, compaign_create_date	String, compaign_status	Int32, compaign_is_deleted	Int32, compaign_sortorder	Int32, compaign_iscomnet	Int32, compaign_api_id	Int32, compaign_exports	Int32, compaign_isipv	Int32, geoLocation_name String, geoLocation_type Int32, geoLocation_code Int32, geoLocation_census_pop Int32, geoLocation_target Int32, geoLocation_status Int32, geoLocation_pname String, geoLocation_dname String, geoLocation_namedistrict String, geoLocation_codedistrict String, geoLocation_tname String, geoLocation_provincecode Int32, geoLocation_districtcode Int32, geoLocation_tehsilcode Int32, geoLocation_priority Int32, geoLocation_commnet Int32, geoLocation_hr Int32, geoLocation_fcm Int32, geoLocation_tier Int32, geoLocation_block String, geoLocation_division String, geoLocation_cordinates String, geoLocation_latitude String, geoLocation_longitude String, geoLocation_x String, geoLocation_y String, geoLocation_imagepath String, geoLocation_isccpv Int32, geoLocation_rank Int32, geoLocation_rank_score String, geoLocation_ishealthcamp Int32, geoLocation_isdsc Int32, geoLocation_ucorg String, geoLocation_organization String, geoLocation_tierfromaug161 Int32, geoLocation_tierfromsep171 Int32, geoLocation_tierfromdec181 Int32, geoLocation_mtap Int32, geoLocation_rspuc Int32, geoLocation_issmt Int32, geoLocation_updateddatetime String, geoLocation_x_code Int32, geoLocation_draining_uc Int32, geoLocation_upap_districts Int32, geoLocation_shruc Int32, geoLocation_khidist_id String)ENGINE = MergeTree PRIMARY KEY catchup_ID ORDER BY catchup_ID"
    client.execute(sql)
    logger.info(' Table  \'test.xbi_catchUp\' Created')
    cols = "test.get_catch_up.ID, test.get_catch_up.UserName, test.get_catch_up.IDcampCat, test.get_catch_up.ActivityID, test.get_catch_up.TimeStamp, test.get_catch_up.Yr, test.get_catch_up.TehsilID, test.get_catch_up.UCID, test.get_catch_up.DistID, test.get_catch_up.DivID, test.get_catch_up.ProvID, test.get_catch_up.Cday, test.get_catch_up.CCovNA011, test.get_catch_up.CCovNA1259, test.get_catch_up.CCovRef011, test.get_catch_up.CCovRef1259, test.get_catch_up.COutofH011, test.get_catch_up.COutofH1259, test.get_catch_up.CGuests, test.get_catch_up.CCovMob, test.get_catch_up.CNewBorn, test.get_catch_up.PersistentlyMC, test.get_catch_up.PersistentlyMCHRMP, test.get_catch_up.CAlreadyVaccinated, test.get_catch_up.CFixSite011, test.get_catch_up.CFixSite1259, test.get_catch_up.CTransit011, test.get_catch_up.CTransit1259, test.get_catch_up.UnRecorded_Cov, test.get_catch_up.OPVGiven, test.get_catch_up.OPVUsed, test.get_catch_up.OPVReturned, test.get_catch_up.AFP_CaseRpt, test.get_catch_up.ZeroDos1, test.get_catch_up.V_Age611_CP, test.get_catch_up.v_Age1259_CP, test.get_catch_up.status, test.get_catch_up.trash, test.get_catch_up.isSync, test.get_catch_up.Remarks, test.get_catch_up.location_code,test.eoc_compaign_tbl.id, test.eoc_compaign_tbl.compaign_name, test.eoc_compaign_tbl.type, test.eoc_compaign_tbl.month_date, test.eoc_compaign_tbl.comp_year, test.eoc_compaign_tbl.start_date, test.eoc_compaign_tbl.end_date, test.eoc_compaign_tbl.code, test.eoc_compaign_tbl.created_by, test.eoc_compaign_tbl.create_date, test.eoc_compaign_tbl.status, test.eoc_compaign_tbl.is_deleted, test.eoc_compaign_tbl.sortorder, test.eoc_compaign_tbl.iscomnet, test.eoc_compaign_tbl.api_id, test.eoc_compaign_tbl.exports, test.eoc_compaign_tbl.isipv, test.eoc_geolocation_t.name, test.eoc_geolocation_t.type, test.eoc_geolocation_t.code, test.eoc_geolocation_t.census_pop, test.eoc_geolocation_t.target, test.eoc_geolocation_t.status, test.eoc_geolocation_t.pname, test.eoc_geolocation_t.dname, test.eoc_geolocation_t.namedistrict, test.eoc_geolocation_t.codedistrict, test.eoc_geolocation_t.tname, test.eoc_geolocation_t.provincecode, test.eoc_geolocation_t.districtcode, test.eoc_geolocation_t.tehsilcode, test.eoc_geolocation_t.priority, test.eoc_geolocation_t.commnet, test.eoc_geolocation_t.hr, test.eoc_geolocation_t.fcm, test.eoc_geolocation_t.tier, test.eoc_geolocation_t.block, test.eoc_geolocation_t.division, test.eoc_geolocation_t.cordinates, test.eoc_geolocation_t.latitude, test.eoc_geolocation_t.longitude, test.eoc_geolocation_t.x, test.eoc_geolocation_t.y, test.eoc_geolocation_t.imagepath, test.eoc_geolocation_t.isccpv, test.eoc_geolocation_t.rank, test.eoc_geolocation_t.rank_score, test.eoc_geolocation_t.ishealthcamp, test.eoc_geolocation_t.isdsc, test.eoc_geolocation_t.ucorg, test.eoc_geolocation_t.organization, test.eoc_geolocation_t.tierfromaug161, test.eoc_geolocation_t.tierfromsep171, test.eoc_geolocation_t.tierfromdec181, test.eoc_geolocation_t.mtap, test.eoc_geolocation_t.rspuc, test.eoc_geolocation_t.issmt, test.eoc_geolocation_t.updateddatetime, test.eoc_geolocation_t.x_code, test.eoc_geolocation_t.draining_uc, test.eoc_geolocation_t.upap_districts, test.eoc_geolocation_t.shruc, test.eoc_geolocation_t.khidist_id "
    sql = "SELECT " + cols + " FROM test.get_catch_up eoc_1 left JOIN test.eoc_geolocation_t eoc_2 ON eoc_1.UCID  = eoc_2.code left JOIN test.eoc_compaign_tbl eoc_3 ON eoc_1.IDcampCat  = eoc_3.id"
    data = client.execute(sql)
    apiDataFrame = pd.DataFrame(data)
    # print(apiDataFrame)
    all_columns = list(apiDataFrame)  # Creates list of all column headers
    cols = apiDataFrame.iloc[0]
    apiDataFrame[all_columns] = apiDataFrame[all_columns].astype(str)
    d = 'catchup_ID', 'catchup_UserName', 'catchup_IDcampCat', 'catchup_ActivityID', 'catchup_TimeStamp', 'catchup_Yr', 'catchup_TehsilID', 'catchup_UCID', 'catchup_DistID', 'catchup_DivID', 'catchup_ProvID', 'catchup_Cday', 'catchup_CCovNA011', 'catchup_CCovNA1259', 'catchup_CCovRef011', 'catchup_CCovRef1259', 'catchup_COutofH011', 'catchup_COutofH1259', 'catchup_CGuests', 'catchup_CCovMob', 'catchup_CNewBorn', 'catchup_PersistentlyMC', 'catchup_PersistentlyMCHRMP', 'catchup_CAlreadyVaccinated', 'catchup_CFixSite011', 'catchup_CFixSite1259', 'catchup_CTransit011', 'catchup_CTransit1259', 'catchup_UnRecorded_Cov', 'catchup_OPVGiven', 'catchup_OPVUsed', 'catchup_OPVReturned', 'catchup_AFP_CaseRpt', 'catchup_ZeroDos1', 'catchup_V_Age611_CP', 'catchup_v_Age1259_CP', 'catchup_status', 'catchup_trash', 'catchup_isSync', 'catchup_Remarks', 'catchup_location_code', 'compaign_id', 'compaign_compaign_name', 'compaign_type', 'compaign_month_date', 'compaign_comp_year', 'compaign_start_date', 'compaign_end_date', 'compaign_code', 'compaign_created_by', 'compaign_create_date', 'compaign_status', 'compaign_is_deleted', 'compaign_sortorder', 'compaign_iscomnet', 'compaign_api_id', 'compaign_exports', 'compaign_isipv', 'geoLocation_name', 'geoLocation_type', 'geoLocation_code', 'geoLocation_census_pop', 'geoLocation_target', 'geoLocation_status', 'geoLocation_pname', 'geoLocation_dname', 'geoLocation_namedistrict', 'geoLocation_codedistrict', 'geoLocation_tname', 'geoLocation_provincecode', 'geoLocation_districtcode', 'geoLocation_tehsilcode', 'geoLocation_priority', 'geoLocation_commnet', 'geoLocation_hr', 'geoLocation_fcm', 'geoLocation_tier', 'geoLocation_block', 'geoLocation_division', 'geoLocation_cordinates', 'geoLocation_latitude', 'geoLocation_longitude', 'geoLocation_x', 'geoLocation_y', 'geoLocation_imagepath', 'geoLocation_isccpv', 'geoLocation_rank', 'geoLocation_rank_score', 'geoLocation_ishealthcamp', 'geoLocation_isdsc', 'geoLocation_ucorg', 'geoLocation_organization', 'geoLocation_tierfromaug161', 'geoLocation_tierfromsep171', 'geoLocation_tierfromdec181', 'geoLocation_mtap', 'geoLocation_rspuc', 'geoLocation_issmt', 'geoLocation_updateddatetime', 'geoLocation_x_code', 'geoLocation_draining_uc', 'geoLocation_upap_districts', 'geoLocation_shruc', 'geoLocation_khidist_id'

    dff = pd.DataFrame(columns=d)
    for index, item in enumerate(d):
        dff[item] = apiDataFrame[index].values
    df2 = client.query_dataframe(
        "SELECT * FROM test.xbi_catchUp")

    if df2.empty:
        print('\n\n Empty', dff.head())
        client.insert_dataframe(
            'INSERT INTO test.xbi_catchUp  VALUES', dff)

        a = client.execute(
            "select count(*) from test.xbi_catchUp")

        sql = "DROP table if exists test.get_catch_up"
        client.execute(sql)
        logger.info(
            ' Table  \'test.get_catch_up\' has been Dropped')

    else:
        df = pd.concat([dff, df2])
        df = df.astype('str')
        df = df.drop_duplicates(subset='catchup_ID',
                                keep="first", inplace=False)
        sql = "DROP TABLE test.xbi_catchUp;"
        client.execute(sql)
        sql = "CREATE TABLE if not exists test.xbi_catchUp (catchup_ID String, catchup_UserName String, catchup_IDcampCat Int32, catchup_ActivityID String, catchup_TimeStamp String, catchup_Yr String, catchup_TehsilID String, catchup_UCID String, catchup_DistID String, catchup_DivID String, catchup_ProvID String, catchup_Cday String, catchup_CCovNA011 String, catchup_CCovNA1259 String, catchup_CCovRef011 String, catchup_CCovRef1259 String, catchup_COutofH011 String, catchup_COutofH1259 String, catchup_CGuests String, catchup_CCovMob String, catchup_CNewBorn String, catchup_PersistentlyMC String, catchup_PersistentlyMCHRMP String, catchup_CAlreadyVaccinated String, catchup_CFixSite011 String, catchup_CFixSite1259 String, catchup_CTransit011 String, catchup_CTransit1259 String, catchup_UnRecorded_Cov String, catchup_OPVGiven String, catchup_OPVUsed String, catchup_OPVReturned String, catchup_AFP_CaseRpt String, catchup_ZeroDos1 String, catchup_V_Age611_CP String, catchup_v_Age1259_CP String, catchup_status String, catchup_trash String, catchup_isSync String, catchup_Remarks String, catchup_location_code String, compaign_id	Int32, compaign_compaign_name	String, compaign_type	Int32, compaign_month_date	Int32, compaign_comp_year	Int32, compaign_start_date	String, compaign_end_date	String, compaign_code	String, compaign_created_by	Int32, compaign_create_date	String, compaign_status	Int32, compaign_is_deleted	Int32, compaign_sortorder	Int32, compaign_iscomnet	Int32, compaign_api_id	Int32, compaign_exports	Int32, compaign_isipv	Int32, geoLocation_name String, geoLocation_type Int32, geoLocation_code Int32, geoLocation_census_pop Int32, geoLocation_target Int32, geoLocation_status Int32, geoLocation_pname String, geoLocation_dname String, geoLocation_namedistrict String, geoLocation_codedistrict String, geoLocation_tname String, geoLocation_provincecode Int32, geoLocation_districtcode Int32, geoLocation_tehsilcode Int32, geoLocation_priority Int32, geoLocation_commnet Int32, geoLocation_hr Int32, geoLocation_fcm Int32, geoLocation_tier Int32, geoLocation_block String, geoLocation_division String, geoLocation_cordinates String, geoLocation_latitude String, geoLocation_longitude String, geoLocation_x String, geoLocation_y String, geoLocation_imagepath String, geoLocation_isccpv Int32, geoLocation_rank Int32, geoLocation_rank_score String, geoLocation_ishealthcamp Int32, geoLocation_isdsc Int32, geoLocation_ucorg String, geoLocation_organization String, geoLocation_tierfromaug161 Int32, geoLocation_tierfromsep171 Int32, geoLocation_tierfromdec181 Int32, geoLocation_mtap Int32, geoLocation_rspuc Int32, geoLocation_issmt Int32, geoLocation_updateddatetime String, geoLocation_x_code Int32, geoLocation_draining_uc Int32, geoLocation_upap_districts Int32, geoLocation_shruc Int32, geoLocation_khidist_id String)ENGINE = MergeTree PRIMARY KEY catchup_ID ORDER BY catchup_ID"
        client.execute(sql)
        logger.info(' Table  \'test.xbi_catchUp\' has been Created')
        logger.info('Start Inserting into Table  \'test.xbi_catchUp\'')
        client.insert_dataframe(
            'INSERT INTO test.xbi_catchUp  VALUES', df)
        logger.info(
            ' Data has been inserted into Table\'test.xbi_catchUp\' ')

        a = client.execute(
            "select count(*) from test.xbi_catchUp")
        logger.info(
            'Number of Rows Inserted in Table \'test.xbi_catchUp\'  :\t  %s', (a))

        sql = "DROP table if exists test.get_catch_up"
        client.execute(sql)
        logger.info(
            ' Table  \'test.get_catch_up\' has been Dropped')


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
    "CatchUp",
    start_date=datetime.now(),
    max_active_runs=1,
    schedule_interval='0 */1 * * * ',  # will run every 1 hour
    # schedule_interval=timedelta(minutes=2),
    catchup=False  # enable if you don't want historical dag runs to run

) as dag:
    GetAndInsertApiDataCatchUp = PythonOperator(
        task_id='GetAndInsertApiDataCatchUp',
        python_callable=GetAndInsertApiDataCatchUp,
    )
    CreateJoinTableOfCatchUp = PythonOperator(
        task_id='CreateJoinTableOfCatchUp',
        python_callable=CreateJoinTableOfCatchUp,
    )
GetAndInsertApiDataCatchUp >> CreateJoinTableOfCatchUp
