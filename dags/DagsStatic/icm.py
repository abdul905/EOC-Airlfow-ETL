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
f_handler = logging.FileHandler('ICM.log')
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


def GetAndInsertApiDataICM(**kwargs):
    logger.info('Function \'GetAndInsertApiDataICM\' Started Off')
    client = Client(host='161.97.136.95',
                    user='default',
                    password='pakistan',
                    port='9000', settings={"use_numpy": True})

    url = 'https://idims.eoc.gov.pk/api_who/api/get_icm/5468XE2LN6CzR7qRG041/team/247'
    logger.info('Requested Data From Api \'get_icm\' ')

    r = requests.get(url)
    data = r.json()
    logger.info('Received Data From Api \'get_icm\' ')

    rowsData = data["data"]["data"]
    columnNames = data["data"]["keys"]
    apiDataFrame = pd.DataFrame(rowsData)

    sql = "CREATE TABLE if not exists test.get_icm ( pk_icm_team_21_id Int32, fk_form_id Int32, fk_user_id Int32, date_created String, campaign_id Int32, surveyor_name String, designation String, surveyor_affliate String, fk_prov_id Int32, fk_dist_id Int32, fk_tehsil_id Int32, fk_uc_id Int32, g1_cbv_noncbv String, g1_aic_name String, g1_team_no String, g1_day String, g1_team_composition_government String, g1_team_composition_local String, g1_team_composition_female String, g1_team_composition_adult String, g1_NamesMP_match String, g1_valid_marker String, g1_T_memTrained String, g1_Tdate String, g1_opv_dry_valid_vvm String, g1_opv_dry_cool String, g1_opv_dry_dry String, g1_t_sheetcorrect String, g1_vitamin_a String, g1_T_recordedMissed String, g1_T_mapAvailable String, g1_Tworkload String, g1_Comment String, g2_obsorve String, g2_T_chiledAwayHome Int32, g2_T_rmAFP Int32, g2_T_markerUsed Int32, g2_T_CfMarked Int32, g2_T_childVaccinatedToday Int32, g2_Comments String, covid_indi String, covid_brief_guidline Int32, covid_symptoms Int32, covid_wear_mask Int32, covid_use_santzr Int32, covid_age_member Int32, covid_hold_child Int32, covid_chllenges String, covid_chllenges_dscrptn String, unique_id String, dev_remarks String, app_version String, cold_chain String, g1_opv_dry_intact String, g1_opv_dry_re_capped String, g1_matched_admin_team String)ENGINE = MergeTree PRIMARY KEY pk_icm_team_21_id ORDER BY pk_icm_team_21_id"
    client.execute(sql)
    logger.info('Created Table \'test.get_icm\' ')

    df2 = client.query_dataframe(
        "SELECT * FROM test.get_icm")
    if df2.empty:
        logger.info(' Table \'test.get_icm\' is Empty')
        logger.info(
            'Inserting into Table \'test.get_icm\' ')
        client.insert_dataframe(
            'INSERT INTO test.get_icm VALUES', apiDataFrame)
        a = client.execute(
            "select count(*) from test.get_icm")
        logger.info(
            'Data inserted into Table \'test.get_icm\' ')
        logger.info(
            'Number of Rows Inserted in Table \'test.get_icm\'  :\t  %s', (a))
    else:
        logger.info(' Table \'test.get_icm\' is not Empty')


def CreateJoinTableOfICM():
    logger.info(' Function  \'CreateJoinTableOfICM\' Initiated')
    client = Client(host='161.97.136.95',
                    user='default',
                    password='pakistan',
                    port='9000', settings={"use_numpy": True})
    sql = "CREATE TABLE if not exists test.xbi_icm (icm_pk_icm_team_21_id	Int32, icm_fk_form_id	Int32, icm_fk_user_id	Int32, icm_date_created	String, icm_campaign_id	Int32, icm_surveyor_name	String, icm_designation	String, icm_surveyor_affliate	String, icm_fk_prov_id	Int32, icm_fk_dist_id	Int32, icm_fk_tehsil_id	Int32, icm_fk_uc_id	Int32, icm_g1_cbv_noncbv	String, icm_g1_aic_name	String, icm_g1_team_no	String, icm_g1_day	String, icm_g1_team_composition_government	String, icm_g1_team_composition_local	String, icm_g1_team_composition_female	String, icm_g1_team_composition_adult	String, icm_g1_NamesMP_match	String, icm_g1_valid_marker	String, icm_g1_T_memTrained	String, icm_g1_Tdate	String, icm_g1_opv_dry_valid_vvm	String, icm_g1_opv_dry_cool	String, icm_g1_opv_dry_dry	String, icm_g1_t_sheetcorrect	String, icm_g1_vitamin_a	String, icm_g1_T_recordedMissed	String, icm_g1_T_mapAvailable	String, icm_g1_Tworkload	String, icm_g1_Comment	String, icm_g2_obsorve	String, icm_g2_T_chiledAwayHome	Int32, icm_g2_T_rmAFP	Int32, icm_g2_T_markerUsed	Int32, icm_g2_T_CfMarked	Int32, icm_g2_T_childVaccinatedToday	Int32, icm_g2_Comments	String, icm_covid_indi	String, icm_covid_brief_guidline	Int32, icm_covid_symptoms	Int32, icm_covid_wear_mask	Int32, icm_covid_use_santzr	Int32, icm_covid_age_member	Int32, icm_covid_hold_child	Int32, icm_covid_chllenges	String, icm_covid_chllenges_dscrptn	String, icm_unique_id	String, icm_dev_remarks	String, icm_app_version	String, icm_cold_chain	String, icm_g1_opv_dry_intact	String, icm_g1_opv_dry_re_capped	String, icm_g1_matched_admin_team	String, compaign_id	Int32, compaign_compaign_name	String, compaign_type	Int32, compaign_month_date	Int32, compaign_comp_year	Int32, compaign_start_date	String, compaign_end_date	String, compaign_code	String, compaign_created_by	Int32, compaign_create_date	String, compaign_status	Int32, compaign_is_deleted	Int32, compaign_sortorder	Int32, compaign_iscomnet	Int32, compaign_api_id	Int32, compaign_exports	Int32, compaign_isipv	Int32, geoLocation_name String, geoLocation_type Int32, geoLocation_code Int32, geoLocation_census_pop Int32, geoLocation_target Int32, geoLocation_status Int32, geoLocation_pname String, geoLocation_dname String, geoLocation_namedistrict String, geoLocation_codedistrict String, geoLocation_tname String, geoLocation_provincecode Int32, geoLocation_districtcode Int32, geoLocation_tehsilcode Int32, geoLocation_priority Int32, geoLocation_commnet Int32, geoLocation_hr Int32, geoLocation_fcm Int32, geoLocation_tier Int32, geoLocation_block String, geoLocation_division String, geoLocation_cordinates String, geoLocation_latitude String, geoLocation_longitude String, geoLocation_x String, geoLocation_y String, geoLocation_imagepath String, geoLocation_isccpv Int32, geoLocation_rank Int32, geoLocation_rank_score String, geoLocation_ishealthcamp Int32, geoLocation_isdsc Int32, geoLocation_ucorg String, geoLocation_organization String, geoLocation_tierfromaug161 Int32, geoLocation_tierfromsep171 Int32, geoLocation_tierfromdec181 Int32, geoLocation_mtap Int32, geoLocation_rspuc Int32, geoLocation_issmt Int32, geoLocation_updateddatetime String, geoLocation_x_code Int32, geoLocation_draining_uc Int32, geoLocation_upap_districts Int32, geoLocation_shruc Int32, geoLocation_khidist_id String)ENGINE = MergeTree PRIMARY KEY icm_pk_icm_team_21_id ORDER BY icm_pk_icm_team_21_id"
    client.execute(sql)
    logger.info(' Table  \'test.xbi_icm\' Created')
    # logger.info(
    #     'Data Processing has been started for Joined Insertion in the Data Table')
    # cols = "test.get_allcontrollroom_combined.ID,test.get_allcontrollroom_combined.IDcampCat,test.get_allcontrollroom_combined.ActivityID,test.get_allcontrollroom_combined.Yr,test.get_allcontrollroom_combined.TimeStamp,test.get_allcontrollroom_combined.Cday,test.get_allcontrollroom_combined.TehsilID,test.get_allcontrollroom_combined.UCID,test.get_allcontrollroom_combined.ccpvTargets,test.get_allcontrollroom_combined.VaccinationDate,test.get_allcontrollroom_combined.TeamsRpt,test.get_allcontrollroom_combined.HH011_MP,test.get_allcontrollroom_combined.HH1259_MP,test.get_allcontrollroom_combined.HH011_TS,test.get_allcontrollroom_combined.HH1259_TS,test.get_allcontrollroom_combined.HH011,test.get_allcontrollroom_combined.HH1259,test.get_allcontrollroom_combined.OutofH011,test.get_allcontrollroom_combined.OutofH1259,test.get_allcontrollroom_combined.RecNA011,test.get_allcontrollroom_combined.RecNA1259,test.get_allcontrollroom_combined.RecRef011,test.get_allcontrollroom_combined.RecRef1259,test.get_allcontrollroom_combined.CovNA011,test.get_allcontrollroom_combined.CovNA1259,test.get_allcontrollroom_combined.CovRef011,test.get_allcontrollroom_combined.CovRef1259,test.get_allcontrollroom_combined.Guests,test.get_allcontrollroom_combined.VaccinatedSchool,test.get_allcontrollroom_combined.NewBorn, test.eoc_geolocation_t.AlreadyVaccinated,test.get_allcontrollroom_combined.FixSite011,test.get_allcontrollroom_combined.FixSite1259,test.get_allcontrollroom_combined.Transit011,test.get_allcontrollroom_combined.Transit1259,test.get_allcontrollroom_combined.CovMob,test.get_allcontrollroom_combined.HHvisit,test.get_allcontrollroom_combined.HHPlan,test.get_allcontrollroom_combined.MultipleFamily,test.get_allcontrollroom_combined.Weakness,test.get_allcontrollroom_combined.ZeroRt,test.get_allcontrollroom_combined.OPVGiven,test.get_allcontrollroom_combined.OPVUsed,test.get_allcontrollroom_combined.OPVReturned,test.get_allcontrollroom_combined.Stage34,test.get_allcontrollroom_combined.Inaccessible,test.get_allcontrollroom_combined.Remarks,test.get_allcontrollroom_combined.V_Age611,test.get_allcontrollroom_combined.v_Age1259,test.get_allcontrollroom_combined.V_capGiven,test.get_allcontrollroom_combined.V_capUsed,test.get_allcontrollroom_combined.V_capRet,test.get_allcontrollroom_combined.status,test.get_allcontrollroom_combined.trash,test.get_allcontrollroom_combined.isSync,test.get_allcontrollroom_combined.trigers,test.get_allcontrollroom_combined.Covid19,test.get_allcontrollroom_combined.location_code,test.get_allcontrollroom_combined.campcat_name, test.eoc_compaign_tbl.id, test.eoc_compaign_tbl.compaign_name, test.eoc_compaign_tbl.type, test.eoc_compaign_tbl.month_date, test.eoc_compaign_tbl.comp_year, test.eoc_compaign_tbl.start_date, test.eoc_compaign_tbl.end_date, test.eoc_compaign_tbl.code, test.eoc_compaign_tbl.created_by, test.eoc_compaign_tbl.create_date, test.eoc_compaign_tbl.status, test.eoc_compaign_tbl.is_deleted, test.eoc_compaign_tbl.sortorder, test.eoc_compaign_tbl.iscomnet, test.eoc_compaign_tbl.api_id, test.eoc_compaign_tbl.exports, test.eoc_compaign_tbl.isipv, test.eoc_geolocation_t.name, test.eoc_geolocation_t.type, test.eoc_geolocation_t.code, test.eoc_geolocation_t.census_pop, test.eoc_geolocation_t.target, test.eoc_geolocation_t.status, test.eoc_geolocation_t.pname, test.eoc_geolocation_t.dname, test.eoc_geolocation_t.namedistrict, test.eoc_geolocation_t.codedistrict, test.eoc_geolocation_t.tname, test.eoc_geolocation_t.provincecode, test.eoc_geolocation_t.districtcode, test.eoc_geolocation_t.tehsilcode, test.eoc_geolocation_t.priority, test.eoc_geolocation_t.commnet, test.eoc_geolocation_t.hr, test.eoc_geolocation_t.fcm, test.eoc_geolocation_t.tier, test.eoc_geolocation_t.block, test.eoc_geolocation_t.division, test.eoc_geolocation_t.cordinates, test.eoc_geolocation_t.latitude, test.eoc_geolocation_t.longitude, test.eoc_geolocation_t.x, test.eoc_geolocation_t.y, test.eoc_geolocation_t.imagepath, test.eoc_geolocation_t.isccpv, test.eoc_geolocation_t.rank, test.eoc_geolocation_t.rank_score, test.eoc_geolocation_t.ishealthcamp, test.eoc_geolocation_t.isdsc, test.eoc_geolocation_t.ucorg, test.eoc_geolocation_t.organization, test.eoc_geolocation_t.tierfromaug161, test.eoc_geolocation_t.tierfromsep171, test.eoc_geolocation_t.tierfromdec181, test.eoc_geolocation_t.mtap, test.eoc_geolocation_t.rspuc, test.eoc_geolocation_t.issmt, test.eoc_geolocation_t.updateddatetime, test.eoc_geolocation_t.x_code, test.eoc_geolocation_t.draining_uc, test.eoc_geolocation_t.upap_districts, test.eoc_geolocation_t.shruc, test.eoc_geolocation_t.khidist_id"
    # sql = "SELECT  * FROM test.get_icm eoc_1 left JOIN test.eoc_geolocation_t eoc_2 ON eoc_1.fk_uc_id  = eoc_2.code left JOIN test.eoc_compaign_tbl eoc_3 ON eoc_1.IDcampCat  = eoc_3.id"

    cols = "test.get_icm.pk_icm_team_21_id, test.get_icm.fk_form_id, test.get_icm.fk_user_id, test.get_icm.date_created, test.get_icm.campaign_id, test.get_icm.surveyor_name, test.get_icm.designation, test.get_icm.surveyor_affliate, test.get_icm.fk_prov_id, test.get_icm.fk_dist_id, test.get_icm.fk_tehsil_id, test.get_icm.fk_uc_id, test.get_icm.g1_cbv_noncbv, test.get_icm.g1_aic_name, test.get_icm.g1_team_no, test.get_icm.g1_day, test.get_icm.g1_team_composition_government, test.get_icm.g1_team_composition_local, test.get_icm.g1_team_composition_female, test.get_icm.g1_team_composition_adult, test.get_icm.g1_NamesMP_match, test.get_icm.g1_valid_marker, test.get_icm.g1_T_memTrained, test.get_icm.g1_Tdate, test.get_icm.g1_opv_dry_valid_vvm, test.get_icm.g1_opv_dry_cool, test.get_icm.g1_opv_dry_dry, test.get_icm.g1_t_sheetcorrect, test.get_icm.g1_vitamin_a, test.get_icm.g1_T_recordedMissed, test.get_icm.g1_T_mapAvailable, test.get_icm.g1_Tworkload, test.get_icm.g1_Comment, test.get_icm.g2_obsorve, test.get_icm.g2_T_chiledAwayHome, test.get_icm.g2_T_rmAFP, test.get_icm.g2_T_markerUsed, test.get_icm.g2_T_CfMarked, test.get_icm.g2_T_childVaccinatedToday, test.get_icm.g2_Comments, test.get_icm.covid_indi, test.get_icm.covid_brief_guidline, test.get_icm.covid_symptoms, test.get_icm.covid_wear_mask, test.get_icm.covid_use_santzr, test.get_icm.covid_age_member, test.get_icm.covid_hold_child, test.get_icm.covid_chllenges, test.get_icm.covid_chllenges_dscrptn, test.get_icm.unique_id, test.get_icm.dev_remarks, test.get_icm.app_version, test.get_icm.cold_chain, test.get_icm.g1_opv_dry_intact, test.get_icm.g1_opv_dry_re_capped, test.get_icm.g1_matched_admin_team,test.eoc_compaign_tbl.id, test.eoc_compaign_tbl.compaign_name, test.eoc_compaign_tbl.type, test.eoc_compaign_tbl.month_date, test.eoc_compaign_tbl.comp_year, test.eoc_compaign_tbl.start_date, test.eoc_compaign_tbl.end_date, test.eoc_compaign_tbl.code, test.eoc_compaign_tbl.created_by, test.eoc_compaign_tbl.create_date, test.eoc_compaign_tbl.status, test.eoc_compaign_tbl.is_deleted, test.eoc_compaign_tbl.sortorder, test.eoc_compaign_tbl.iscomnet, test.eoc_compaign_tbl.api_id, test.eoc_compaign_tbl.exports, test.eoc_compaign_tbl.isipv, test.eoc_geolocation_t.name, test.eoc_geolocation_t.type, test.eoc_geolocation_t.code, test.eoc_geolocation_t.census_pop, test.eoc_geolocation_t.target, test.eoc_geolocation_t.status, test.eoc_geolocation_t.pname, test.eoc_geolocation_t.dname, test.eoc_geolocation_t.namedistrict, test.eoc_geolocation_t.codedistrict, test.eoc_geolocation_t.tname, test.eoc_geolocation_t.provincecode, test.eoc_geolocation_t.districtcode, test.eoc_geolocation_t.tehsilcode, test.eoc_geolocation_t.priority, test.eoc_geolocation_t.commnet, test.eoc_geolocation_t.hr, test.eoc_geolocation_t.fcm, test.eoc_geolocation_t.tier, test.eoc_geolocation_t.block, test.eoc_geolocation_t.division, test.eoc_geolocation_t.cordinates, test.eoc_geolocation_t.latitude, test.eoc_geolocation_t.longitude, test.eoc_geolocation_t.x, test.eoc_geolocation_t.y, test.eoc_geolocation_t.imagepath, test.eoc_geolocation_t.isccpv, test.eoc_geolocation_t.rank, test.eoc_geolocation_t.rank_score, test.eoc_geolocation_t.ishealthcamp, test.eoc_geolocation_t.isdsc, test.eoc_geolocation_t.ucorg, test.eoc_geolocation_t.organization, test.eoc_geolocation_t.tierfromaug161, test.eoc_geolocation_t.tierfromsep171, test.eoc_geolocation_t.tierfromdec181, test.eoc_geolocation_t.mtap, test.eoc_geolocation_t.rspuc, test.eoc_geolocation_t.issmt, test.eoc_geolocation_t.updateddatetime, test.eoc_geolocation_t.x_code, test.eoc_geolocation_t.draining_uc, test.eoc_geolocation_t.upap_districts, test.eoc_geolocation_t.shruc, test.eoc_geolocation_t.khidist_id "
    sql = "SELECT " + cols + " FROM test.get_icm eoc_1 left JOIN test.eoc_geolocation_t eoc_2 ON eoc_1.fk_uc_id  = eoc_2.code left JOIN test.eoc_compaign_tbl eoc_3 ON eoc_1.fk_form_id  = eoc_3.id"
    data = client.execute(sql)
    apiDataFrame = pd.DataFrame(data)
    # print(apiDataFrame)
    all_columns = list(apiDataFrame)  # Creates list of all column headers
    cols = apiDataFrame.iloc[0]
    apiDataFrame[all_columns] = apiDataFrame[all_columns].astype(str)
    d = 'icm_pk_icm_team_21_id', 'icm_fk_form_id', 'icm_fk_user_id', 'icm_date_created', 'icm_campaign_id', 'icm_surveyor_name', 'icm_designation', 'icm_surveyor_affliate', 'icm_fk_prov_id', 'icm_fk_dist_id', 'icm_fk_tehsil_id', 'icm_fk_uc_id', 'icm_g1_cbv_noncbv', 'icm_g1_aic_name', 'icm_g1_team_no', 'icm_g1_day', 'icm_g1_team_composition_government', 'icm_g1_team_composition_local', 'icm_g1_team_composition_female', 'icm_g1_team_composition_adult', 'icm_g1_NamesMP_match', 'icm_g1_valid_marker', 'icm_g1_T_memTrained', 'icm_g1_Tdate', 'icm_g1_opv_dry_valid_vvm', 'icm_g1_opv_dry_cool', 'icm_g1_opv_dry_dry', 'icm_g1_t_sheetcorrect', 'icm_g1_vitamin_a', 'icm_g1_T_recordedMissed', 'icm_g1_T_mapAvailable', 'icm_g1_Tworkload', 'icm_g1_Comment', 'icm_g2_obsorve', 'icm_g2_T_chiledAwayHome', 'icm_g2_T_rmAFP', 'icm_g2_T_markerUsed', 'icm_g2_T_CfMarked', 'icm_g2_T_childVaccinatedToday', 'icm_g2_Comments', 'icm_covid_indi', 'icm_covid_brief_guidline', 'icm_covid_symptoms', 'icm_covid_wear_mask', 'icm_covid_use_santzr', 'icm_covid_age_member', 'icm_covid_hold_child', 'icm_covid_chllenges', 'icm_covid_chllenges_dscrptn', 'icm_unique_id', 'icm_dev_remarks', 'icm_app_version', 'icm_cold_chain', 'icm_g1_opv_dry_intact', 'icm_g1_opv_dry_re_capped', 'icm_g1_matched_admin_team', 'compaign_id', 'compaign_compaign_name', 'compaign_type', 'compaign_month_date', 'compaign_comp_year', 'compaign_start_date', 'compaign_end_date', 'compaign_code', 'compaign_created_by', 'compaign_create_date', 'compaign_status', 'compaign_is_deleted', 'compaign_sortorder', 'compaign_iscomnet', 'compaign_api_id', 'compaign_exports', 'compaign_isipv', 'geoLocation_name', 'geoLocation_type', 'geoLocation_code', 'geoLocation_census_pop', 'geoLocation_target', 'geoLocation_status', 'geoLocation_pname', 'geoLocation_dname', 'geoLocation_namedistrict', 'geoLocation_codedistrict', 'geoLocation_tname', 'geoLocation_provincecode', 'geoLocation_districtcode', 'geoLocation_tehsilcode', 'geoLocation_priority', 'geoLocation_commnet', 'geoLocation_hr', 'geoLocation_fcm', 'geoLocation_tier', 'geoLocation_block', 'geoLocation_division', 'geoLocation_cordinates', 'geoLocation_latitude', 'geoLocation_longitude', 'geoLocation_x', 'geoLocation_y', 'geoLocation_imagepath', 'geoLocation_isccpv', 'geoLocation_rank', 'geoLocation_rank_score', 'geoLocation_ishealthcamp', 'geoLocation_isdsc', 'geoLocation_ucorg', 'geoLocation_organization', 'geoLocation_tierfromaug161', 'geoLocation_tierfromsep171', 'geoLocation_tierfromdec181', 'geoLocation_mtap', 'geoLocation_rspuc', 'geoLocation_issmt', 'geoLocation_updateddatetime', 'geoLocation_x_code', 'geoLocation_draining_uc', 'geoLocation_upap_districts', 'geoLocation_shruc', 'geoLocation_khidist_id'

    dff = pd.DataFrame(columns=d)
    for index, item in enumerate(d):
        dff[item] = apiDataFrame[index].values
    df2 = client.query_dataframe(
        "SELECT * FROM test.xbi_icm")
    print('\n\ndf2\n\n',df2.head())

    if df2.empty:
        print('\n\n Empty', dff.head())
        # dff.to_csv('D:/Micromerger/ClickHouse With Python/icm.csv')
        # dff.to_csv('D:/Micromerger/ClickHouse With Python/icm.csv')
        client.insert_dataframe(
            'INSERT INTO test.xbi_icm  VALUES', dff)

        a = client.execute(
            "select count(*) from test.xbi_icm")

        sql = "DROP table if exists test.get_icm"
        client.execute(sql)
        logger.info(
            ' Table  \'test.get_icm\' has been Dropped')

    else:
        df = pd.concat([dff, df2])
        df = df.astype('str')
        df = df.drop_duplicates(subset='icm_pk_icm_team_21_id',
                                keep="first", inplace=False)
        sql = "DROP TABLE if exists  test.xbi_icm;"
        client.execute(sql)
        sql = "CREATE TABLE if not exists test.xbi_icm (icm_pk_icm_team_21_id	Int32, icm_fk_form_id	Int32, icm_fk_user_id	Int32, icm_date_created	String, icm_campaign_id	Int32, icm_surveyor_name	String, icm_designation	String, icm_surveyor_affliate	String, icm_fk_prov_id	Int32, icm_fk_dist_id	Int32, icm_fk_tehsil_id	Int32, icm_fk_uc_id	Int32, icm_g1_cbv_noncbv	String, icm_g1_aic_name	String, icm_g1_team_no	String, icm_g1_day	String, icm_g1_team_composition_government	String, icm_g1_team_composition_local	String, icm_g1_team_composition_female	String, icm_g1_team_composition_adult	String, icm_g1_NamesMP_match	String, icm_g1_valid_marker	String, icm_g1_T_memTrained	String, icm_g1_Tdate	String, icm_g1_opv_dry_valid_vvm	String, icm_g1_opv_dry_cool	String, icm_g1_opv_dry_dry	String, icm_g1_t_sheetcorrect	String, icm_g1_vitamin_a	String, icm_g1_T_recordedMissed	String, icm_g1_T_mapAvailable	String, icm_g1_Tworkload	String, icm_g1_Comment	String, icm_g2_obsorve	String, icm_g2_T_chiledAwayHome	Int32, icm_g2_T_rmAFP	Int32, icm_g2_T_markerUsed	Int32, icm_g2_T_CfMarked	Int32, icm_g2_T_childVaccinatedToday	Int32, icm_g2_Comments	String, icm_covid_indi	String, icm_covid_brief_guidline	Int32, icm_covid_symptoms	Int32, icm_covid_wear_mask	Int32, icm_covid_use_santzr	Int32, icm_covid_age_member	Int32, icm_covid_hold_child	Int32, icm_covid_chllenges	String, icm_covid_chllenges_dscrptn	String, icm_unique_id	String, icm_dev_remarks	String, icm_app_version	String, icm_cold_chain	String, icm_g1_opv_dry_intact	String, icm_g1_opv_dry_re_capped	String, icm_g1_matched_admin_team	String, compaign_id	Int32, compaign_compaign_name	String, compaign_type	Int32, compaign_month_date	Int32, compaign_comp_year	Int32, compaign_start_date	String, compaign_end_date	String, compaign_code	String, compaign_created_by	Int32, compaign_create_date	String, compaign_status	Int32, compaign_is_deleted	Int32, compaign_sortorder	Int32, compaign_iscomnet	Int32, compaign_api_id	Int32, compaign_exports	Int32, compaign_isipv	Int32, geoLocation_name String, geoLocation_type Int32, geoLocation_code Int32, geoLocation_census_pop Int32, geoLocation_target Int32, geoLocation_status Int32, geoLocation_pname String, geoLocation_dname String, geoLocation_namedistrict String, geoLocation_codedistrict String, geoLocation_tname String, geoLocation_provincecode Int32, geoLocation_districtcode Int32, geoLocation_tehsilcode Int32, geoLocation_priority Int32, geoLocation_commnet Int32, geoLocation_hr Int32, geoLocation_fcm Int32, geoLocation_tier Int32, geoLocation_block String, geoLocation_division String, geoLocation_cordinates String, geoLocation_latitude String, geoLocation_longitude String, geoLocation_x String, geoLocation_y String, geoLocation_imagepath String, geoLocation_isccpv Int32, geoLocation_rank Int32, geoLocation_rank_score String, geoLocation_ishealthcamp Int32, geoLocation_isdsc Int32, geoLocation_ucorg String, geoLocation_organization String, geoLocation_tierfromaug161 Int32, geoLocation_tierfromsep171 Int32, geoLocation_tierfromdec181 Int32, geoLocation_mtap Int32, geoLocation_rspuc Int32, geoLocation_issmt Int32, geoLocation_updateddatetime String, geoLocation_x_code Int32, geoLocation_draining_uc Int32, geoLocation_upap_districts Int32, geoLocation_shruc Int32, geoLocation_khidist_id String)ENGINE = MergeTree PRIMARY KEY icm_pk_icm_team_21_id ORDER BY icm_pk_icm_team_21_id"
        client.execute(sql)
        logger.info(' Table  \'test.xbi_icm\' has been Created')
        logger.info('Start Inserting into Table  \'test.xbi_icm\'')
        client.insert_dataframe(
            'INSERT INTO test.xbi_icm  VALUES', df)
        logger.info(
            ' Data has been inserted into Table\'test.xbi_icm\' ')

        a = client.execute(
            "select count(*) from test.xbi_icm")
        logger.info(
            'Number of Rows Inserted in Table \'test.xbi_icm\'  :\t  %s', (a))

        sql = "DROP table if exists test.get_icm"
        client.execute(sql)
        logger.info(
            ' Table  \'test.get_icm\' has been Dropped')


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
    "ICM",
    start_date=datetime.now(),
    max_active_runs=1,
    schedule_interval='0 */1 * * * ',  # will run every 1 hour
    # schedule_interval=timedelta(minutes=2),
    catchup=False  # enable if you don't want historical dag runs to run

) as dag:
    GetAndInsertApiDataICM = PythonOperator(
        task_id='GetAndInsertApiDataICM',
        python_callable=GetAndInsertApiDataICM,
    )
    CreateJoinTableOfICM = PythonOperator(
        task_id='CreateJoinTableOfICM',
        python_callable=CreateJoinTableOfICM,
    )
GetAndInsertApiDataICM >> CreateJoinTableOfICM
