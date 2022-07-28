from airflow import DAG
import pandas as pd
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import requests
from clickhouse_driver import Client
import logging

default_args = {"owner": "airflow", "start_date": datetime(2021, 1, 1)}

logger = logging.getLogger(__name__)
# Create handlers
c_handler = logging.StreamHandler()
f_handler = logging.FileHandler("logger-file_name")
c_handler.setLevel(logging.WARNING)
c_handler.setLevel(logging.INFO)
# f_handler.setLevel(logging.ERROR)
# Create formatters and add it to handlers
c_format = logging.Formatter("%(name)s - %(levelname)s - %(message)s")
f_format = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
c_handler.setFormatter(c_format)
f_handler.setFormatter(f_format)
# Add handlers to the logger
logger.addHandler(c_handler)
logger.addHandler(f_handler)

#-------------------------------------------------------------------------------------------------------------------------------#
#-------------------------------------------------- GENERATE DAG FILES  --------------------------------------------------------#
#------------------------------------------------ Author: BABAR ALI SHAH -------------------------------------------------------#
#-------------------------------------------------------------------------------------------------------------------------------#

def GetAndInsertApiDataControlRoomCoverage():
    logger.info("Function ' GetAndInsertApiDataControlRoomCoverage ' Started Off")
    client = Client(
        host="161.97.136.95",
        user="default",
        password="pakistan",
        port="9000",
        settings={"use_numpy": True},
    )

    url = "http://idims.eoc.gov.pk/api_who/api/get_allcontrollroom_combined/5468XE2LN6CzR7qRG041/1/2022"
    logger.info(
        "Requested Data From Api URL:  ' http://idims.eoc.gov.pk/api_who/api/get_allcontrollroom_combined/5468XE2LN6CzR7qRG041/1/2022 ' "
    )

    r = requests.get(url)
    data = r.json()
    logger.info(
        "Received Data  From Api URL:  ' http://idims.eoc.gov.pk/api_who/api/get_allcontrollroom_combined/5468XE2LN6CzR7qRG041/1/2022 ' "
    )

    rowsData = data["data"]["data"]
    apiDataFrame = pd.DataFrame(rowsData)
    sql = "CREATE TABLE if not exists test.get_controlroomCoverage  (ID Int32, IDcampCat Int32, ActivityID Int32, Yr Int32, TimeStamp varchar(10), Cday Int32, TehsilID Int32, UCID Int32, ccpvTargets Int32, VaccinationDate varchar(50), TeamsRpt Int32, HH011_MP Int32, HH1259_MP Int32, HH011_TS Int32, HH1259_TS Int32, HH011 Int32, HH1259 Int32, OutofH011 Int32, OutofH1259 Int32, RecNA011 Int32, RecNA1259 Int32, RecRef011 Int32, RecRef1259 Int32, CovNA011 Int32, CovNA1259 Int32, CovRef011 Int32, CovRef1259 Int32, Guests Int32, VaccinatedSchool Int32, NewBorn Int32, AlreadyVaccinated Int32, FixSite011 Int32, FixSite1259 Int32, Transit011 Int32, Transit1259 Int32, CovMob Int32, HHvisit Int32, HHPlan Int32, MultipleFamily String, Weakness Int32, ZeroRt Int32, PRIMARY KEY(ID ))ENGINE = MergeTree"
    client.execute(sql)

    df2 = client.query_dataframe("SELECT * FROM test.get_controlroomCoverage")
    if df2.empty:
        client.insert_dataframe(
            "INSERT INTO test.get_controlroomCoverage VALUES", apiDataFrame
        )


def CreateJoinTableOfControlRoomCoverage():
    logger.info(
        " Function  ' CreateJoinTableOfControlRoomCoverage ' has been Initiated"
    )
    client = Client(
        host="161.97.136.95",
        user="default",
        password="pakistan",
        port="9000",
        settings={"use_numpy": True},
    )
    sql = "CREATE TABLE if not exists test.xbi_controlroomCoverage (Yr Int32, ActivityID Int32,TehsilID Int32,UCID Int32,location_code Int32, HH011_MP Int32, HH1259_MP  Int32, HH011_TS  Int32, HH1259_TS  Int32, HH011  Int32, HH1259  Int32, OutofH011  Int32, OutofH1259  Int32, RecNA011  Int32, RecNA1259  Int32, RecRef011  Int32, RecRef1259  Int32, CovNA011  Int32, CovNA1259  Int32, CovRef011  Int32, CovRef1259  Int32, Guests  Int32, VaccinatedSchool  Int32, NewBorn  Int32, AlreadyVaccinated  Int32, FixSite011  Int32, FixSite1259  Int32, Transit011  Int32, Transit1259  Int32, CovMob  Int32, HHvisit  Int32, HHPlan  Int32, Weakness  Int32, ZeroRt  Int32 )ENGINE = MergeTree PRIMARY KEY Yr ORDER BY Yr"
    client.execute(sql)
    cols = "test.get_controlroomCoverage.Yr,test.get_controlroomCoverage.ActivityID,test.get_controlroomCoverage.TehsilID,test.get_controlroomCoverage.UCID,sum(test.get_controlroomCoverage.HH011_MP, test.get_controlroomCoverage.HH1259_MP, test.get_controlroomCoverage.HH011_TS, test.get_controlroomCoverage.HH1259_TS, test.get_controlroomCoverage.HH011, test.get_controlroomCoverage.HH1259, test.get_controlroomCoverage.OutofH011, test.get_controlroomCoverage.OutofH1259, test.get_controlroomCoverage.RecNA011, test.get_controlroomCoverage.RecNA1259, test.get_controlroomCoverage.RecRef011, test.get_controlroomCoverage.RecRef1259, test.get_controlroomCoverage.CovNA011, test.get_controlroomCoverage.CovNA1259, test.get_controlroomCoverage.CovRef011, test.get_controlroomCoverage.CovRef1259, test.get_controlroomCoverage.Guests, test.get_controlroomCoverage.VaccinatedSchool, test.get_controlroomCoverage.NewBorn, test.get_controlroomCoverage.AlreadyVaccinated, test.get_controlroomCoverage.FixSite011, test.get_controlroomCoverage.FixSite1259, test.get_controlroomCoverage.Transit011, test.get_controlroomCoverage.Transit1259, test.get_controlroomCoverage.CovMob, test.get_controlroomCoverage.HHvisit, test.get_controlroomCoverage.HHPlan, test.get_controlroomCoverage.Weakness, test.get_controlroomCoverage.ZeroRt)"
    sql = (
        "SELECT "
        + cols
        + "  FROM test.get_controlroomCoverage eoc_1  Group By eoc_1.Yr,eoc_1.ActivityID, eoc_1.TehsilID, eoc_1.UCID"
    )

    data = client.execute(sql)
    apiDataFrame = pd.DataFrame(data)
    all_columns = list(apiDataFrame)  # Creates list of all column headers
    cols = apiDataFrame.iloc[0]
    apiDataFrame[all_columns] = apiDataFrame[all_columns].astype(str)
    d = (
        "Yr",
        "ActivityID",
        "TehsilID",
        "UCID",
        "location_code",
        "HH011_MP",
        "HH1259_MP",
        "HH011_TS",
        "HH1259_TS",
        "HH011",
        "HH1259",
        "OutofH011",
        "OutofH1259",
        "RecNA011",
        "RecNA1259",
        "RecRef011",
        "RecRef1259",
        "CovNA011",
        "CovNA1259",
        "CovRef011",
        "CovRef1259",
        "Guests",
        "VaccinatedSchool",
        "NewBorn",
        "AlreadyVaccinated",
        "FixSite011",
        "FixSite1259",
        "Transit011",
        "Transit1259",
        "CovMob",
        "HHvisit",
        "HHPlan",
        "Weakness",
        "ZeroRt",
    )
    dff = pd.DataFrame(columns=d)
    for index, item in enumerate(d):
        dff[item] = apiDataFrame[index].values
    df2 = client.query_dataframe("SELECT * FROM test.xbi_controlroomCoverage")
    if df2.empty:
        client.insert_dataframe("INSERT INTO test.xbi_controlroomCoverage  VALUES", dff)
        logger.info(
            " Data has been inserted into Table' INSERT INTO test.xbi_controlroomCoverage  VALUES ' "
        )

        sql = "DROP table if exists test.get_controlroomCoverage"
        client.execute(sql)

    else:
        df = pd.concat([dff, df2])
        df = df.astype("str")
        df = df.drop_duplicates(subset="Yr", keep="first", inplace=False)
        sql = "DROP TABLE if exists test.xbi_controlroomCoverage;"
        client.execute(sql)
        sql = "CREATE TABLE if not exists test.xbi_controlroomCoverage (Yr Int32, ActivityID Int32,TehsilID Int32,UCID Int32,location_code Int32, HH011_MP Int32, HH1259_MP  Int32, HH011_TS  Int32, HH1259_TS  Int32, HH011  Int32, HH1259  Int32, OutofH011  Int32, OutofH1259  Int32, RecNA011  Int32, RecNA1259  Int32, RecRef011  Int32, RecRef1259  Int32, CovNA011  Int32, CovNA1259  Int32, CovRef011  Int32, CovRef1259  Int32, Guests  Int32, VaccinatedSchool  Int32, NewBorn  Int32, AlreadyVaccinated  Int32, FixSite011  Int32, FixSite1259  Int32, Transit011  Int32, Transit1259  Int32, CovMob  Int32, HHvisit  Int32, HHPlan  Int32, Weakness  Int32, ZeroRt  Int32 )ENGINE = MergeTree PRIMARY KEY Yr ORDER BY Yr"
        client.execute(sql)
        client.insert_dataframe("INSERT INTO test.xbi_controlroomCoverage  VALUES", df)

        sql = "DROP table if exists test.get_controlroomCoverage"
        client.execute(sql)


dag = DAG(
    "ControlRoomCoverage_Automated",
    schedule_interval="*/59 * * * *",  # will run every 10 min.
    default_args=default_args,
    catchup=False,
)

with dag:
    GetAndInsertApiDataControlRoomCoverage = PythonOperator(
        task_id="GetAndInsertApiDataControlRoomCoverage",
        python_callable=GetAndInsertApiDataControlRoomCoverage,
    )
    CreateJoinTableOfControlRoomCoverage = PythonOperator(
        task_id="CreateJoinTableOfControlRoomCoverage",
        python_callable=CreateJoinTableOfControlRoomCoverage,
    )
GetAndInsertApiDataControlRoomCoverage >> CreateJoinTableOfControlRoomCoverage
