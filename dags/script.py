import mysql.connector as sql
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import pymysql.cursors
import MySQLdb

connection = pymysql.connect(host='propx.cluster-cdtaeoxnqruq.us-west-2.rds.amazonaws.com', database='eoc_cube_db', user='admin', password='PropAWS1!')

try:
    with connection.cursor() as cursor:
        print("Connected to database")
        query = "SELECT * FROM eoc_cube_db.eoc_coronatracker_tbl;"
        print(query)

        chunks = []
        print("Fetching data...")

        for chunk in pd.read_sql(query, connection, chunksize=1000):
            chunks.append(chunk)
        print("Data fetched")
        result = pd.concat(chunks, ignore_index=True)
        print("Data concatenated")
        connection.close()
        engine = create_engine("mysql://tiger:MM_DB_eoc_3@172.16.3.68/eoc_cube_db_Migrated")

        print("writing to database")
        result.to_sql(
            "eoc_coronatracker_tbl",
            con=engine,
            index=False,
            # dtype={"camp_start_date": sqlalchemy.DateTime()},
        )
        print("writing to database completed")

finally:
    print("Done!")

    








# import datetime
# import warnings

# warnings.filterwarnings("ignore")

# import MySQLdb
# from sqlalchemy import create_engine
# import pandas as pd

# #conn = MySQLdb.connect("propx.cluster-cdtaeoxnqruq.us-west-2.rds.amazonaws.com", "admin", "PropAWS1!")
# import mysql.connector as sql

# conn = sql.connect(host='propx.cluster-cdtaeoxnqruq.us-west-2.rds.amazonaws.com', database='eoc_cube_db', user='admin', password='PropAWS1!', use_pure=True)
# curs = conn.cursor()
# print("Connected to RDS")
# SQL_Query = pd.read_sql_query("""select * from eoc_cube_db.eoc_coronatracker_tbl""", conn)
# print("Query executed")
# df = pd.DataFrame(SQL_Query)
# print("Dataframe created")
# engine = create_engine("mysql://tiger:MM_DB_eoc_3@172.16.3.68/eoc_cube_db_Migrated")

# print("writing to database")
# print('df.shape(): ' + str(df.shape))
# print('df.size(): ' + str(df.size))
# print("time started: " + str(datetime.datetime.now()))
# df.to_sql(
#     "eoc_coronatracker_tbl",
#     con=engine,
#     index=False,
#     # dtype={"camp_start_date": sqlalchemy.DateTime()},
# )
# print("time ended: " + str(datetime.datetime.now()))
# print("writing to database completed")

