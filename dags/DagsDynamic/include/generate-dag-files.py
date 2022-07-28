import json
import os
import shutil

#-------------------------------------------------------------------------------------------------------------------------------#
#-------------------------------------------------- GENERATE DAG FILES  --------------------------------------------------------#
#------------------------------------------------ Author: BABAR ALI SHAH -------------------------------------------------------#
#-------------------------------------------------------------------------------------------------------------------------------#

config_filepath = 'dags/DagsDynamic/config-files/'
dag_template_filename = 'dags/DagsDynamic/include/dag-template.py'

for filename in os.listdir(config_filepath):
    f = open(config_filepath + filename)
    config = json.load(f)
    new_filename = 'dags/'+config['DagId']+'.py'
    shutil.copyfile(dag_template_filename, new_filename)
    with open(new_filename, 'r') as file:
        filedata = file.read()
        filedata = filedata.replace('\'dag_id\'', "'"+config['DagId']+"'")
        filedata = filedata.replace("t1", config['t1'])
        filedata = filedata.replace("t2", config['t2'])
        filedata = filedata.replace("primary-key", config['primary-key'])
        filedata = filedata.replace(
            "query-drop-xbi_table", config['query-drop-xbi_table'])
        filedata = filedata.replace(
            "query-drop-api_table", config['query-drop-api_table'])
        filedata = filedata.replace(
            "query-insert-xbi_table", config['query-insert-xbi_table'])
        filedata = filedata.replace(
            "query-get-xbi_table", config['query-get-xbi_table'])
        filedata = filedata.replace(
            "all-column-names", config['all-column-names'])
        filedata = filedata.replace(
            "query-joined_table", config['query-joined_table'])
        filedata = filedata.replace("xbi_columns-names", config['xbi_columns-names'])
        filedata = filedata.replace(
            "query-create-xbi_table", config['query-create-xbi_table'])
        filedata = filedata.replace("URL-HERE", config['URL-HERE'])
        filedata = filedata.replace("query-insert-into_api_table", config['query-insert-into_api_table'])

        filedata = filedata.replace(
            "query-get-api_table", config['query-get-api_table'])
        filedata = filedata.replace(
            "query-create-api_table", config['query-create-api_table'])
    with open(new_filename, 'w') as file:
        file.write(filedata)
