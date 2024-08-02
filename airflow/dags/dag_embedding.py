import os
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import json
import requests
import google.oauth2.id_token
import google.auth.transport.requests
from pymongo import MongoClient
import configparser
from bson.objectid import ObjectId

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/opt/airflow/dags/cloudfunction.json'
request = google.auth.transport.requests.Request()

config = configparser.ConfigParser()
config.read('/opt/airflow/dags/configuration.properties')
mongo_url = config['MongoDB']['mongo_url']
db_name = config['MongoDB']['db_name']
collection_name = config['MongoDB']['collection_name']
collection_name_markdown = config['MongoDB']['collection_name_markdown']

def call_generateMarkdown(topicId):
    audience = 'https://us-central1-assignment-5-419501.cloudfunctions.net/generate-markdown'
    TOKEN = google.oauth2.id_token.fetch_id_token(request, audience)
    r = requests.post(
        audience, 
        headers={'Authorization': f"Bearer {TOKEN}", "Content-Type": "application/json"},
        data=json.dumps({"mongoId": topicId})
    )
    return r.text

def call_embeddingMarkdown(topicId):
    
    # create client
    client = MongoClient(mongo_url)
    db = client[db_name]
    collection_data = db[collection_name]
    collection_markdown = db[collection_name_markdown]
    
    mongo_result = collection_data.find_one({"_id": ObjectId(topicId)})
    
    if mongo_result is None:
        print("No result found in {} for id {}".format(collection_name, topicId))
        return
    
    topicName = mongo_result.get("NameOfTheTopic")
    cursor = collection_markdown.find({"NameOfTheTopic":topicName})
    mongoIds = []
    for i in cursor:
        mongoIds.append(str(i["_id"]))
    
    audience = 'https://us-central1-assignment-5-419501.cloudfunctions.net/embed-markdown'
    TOKEN = google.oauth2.id_token.fetch_id_token(request, audience)
    r = requests.post(
        audience, 
        headers={'Authorization': f"Bearer {TOKEN}", "Content-Type": "application/json"},
        data=json.dumps({"mongoIds": mongoIds})
    )
    client.close()
    return r.text

dag = DAG(
    dag_id="dag_embedding",
    schedule=None,
    start_date=days_ago(0),
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
    max_active_runs=10
)

def generate_data(**kwargs):
    topicId = kwargs['dag_run'].conf.get("mongoId")
    status = call_generateMarkdown(topicId)
    if status=="Fail":
        raise RuntimeError("Task (generate_data) failed on GCP cloud functon")

def embedding_data(**kwargs):
    topicId = kwargs['dag_run'].conf.get("mongoId")
    status = call_embeddingMarkdown(topicId)
    if status=="Fail":
        raise RuntimeError("Task (embedding_data) failed on GCP cloud functon")

with dag:
    generate_data_task = PythonOperator(
        task_id='generate_data',
        python_callable=generate_data,
        provide_context=True,
        dag=dag,
    )

    embedding_data_task = PythonOperator(
        task_id='embedding_data',
        python_callable=embedding_data,
        provide_context=True,
        dag=dag,
    )

    generate_data_task >> embedding_data_task