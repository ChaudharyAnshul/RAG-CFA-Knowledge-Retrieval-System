from datetime import datetime
from bson.objectid import ObjectId
from fastapi import APIRouter, Header, status, HTTPException
from pymongo import MongoClient
import configparser
import jwt
from pydantic import BaseModel
import random
import requests

router = APIRouter()

config = configparser.ConfigParser()
config.read('configuration.properties')

# JWT config
SECRET_KEY = config['auth-api']['SECRET_KEY']
ALGORITHM = config['auth-api']['ALGORITHM']

# mongo config
mongo_url = config['MongoDB']['mongo_url']
db_name = config['MongoDB']['db_name']
collection_name_data = config['MongoDB']['collection_name_data']
collection_name_markdown = config['MongoDB']['collection_name_markdown']


@router.get('/topics')
async def get_topic_list( authorization: str = Header(None)):
    ''' get loaded topic list '''
    if authorization is None:
        raise HTTPException(status_code=401, detail="Unauthorized")
    parts = authorization.split()
    if len(parts) != 2 or parts[0].lower() != "bearer":
        raise HTTPException(status_code=401, detail="Invalid authorization header")
    token = parts[1]
    try:
        token_decode = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM, ])
        email: str = token_decode.get("sub")
    except Exception as e:
        print(e)
        raise HTTPException(status_code=401, detail="Token has expired")
    # create client
    client = MongoClient(mongo_url)
    db = client[db_name]
    collection = db[collection_name_data]
    query = {"Status": True}
    cursor = collection.find(query)
    records = [ i["NameOfTheTopic"] for i in cursor]
    client.close()
    return {"topics": records}

class markdown_topic(BaseModel):
    topic: str

@router.get('/markdown')
async def get_markdown( payload: markdown_topic, authorization: str = Header(None)):
    ''' get markdown for topic '''
    if authorization is None:
        raise HTTPException(status_code=401, detail="Unauthorized")
    parts = authorization.split()
    if len(parts) != 2 or parts[0].lower() != "bearer":
        raise HTTPException(status_code=401, detail="Invalid authorization header")
    token = parts[1]
    try:
        token_decode = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM, ])
        email: str = token_decode.get("sub")
    except Exception as e:
        print(e)
        raise HTTPException(status_code=401, detail="Token has expired")
    # create client
    client = MongoClient(mongo_url)
    db = client[db_name]
    collection = db[collection_name_markdown]
    query = {"NameOfTheTopic": payload.topic}
    cursor = collection.find(query)
    learning_map = {}
    for i in cursor:
        learning_map[i["Learning"]] = i["LearningSummary"]
    client.close()
    return {"markdown": learning_map}

@router.get('/new_topics')
async def get_unloaded_topic_list( authorization: str = Header(None)):
    ''' get new topic list '''
    if authorization is None:
        raise HTTPException(status_code=401, detail="Unauthorized")
    parts = authorization.split()
    if len(parts) != 2 or parts[0].lower() != "bearer":
        raise HTTPException(status_code=401, detail="Invalid authorization header")
    token = parts[1]
    try:
        token_decode = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM, ])
        email: str = token_decode.get("sub")
    except Exception as e:
        print(e)
        raise HTTPException(status_code=401, detail="Token has expired")
    # create client 
    client = MongoClient(mongo_url)
    db = client[db_name]
    collection = db[collection_name_data]
    query = {"Status": False}
    cursor = collection.find(query)
    records = {}
    for i in cursor:
        records[i["NameOfTheTopic"]] =  str(i["_id"])
    client.close()
    return {"topics": records}

class airflow_trigger(BaseModel):
    topicId: str

@router.post('/triggre_markdown')
async def triggre_markdown( payload: airflow_trigger, authorization: str = Header(None)):
    ''' triggre markdown pipeline '''
    if authorization is None:
        raise HTTPException(status_code=401, detail="Unauthorized")
    parts = authorization.split()
    if len(parts) != 2 or parts[0].lower() != "bearer":
        raise HTTPException(status_code=401, detail="Invalid authorization header")
    token = parts[1]
    try:
        token_decode = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM, ])
        email: str = token_decode.get("sub")
    except Exception as e:
        print(e)
        raise HTTPException(status_code=401, detail="Token has expired")
    
    base_url_ariflow = config['airflow']['base_url_airflow']
    username = config['airflow']['username']
    password = config['airflow']['password']
    
    rand1 = random.randint(1,1000)
    rand2 = random.randint(1,1000)
    dag_run_id = str("id_run_" +str(rand1)+str(rand2))
    
    url = base_url_ariflow + "/dags/dag_embedding/dagRuns"
    
    response = requests.post(
        url,
        auth=(username, password),
        json={"conf": {"mongoId": payload.topicId}, "dag_run_id": dag_run_id},
        headers={"Content-Type": "application/json"},
    )
    
    if response.status_code == 200:
        return {"message": "DAG triggered successfully"}
    else:
        return {"error": "Failed to trigger DAG"}
