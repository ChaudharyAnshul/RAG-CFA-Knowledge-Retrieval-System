import os
import json
import requests
import google.oauth2.id_token
import google.auth.transport.requests
from pymongo import MongoClient
import configparser

config = configparser.ConfigParser()
config.read('configuration.properties')

mongo_url = config['MongoDB']['mongo_url']
db_name = config['MongoDB']['db_name']
collection_name_data = config['MongoDB']['collection_name_data']

def embeddingQuestions(list_ids):
  os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'cloudfunction.json'
  request = google.auth.transport.requests.Request()
  audience = 'https://us-central1-assignment-5-419501.cloudfunctions.net/embed-questions'
  TOKEN = google.oauth2.id_token.fetch_id_token(request, audience)

  r = requests.post(
    audience, 
    headers={'Authorization': f"Bearer {TOKEN}", "Content-Type": "application/json"},
    data=json.dumps({"mongoIds": list_ids})
  )
  
  return r

def split_into_batches(lst, batch_size):
  return [lst[i:i+batch_size] for i in range(0, len(lst), batch_size)]

def start():
  client = MongoClient(mongo_url)
  db = client[db_name]
  collection = db[collection_name_data]
  res = collection.find({})
  list_ids = [str(i["_id"]) for i in res]
  batch_size = 5
  results = split_into_batches(list_ids, batch_size)
  for result in results:
    embeddingQuestions(result)
start()