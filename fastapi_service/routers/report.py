from fastapi import APIRouter, Header, status, HTTPException
from pymongo import MongoClient
import configparser
import jwt


router = APIRouter()

config = configparser.ConfigParser()
config.read('configuration.properties')

# JWT config
SECRET_KEY = config['auth-api']['SECRET_KEY']
ALGORITHM = config['auth-api']['ALGORITHM']

# mongo config
mongo_url = config['MongoDB']['mongo_url']
db_name = config['MongoDB']['db_name']
collection_name_part3 = config['MongoDB']['collection_name_part3']
collection_name_part4 = config['MongoDB']['collection_name_part4']


@router.get('/part3')
async def get_topic_list( authorization: str = Header(None)):
  ''' get setA list '''
  print(1)
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
  print(2)
  client = MongoClient(mongo_url)
  db = client[db_name]
  collection = db[collection_name_part3]
  query = {}
  cursor = collection.find(query)
  client.close()
  documents = [{key: value for key, value in doc.items() if key != '_id'} for doc in cursor]
  return {"part3": documents}
  
  
@router.get('/part4')
async def get_topic_list( authorization: str = Header(None)):
  ''' get setA list '''
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
  collection = db[collection_name_part4]
  query = {}
  cursor = collection.find(query)
  client.close()
  documents = [{key: value for key, value in doc.items() if key != '_id'} for doc in cursor]
  return {"part4": documents}