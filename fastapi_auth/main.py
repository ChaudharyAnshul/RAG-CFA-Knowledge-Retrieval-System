from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from passlib.context import CryptContext
import jwt
from datetime import datetime, timedelta
from pydantic import BaseModel
from typing import Optional
from pymongo import MongoClient
import configparser

class signup_data(BaseModel):
  email: str
  username: str
  password: str

class login_data(BaseModel):
  email: str
  password: str

app = FastAPI()

config = configparser.ConfigParser()
config.read('configuration.properties')

# JWT config
SECRET_KEY = config['auth-api']['SECRET_KEY']
ALGORITHM = config['auth-api']['ALGORITHM']
ACCESS_TOKEN_EXPIRE_MINUTES = int(config['auth-api']['ACCESS_TOKEN_EXPIRE_MINUTES'])

# Mongo config
mongo_url = config['MongoDB']['mongo_url']
db_name = config['MongoDB']['db_name']
collection_name = config['MongoDB']['collection_name']

# oauth2 scheme
tokenUrl = config['password']['tokenUrl']
oauth2_scheme = OAuth2PasswordBearer(tokenUrl=tokenUrl)

# password encryption
schemes = config['password']['schemes']
deprecated = config['password']['deprecated']
pwd_context = CryptContext(schemes=schemes, deprecated=deprecated)

def get_mongo_clien():
  ''' get the db object '''
  return MongoClient(mongo_url)

def verify_password(plain_password, hashed_password):
  ''' verify the passowrd for login '''
  return pwd_context.verify(plain_password, hashed_password)

def get_user(email: str):
  ''' get user data from db with email '''
  client = get_mongo_clien()
  db = client[db_name]
  collection = db[collection_name]
  result = collection.find_one({"email": email})
  client.close()
  return result

def create_user(email: str, password: str, username: str):
  ''' add new user in db '''
  client = get_mongo_clien()
  db = client[db_name]
  collection = db[collection_name]
  
  hashed_password = pwd_context.hash(password)
  document = {
    "email": email,
    "username": username,
    "password": hashed_password
  }
  collection.insert_one(document)
  client.close()
  return

def authenticate_user(email: str, password: str):
  ''' Authenticate user Login '''
  user = get_user(email)
  if not user:
    return False
  if not verify_password(password, user["password"]):
    return False
  return user

def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
  ''' Create access token '''
  to_encode = data.copy()
  if expires_delta:
      expire = datetime.utcnow() + expires_delta
  else:
      expire = datetime.utcnow() + timedelta(minutes=60)
  to_encode.update({"exp": expire})
  encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
  return encoded_jwt

@app.post("/signup")
async def register(payload: signup_data):
  ''' Endpoint Sign Up new user '''
  email = payload.email
  password = payload.password
  username = payload.username
  if get_user(email):
    raise HTTPException(status_code=400, detail="Email already registered")
  create_user(email, password, username)
  return {"message": "User registered successfully"}

@app.post("/login")
async def login_for_access_token(payload: login_data):
  ''' Endpoint Login Existing user '''
  email = payload.email
  password = payload.password
  user = authenticate_user(email, password)
  if not user:
    raise HTTPException(
      status_code=status.HTTP_401_UNAUTHORIZED,
      detail="Incorrect email or password",
      headers={"WWW-Authenticate": "Bearer"},
    )
  access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
  access_token = create_access_token(
    data={"sub": user["email"]}, expires_delta=access_token_expires
  )
  return {"access_token": access_token, "token_type": "bearer"}