import streamlit as st
import requests
import configparser
import pandas as pd 

config = configparser.ConfigParser()
config.read('./configuration.properties')
base_url = config['APIs']['base_url_service']

def questions():
  ''' data collection menu page '''
  tab1, tab2 = st.tabs(["SetA", "SetB"])
  with tab1:
    show_A()
  with tab2:
    show_B()
    
def show_A():
  ''' show loaded data '''
  response = get_A()
  res = None
  if response.status_code == 200:
    res = response.json()["setA"]
  else: 
    st.error("Token Expired")
    
  if res is None:
    st.write("No Questions found")
  else:
    df_a = pd.DataFrame(res)
    st.write(df_a)

@st.cache_data
def get_A():
  ''' get data from server '''
  url = base_url + '/questions/setA'
  access_token = st.session_state["access_token"]
  token_type = st.session_state["token_type"]
  # Making the POST request
  headers = {
    "Authorization": "{} {}".format(token_type, access_token),
    'Content-Type': 'application/json',
  }
  response = requests.get(url, headers=headers)
  return response

def show_B():
  ''' show loaded data '''
  response = get_B()
  res = None
  if response.status_code == 200:
    res = response.json()["setA"]
  else: 
    st.error("Token Expired")
    
  if res is None:
    st.write("No Questions found")
  else:
    df_a = pd.DataFrame(res)
    st.write(df_a)

@st.cache_data
def get_B():
  ''' get data from server '''
  url = base_url + '/questions/setB'
  access_token = st.session_state["access_token"]
  token_type = st.session_state["token_type"]
  # Making the POST request
  headers = {
    "Authorization": "{} {}".format(token_type, access_token),
    'Content-Type': 'application/json',
  }
  response = requests.get(url, headers=headers)
  return response