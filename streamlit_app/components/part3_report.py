import streamlit as st
import requests
import configparser
import pandas as pd 

config = configparser.ConfigParser()
config.read('./configuration.properties')
base_url = config['APIs']['base_url_service']

def part3report():
  ''' data collection menu page '''
  response = get_report()
  res = None
  if response.status_code == 200:
    res = response.json()["part3"]
  else: 
    st.error("Token Expired")

  if res is None:
    st.write("No Report found")
  else:
    df_a = pd.DataFrame(res)
    tab1, tab2 = st.tabs(["Report", "Data"])
    with tab1:
      show_report(df_a)
    with tab2:
      show_data(df_a)
    
def show_report(df_a):
  ''' show loaded data '''
  st.bar_chart(df_a['Match'].value_counts())

def show_data(df_a):
  ''' show loaded data '''
  st.write(df_a)
    
@st.cache_data
def get_report():
  ''' get data from server '''
  url = base_url + '/report/part3'
  access_token = st.session_state["access_token"]
  token_type = st.session_state["token_type"]
  # Making the POST request
  headers = {
    "Authorization": "{} {}".format(token_type, access_token),
    'Content-Type': 'application/json',
  }
  response = requests.get(url, headers=headers)
  return response
