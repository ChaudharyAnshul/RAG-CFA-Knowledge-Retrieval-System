import streamlit as st
import requests
import configparser
import pandas as pd 

config = configparser.ConfigParser()
config.read('./configuration.properties')
base_url = config['APIs']['base_url_service']

def part4report():
  ''' data collection menu page '''
  response = get_report_part4()
  res = None
  if response.status_code == 200:
    res = response.json()["part4"]
  else: 
    st.error("Token Expired")

  if res is None:
    st.write("No Report found")
  else:
    df_b = pd.DataFrame(res)
    tab1, tab2 = st.tabs(["Report", "Data"])
    with tab1:
      show_report(df_b)
    with tab2:
      show_data(df_b)
    
def show_report(df_b):
  ''' show loaded data '''
  grouped = df_b.groupby('Set')['Match'].value_counts().unstack(fill_value=0)
  st.bar_chart(grouped)

def show_data(df_a):
  ''' show loaded data '''
  st.write(df_a)

@st.cache_data
def get_report_part4():
  ''' get data from server '''
  url = base_url + '/report/part4'
  access_token = st.session_state["access_token"]
  token_type = st.session_state["token_type"]
  # Making the POST request
  headers = {
    "Authorization": "{} {}".format(token_type, access_token),
    'Content-Type': 'application/json',
  }
  response = requests.get(url, headers=headers)
  return response
