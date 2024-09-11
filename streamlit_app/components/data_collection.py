import streamlit as st
import requests
import configparser

config = configparser.ConfigParser()
config.read('./configuration.properties')
base_url = config['APIs']['base_url_service']

def data_collection():
  ''' data collection menu page '''
  tab1, tab2 = st.tabs(["View Topics", "Load New Topic"])
  with tab1:
    show_data()
  with tab2:
    load_new_data()

def show_data():
  ''' show loaded data '''
  response = get_topics()
  topic = tuple([""])
  if response.status_code == 200:
    topic += tuple(response.json()["topics"])
  else: 
    st.error("Token Expired")
    
  if topic == (""):
    st.write("No Topics found")
  else:
    option = st.selectbox(
      'Select A Topic',
      topic
    )
    if option:
      show_markdown(option)

def get_topics():
  ''' get data from server '''
  url = base_url + '/collection/topics'
  access_token = st.session_state["access_token"]
  token_type = st.session_state["token_type"]
  # Making the POST request
  headers = {
    "Authorization": "{} {}".format(token_type, access_token),
    'Content-Type': 'application/json',
  }
  response = requests.get(url, headers=headers)
  return response

def show_markdown(topic):
  ''' show the makrdown '''
  response = get_markdown_data(topic)
  learnings = tuple([""])
  learnings_map = {}
  if response.status_code == 200:
    learnings_map = response.json()["markdown"]
    learnings += tuple(response.json()["markdown"].keys())
  else: 
    st.error("Token Expired")
  if learnings == (""):
    st.write("No Topics found")
  else:
    learning = st.selectbox(
      'Select A LOS',
      learnings
    )
    bt_2 = st.button("Load Summary")
    if bt_2:
      st.markdown(learnings_map[learning])

@st.cache_data
def get_markdown_data(topic):
  url = base_url + '/collection/markdown'
  access_token = st.session_state["access_token"]
  token_type = st.session_state["token_type"]
  # Making the POST request
  headers = {
    "Authorization": "{} {}".format(token_type, access_token),
    'Content-Type': 'application/json',
  }
  body = {
    "topic": topic
  }
  response = requests.get(url, headers=headers, json=body)
  return response

def load_new_data():
  ''' load new data '''
  response = get_unloaded_topics()
  topic = tuple([""])
  res = {}
  if response.status_code == 200:
    res = response.json()["topics"]
    topic += tuple(res.keys())
  else: 
    st.error("Token Expired")
  if topic == (""):
    st.write("No Topics found")
  else:
    option = st.selectbox(
      'Select A Topic',
      topic
    )
    bt_2 = st.button("Generate Markdown")
    if bt_2:
      triggre_pipeline(res[option])

def get_unloaded_topics():
  ''' get unloaded data from server '''
  url = base_url + '/collection/new_topics'
  access_token = st.session_state["access_token"]
  token_type = st.session_state["token_type"]
  # Making the POST request
  headers = {
    "Authorization": "{} {}".format(token_type, access_token),
    'Content-Type': 'application/json',
  }
  response = requests.get(url, headers=headers)
  return response

def triggre_pipeline(mongoId):
  ''' Triggre airflow pipeline '''
  url = base_url + '/collection/triggre_markdown'
  access_token = st.session_state["access_token"]
  token_type = st.session_state["token_type"]
  # Making the POST request
  headers = {
    "Authorization": "{} {}".format(token_type, access_token),
    'Content-Type': 'application/json',
  }
  body = {
    "topicId": mongoId
  }
  response = requests.post(url, headers=headers, json=body)
  if response.status_code == 200:
    st.success(response.json()["message"])
  elif response.status_code == 401: 
    st.error("Token Expired")
  else:
    st.error(response.json()["error"])