import streamlit as st
import requests
import json
import re
from streamlit_option_menu import option_menu
import configparser

config = configparser.ConfigParser()
config.read('./configuration.properties')
base_url = config['APIs']['base_url_auth']

def menu_login():
    ''' navigation menu for login/signup '''
    login_menu = option_menu(None, ["Login", "Sign Up"], 
        icons=['person-fill', "person-plus-fill"], 
        menu_icon="cast", 
        key='login_menu',
        default_index=0, 
        orientation="horizontal"
    )
    login_menu
    if st.session_state["login_menu"] == "Login" or st.session_state["login_menu"] == None:
        login()
    elif st.session_state["login_menu"] == "Sign Up":
        sign_up()

def sign_up():
  with st.form(key='signup', clear_on_submit=True):
    st.subheader(':red[Sign Up]')
    email = st.text_input(':blue[Email]', placeholder='Enter Your Email')
    username = st.text_input(':blue[Username]', placeholder='Enter Your Username')
    password1 = st.text_input(':blue[Password]', placeholder='Enter Your Password', type='password')
    password2 = st.text_input(':blue[Confirm Password]', placeholder='Confirm Your Password', type='password')
    btn1, btn2, btn3 = st.columns([3, 1, 3])

    with btn2:
      sub = st.form_submit_button('Sign Up')

    if sub:
      if validate_email_signup(email) and validate_username_signup(username) and validate_password_signup(password1, password2):
        url = base_url + '/signup'
        # Data to be sent with the POST request (as a dictionary)
        payload = {
          "email": email,
          "password": password1,
          "username": username,
        }
        # Convert the data dictionary to JSON
        json_data = json.dumps(payload)
        # Making the POST request
        headers = {
          'Content-Type': 'application/json',  # Example header, adjust as needed
        }
        response = requests.post(url, headers=headers, data=json_data)

        if response.status_code == 200:
          st.success("User Registered!")
        else: 
          st.error("Error Try Again")

def login():
  with st.form(key='login', clear_on_submit=True):
    st.subheader(':red[Login]')
    username = st.text_input(':blue[Email]', placeholder='Enter Your Email')
    password = st.text_input(':blue[Password]', placeholder='Enter Your Password', type='password')
    btn1, btn2, btn3 = st.columns([3, 1, 3])
    with btn2:
      sub = st.form_submit_button('Login')
      
    if sub:
      if validate_username(username) and validate_password(password):
        url = base_url + '/login'
        # Data to be sent with the POST request (as a dictionary)
        payload = {
          'email': username, 
          'password': password
        }
        # Convert the data dictionary to JSON
        json_data = json.dumps(payload)
        # Making the POST request
        headers = {
          'Content-Type': 'application/json',  # Example header, adjust as needed
        }
        response = requests.post(url, headers=headers, data=json_data)
        if response.status_code == 200:
          st.session_state["auth_status"] = True
          st.session_state["username"] = username
          st.session_state["access_token"] = response.json()["access_token"]
          st.session_state["token_type"] = response.json()["token_type"]
          st.rerun()
        else: 
          st.error("Invalid Credential")

def validate_email_signup(email):
  # Regular expression for email validation
  regex = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
  if email:
    if re.match(regex, email):
      return True
    else:
      st.warning('Invalid Email')
      return False
  else:
    st.warning('Enter an Email')
    return False

def validate_username_signup(username):
  if username:
    if len(username) > 3:
      return True
    else:
      st.warning('Invalid Username')
      return False
  else:
    st.warning('Enter an Username')
    return False

def validate_password_signup(password1, password2):
  if password1:
    if password2:
      if password1 == password2:
        return True
      else:
        st.warning("Password Don't Match")
        return False
    else:
      st.warning('Enter Confirm Password')
      return False
  else:
    st.warning('Enter Password')
    return False

def validate_username(username):
  if username:
    return True
  else:
    st.warning('Enter an Username')
    return False

def validate_password(password):
  if password:
    return True
  else:
    st.warning('Enter Password')
    return False