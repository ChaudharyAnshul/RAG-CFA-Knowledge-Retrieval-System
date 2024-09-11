import streamlit as st
from streamlit_option_menu import option_menu
from components.data_collection import data_collection
from components.question_data import questions
from components.part3_report import part3report
from components.part4_report import part4report
def tabs():
  options = ["Collection", "Question", "Part3", "Part4"]
  icons = ['cloud-upload-fill','gear-fill', 'clipboard-data-fill', 'clipboard-data-fill'] 

  login_menu = option_menu(None, options, 
    icons=icons, 
    menu_icon="cast", 
    key='nav_menu',
    default_index=0, 
    orientation="horizontal"
  )

  login_menu

  if st.session_state["nav_menu"] == "Collection" or st.session_state["nav_menu"] == None:
    data_collection()
  elif st.session_state["nav_menu"] == "Question":
    questions()
  elif st.session_state["nav_menu"] == "Part3":
    part3report()
  elif st.session_state["nav_menu"] == "Part4":
    part4report()