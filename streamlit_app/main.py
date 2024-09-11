import streamlit as st
from components.login_signup import menu_login
from components.navigation import tabs

st.set_page_config(page_title='Streamlit App', page_icon='🤧')

if "auth_status" not in st.session_state:
  st.session_state.auth_status = False

if not st.session_state["auth_status"]:
  menu_login()
else:
  col1, col2 = st.columns([6, 1])

  with col1:
    st.subheader("Hello {}".format(st.session_state["username"]))
  with col2:
    logout = st.button("log out")
    if logout:
      st.session_state["auth_status"] = False
      del st.session_state["username"]
      del st.session_state["access_token"]
      del st.session_state["token_type"]
      st.rerun()
  tabs()