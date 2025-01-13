import streamlit as st

# Navigation sidebar
def navigate():
	st.sidebar.page_link("streamlit_app.py", label="Cluster Operations")
	st.sidebar.page_link("pages/collection_objects.py", label="Object Opertions")
