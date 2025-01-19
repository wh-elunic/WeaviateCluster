import streamlit as st

# Navigation sidebar
def navigate():
	st.sidebar.title("Go to: ")
	st.sidebar.page_link("streamlit_app.py", label="Cluster Operations", icon="🔍")
	st.sidebar.page_link("pages/collection_objects.py", label="Object Opertions", icon="📦")
	st.sidebar.page_link("pages/multitenancy.py", label="Multi Tenancy Operations", icon="🏢")
	st.sidebar.markdown("---")
