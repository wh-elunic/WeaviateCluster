import streamlit as st

# Navigation sidebar
def navigate():
	st.sidebar.title("Go to: ")
	st.sidebar.page_link("streamlit_app.py", label="Cluster", icon="🔍")
	st.sidebar.page_link("pages/object.py", label="Object", icon="📦")
	st.sidebar.page_link("pages/multitenancy.py", label="Multi Tenancy", icon="📒")
	st.sidebar.page_link("pages/data.py", label="Data", icon="📁")
	st.sidebar.page_link("pages/delete.py", label="Delete", icon="🗑️")
	st.sidebar.markdown("---")
