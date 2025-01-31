import streamlit as st

# Navigation sidebar
def navigate():
	st.sidebar.title("Go to: ")
	st.sidebar.page_link("streamlit_app.py", label="Cluster", icon="🔍")
	st.sidebar.page_link("pages/collection_objects.py", label="Object", icon="📦")
	st.sidebar.page_link("pages/multitenancy.py", label="Multi Tenancy", icon="🏢")
	st.sidebar.page_link("pages/collections_data.py", label="Collections", icon="📊")
	st.sidebar.markdown("---")
