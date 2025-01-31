import streamlit as st
from utils.connection.navigation import navigate
from utils.sidebar.helper import update_side_bar_labels
from utils.collections.objects import list_all_collections, get_tenant_names, fetch_collection_data

def get_all_objects_of_collections_and_tenants():
	client = st.session_state.client

	# Initialize and sort collections list
	if "collections_list" not in st.session_state:
		collections = list_all_collections(client)
		if not isinstance(collections, list):
			collections = list(collections.keys())
		collections.sort()
		st.session_state.collections_list = collections

	# Collection selection
	selected_collection = st.selectbox(
		"Select a Collection",
		st.session_state.collections_list,
		key="main_collection_select"
	)

	# Get and sort tenant names - Sort tenants alphabetically
	tenant_names = get_tenant_names(client, selected_collection)
	if tenant_names:
		tenant_names = sorted(tenant_names)

	selected_tenant = None
	if tenant_names:
		selected_tenant = st.selectbox(
			"Select a Tenant",
			tenant_names,
			key="main_tenant_select"
		)

	if st.button("Read All Objects", use_container_width=True):
		st.info("Please note that fetching all objects from a collection/tenant can take some time, depending on the number of objects. For example, fetching 5000 objects may take 5 seconds ⏱️, considering network latency. Please be patient as it iterates through all objects.")
		st.info("Fetching all objects... ⤵️")
		if tenant_names and not selected_tenant:
			st.error("Please select a tenant for this collection")
		else:
			df = fetch_collection_data(client, selected_collection, selected_tenant)
			if not df.empty:
				st.dataframe(df, use_container_width=True)
			else:
				st.warning("No data found")

def main():
	st.title("Collections Data 📊")
	navigate()

	if st.session_state.get("client_ready"):
		update_side_bar_labels()
		get_all_objects_of_collections_and_tenants()
	else:
		st.warning("Please Establish a connection to Weaviate in Cluster Operations page!")

if __name__ == "__main__":
	main()
