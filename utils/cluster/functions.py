
import pandas as pd
import streamlit as st

from connection import connect_to_weaviate, connect_weaviate_local, status
from utils.cluster.collection import aggregate_collections, get_schema, list_collections, process_collection_config, fetch_collection_config
from utils.cluster.cluster import fetch_cluster_statistics, process_statistics, get_shards_info, process_shards_data, display_shards_table, get_metadata, check_shard_consistency

# --------------------------------------------------------------------------
# Helper Functions
# --------------------------------------------------------------------------
def initialize_client(cluster_endpoint, api_key, use_local=False):
	"""
	Initializes a local or cloud Weaviate client, 
	stores client in st.session_state, 
	and returns True if connected/ready.
	"""

	if "client" not in st.session_state:
		st.session_state.client = None
		st.session_state.client_ready = False
		st.session_state.server_version = "N/A"
		st.session_state.client_version = "N/A"
		st.session_state.cluster_url = ""

	try:
		if use_local:
			client = connect_weaviate_local()
			st.session_state.cluster_url = "http://localhost:8080"
		else:
			client = connect_to_weaviate(cluster_endpoint, api_key)
			st.session_state.cluster_url = cluster_endpoint
			st.session_state.cluster_api_key = api_key 

		if client:
			ready, server_version, client_version = status(client)
			st.session_state.client = client
			st.session_state.client_ready = ready
			st.session_state.server_version = server_version
			st.session_state.client_version = client_version
			return True

	except Exception as e:
		st.sidebar.error(f"Connection Error: {e}")
		st.session_state.client = None
		st.session_state.client_ready = False
		return False

	return False

def disconnect_client():
	if st.session_state.get("client_ready"):
		client = st.session_state.client
		if client:
			try:
				client.close()
			except Exception as e:
				st.sidebar.error(f"Error while disconnecting: {e}")

	# Reset
	st.session_state.client = None
	st.session_state.client_ready = False
	st.session_state.server_version = "N/A"
	st.session_state.client_version = "N/A"
	st.session_state.cluster_url = ""
	st.sidebar.warning("Disconnected!")

# --------------------------------------------------------------------------
# Action Handlers (one function per button)
# --------------------------------------------------------------------------
def action_nodes_and_shards():
	node_info = get_shards_info(st.session_state.client)
	if node_info:
		processed_data = process_shards_data(node_info)
		node_table, shard_table = display_shards_table(processed_data)

		st.markdown("#### Node Details")
		if not node_table.empty:
			st.dataframe(node_table, use_container_width=True)
		else:
			st.warning("No node details available.")

		st.markdown("#### Shard Details")
		if not shard_table.empty:
			st.dataframe(shard_table, use_container_width=True)
		else:
			st.warning("No shard details available.")
	else:
		st.error("Failed to retrieve node and shard details.")

def action_check_shard_consistency():
	"""
	Fetch node info and check for shard consistency.
	Display results in a table if inconsistencies are found.
	"""
	node_info = get_shards_info(st.session_state.client)
	if node_info:
		# Call the function from cluster.py
		df_inconsistent_shards = check_shard_consistency(node_info)
		if df_inconsistent_shards is not None:
			st.markdown("#### Inconsistent Shards Found")
			st.dataframe(df_inconsistent_shards, use_container_width=True)
		else:
			st.success("All shards are consistent.")
	else:
		st.error("Failed to retrieve node and shard details.")

def action_collections():
	st.markdown("#### Collections & Tenants Details")
	result = aggregate_collections(st.session_state.client)
	if "error" in result:
		st.error(f"Error retrieving collections: {result['error']}")
		return

	collection_count = result["collection_count"]
	st.markdown(f"###### Total Number of Collections: **{collection_count}**\n")

	total_tenants_count = result["total_tenants_count"]
	if total_tenants_count > 0:
		st.markdown(f"###### Total Number of Tenants: **{total_tenants_count}**\n")

	result_df = result["result_df"]
	if not result_df.empty:
		st.table(result_df)
	else:
		st.warning("No data to display.")

def action_schema():
	schema = get_schema(st.session_state.client)
	if "error" in schema:
		st.error(schema["error"])
	elif schema:
		st.markdown("#### Schema Details")
		for collection_name, collection_details in schema.items():
			with st.expander(f"Collection: {collection_name}", expanded=False):
				st.markdown(f"**Name:** {collection_details.name}")
				st.markdown(f"**Description:** {collection_details.description or 'None'}")
				st.markdown(f"**Vectorizer:** {collection_details.vectorizer or 'None'}")

				if collection_details.generative_config:
					st.markdown("**Generative Config:**")
					st.markdown(f"- Generative Model: {collection_details.generative_config.model or 'None'}")
				else:
					st.markdown("**Generative Config:** None")

				if collection_details.reranker_config:
					st.markdown(f"**Reranker Config:** {collection_details.reranker_config.reranker or 'None'}")
				else:
					st.markdown("**Reranker Config:** None")

				st.markdown("#### Properties:")
				properties_data = []
				for prop in collection_details.properties:
					properties_data.append({
						"Property Name": prop.name or "None",
						"Description": prop.description or "None",
						"Data Type": str(prop.data_type) or "None",
						"Searchable": prop.index_searchable,
						"Filterable": prop.index_filterable,
						"Tokenization": str(prop.tokenization) or "None",
						"Vectorizer": prop.vectorizer or "None",
					})
				if properties_data:
					st.dataframe(pd.DataFrame(properties_data), use_container_width=True)
				else:
					st.markdown("*No properties found.*")
	else:
		st.warning("No schema details available.")

def action_statistics(cluster_endpoint, api_key):
	st.markdown("#### Cluster Statistics Details")
	try:
		stats = fetch_cluster_statistics(cluster_endpoint, api_key)
		if "error" in stats:
			st.error(stats["error"])
			return

		processed_stats = process_statistics(stats)
		if "error" in processed_stats:
			st.error(processed_stats["error"])
			return

		synchronized = processed_stats["synchronized"]
		if synchronized:
			st.success("Cluster is Synchronized: ✅")
		else:
			st.error("Cluster is Synchronized: ❌")

		flattened_data = processed_stats["data"]
		df = pd.DataFrame(flattened_data)
		st.dataframe(df, use_container_width=True)

	except Exception as e:
		st.error(f"Error fetching cluster statistics: {e}")

def action_metadata(cluster_endpoint, api_key):
	st.markdown("#### Cluster Metadata Details")
	metadata_result = get_metadata(cluster_endpoint, api_key)

	if "error" in metadata_result:
		st.error(metadata_result["error"])
	else:
		general_metadata_df = metadata_result["general_metadata_df"]
		st.markdown("###### General Metadata Information")
		st.dataframe(general_metadata_df, use_container_width=True)

		modules_df = metadata_result["modules_df"]
		if not modules_df.empty:
			st.markdown("###### Module Details")
			st.dataframe(modules_df, use_container_width=True)

		nested_module_data = metadata_result["nested_module_data"]
		if nested_module_data:
			for module_name, nested_df in nested_module_data.items():
				st.markdown(f"###### Details for Module: **{module_name}**")
				st.dataframe(nested_df, use_container_width=True)

# Collections Configuration
def action_collections_configuration(cluster_endpoint, api_key):
	"""
	This function is triggered by the "Collections Configuration" button.
	It:
	  1. Checks if connected (else warns).
	  2. Loads collection list once (and stores in session_state).
	  3. Displays a selectbox to pick a collection.
	  4. Provides a "Get Configuration" button to fetch + display config immediately.
	"""

	# Load collections list if not already loaded in session_state
	if "collections_list" not in st.session_state:
		collection_list = list_collections(st.session_state.client)
		if collection_list and not isinstance(collection_list, dict):
			st.session_state["collections_list"] = collection_list
		else:
			st.session_state["collections_list"] = []

	# Show a selectbox for the user to choose a collection
	if st.session_state["collections_list"]:
		selected_collection = st.selectbox(
			"Select a Collection",
			st.session_state["collections_list"],
			key="selected_collection", # store user choice in session_state
		)
	else:
		# If no collections, just warn
		st.warning("No collections available to display.")
		return

	# Button to fetch the chosen collection’s config
	#    When pressed, triggers a rerun, but we still have the user’s selection in session_state
	if st.button("Get Configuration", use_container_width=True):
		config = fetch_collection_config(cluster_endpoint, api_key, selected_collection)
		if "error" in config:
			st.error(config["error"])
		else:
			processed_config = process_collection_config(config)
			st.markdown(f"##### **{selected_collection}** Configurations: ")

			# Same logic you had before for displaying processed config
			for section, details in processed_config.items():
				st.markdown(f"###### {section}:")
				if isinstance(details, dict):
					df = pd.DataFrame(details.items(), columns=["Key", "Value"])
					st.dataframe(df, use_container_width=True)
				else:
					st.markdown(f"**{details}**")
