import streamlit as st
import pandas as pd

from connection import connect_to_weaviate, status
from collection import aggregate_collections, get_schema, list_collections, process_collection_config, fetch_collection_config
from cluster import fetch_cluster_statistics, process_statistics, get_shards_info, process_shards_data, display_shards_table, get_metadata

# --------------------------------------------------------------------------
# Helper Functions
# --------------------------------------------------------------------------
def initialize_client(cluster_endpoint, api_key):
	if "client" not in st.session_state:
		st.session_state.client = None
		st.session_state.client_ready = False
		st.session_state.server_version = "N/A"
		st.session_state.client_version = "N/A"

	try:
		client = connect_to_weaviate(cluster_endpoint, api_key)
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

	st.session_state.client = None
	st.session_state.client_ready = False
	st.session_state.server_version = "N/A"
	st.session_state.client_version = "N/A"
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
    #    By giving 'key="selected_collection"', Streamlit remembers user selection
    if st.session_state["collections_list"]:
        selected_collection = st.selectbox(
            "Select a Collection",
            st.session_state["collections_list"],
            key="selected_collection",  # store user choice in session_state
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

# --------------------------------------------------------------------------
# Streamlit Page Config
# --------------------------------------------------------------------------
st.set_page_config(
	page_title="Weaviate Cluster Operations",
	layout="wide",
	initial_sidebar_state="expanded",
	page_icon="🔍",
)

st.sidebar.title("Weaviate Connection 🔗")

cluster_endpoint = st.sidebar.text_input(
	"Cluster Endpoint", placeholder="Enter Cluster Endpoint (URL)"
)
api_key = st.sidebar.text_input(
	"Cluster API Key", placeholder="Enter Cluster Read API Key", type="password"
)

# Connect/Disconnect
if st.sidebar.button("Connect", use_container_width=True, type="secondary"):
	if not cluster_endpoint or not api_key:
		st.sidebar.error("Please insert the cluster endpoint and API key!")
	else:
		if initialize_client(cluster_endpoint, api_key):
			st.sidebar.success("Connected successfully!")
		else:
			st.sidebar.error("Connection failed!")

if st.sidebar.button("Disconnect", use_container_width=True, type="primary"):
	disconnect_client()

# Connection Info
if st.session_state.get("client_ready"):
	st.sidebar.info("Connection Status: Ready")
	st.sidebar.info(f"Client Version: {st.session_state.client_version}")
	st.sidebar.info(f"Server Version: {st.session_state.server_version}")
else:
	st.sidebar.warning("No active connection")

st.title("Weaviate Cluster Operations 🔍")
st.markdown("---")

# --------------------------------------------------------------------------
# Buttons (each sets an active_button or calls a function)
# --------------------------------------------------------------------------
col1, col2, col3 = st.columns([1, 1, 1])
col4, col5, col6 = st.columns([1, 1, 1])

# Dictionary: button name => action function
button_actions = {
	"nodes": action_nodes_and_shards,
	"collections": action_collections,
	"schema": action_schema,
	"collections_configuration": lambda: action_collections_configuration(cluster_endpoint, api_key),
	"statistics": lambda: action_statistics(cluster_endpoint, api_key),
	"metadata": lambda: action_metadata(cluster_endpoint, api_key),
}

with col1:
	if st.button("Collections & Tenants", use_container_width=True):
		st.session_state["active_button"] = "collections"

with col2:
	if st.button("Schema", use_container_width=True):
		st.session_state["active_button"] = "schema"

with col3:
	if st.button("Collections Configuration", use_container_width=True):
		st.session_state["active_button"] = "collections_configuration"

with col4:
	if st.button("Nodes & Shards", use_container_width=True):
		st.session_state["active_button"] = "nodes"

with col5:
	if st.button("Statistics", use_container_width=True):
		st.session_state["active_button"] = "statistics"

with col6:
	if st.button("Metadata",use_container_width=True):
		st.session_state["active_button"] = "metadata"

st.markdown("---")

# --------------------------------------------------------------------------
# Execute the active button's action
# --------------------------------------------------------------------------
active_button = st.session_state.get("active_button")
if active_button and st.session_state.get("client_ready"):
	action_fn = button_actions.get(active_button)
	if action_fn:
		action_fn()
	else:
		st.warning("No action mapped for this button. Please report this issue.")
elif not st.session_state.get("client_ready"):
	st.warning("Connect to Weaviate first!")
