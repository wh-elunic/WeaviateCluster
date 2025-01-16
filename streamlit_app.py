import streamlit as st
from utils.cluster.functions import initialize_client, disconnect_client
from utils.cluster.functions import action_check_shard_consistency, action_aggregate_collections_tenants, action_collections_configuration, action_metadata, action_nodes_and_shards, action_schema, action_statistics
from utils.connection.navigation import navigate

# ------------------------ß--------------------------------------------------
# Streamlit Page Config
# --------------------------------------------------------------------------

st.set_page_config(
	page_title="Weaviate Cluster Operations",
	layout="wide",
	initial_sidebar_state="expanded",
	page_icon="🔍",
)

# --------------------------------------------------------------------------
# Navigation
# --------------------------------------------------------------------------
navigate()

st.sidebar.markdown("---")

st.sidebar.title("Weaviate Connection 🔗")

use_local = st.sidebar.checkbox("Local Cluster", value=False)

if use_local:
	st.sidebar.info("Local Weaviate at http://localhost:8080")
	cluster_endpoint = "http://localhost:8080"
	api_key = ""
else:
	cluster_endpoint = st.sidebar.text_input(
		"Cluster Endpoint", placeholder="Enter Cluster Endpoint (URL)"
	)
	api_key = st.sidebar.text_input(
		"Cluster API Key", placeholder="Enter Cluster Read API Key", type="password"
	)

if st.sidebar.button("Connect", use_container_width=True, type="secondary"):

	if use_local:
		# Connect to local
		if initialize_client(cluster_endpoint, api_key, use_local=True):
			st.sidebar.success("Connected to local successfully!")
		else:
			st.sidebar.error("Connection failed!")
	else:
		# Cloud
		if not cluster_endpoint or not api_key:
			st.sidebar.error("Please insert the cluster endpoint and API key!")
		else:
			if initialize_client(cluster_endpoint, api_key, use_local=False):
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
col7 = st.columns([1])

# Dictionary: button name => action function
button_actions = {
	"nodes": action_nodes_and_shards,
	"aggregate_collections_tenants": action_aggregate_collections_tenants,
	"collection_properties": action_schema,
	"collections_configuration": lambda: action_collections_configuration(cluster_endpoint, api_key),
	"statistics": lambda: action_statistics(cluster_endpoint, api_key),
	"metadata": lambda: action_metadata(cluster_endpoint, api_key),
	"check_shard_consistency": action_check_shard_consistency,
}

with col1:
	if st.button("Aggregate Collections & Tenants", use_container_width=True):
		st.session_state["active_button"] = "aggregate_collections_tenants"

with col2:
	if st.button("Collection Properties", use_container_width=True):
		st.session_state["active_button"] = "collection_properties"

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

with col7[0]:
	if st.button("Check Shard Consistency", use_container_width=True):
		st.session_state["active_button"] = "check_shard_consistency"

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
