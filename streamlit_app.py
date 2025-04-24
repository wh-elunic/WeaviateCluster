import streamlit as st
from utils.connection.weaviate_client import initialize_client
from utils.cluster.cluster_operations_handlers import action_check_shard_consistency, action_aggregate_collections_tenants, action_collections_configuration, action_metadata, action_nodes_and_shards, action_collection_schema, action_statistics, action_read_repairs
from utils.sidebar.navigation import navigate
from utils.connection.weaviate_connection import close_weaviate_client
from utils.sidebar.helper import update_side_bar_labels, clear_session_state

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
# Navigation on side bar
# --------------------------------------------------------------------------
navigate()

# Connect to Weaviate
st.sidebar.title("Weaviate Connection 🔗")
use_local = st.sidebar.checkbox("Local Cluster", value=False)

if use_local:
	st.sidebar.markdown(
		'This option requires cloning the repository from [**Shah91n -> WeaviateCluster**](https://github.com/Shah91n/WeaviateCluster) GitHub and following the installation requirements. Then ensure that you have a local Weaviate instance (http://localhost:8080) running on your machine before attempting to connect.'
	)
	cluster_endpoint = st.sidebar.text_input(
		"Local Cluster Endpoint", 
		value="http://localhost:8080", 
		disabled=True
	).strip()
	cluster_api_key = st.sidebar.text_input(
		"Local Cluster API Key", 
		placeholder="Enter Cluster Admin Key", 
		type="password", 
		value=st.session_state.get("cluster_api_key", "")
	).strip()
else:
	cluster_endpoint = st.sidebar.text_input(
		"Cloud Cluster Endpoint", 
		placeholder="Enter Cluster Endpoint (URL)", 
		value=st.session_state.get("cluster_endpoint", "")
	).strip()
	# Check if the URL has https:// prefix, if not add it
	if cluster_endpoint and not cluster_endpoint.startswith('https://'):
		cluster_endpoint = f"https://{cluster_endpoint}"
		
	cluster_api_key = st.sidebar.text_input(
		"Cloud Cluster API Key", 
		placeholder="Enter Cluster Admin Key", 
		type="password", 
		value=st.session_state.get("cluster_api_key", "")
	).strip()

if st.sidebar.button("Connect", use_container_width=True, type="secondary"):
	close_weaviate_client()
	clear_session_state()
	if use_local:
		if initialize_client(cluster_endpoint, cluster_api_key, use_local=True):
			st.sidebar.success("Connected to local successfully!")
		else:
			st.sidebar.error("Connection failed!")
	else:
		if not cluster_endpoint or not cluster_api_key:
			st.sidebar.error("Please insert the cluster endpoint and API key!")
		else:
			if initialize_client(cluster_endpoint, cluster_api_key, use_local=False):
				st.sidebar.success("Connected successfully!")
			else:
				st.sidebar.error("Connection failed!")

if st.sidebar.button("Disconnect", use_container_width=True, type="primary"):
	if st.session_state.get("client_ready"):
		message = close_weaviate_client()
		clear_session_state()
		st.rerun()
		st.sidebar.warning(message)

# Essential run for the first time
update_side_bar_labels()

# --------------------------------------------------------------------------
# Main Page Content (Cluster Operations)
# --------------------------------------------------------------------------

st.title("Weaviate Cluster 🔍")
st.markdown("---")
st.markdown("###### Any function with (APIs) means it is run using RESTful endpoints. Otherwise, it is executed through the DB client.")

# --------------------------------------------------------------------------
# Buttons (calls a function)
# --------------------------------------------------------------------------
col1, col2, col3 = st.columns([1, 1, 1])
col4, col5, col6 = st.columns([1, 1, 1])
col7, col8 = st.columns([1,1])

# Dictionary: button name => action function
button_actions = {
	"nodes": action_nodes_and_shards,
	"aggregate_collections_tenants": action_aggregate_collections_tenants,
	"collection_properties": action_collection_schema,
	"collections_configuration": lambda: action_collections_configuration(st.session_state.get("cluster_endpoint"), st.session_state.get("cluster_api_key")),
	"statistics": lambda: action_statistics(st.session_state.get("cluster_endpoint"), st.session_state.get("cluster_api_key")),
	"metadata": lambda: action_metadata(st.session_state.get("cluster_endpoint"), st.session_state.get("cluster_api_key")),
	"check_shard_consistency": action_check_shard_consistency,
	"read_repairs": lambda: action_read_repairs(st.session_state.get("cluster_endpoint"), st.session_state.get("cluster_api_key")),
}

with col1:
	if st.button("Aggregate Collections & Tenants", use_container_width=True):
		st.session_state["active_button"] = "aggregate_collections_tenants"

with col2:
	if st.button("Collection Properties", use_container_width=True):
		st.session_state["active_button"] = "collection_properties"

with col3:
	if st.button("Collections Configuration (APIs)", use_container_width=True):
		st.session_state["active_button"] = "collections_configuration"

with col4:
	if st.button("Nodes & Shards", use_container_width=True):
		st.session_state["active_button"] = "nodes"

with col5:
	if st.button("Raft Statistics (APIs)", use_container_width=True):
		st.session_state["active_button"] = "statistics"

with col6:
	if st.button("Metadata",use_container_width=True):
		st.session_state["active_button"] = "metadata"

with col7:
	if st.button("Check Shard Consistency For Repairs", use_container_width=True):
		st.session_state["active_button"] = "check_shard_consistency"

with col8:
	if st.button("Read Repair (APIs)", use_container_width=True):
		st.session_state["active_button"] = "read_repairs"

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
		st.warning("No action mapped for this button. Please report this issue to Mohamed Shahin in Weaviate Community Slack.")
elif not st.session_state.get("client_ready"):
	st.warning("Connect to Weaviate first!")
