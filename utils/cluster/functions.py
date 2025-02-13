import pandas as pd
import streamlit as st
from utils.cluster.collection import aggregate_collections, get_schema, list_collections, process_collection_config, fetch_collection_config, get_collectios_count
from utils.cluster.cluster import fetch_cluster_statistics, process_statistics, get_shards_info, process_shards_data, get_metadata, check_shard_consistency

# --------------------------------------------------------------------------
# Action Handlers (one function per button)
# --------------------------------------------------------------------------
def action_nodes_and_shards():
	node_info = get_shards_info(st.session_state.client)
	if node_info:
		processed_data = process_shards_data(node_info)
		node_table = processed_data["node_data"]
		shard_table = processed_data["shard_data"]
		collection_shard_table = processed_data["collection_shard_data"]

		st.markdown("#### Node Details")
		if not node_table.empty:
			st.dataframe(node_table.astype(str), use_container_width=True)
		else:
			st.warning("No node details available.")

		# Display Shard Collections Details Count under Node Details
		st.markdown("#### Shard Count")
		if not collection_shard_table.empty:
			st.dataframe(collection_shard_table, use_container_width=True)
		else:
			st.warning("No shard collection details available.")

		st.markdown("#### Shard Details")
		if not shard_table.empty:
			st.dataframe(shard_table.astype(str), use_container_width=True)
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
			st.dataframe(df_inconsistent_shards.astype(str), use_container_width=True)
		else:
			st.success("All shards are consistent.")
	else:
		st.error("Failed to retrieve node and shard details.")

def action_aggregate_collections_tenants():
	st.markdown("###### Collections & Tenants aggregation can take a while to complete due to the large amount of data and the loop through all collections & Tenants.")
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
		st.dataframe(result_df.astype(str), use_container_width=True)
	else:
		st.warning("No data to display.")

def action_schema():
	schema = get_schema(st.session_state.client)
	if "error" in schema:
		st.error(schema["error"])
	elif schema:
		st.markdown("#### Collection Properties")
		for collection_name, collection_details in schema.items():
			with st.expander(f"Collection: {collection_name}", expanded=False):
				st.markdown(f"**Name:** {collection_details.name}")
				st.markdown(f"**Description:** {collection_details.description or 'None'}")
				st.markdown(f"**Vectorizer:** {collection_details.vectorizer or 'If no vectorizer then could be NamedVectors (check collections config) or its BYOV'}")
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
		st.dataframe(df.astype(str), use_container_width=True)

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
		st.dataframe(general_metadata_df.astype(str), use_container_width=True)

		modules_df = metadata_result["modules_df"]
		if not modules_df.empty:
			st.markdown("###### Module Details")
			st.dataframe(modules_df.astype(str), use_container_width=True)

		nested_module_data = metadata_result["nested_module_data"]
		if nested_module_data:
			for module_name, nested_df in nested_module_data.items():
				st.markdown(f"###### Details for Module: **{module_name}**")
				st.dataframe(nested_df.astype(str), use_container_width=True)

def action_collections_configuration(cluster_endpoint, api_key):
	"""
	  1. Checks if connected.
	  2. Loads collection list (and stores in session_state).
	  3. Displays a selectbox.
	  4. Display "Get Configuration" button to fetch + display config.
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
		collection_count = get_collectios_count(st.session_state.client)
		st.markdown(f"###### Total Number of Collections in the list: **{collection_count}**\n")
		selected_collection = st.selectbox(
			"Select a Collection",
			st.session_state["collections_list"],
		)
	else:
		st.warning("No collections available to display.")
		return

# Fetch the chosen collection’s config
	if st.button("Get Configuration", use_container_width=True):
		config = fetch_collection_config(cluster_endpoint, api_key, selected_collection)
		if "error" in config:
			st.error(config["error"])
		else:
			processed_config = process_collection_config(config)
			st.markdown(f"##### **{selected_collection}** Configurations: ")

			for section, details in processed_config.items():
				# Handle Named Vectors Config
				if section == "Named Vectors Config" and isinstance(details, dict):
					for vector_name, vector_info in details.items():
						# Print the Named Vector title
						st.markdown(f"##### Named Vector: {vector_name}")

						# 1. Display Vectorizer section first if it exists
						if "Vectorizer" in vector_info and isinstance(vector_info["Vectorizer"], dict):
							for vec_name, vec_config in vector_info["Vectorizer"].items():
								st.markdown(f"###### Vectorizer: **{vec_name}**")
								if isinstance(vec_config, dict) and vec_config:
									df = pd.DataFrame(list(vec_config.items()), columns=["Key", "Value"])
									st.dataframe(df.astype(str), use_container_width=True)
								else:
									st.markdown(f"**{vec_config}**")

						# 2. Display Vector Index Type if it exists
						if "Vector Index Type" in vector_info:
							sub_details = vector_info["Vector Index Type"]
							st.markdown(f"###### Vector Index Type: **{sub_details}**")

						# 3. Display Vector Index Config if it exists
						if "Vector Index Config" in vector_info:
							sub_details = vector_info["Vector Index Config"]
							st.markdown(f"###### Vector Index Config:")
							if isinstance(sub_details, dict) and sub_details:
								df = pd.DataFrame(list(sub_details.items()), columns=["Key", "Value"])
								st.dataframe(df.astype(str), use_container_width=True)
							else:
								st.markdown(f"**{sub_details}**")

						# 4. Handle any additional subsections
						for sub_section, sub_details in vector_info.items():
							if sub_section not in ["Vectorizer", "Vector Index Type", "Vector Index Config"]:
								st.markdown(f"###### {sub_section}:")
								if isinstance(sub_details, dict) and sub_details:
									df = pd.DataFrame(list(sub_details.items()), columns=["Key", "Value"])
									st.dataframe(df.astype(str), use_container_width=True)
								else:
									st.markdown(f"**{sub_details}**")

				# Handle Vectorizer Config in NoNamed Vectors Config found
				elif section == "Vectorizer Config" and isinstance(details, dict):
					st.markdown(f"####### {section}:")
					for vec_name, vec_config in details.items():
						st.markdown(f"###### Vectorizer: **{vec_name}**")
						if isinstance(vec_config, dict) and vec_config:
							df = pd.DataFrame(list(vec_config.items()), columns=["Key", "Value"])
							st.dataframe(df.astype(str), use_container_width=True)
						else:
							st.markdown(f"**{vec_config}**")

						# Retrieve and display module configuration for this vectorizer if available
						module_conf = config.get("moduleConfig", {}).get(vec_name)
						if module_conf:
							st.markdown(f"###### Module Config for {vec_name}:") # Subsection heading
							if isinstance(module_conf, dict) and module_conf:
								df_module = pd.DataFrame(list(module_conf.items()), columns=["Key", "Value"])
								st.dataframe(df_module.astype(str), use_container_width=True)
							else:
								st.markdown(f"**{module_conf}**")

				# Handle other sections if any
				else:
					st.markdown(f"###### {section}:")
					if isinstance(details, dict) and details:
						df = pd.DataFrame(list(details.items()), columns=["Key", "Value"])
						st.dataframe(df.astype(str), use_container_width=True)
					else:
						st.markdown(f"**{details}**")
