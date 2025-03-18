import pandas as pd
import streamlit as st
import requests
import time
from utils.cluster.collection import aggregate_collections, get_schema, list_collections, process_collection_config, fetch_collection_config, get_collectios_count
from utils.cluster.cluster_operations import fetch_cluster_statistics, process_statistics, get_shards_info, process_shards_data, get_metadata, check_shard_consistency, read_repairs

# --------------------------------------------------------------------------
# Action Handlers (one function per button) for Cluster Operations
# --------------------------------------------------------------------------

# Fetch node info and display node and shard details.
def action_nodes_and_shards():
	print("Fetching node and shard details...")
	node_info = get_shards_info(st.session_state.client)
	if node_info:
		processed_data = process_shards_data(node_info)
		node_table = processed_data["node_data"]
		shard_table = processed_data["shard_data"]
		collection_shard_table = processed_data["collection_shard_data"]
		readonly_shards_table = processed_data["readonly_shards"]

		st.markdown("#### Node Details")
		if not node_table.empty:
			st.dataframe(node_table.astype(str), use_container_width=True)
		else:
			st.warning("No node details available.")

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

		# Readonly shards section
		st.markdown("#### Read-only Shards")
		if not readonly_shards_table.empty:
			st.dataframe(readonly_shards_table[["Node Name", "Class", "Shard Name", "Object Count"]].astype(str), use_container_width=True)
		else:
			st.info("No read-only shards found in the cluster.")

	else:
		st.error("Failed to retrieve node and shard details.")

# Check for shard consistency.
def action_check_shard_consistency():
	print("Checking shard consistency...")
	node_info = get_shards_info(st.session_state.client)
	if node_info:
		df_inconsistent_shards = check_shard_consistency(node_info)
		if df_inconsistent_shards is not None:
			inconsistent_collections = list(df_inconsistent_shards["Collection"].unique())
			total = len(inconsistent_collections)
			st.markdown(f"#### Inconsistent Shards Table with {total} Inconsistent collections")
			st.dataframe(df_inconsistent_shards.astype(str), use_container_width=True)
		else:
			st.success("All shards are consistent.")
	else:
		st.error("Failed to retrieve node and shard details.")

# Aggregate collections and tenants.
def action_aggregate_collections_tenants():
	print("Aggregating collections and tenants...")
	st.markdown("###### Collections & Tenants aggregation time may vary depending on the dataset size, as it iterates through all collections and tenants. Check below for tables with statistics.")
	result = aggregate_collections(st.session_state.client)
	if "error" in result:
		st.error(f"Error retrieving collections: {result['error']}")
		return

	# Display collection statistics
	collection_count = result["collection_count"]
	st.markdown(f"###### Total Number of Collections: **{collection_count}**")
	
	# Display empty collections with yellow warning
	empty_collections = result["empty_collections"]
	if empty_collections > 0:
		st.warning(f"###### Total Number of Collections with Zero Objects: **{empty_collections}**")
	else:
		st.markdown(f"###### Total Number of Collections with Zero Objects: **N/A**")

	# Display tenant statistics
	total_tenants_count = result["total_tenants_count"]
	if total_tenants_count > 0:
		st.markdown(f"###### Total Number of Tenants: **{total_tenants_count}**")
		
		# Display empty tenants with yellow warning
		empty_tenants = result["empty_tenants"]
		if empty_tenants > 0:
			st.warning(f"###### Total Number of Tenants with Zero Objects: **{empty_tenants}**")
		else:
			st.markdown(f"###### Total Number of Tenants with Zero Objects: **N/A**")
	
	# Display object counts
	total_regular = result["total_objects_regular"]
	if total_regular > 0:
		st.markdown(f"###### Total Objects in Regular Collections: **{total_regular:,}**")
	else:
		st.markdown(f"###### Total Objects in Regular Collections: **N/A**")

	total_multitenancy = result["total_objects_multitenancy"]
	if total_multitenancy > 0:
		st.markdown(f"###### Total Objects in Multitenancy Collections: **{total_multitenancy:,}**")
	else:
		st.markdown(f"###### Total Objects in Multitenancy Collections: **N/A**")

	total_combined = result["total_objects_combined"]
	if total_combined > 0:
		st.markdown(f"###### Total Objects (All Collections Combined): **{total_combined:,}**")
	else:
		st.markdown(f"###### Total Objects (All Collections Combined): **N/A**")

	# Display the main dataframe
	result_df = result["result_df"]
	if not result_df.empty:
		st.dataframe(result_df.astype(str), use_container_width=True)
	else:
		st.warning("No data to display.")

	# Display empty collections table if any exist
	empty_collections_list = result["empty_collections_list"]
	if empty_collections_list:
		st.markdown("#### Collections with Zero Objects")
		empty_collections_df = pd.DataFrame(empty_collections_list)
		st.dataframe(empty_collections_df, use_container_width=True)

	# Display empty tenants table if any exist
	empty_tenants_details = result["empty_tenants_details"]
	if empty_tenants_details:
		st.markdown("#### Tenants with Zero Objects")
		empty_tenants_df = pd.DataFrame(empty_tenants_details)
		st.dataframe(empty_tenants_df, use_container_width=True)

# Fetch and display collection properties.
def action_collection_schema():
	print("Fetching schema...")
	schema = get_schema(st.session_state.client)
	if schema is not None:
		if "error" in schema:
			st.error(schema["error"])
		else:
			st.markdown("#### Collection Schema & Properties")
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
		st.warning("No collection(s) available.")

# Fetch and display cluster statistics (RAFT).
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

		# Display main statistics
		flattened_data = processed_stats["data"]
		st.dataframe(flattened_data, use_container_width=True)

		# Display network information
		st.markdown("##### Network Information")
		network_df = processed_stats["network_info"]
		if not network_df.empty:
			st.dataframe(network_df, use_container_width=True)

		# Display latest configuration
		st.markdown("##### Latest Configuration")
		latest_config_df = processed_stats["latest_config"]
		if not latest_config_df.empty:
			st.dataframe(latest_config_df, use_container_width=True)

	except Exception as e:
		st.error(f"Error fetching cluster statistics: {e}")

# Fetch and display cluster metadata.
def action_metadata(cluster_endpoint, api_key):
	print("Fetching metadata...")
	st.markdown("#### Cluster Metadata Details")
	metadata_result = get_metadata(cluster_endpoint, api_key)

	if "error" in metadata_result:
		st.error(metadata_result["error"])
	else:
		# Display general metadata
		general_metadata_df = metadata_result["general_metadata_df"]
		st.markdown("##### General Information")
		st.dataframe(general_metadata_df, use_container_width=True)

		# Display standard modules
		standard_modules_df = metadata_result["standard_modules_df"]
		if not standard_modules_df.empty:
			st.markdown("##### Modules")
			st.dataframe(standard_modules_df, use_container_width=True)

		# Display other modules
		other_modules_df = metadata_result["other_modules_df"]
		if not other_modules_df.empty:
			st.markdown("##### Other Modules")
			st.dataframe(other_modules_df, use_container_width=True)

# Fetch and display collection configurations.
def action_collections_configuration(cluster_endpoint, api_key):
	print("Fetching collection configurations...")
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

# Fetch the chosen collection's config
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

def action_read_repairs(cluster_endpoint, api_key):
    # Step 1: Run shard consistency check and extract collection names.
    node_info = get_shards_info(st.session_state.client)
    if not node_info:
        st.error("Failed to retrieve node and shard details.")
        return

    df_inconsistent = check_shard_consistency(node_info)
    if df_inconsistent is None:
        st.success("All shards are consistent. No read repairs needed.")
        return

    inconsistent_collections = list(df_inconsistent["Collection"].unique())
    total = len(inconsistent_collections)

    # Store the inconsistent collections list in session state if not already set.
    if "repair_collections" not in st.session_state:
        st.session_state.repair_collections = inconsistent_collections

    st.markdown(f"### Inconsistent {total} collections")
    st.dataframe(df_inconsistent.astype(str), use_container_width=True)

    # Step 2: Synchronize selected_collection with repair_collections.
    if "selected_collection" not in st.session_state or st.session_state.selected_collection not in st.session_state.repair_collections:
        st.session_state.selected_collection = st.session_state.repair_collections[0] if st.session_state.repair_collections else None

    # Radio button for selecting the collection to repair, only if there are collections.
    if st.session_state.repair_collections:
        selected_collection = st.radio(
            "Select a collection to repair",
            st.session_state.repair_collections,
            index=st.session_state.repair_collections.index(st.session_state.selected_collection) if st.session_state.selected_collection in st.session_state.repair_collections else 0,
            key="collection_radio"
        )
        st.session_state.selected_collection = selected_collection
    else:
        st.info("No inconsistent collections to repair.")
        st.session_state.selected_collection = None

    # Stop any ongoing read repairs.
    if st.button("Stop the process", use_container_width=True):
        print("Stopping read repairs...")
        if 'repair_in_progress' in st.session_state:
            del st.session_state.repair_in_progress
        if 'all_uuids' in st.session_state:
            del st.session_state.all_uuids
        if 'current_batch_index' in st.session_state:
            del st.session_state.current_batch_index
        if 'progress' in st.session_state:
            del st.session_state.progress
        st.stop()
        st.success("Read repairs stopped.")

    # Refresh the collections list when the button is clicked.
    if st.button("Refresh Collections", use_container_width=True):
       st.success("Collections list refreshed.")
		
    # Step 3: Trigger read repairs.
    if st.button("Start Read Repairs", use_container_width=True):
        print("Starting read repairs...")
        if 'repair_in_progress' in st.session_state:
            del st.session_state.repair_in_progress
        if 'all_uuids' in st.session_state:
            del st.session_state.all_uuids
        if 'current_batch_index' in st.session_state:
            del st.session_state.current_batch_index
        if 'progress' in st.session_state:
            del st.session_state.progress
        # Ensure the selected collection is still valid.
        if selected_collection not in st.session_state.repair_collections:
            st.error("Selected collection no longer exists in repair list")
            return

        # If repairs are not already in progress, initialize the repair state.
        if 'repair_in_progress' not in st.session_state:
            st.markdown(f"**Starting read repairs for collection** (1 iteration only & 500 UUID per batch): `{selected_collection}`")
            
            # Store the cluster endpoint and API key for use in subsequent reruns.
            st.session_state.repair_base_url = cluster_endpoint
            st.session_state.repair_api_key = api_key

            # Step 3.1: Fetch all object UUIDs for the selected collection.
            base_url = cluster_endpoint
            bearer_token = api_key
            headers = {"Authorization": f"Bearer {bearer_token}"}

            limit = 1000
            offset = 0
            all_uuids = []
            st.session_state["repair_logs"] = "Fetching objects...\n"
            
            while True:
                params_list = {"limit": limit, "offset": offset, "class": selected_collection, "consistency_level": "ALL"}
                resp = requests.get(f"{base_url}/v1/objects", params=params_list, headers=headers)
                if resp.status_code != 200:
                    st.error(f"Error fetching objects: {resp.status_code} {resp.text}")
                    return
                data = resp.json()
                objects_batch = data.get("objects", [])
                if not objects_batch:
                    break
                all_uuids.extend(obj["id"] for obj in objects_batch)
                offset += limit

            st.session_state.repair_logs += f"Fetched {len(all_uuids)} objects.\n=== Starting Iteration 1 ===\n"

            # Initialize repair state.
            st.session_state.repair_in_progress = True
            st.session_state.all_uuids = all_uuids
            st.session_state.current_batch_index = 0
            st.session_state.progress = 0.0
            st.session_state.batch_size = 500  # Process 500 UUIDs per batch

    # If a repair is in progress, process the next batch.
    if st.session_state.get("repair_in_progress"):
        # Retrieve the stored cluster endpoint and API key.
        base_url = st.session_state.get("repair_base_url")
        bearer_token = st.session_state.get("repair_api_key")
        selected_collection = st.session_state.selected_collection

        log_container = st.empty()
        progress_bar = st.progress(st.session_state.progress)

        all_uuids = st.session_state.all_uuids
        batch_size = st.session_state.batch_size
        current_batch_index = st.session_state.current_batch_index
        total_uuids = len(all_uuids)
        headers = {"Authorization": f"Bearer {bearer_token}"}

        # Process the current batch.
        for i in range(current_batch_index, min(current_batch_index + batch_size, total_uuids)):
            uuid = all_uuids[i]
            url = f"{base_url}/v1/objects/{selected_collection}/{uuid}"
            params_single = {"consistency_level": "ALL"}
            resp_single = requests.get(url, params=params_single, headers=headers)
            index = i + 1

            if resp_single.status_code == 200:
                log_entry = f"[Iteration 1] [{index}/{total_uuids}] UUID={uuid}\n"
                log_container.text_area("Read Repair Logs", st.session_state.repair_logs, height=300)
                print(log_entry)
            elif resp_single.status_code == 404:
                log_entry = f"[Iteration 1] [{index}/{total_uuids}] UUID={uuid} => Not found.\n"
                log_container.text_area("Read Repair Logs", st.session_state.repair_logs, height=300)
                print(log_entry)
            else:
                log_entry = f"[Iteration 1] [{index}/{total_uuids}] UUID={uuid} => Error {resp_single.status_code}\n"
                log_container.text_area("Read Repair Logs", st.session_state.repair_logs, height=300)
                print(log_entry)

            st.session_state.repair_logs += log_entry
            st.session_state.progress = index / total_uuids

        # Update the current batch index.
        st.session_state.current_batch_index = min(current_batch_index + batch_size, total_uuids)

        # Update the UI with logs and progress.
        log_container.text_area("Read Repair Logs", st.session_state.repair_logs, height=300)
        progress_bar.progress(st.session_state.progress)

        # Check if all UUIDs have been processed.
        if st.session_state.current_batch_index >= total_uuids:
            st.session_state.repair_logs += "=== Iteration 1 Complete ==="
            log_container.text_area("Read Repair Logs", st.session_state.repair_logs, height=300)
            st.success("Read repairs completed!")
            # Clean up repair state variables.
            del st.session_state.repair_in_progress
            del st.session_state.all_uuids
            del st.session_state.current_batch_index
            del st.session_state.progress
        else:
            # Force a rerun to process the next batch.
            time.sleep(0.5)
            st.rerun()