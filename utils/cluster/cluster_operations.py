import requests
import pandas as pd
from collections import defaultdict
import streamlit as st
import json

# Get shards information
def get_shards_info(client):
	node_info = client.cluster.nodes(output="verbose")
	return node_info

def process_shards_data(node_info):
    node_data = []
    shard_data = []
    collection_shard_counts = []
    readonly_shards = []

    for node in node_info:
        print(f"Processing node: {node.name}")
        
        # Node-level data
        node_data.append({
            "Node Name": node.name,
            "Git Hash": node.git_hash,
            "Version": node.version,
            "Status": node.status,
            "Object Count (Stats)": node.stats.object_count,
            "Shard Count (Stats)": node.stats.shard_count,
        })

        # Dictionary to count shards per collection for this node
        collection_counts = {}

        # Shard-level data for each node
        for shard in node.shards:            
            shard_info = {
                "Node Name": node.name,
                "Class": shard.collection,
                "Shard Name": shard.name,
                "Object Count": shard.object_count,
                "Index Status": shard.vector_indexing_status,
                "Vector Queue Length": shard.vector_queue_length,
                "Compressed": shard.compressed,
                "Loaded": shard.loaded
            }
            shard_data.append(shard_info)

            # Check specifically for READONLY status
            if hasattr(shard, 'vector_indexing_status') and shard.vector_indexing_status == "READONLY":
                readonly_shards.append(shard_info)

            # Increment count for this collection on the current node
            collection_counts[shard.collection] = collection_counts.get(shard.collection, 0) + 1

        # Append shard collection counts for the current node
        for collection, count in collection_counts.items():
            collection_shard_counts.append({
                "Node Name": node.name,
                "Collection": collection,
                "Shard Count": count
            })

    return {
        "node_data": pd.DataFrame(node_data),
        "shard_data": pd.DataFrame(shard_data), 
        "collection_shard_data": pd.DataFrame(collection_shard_counts),
        "readonly_shards": pd.DataFrame(readonly_shards) if readonly_shards else pd.DataFrame()
    }


def display_shards_table(processed_data):
	return processed_data["node_data"], processed_data["shard_data"]

def check_shard_consistency(node_info):
    """
    Check consistency of shard object counts across nodes.
    Returns a DataFrame of inconsistencies, or None if consistent.
    """
    shard_data = defaultdict(list)
    for node in node_info:
        # node.shards is a list of shards for this node
        for shard in node.shards:
            shard_key = (shard.collection, shard.name)
            shard_data[shard_key].append((node.name, shard.object_count))

    inconsistent_shards = []
    for (collection, shard_name), details in shard_data.items():
        object_counts = [obj_count for _, obj_count in details]
        # Inconsistent if not all object counts are identical
        if len(set(object_counts)) > 1:
            for node_name, object_count in details:
                inconsistent_shards.append({
                    "Collection": collection,
                    "Shard": shard_name,
                    "Node": node_name,
                    "Object Count": object_count,
                })

    if inconsistent_shards:
        df_inconsistent_shards = pd.DataFrame(inconsistent_shards)
        return df_inconsistent_shards

    return None

# Get cluster Schema
def get_schema(cluster_url, api_key):
	try:
		url = f"{cluster_url}/v1/schema"
		headers = {"Authorization": f"Bearer {api_key}"}
		response = requests.get(url, headers=headers)
		response.raise_for_status()
		return response.json() 
	except requests.exceptions.RequestException as e:
		return {"error": f"Failed to fetch cluster statistics: {e}"}

# Get cluster statistics
def fetch_cluster_statistics(cluster_url, api_key):
	try:
		url = f"{cluster_url}/v1/cluster/statistics"
		headers = {"Authorization": f"Bearer {api_key}"}
		response = requests.get(url, headers=headers)
		response.raise_for_status() 

		return response.json() 
	except requests.exceptions.RequestException as e:
		return {"error": f"Failed to fetch cluster statistics: {e}"}


def process_statistics(stats):
    if "statistics" not in stats:
        return {"error": "Invalid statistics data received."}

    flattened_data = []
    latest_config_data = []
    network_info = []
    synchronized = stats.get("synchronized", False)
    
    for node in stats["statistics"]:
        # Base data for node statistics
        base_data = {
            "Node Name": node.get("name", "N/A"),
            "Leader ID": node.get("leaderId", "N/A"),
            "Leader Address": node.get("leaderAddress", "N/A"),
            "State": node.get("raft", {}).get("state", "N/A"),
            "Status": node.get("status", "N/A"),
            "Ready": node.get("ready", "N/A"),
            "DB Loaded": node.get("dbLoaded", "N/A"),
            "Open": node.get("open", "N/A"),
            "Is Voter": node.get("isVoter", "N/A"),
            "Applied Index": node.get("raft", {}).get("appliedIndex", "N/A"),
            "Commit Index": node.get("raft", {}).get("commitIndex", "N/A"),
            "Last Contact": node.get("raft", {}).get("lastContact", "N/A"),
            "Last Log Index": node.get("raft", {}).get("lastLogIndex", "N/A"),
            "Last Log Term": node.get("raft", {}).get("lastLogTerm", "N/A"),
            "Initial Last Applied Index": node.get("initialLastAppliedIndex", "N/A"),
            "Num Peers": node.get("raft", {}).get("numPeers", "N/A"),
            "Term": node.get("raft", {}).get("term", "N/A"),
            "FSM Pending": node.get("raft", {}).get("fsmPending", "N/A"),
            "Last Snapshot Index": node.get("raft", {}).get("lastSnapshotIndex", "N/A"),
            "Last Snapshot Term": node.get("raft", {}).get("lastSnapshotTerm", "N/A"),
            "Protocol Version": node.get("raft", {}).get("protocolVersion", "N/A"),
            "Protocol Version Max": node.get("raft", {}).get("protocolVersionMax", "N/A"),
            "Protocol Version Min": node.get("raft", {}).get("protocolVersionMin", "N/A"),
            "Snapshot Version Max": node.get("raft", {}).get("snapshotVersionMax", "N/A"),
            "Snapshot Version Min": node.get("raft", {}).get("snapshotVersionMin", "N/A"),
        }
        flattened_data.append(base_data)

        # Process latestConfiguration
        latest_config = node.get("raft", {}).get("latestConfiguration", [])
        for config in latest_config:
            # Extract network info
            address = config.get("address", "N/A")
            if ":" in address:
                ip, port = address.rsplit(":", 1)
                network_info.append({
                    "Pod": config.get("id", "N/A"),
                    "IP": ip,
                    "Port": port
                })

            config_data = {
                "Node Name": node.get("name", "N/A"),
                "Node State": node.get("raft", {}).get("state", "N/A"),
                "Peer ID": config.get("id", "N/A"),
                "Peer Address": address,
                "Peer Suffrage": "Voter" if config.get("suffrage") == 0 else "Non-Voter"
            }
            latest_config_data.append(config_data)

    df_data = pd.DataFrame(flattened_data).fillna("N/A")
    df_config = pd.DataFrame(latest_config_data).fillna("N/A")
    df_network = pd.DataFrame(network_info).drop_duplicates().fillna("N/A")

    return {
        "data": df_data,
        "synchronized": synchronized,
        "latest_config": df_config,
        "network_info": df_network
    }

def get_metadata(cluster_url, api_key):
    try:
        metadata = st.session_state.client.get_meta()

        # Process general metadata (excluding modules)
        general_metadata = {
            key: str(value) for key, value in metadata.items() if key != "modules"
        }
        general_metadata_df = pd.DataFrame(general_metadata.items(), columns=["Key", "Value"])

        # Process modules
        modules_data = metadata.get("modules", {})
        standard_modules = []  # For modules with standard structure (name + documentationHref)
        other_modules = []     # For modules with different structure

        for module_name, module_details in modules_data.items():
            if isinstance(module_details, dict):
                if "name" in module_details and "documentationHref" in module_details:
                    standard_modules.append({
                        "Module": str(module_name),
                        "Name": str(module_details.get("name", "N/A")),
                        "Documentation": str(module_details.get("documentationHref", "N/A"))
                    })
                else:
                    # Other module format
                    other_module = {"Module": str(module_name)}
                    other_module.update({k: str(v) if v is not None else "N/A" 
                                       for k, v in module_details.items()})
                    other_modules.append(other_module)

        standard_modules_df = pd.DataFrame(standard_modules) if standard_modules else pd.DataFrame()
        other_modules_df = pd.DataFrame(other_modules) if other_modules else pd.DataFrame()

        return {
            "general_metadata_df": general_metadata_df,
            "standard_modules_df": standard_modules_df,
            "other_modules_df": other_modules_df
        }

    except Exception as e:
        return {"error": f"Failed to fetch cluster metadata: {e}"}

# Trigger read repairs for a collection to force consistency
def read_repairs(cluster_url, api_key, collection_name):
    base_url = cluster_url
    class_name = collection_name
    bearer_token = api_key

    headers = {
        "Authorization": f"Bearer {bearer_token}"
    }

    # Step 1: Fetch all UUIDs for a class
    limit = 500
    offset = 0
    all_uuids = []

    print(f"=== Fetching all objects for class '{class_name}' ===")
    while True:
        params_list = {
            "limit": limit,
            "offset": offset,
            "class": class_name 
        }
        resp = requests.get(f"{base_url}/v1/objects", params=params_list, headers=headers)

        if resp.status_code != 200:
            print(f"Error listing objects for '{class_name}': {resp.status_code} {resp.text}")
            break

        data = resp.json()
        objects_batch = data.get("objects", [])
        if not objects_batch:
            break

        for i, obj in enumerate(objects_batch, start=offset):
            uuid = obj.get("id")
            all_uuids.append(uuid)
            print(f"Found object #{i}: {uuid}")

        offset += limit

    print(f"\nFetched {len(all_uuids)} total objects in class '{class_name}'.\n")

    # Step 2: Fetch each UUID with consistency_level=ALL
    print(f"=== Checking objects for class '{class_name}' ===")
    for index, uuid in enumerate(all_uuids):
        url = f"{base_url}/v1/objects/{class_name}/{uuid}"
        params_single = {
            "consistency_level": "ALL"
        }
        resp_single = requests.get(url, params=params_single, headers=headers)

        if resp_single.status_code == 200:
            obj_data = resp_single.json()
            name_val = obj_data.get("properties", {}).get("name")
            print(f"[{index}] UUID={uuid} => name={name_val}")
        elif resp_single.status_code == 404:
            print(f"[{index}] UUID={uuid} => Not found.")
        else:
            print(f"[{index}] UUID={uuid} => Error {resp_single.status_code}: {resp_single.text}")
