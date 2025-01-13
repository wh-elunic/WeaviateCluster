import requests
import pandas as pd
from collections import defaultdict

# Get shards information
def get_shards_info(client):
	node_info = client.cluster.nodes(output="verbose")
	return node_info

def process_shards_data(node_info):
	node_data = []
	shard_data = []

	for node in node_info:
		# Node-level data
		node_data.append({
			"Node Name": node.name,
			"Git Hash": node.git_hash,
			"Version": node.version,
			"Status": node.status,
			"Object Count (Stats)": node.stats.object_count,
			"Shard Count (Stats)": node.stats.shard_count,
		})

		# Shard-level data for each node
		for shard in node.shards:
			shard_data.append({
				"Node Name": node.name,
				"Class": shard.collection,
				"Shard Name": shard.name,
				"Object Count": shard.object_count,
				"Index Status": shard.vector_indexing_status,
				"Vector Queue Length": shard.vector_queue_length,
				"Compressed": shard.compressed,
				"Loaded": shard.loaded,
			})

	return {
		"node_data": pd.DataFrame(node_data),
		"shard_data": pd.DataFrame(shard_data),
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
	synchronized = stats.get("synchronized", False)

	for node in stats["statistics"]:
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

	return {"data": flattened_data, "synchronized": synchronized}

def get_metadata(cluster_url, api_key):
	try:
		url = f"{cluster_url}/v1/meta"
		headers = {"Authorization": f"Bearer {api_key}"}
		response = requests.get(url, headers=headers)
		response.raise_for_status()

		metadata = response.json()

		# General metadata (excluding 'modules')
		general_metadata = {
			key: value for key, value in metadata.items() if key != "modules"
		}
		general_metadata_df = pd.DataFrame(general_metadata.items(), columns=["Key", "Value"])

		# Extract module details
		modules_data = metadata.get("modules", {})
		module_list = []
		nested_module_data = {}

		for module_name, module_details in modules_data.items():
			# Basic module info
			module_info = {
				"Module Name": module_name,
				"Description": module_details.get("name", "N/A"),
				"Documentation URL": module_details.get("documentationHref", "N/A"),
			}
			module_list.append(module_info)

			# Nested details (if present) for a specific module
			nested_data = {
				key: value
				for key, value in module_details.items()
				if key not in ["name", "documentationHref"]
			}
			if nested_data:
				nested_module_data[module_name] = pd.DataFrame(
					nested_data.items(), columns=["Key", "Value"]
				)

		modules_df = pd.DataFrame(module_list) if module_list else pd.DataFrame()

		return {
			"general_metadata_df": general_metadata_df,
			"modules_df": modules_df,
			"nested_module_data": nested_module_data,
		}

	except requests.exceptions.RequestException as e:
		return {"error": f"Failed to fetch cluster metadata: {e}"}
