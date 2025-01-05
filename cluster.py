import requests
import pandas as pd

# Get shards information
def get_shards_info(client):
    node_info = client.cluster.nodes(output="verbose")
    return node_info

def process_shards_data(node_info):
    data = []
    for node in node_info:
        for shard in node.shards:
            data.append({
                "Node Name": node.name,
                "Class": shard.collection,
                "Shard Name": shard.name,
                "Object Count": shard.object_count,
                "Index Status": shard.vector_indexing_status,
                "Loaded": shard.loaded,
            })
    return data

def display_shards_table(data):
    df = pd.DataFrame(data)
    return df

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