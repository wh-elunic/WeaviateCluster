import pandas as pd
import requests

def get_object(client, collection_name, uuid):
	collection = client.collections.get(collection_name)
	data_object = collection.query.fetch_object_by_id(uuid)

	if data_object is None:
		print(f"Object with UUID '{uuid}' not found.")
		return None

	return data_object


def display_object_as_table(data_object):
	if data_object is None:
		print("No data to display.")
		return

	meta_fields = {
		"metadata.creation_time": data_object.metadata.creation_time,
		"metadata.last_update_time": data_object.metadata.last_update_time,
		"metadata.is_consistent": data_object.metadata.is_consistent
	}

	flattened_data = {
		"uuid": str(data_object.uuid),
		"collection": data_object.collection
	}

	flattened_data.update(meta_fields)

	if data_object.properties:
		for key, value in data_object.properties.items():
			flattened_data[key] = value

	df = pd.DataFrame([flattened_data])

	return df


def list_all_uuids(client, collection_name):
	"""Fetch all UUIDs from the collection."""
	collection = client.collections.get(collection_name)
	return [item.uuid for item in collection.iterator()]

def find_objects_on_nodes(client_endpoint, api_key, collection_name, object_uuid):

	node_names = [
		"weaviate-0", "weaviate-1", "weaviate-2", "weaviate-3",
		"weaviate-4", "weaviate-5", "weaviate-6", "weaviate-7",
		"weaviate-8", "weaviate-9", "weaviate-10", "weaviate-11"
	]

	headers = {"Authorization": f"Bearer {api_key}"}
	results = {}

	for node in node_names:
		url = f"{client_endpoint}/v1/objects/{collection_name}/{object_uuid}"
		params_single = {"node_name": node}

		resp_single = requests.get(url, params=params_single, headers=headers)

		if resp_single.status_code == 200:
			results[node] = "✔" # Found
		elif resp_single.status_code == 404:
			results[node] = "✖" # Not Found
		elif resp_single.status_code == 500:
			results[node] = "N/A" # Not applicable as the node does not exist
		else:
			results[node] = f"Error {resp_single.status_code}" # Error

	df = pd.DataFrame([results], index=[object_uuid])
	return df
