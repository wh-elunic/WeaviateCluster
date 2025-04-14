import pandas as pd
import requests

# Get object in Non Multitenant collection
def get_object_in_collection(client, collection_name, uuid):
	collection = client.collections.get(collection_name)
	data_object = collection.query.fetch_object_by_id(uuid, include_vector=True)

	if data_object is None:
		print(f"Object with UUID '{uuid}' not found.")
		return None

	return data_object

# Get object in Multitenant collection
def get_object_in_tenant(client, collection_name, uuid, tenant):
	collection = client.collections.get(collection_name).with_tenant(tenant)
	data_object = collection.query.fetch_object_by_id(uuid, include_vector=True)

	if data_object is None:
		print(f"Object with UUID '{uuid}' not found.")
		return None

	return data_object

def display_object_as_table(data_object):
	if data_object is None:
		print("No data to display.")
		return

	metadata_fields = {
		"Creation Time": data_object.metadata.creation_time,
		"Last Update Time": data_object.metadata.last_update_time,
	}

	additional_data = {
		"UUID": str(data_object.uuid),
		"Collection": data_object.collection,
		"Vectors": data_object.vector
	}

	additional_data.update(metadata_fields)

	if data_object.properties:
		for key, value in data_object.properties.items():
			additional_data[key] = value

	df = pd.DataFrame([additional_data])

	return df

def find_object_in_collection_on_nodes(client_endpoint, api_key, collection_name, object_uuid):

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

def find_object_in_tenant_on_nodes(client_endpoint, api_key, collection_name, object_uuid, tenant):

	node_names = [
		"weaviate-0", "weaviate-1", "weaviate-2", "weaviate-3",
		"weaviate-4", "weaviate-5", "weaviate-6", "weaviate-7",
		"weaviate-8", "weaviate-9", "weaviate-10", "weaviate-11"
	]

	headers = {"Authorization": f"Bearer {api_key}"}
	results = {}

	for node in node_names:
		url = f"{client_endpoint}/v1/objects/{collection_name}/{object_uuid}"
		params_single = {"node_name": node, "tenant": tenant}

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
