import pandas as pd
import requests

def get_collectios_count(client):
	collections = client.collections.list_all()
	collection_count = len(collections)
	return collection_count

def aggregate_collections(client):
	try:
		collections = client.collections.list_all()
		total_tenants_count = 0
		result_data = []
		collection_count = 0

		if collections:
			collection_count = len(collections)

			for collection_name in collections:
				collection_row = {"Collection": collection_name, "Count": "", "Tenant": "", "Tenant Count": ""}
				result_data.append(collection_row)

				collection = client.collections.get(collection_name)
				try:
					# Attempt to get tenants for the collection (check if multi-tenancy is enabled)
					tenants = collection.tenants.get()

					if tenants: 
						tenant_count = len(tenants)
						total_tenants_count += tenant_count

						for tenant_name, tenant in tenants.items():
							tenant_collection = collection.with_tenant(tenant_name)
							response = tenant_collection.aggregate.over_all(total_count=True).total_count
							tenant_row = {"Collection": "", "Count": "", "Tenant": tenant_name, "Tenant Count": response}
							result_data.append(tenant_row)
					else:
						response = collection.aggregate.over_all(total_count=True).total_count
						collection_row["Count"] = response

				except Exception as e:
					if "multi-tenancy is not enabled" in str(e):
						response = collection.aggregate.over_all(total_count=True).total_count
						collection_row["Count"] = response

			result_df = pd.DataFrame(result_data)

			return {
				"collection_count": collection_count,
				"total_tenants_count": total_tenants_count,
				"result_df": result_df,
			}

		return {
			"collection_count": 0,
			"total_tenants_count": 0,
			"result_df": pd.DataFrame(),
		}

	except Exception as e:
		return {"error": str(e)}


def get_schema(client):
	try:
		schema = client.collections.list_all()
		return schema if schema else None
	except Exception as e:
		return {"error": f"Error retrieving schema: {str(e)}"}


def list_collections(client):
	try:
		collections = client.collections.list_all()
		return list(collections.keys()) if collections else []
	except Exception as e:
		return {"error": f"Error retrieving collections: {str(e)}"}


def fetch_collection_config(cluster_url, api_key, collection_name):
	headers = {"Authorization": f"Bearer {api_key}"}
	endpoint = f"{cluster_url}/v1/schema/"
	response = requests.get(endpoint, headers=headers)

	if response.status_code == 200:
		schema = response.json().get("classes", [])
		for cls in schema:
			if cls.get("class") == collection_name:
				return cls
	return {"error": f"Error fetching schema: {response.status_code} - {response.text}"}


def process_collection_config(config):
	if not config:
		return {"error": "No configuration available"}

	# Base keys to display for both scenarios (single vector and named vectors)
	keys_to_display = {
		"Inverted Index Config": config.get("invertedIndexConfig", {}),
		"Multi-Tenancy Config": config.get("multiTenancyConfig", {}),
		"Replication Config": config.get("replicationConfig", {}),
		"Sharding Config": config.get("shardingConfig", {}),
	}

	# Dynamically add all module configurations as separate sections
	module_configs = config.get("moduleConfig", {})
	for mod_name, mod_conf in module_configs.items():
		# Create a distinct section name for each module configuration
		section_name = f"{mod_name}"
		keys_to_display[section_name] = mod_conf

	# Handle single vector configuration scenario
	if "vectorIndexConfig" in config and "vectorizer" in config:
		keys_to_display["vectorIndexType"] = config.get("vectorIndexType", {})
		keys_to_display["Vector Index Config"] = config.get("vectorIndexConfig", {})

	# Handle named vector configurations scenario
	if "vectorConfig" in config:
		named_vectors_info = {}
		for vector_name, vector_details in config["vectorConfig"].items():
			# Gather details for each named vector dynamically
			info = {
				"Vector Index Type": vector_details.get("vectorIndexType"),
				"Vector Index Config": vector_details.get("vectorIndexConfig", {}),
				"Vectorizer": vector_details.get("vectorizer", {})
			}
			named_vectors_info[vector_name] = info
		keys_to_display["Named Vectors Config"] = named_vectors_info

	return keys_to_display
