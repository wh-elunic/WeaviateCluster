import pandas as pd
from weaviate.classes.query import Sort

def list_all_collections(client):
	"""
	Retrieves a list of all collection names.
	"""
	try:
		collections = client.collections.list_all()
		return collections
	except Exception as e:
		print(f"Error retrieving collections: {e}")
		return []

def get_tenant_names(client, collection_name):
	"""
	Retrieves tenant names for a given collection if multi-tenancy is enabled.
	Returns a list of tenant names or an empty list if not enabled.
	"""
	try:
		collection = client.collections.get(collection_name)
		tenants = collection.tenants.get()
		return [tenant.name for tenant in tenants.values()] if tenants else []
	except Exception as e:
		if "multi-tenancy is not enabled" in str(e).lower():
			return []
		else:
			print(f"Error retrieving tenants: {e}")
			return []

def fetch_collection_data(client, collection_name, tenant_name=None, page=1, items_per_page=1000):
	"""
	Fetches data from a collection with pagination.
	If tenant_name is provided, fetches data for that tenant.
	"""
	try:
		collection = client.collections.get(collection_name)
		if tenant_name:
			collection = collection.with_tenant(tenant_name)

		# Get total count first
		total_count = collection.aggregate.over_all(total_count=True).total_count

		collection_data = []
		
		# Calculate how many items to skip
		items_to_skip = (page - 1) * items_per_page
		
		#fetch_objects
		query_result = collection.query.fetch_objects(
			limit=items_per_page,
			offset=items_to_skip,
			return_metadata=["creation_time", "last_update_time"],
			include_vector=True,
			sort=Sort.by_property("_id", ascending=True)  # Add proper sort
		)
		
		# Access the objects property of the query result
		for item in query_result.objects:
			row = item.properties.copy()
			row['uuid'] = item.uuid
			row['vector'] = item.vector
			row['creation_time'] = item.metadata.creation_time
			row['last_update_time'] = item.metadata.last_update_time
			if tenant_name:
				row['tenant'] = tenant_name
			collection_data.append(row)

		if collection_data:
			df = pd.DataFrame(collection_data)
			df['collection'] = f"{collection_name} (Tenant: {tenant_name})" if tenant_name else collection_name
			return {
				"data": df,
				"total_count": total_count,
				"total_pages": -(-total_count // items_per_page),
				"current_page": page,
				"items_per_page": items_per_page
			}
		else:
			print(f"No data found (or Tenant is inactive) in collection '{collection_name}'{' for tenant ' + tenant_name if tenant_name else ''}.")
			return {
				"data": pd.DataFrame(),
				"total_count": 0,
				"total_pages": 0,
				"current_page": page,
				"items_per_page": items_per_page
			}
	except Exception as e:
		print(f"Error fetching data from collection '{collection_name}'{' for tenant ' + tenant_name if tenant_name else ''}: {e}")
		return {
			"data": pd.DataFrame(),
			"total_count": 0,
			"total_pages": 0,
			"current_page": page,
			"items_per_page": items_per_page
		}
