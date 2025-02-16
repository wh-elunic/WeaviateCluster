import pandas as pd

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

def fetch_collection_data(client, collection_name, tenant_name=None):
	"""
	Fetches all data from a collection.
	If tenant_name is provided, fetches data for that tenant.
	"""
	try:
		collection = client.collections.get(collection_name)
		if tenant_name:
			collection = collection.with_tenant(tenant_name)

		collection_data = []
		for item in collection.iterator(include_vector=True):
			row = item.properties.copy()
			row['uuid'] = item.uuid
			row['vector'] = item.vector
			if tenant_name:
				row['tenant'] = tenant_name
			collection_data.append(row)

		if collection_data:
			df = pd.DataFrame(collection_data)
			df['collection'] = f"{collection_name} (Tenant: {tenant_name})" if tenant_name else collection_name
			return df
		else:
			print(f"No data found (or Tenant is inactive) in collection '{collection_name}'{' for tenant ' + tenant_name if tenant_name else ''}.")
			return pd.DataFrame()
	except Exception as e:
		print(f"Error fetching data from collection '{collection_name}'{' for tenant ' + tenant_name if tenant_name else ''}: {e}")
		return pd.DataFrame()
