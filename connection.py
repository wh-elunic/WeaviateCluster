import weaviate

# Connect to Weaviate Locally
def connect_weaviate_local():
	return weaviate.connect_to_local(
		skip_init_checks=True
	)

# Connect to Weaviate Cloud
def connect_to_weaviate(cluster_endpoint, api_key):
	cluster_endpoint = cluster_endpoint
	api_key = api_key

	client = weaviate.connect_to_wcs(
		cluster_url=cluster_endpoint,
		auth_credentials=weaviate.auth.AuthApiKey(api_key)
		)
	return client

# Ccheck status and print versions
def status(client):
	try:
		ready = client.is_ready()
		server_version = client.get_meta()["version"]
		client_version = weaviate.__version__
		return ready, server_version, client_version
	except Exception as e:
		print(f"Error: {e}")
		return False, "N/A", "N/A"
