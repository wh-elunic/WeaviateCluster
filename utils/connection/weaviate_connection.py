import weaviate
import atexit
from weaviate.config import AdditionalConfig, Timeout
from urllib.parse import urlparse
import requests
import time

# Module-level variable to hold the singleton client
_client = None

def check_weaviate_server(url):
	"""Check if Weaviate is up and running by making a simple request to the root endpoint."""
	try:
		# Strip any trailing slash
		url = url.rstrip('/')
		
		# Try different endpoints that might work
		endpoints = [
			f"{url}/v1",
			f"{url}/.well-known/ready",
			f"{url}/v1/meta"
		]
		
		for endpoint in endpoints:
			try:
				print(f"Checking endpoint: {endpoint}")
				response = requests.get(endpoint, timeout=5)
				if response.status_code < 500:  # Accept any non-server error response
					print(f"Weaviate is reachable at {endpoint} with status code {response.status_code}")
					return True
			except Exception as e:
				print(f"Failed to connect to {endpoint}: {e}")
				continue
				
		return False
	except Exception as e:
		print(f"Server check failed: {e}")
		return False

def get_weaviate_client(cluster_endpoint=None, cluster_api_key=None, use_local=False):
	print(f"Connecting to Weaviate at {cluster_endpoint}...")
	global _client
	if _client is None:
		# Check if the server is reachable first
		if not check_weaviate_server(cluster_endpoint):
			raise Exception(f"Weaviate server at {cluster_endpoint} is not reachable. Please make sure it's running.")
			
		# Add retry logic
		retry_count = 0
		max_retries = 3
		last_error = None
		
		while retry_count < max_retries:
			try:
				# Use the appropriate connection function based on use_local flag
				auth = weaviate.auth.AuthApiKey(cluster_api_key) if cluster_api_key else None
				
				if use_local:
					# Parse the URL for local connection
					url_parts = urlparse(cluster_endpoint)
					host = url_parts.netloc.split(":")[0] or "localhost"
					port = url_parts.port or 8080
					
					print(f"Connecting to local Weaviate at {host}:{port}")
					_client = weaviate.connect_to_local(
						host=host,
						port=port,
						auth_credentials=auth,
						skip_init_checks=True,
						additional_config=AdditionalConfig(
							timeout=Timeout(init=90, query=900, insert=900)
						)
					)
				else:
					print(f"Connecting to WCS at {cluster_endpoint}")
					_client = weaviate.connect_to_wcs(
						cluster_url=cluster_endpoint,
						auth_credentials=auth,
						skip_init_checks=True, 
						additional_config=AdditionalConfig(
							timeout=Timeout(init=90, query=900, insert=900)
						)
					)
				
				print(f"Connected to {cluster_endpoint} successfully")
				break
			except Exception as e:
				last_error = e
				retry_count += 1
				print(f"Connection attempt {retry_count} failed: {e}")
				if retry_count < max_retries:
					time.sleep(2)  # Wait before retrying
		
		if retry_count == max_retries:
			print(f"Failed to connect after {max_retries} attempts")
			raise last_error
			
		# Register a cleanup function to close the client when the process exits
		atexit.register(close_weaviate_client)
	return _client

def close_weaviate_client():
	print("Disconnecting from Weaviate...")
	global _client
	if _client:
		_client.close()
		_client = None
	return "Disconnected from Weaviate."

# Weaviate Server & Client status and version
def status(client):
	print("Getting Weaviate status...")
	try:
		ready = client.is_ready()
		print(f"Client ready status: {ready}")
		
		try:
			meta = client.get_meta()
			server_version = meta.get("version", "Unknown")
		except Exception as meta_error:
			print(f"Error getting meta: {meta_error}")
			# Try alternative methods to get version
			server_version = "N/A"
		
		client_version = weaviate.__version__
		return ready, server_version, client_version
	except Exception as e:
		print(f"Status check error: {e}")
		return False, "N/A", "N/A"
