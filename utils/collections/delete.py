def delete_collections(client, collection_names):
    """
    Delete one or multiple collections.
    Args:
        client: Weaviate client
        collection_names: List of collection names or single collection name
    Returns:
        tuple: (success: bool, message: str)
    """
    try:
        client.collections.delete(collection_names)
        return True, f"Successfully deleted collections: {', '.join(collection_names if isinstance(collection_names, list) else [collection_names])}"
    except Exception as e:
        return False, f"Error deleting collections: {str(e)}"

def delete_tenants_from_collection(client, collection_name, tenant_names):
    """
    Delete specific tenants from a multi-tenant collection.
    Args:
        client: Weaviate client
        collection_name: Name of the collection
        tenant_names: List of tenant names to delete
    Returns:
        tuple: (success: bool, message: str)
    """
    try:
        collection = client.collections.get(collection_name)
        collection.tenants.remove(tenant_names)
        return True, f"Successfully deleted tenants: {', '.join(tenant_names)} from collection {collection_name}"
    except Exception as e:
        return False, f"Error deleting tenants from collection {collection_name}: {str(e)}"
