import pandas as pd
import streamlit as st
import requests

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

    keys_to_display = {
        "Generative Config": config.get("moduleConfig", {}).get("generative-openai", {}),
        "Inverted Index Config": config.get("invertedIndexConfig", {}),
        "Multi-Tenancy Config": config.get("multiTenancyConfig", {}),
        "Replication Config": config.get("replicationConfig", {}),
        "Reranker Config": config.get("moduleConfig", {}).get("reranker-cohere", {}),
        "Sharding Config": config.get("shardingConfig", {}),
        "Vector Index Config": config.get("vectorIndexConfig", {}),
        "Vectorizer Config": config.get("moduleConfig", {}).get("text2vec-openai", {}),
    }
    return keys_to_display