import streamlit as st
import pandas as pd
from utils.connection.navigation import navigate
from utils.sidebar.helper import update_side_bar_labels
from utils.multitenancy.tenantdetails import get_tenant_details, get_multitenancy_collections, aggregate_tenant_states
from utils.cluster.cluster import get_schema
    
def display_multitenancy(cluster_url, api_key):

    schema = get_schema(cluster_url, api_key)
    if 'error' in schema:
        st.error(schema['error'])
        return

    # Get collections with multi-tenancy enabled
    enabled_collections = get_multitenancy_collections(schema)

    if not enabled_collections:
        st.warning("No collections with enabled multi-tenancy found.")
        return
    
    # Load collections list if not already loaded in session_state
    if "enabled_collections" not in st.session_state:
        if enabled_collections and not isinstance(enabled_collections, dict):
            st.session_state["enabled_collections"] = enabled_collections
        else:
            st.session_state["enabled_collections"] = []

    # Show a selectbox for the user to choose a collection
    if st.session_state["enabled_collections"]:
        collection_count = len(enabled_collections)
        st.markdown(f"###### Total Number of Multi Tenancy Collections in the list: **{collection_count}**\n")
        collection_names = [collection['collection_name'] for collection in st.session_state["enabled_collections"]]
        selected_collection_name = st.selectbox(
            "Select a MT Collection",
            collection_names,
        )
        st.session_state["selected_collection_name"] = selected_collection_name
    else:
        st.warning("No collections available to display.")
        return
    
    if st.button("Get Multi Tenancy Configuration"):
        selected_collection = next((collection for collection in st.session_state["enabled_collections"] if collection['collection_name'] == selected_collection_name), None)
        if selected_collection:
            multi_tenancy_config = selected_collection['multiTenancyConfig']
            multi_tenancy_df = pd.DataFrame([multi_tenancy_config])
            st.dataframe(multi_tenancy_df, use_container_width=True)
        else:
            st.error("Failed to find the selected collection in the available collections.")

def tenant_details():
    if st.button("Get Tenant Details"):
        selected_collection_name = st.session_state.get("selected_collection_name")
        tenants = get_tenant_details(st.session_state.client, selected_collection_name)
        aggregated_states = aggregate_tenant_states(tenants)
        tenant_data = []
        for tenant_id, tenant in tenants.items():
            tenant_data.append({
                'Tenant ID': tenant_id,
                'Name': tenant.name,
                'Activity Status Internal': tenant.activityStatusInternal.name,
                'Activity Status': tenant.activityStatus.name
            })
        st.dataframe(pd.DataFrame(aggregated_states.items(), columns=['Activity Status', 'Count']), use_container_width=True)
        df = pd.DataFrame(tenant_data)
        st.dataframe(df, use_container_width=True)

def main():

    st.title("Multi Tenancy 🏢")

    navigate()

    if st.session_state.get("client_ready"):
        update_side_bar_labels()
        display_multitenancy(st.session_state.cluster_endpoint, st.session_state.cluster_api_key)
        tenant_details()
    else:
        st.warning("Please Establish a connection to Weaviate in Cluster Operations page!")

# Required so Streamlit runs `main()` when this file is opened as a page
if __name__ == "__main__":
    main()