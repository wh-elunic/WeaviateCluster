import streamlit as st
from utils.objects.objects import get_object_in_collection, display_object_as_table, find_object_in_collection_on_nodes, get_object_in_tenant, find_object_in_tenant_on_nodes
from utils.sidebar.navigation import navigate
from utils.sidebar.helper import update_side_bar_labels

def get_object_details():
    collection_name = st.text_input("Collection Name")
    object_uuid = st.text_input("Object UUID")
    with_tenant = st.checkbox("Tenant", value=False)

    tenant_name = None
    if with_tenant:
        tenant_name = st.text_input("Tenant Name")

    col1, col2 = st.columns(2)
    with col1:
        fetch_object_clicked = st.button("Fetch The Object", use_container_width=True)
    with col2:
        check_node_clicked = st.button("Check the Object on the Nodes (APIs)", use_container_width=True)

    # "Fetch Object"
    if fetch_object_clicked:
        if not collection_name.strip() or not object_uuid.strip():
            st.error("Please insert both Collection Name and UUID.")
            return

        try:
            # Fetch and display object
            if with_tenant and tenant_name:
                data_object = get_object_in_tenant(st.session_state.client, collection_name, object_uuid, tenant_name)
            else:
                data_object = get_object_in_collection(st.session_state.client, collection_name, object_uuid)
            
            if data_object:
                display = display_object_as_table(data_object)
                st.session_state.button_result = st.dataframe(display)
            else:
                st.session_state.button_result = st.error(f"Object with UUID '{object_uuid}' not found.")
        except ValueError:
            st.session_state.button_result = st.error("Invalid UUID: Not a valid UUID or unable to extract it.")
        except Exception as e:
            st.session_state.button_result = st.error(f"An error occurred: {e}")

    # "Check Object on a Node"
    if check_node_clicked:
        if not collection_name.strip() or not object_uuid.strip():
            st.error("Please insert both Collection Name and UUID.")
            return

        try:
            # Fetch node data and display table
            api_key = st.session_state.cluster_api_key
            cluster_endpoint = st.session_state.cluster_endpoint
            if with_tenant and tenant_name:
                data_object = find_object_in_tenant_on_nodes(cluster_endpoint, api_key, collection_name, object_uuid, tenant_name)
            else:
                data_object = find_object_in_collection_on_nodes(cluster_endpoint, api_key, collection_name, object_uuid)
            node_df = data_object
            st.session_state.button_result = st.dataframe(node_df, use_container_width=True)
            st.text("✔ Found | ✖ Not Found | N/A The node does not exist (Hardcoded 11 nodes as maximum for now)")
        except Exception as e:
            st.session_state.button_result = st.error(f"An error occurred while checking the object on nodes: {e}")

def main():
    st.title("Object 📦")

    navigate()

    if st.session_state.get("client_ready"):
        update_side_bar_labels()
        get_object_details()
    else:
        st.warning("Please Establish a connection to Weaviate in Cluster page!")
    
# Required so Streamlit runs `main()` when this file is opened as a page
if __name__ == "__main__":
    main()