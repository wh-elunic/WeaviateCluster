import streamlit as st
from utils.objects.objects import get_object, display_object_as_table, find_objects_on_nodes
from utils.connection.navigation import navigate

def main():

    navigate()

    if not st.session_state.get("client_ready"):
        st.warning("Please connect first to Weaviate in Cluster Operations Page!")
        st.stop()

    st.title("Objects")

    collection_name = st.text_input("Collection Name")
    object_uuid = st.text_input("Object UUID")

    # Use session state to store the results and clear when needed
    if "button_result" not in st.session_state:
        st.session_state.button_result = None

    col1, col2 = st.columns(2)
    with col1:
        fetch_object_clicked = st.button("Fetch The Object",use_container_width=True)
    with col2:
        check_node_clicked = st.button("Check The Object on Nodes",use_container_width=True)

    # "Fetch Object"
    if fetch_object_clicked:
        if not collection_name.strip() or not object_uuid.strip():
            st.error("Please insert both Collection Name and UUID.")
            return

        client = st.session_state.client

        try:
            # Fetch and display object
            data_object = get_object(client, collection_name, object_uuid)
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
            cluster_endpoint = st.session_state.cluster_url
            node_df = find_objects_on_nodes(cluster_endpoint, api_key, collection_name, object_uuid)
            st.session_state.button_result = st.dataframe(node_df, use_container_width=True)
            st.text("✔ Found | ✖ Not Found | N/A The node does not exist")
        except Exception as e:
            st.session_state.button_result = st.error(f"An error occurred while checking the object on nodes: {e}")
    
# Required so Streamlit runs `main()` when this file is opened as a page
if __name__ == "__main__":
    main()