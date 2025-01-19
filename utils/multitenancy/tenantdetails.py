# Get tenant States
def get_tenant_details(client, collection):
    col = client.collections.get(collection)
    tenants = col.tenants.get()
    return tenants

def aggregate_tenant_states(tenants):
    tenant_states = {}
    for tenant_id, tenant in tenants.items():
        state = tenant.activityStatusInternal.name
        if state not in tenant_states:
            tenant_states[state] = 0
        tenant_states[state] += 1
    return tenant_states

# Get multi-tenancy collections only
def get_multitenancy_collections(schema):
    enabled_collections = []
    for collection in schema.get('classes', []):
        collection_name = collection.get('class', 'Unknown Class')
        multi_tenancy_config = collection.get('multiTenancyConfig', None)
        # If multiTenancyConfig exists and 'enabled' is True, add the collection to the list
        if multi_tenancy_config and multi_tenancy_config.get('enabled', False):
            enabled_collections.append({
                'collection_name': collection_name,
                'multiTenancyConfig': multi_tenancy_config
            })

    return enabled_collections
