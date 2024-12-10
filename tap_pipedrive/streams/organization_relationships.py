import singer
from tap_pipedrive.stream import PipedriveBaseIterStream

class OrganizationRelationshipsStream(PipedriveBaseIterStream):
    base_endpoint = 'organizations'
    id_endpoint = 'organizationRelationships?org_id={}'
    schema = 'organization_relationships'
    state_field = 'update_time'
    key_properties = ['id']
    org_id = None

    def get_name(self):
        return self.schema
    
    def process_row(self, row):
        row["org_id"] = self.org_id
        row["update_time"] = row["update_time"] if row["update_time"] else row["add_time"]
        return super().process_row(row)

    def update_endpoint(self, org_id):
        self.endpoint = self.id_endpoint.format(org_id)
        self.org_id = org_id