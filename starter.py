### Scratch space to learn the BQ Python Client Library

# Import the BigQuery client library
from google.cloud import bigquery

# Construct a BigQuery client object.
client = bigquery.Client()


query = """
    SELECT name, SUM(number) as total_people
    FROM `bigquery-public-data.usa_names.usa_1910_2013`
    WHERE state = 'TX'
    GROUP BY name, state
    ORDER BY total_people DESC
    LIMIT 20
"""

# Uncomment below to test a query
"""
query_job = client.query(query)  # Make an API request.

print("The query data:")
for row in query_job:
    # Row values can be accessed by field name or index.
    print("name={}, count={}".format(row[0], row["total_people"]))
"""

### Creating a dataset

dataset_id = "{}.client_library_created_dataset".format(client.project)
# Use the bigquery.Dataset() method to create a full Dataset object
new_dataset = bigquery.Dataset(dataset_id)

# Dataset resource documentation found here:
# https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets#Dataset
# Set the location of the dataset to be continental: US
new_dataset.location = "US"
new_dataset.description = "This dataset was created by the Python client library"

dataset_object = client.create_dataset(new_dataset, exists_ok=True, timeout=30)  # Make API request to create
print("Wooohoooo! We've created dataset: {}.{}.".format(client.project, dataset_object.dataset_id))


### Cool, dataset has been created, now let's create a table.

table_id = "{}.{}.first_table".format(client.project, dataset_object.dataset_id)

# Supply a schema
schema = [
    bigquery.SchemaField("full_name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("age", "INTEGER", mode="REQUIRED")
]

# Initialize a bigquery.Table object, passing in full table ID, and schema
new_table = bigquery.Table(table_id, schema=schema)
new_table.requirePartitionFilter = True
new_table.timePartitioning = {"type": "DAY"}
table = client.create_table(new_table, exists_ok=True)  # Make API request to create
print("Wooohoooo! We've created table: {}.{}.{}".format(table.project, table.dataset_id, table.table_id))