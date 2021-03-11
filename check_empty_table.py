### Simple script to handle issues with delayed loads, leading to empty query results
# General flow of logic:
# 1) Check if newest partitioned table is empty or has records
# 2) If it has records, data was not delayed, end
# 3) If it does not have records, the data was delayed and re-trigger scheduled query
# hourly

# Import the BigQuery client library
from google.cloud import bigquery
import datetime

# Construct a BigQuery client object.
client = bigquery.Client()


# Helper function for checking if a BQ table has records in it
def table_has_records(dataset_name, table_name):
    """
    Accept input 'table_name' STRING, that provides resource ID of BigQuery table
    Returns output of boolean True or False, True meaning there are records in the
    table, False meaning there are no records.
    """
    client = bigquery.Client()

    query = """
    SELECT * 
    FROM {0}.{1}.__TABLES__ 
    LIMIT 10000
    """

    query_job = client.query(query.format(client.project, dataset_name)) # Submit the API request

    print("Query Job {} has been submitted, at {}.".format(query_job.job_id, query_job.created))

    # Access rows from the output of the query, API reference on data structure can be found here:
    # https://googleapis.dev/python/bigquery/latest/generated/google.cloud.bigquery.table.Row.html#google.cloud.bigquery.table.Row
    for row in query_job:
        if row.get('table_id') == table_name:
            number_records = row.get('row_count')
            print("There are currently {} records in the table: {}.".format(str(number_records), table_name))
            if number_records == 0:
                return False 
            else:
                return True

    # __TABLES__ metadata table provides a value of "row_count", check to see if value is 0

## STILL WORKING
def trigger_backfill_job():
    # Add logic to resolve delayed data use case using backfill
    pass

def schedule_backfill(override_values={}):
    """
    Accepts a set of override values as a dictionary. This dictionary should have a 
    key "transfer_config_name" which can override the default transfer config
    """

    # [START bigquerydatatransfer_schedule_backfill]
    import datetime

    from google.cloud import bigquery_datatransfer

    transfer_client = bigquery_datatransfer.DataTransferServiceClient()

    transfer_config_name = "projects/1234/locations/us/transferConfigs/abcd"
    # [END bigquerydatatransfer_schedule_backfill]
    # To facilitate testing, we replace values with alternatives
    # provided by the testing harness.
    transfer_config_name = override_values.get(
        "transfer_config_name", transfer_config_name
    )
    # [START bigquerydatatransfer_schedule_backfill]
    now = datetime.datetime.now(datetime.timezone.utc)
    start_time = now - datetime.timedelta(days=5)
    end_time = now - datetime.timedelta(days=2)

    # Some data sources, such as scheduled_query only support daily run.
    # Truncate start_time and end_time to midnight time (00:00AM UTC).
    start_time = datetime.datetime(
        start_time.year, start_time.month, start_time.day, tzinfo=datetime.timezone.utc
    )
    end_time = datetime.datetime(
        end_time.year, end_time.month, end_time.day, tzinfo=datetime.timezone.utc
    )

    response = transfer_client.schedule_transfer_runs(
        parent=transfer_config_name,
        start_time=start_time,
        end_time=end_time,
    )

    print("Started transfer runs:")
    for run in response.runs:
        print(f"backfill: {run.run_time} run: {run.name}")
    # [END bigquerydatatransfer_schedule_backfill]
    return response.runs


def main():
    print("#########################################################")
    print("# RUNNING SCRIPT TO CHECK SUCCESSFUL RECORDS IN BIGQUERY")
    print("#########################################################")
    now = datetime.datetime.now()
    today = now.strftime('%Y%m%d') # Format as YYYYMMDD, for example: 20210218
    yesterday = now - datetime.timedelta(days=1)
    yesterday = yesterday.strftime('%Y%m%d') # Can use yesterday as partition for testing
    # TOADD BY CUSTOMER... Adjust parameters ("dataset_id", "partitioned_table_id")
    if table_has_records("bigquery_logs", "cloudaudit_googleapis_com_activity_{}".format(today)) == True:
        print("There are records in the table, we are all good here!")
    else:
        print("There are are no records in the table, the data is delayed.")
        # Add trigger_backfill_job() function call when completed

if __name__ == "__main__":
    main()