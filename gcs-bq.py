# This file is provided as an example only and is not sutible for use in a production environment
# This script should be used as a GCP Cloud Function and triggered when a data file is uploaded to a GCS
# buicket. The associated GCS bucket should contian at least two subdirectories, one for the Dataflow
# job source files (defined as the JOBSOURCEDIR variable) and one for the temporary processing files
# (defined as the TEMPDIR variable).

# Import the needed Python modules
from googleapiclient.discovery import build
from google.cloud import bigquery
import json
# The oauth2client is not currently included in the base Cloud Function python image. Include this module in your requirements.txt file
from oauth2client.client import GoogleCredentials


# Define a function that will be called by the Cloud Function
def CreateDataflowJob(event, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    file = event
    # Print the name of the uploaded file that triggered the Cloud Function. The Output will be sent to Stackdriver by default
    print(f"Processing file: {file['name']}.")

    # Properly format the name of the uploaded file, removing any quotation marks left by the json output
    rawname = json.dumps(file['name'])
    rawbucket = json.dumps(file['bucket'])
    filename = rawname.strip('"')
    bucketname = rawbucket.strip('"')


    # Verify the file uploaded is a valid data file (in this case using the .csv extension).
    # If the file is valid, proceed with execution. If not, exit the script.
    if filename.endswith('.csv'):

        # ---- Define Variables used to import the data file into BigQuery ----

        # Initiate the BigQuery client
        client = bigquery.Client()
        # Set the destination BigQuery dataset
        BQ_DATASET = 'test'
        # Set the destination BigQuery table
        BQ_TABLE = 'testtbl'
        # Define the URI of the uploaded files
        URI = 'gs://'+bucketname+'/'+filename

        dataset_ref = client.dataset(BQ_DATASET)
        job_config = bigquery.LoadJobConfig()
        job_config.autodetect = True
        job_config.skip_leading_rows = 1
        # The source format defaults to CSV, so the line below is optional.
        job_config.source_format = bigquery.SourceFormat.CSV

        load_job = client.load_table_from_uri(
        URI,
        dataset_ref.table(BQ_TABLE),
        job_config=job_config)  # API request
        print('Starting job {}'.format(load_job.job_id))

        load_job.result()  # Waits for table load to complete.
        print('Job finished.')

        destination_table = client.get_table(dataset_ref.table('us_states'))
        print('Loaded {} rows.'.format(destination_table.num_rows))


    # Exit the script if the uploaded file is not a valid data file
    else:
        print(filename+' is not a csv file. Exiting')
        exit()
