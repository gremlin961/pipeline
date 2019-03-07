# This file is provided as an example only and is not sutible for use in a production environment
# This script should be used as a GCP Cloud Function and triggered when a data file is uploaded to a GCS
# buicket. The associated GCS bucket should contian at least two subdirectories, one for the Dataflow
# job source files (defined as the JOBSOURCEDIR variable) and one for the temporary processing files
# (defined as the TEMPDIR variable).

# Import the needed Python modules
from googleapiclient.discovery import build
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
    filename = rawname.strip('"')

    # Verify the file uploaded is a valid data file (in this case using the .csv extension).
    # If the file is valid, proceed with execution. If not, exit the script.
    if filename.endswith('.csv'):

        # ---- Define Variables used to create the Dataflow job ----


        # The name of your GCP Project.
        PROJECT = '<your_project_name>'
        # The name of the GCS bucket where the files have been uploaded to
        BUCKET = '<your_GCS_bucket_name>'
        # The destination tabled in BigQuery. The format is GCP_Project_ID:dataset:table
        BQ_OUTPUT_TABLE = '<Project_ID:BigQuery_Dataset_Name:BigQuery_Table_Name>'
        # The folder containing the SCHEMA_FILE and RUN_FILE files (assumes the folder resides in the same GCS bucket)
        JOBSOURCEDIR = 'source'
        # The folder to store temporary Dataflow job files (assumes the folder resides in the same GCS bucket)
        TEMPDIR = 'temp'


        # Authenticate to other Google services using the associated Cloud Function service account
        credentials = GoogleCredentials.get_application_default()
        # Define the "service" variable to create a new Dataflow job
        service = build('dataflow', 'v1b3', credentials=credentials)
    	# Define the 'JOBNAME' variable as the name of the uploaded file.
        JOBNAME = 'run-'+filename.lower()
        # Define the Dataflow template to use. More templates can be found at https://cloud.google.com/dataflow/docs/guides/templates/provided-templates
        TEMPLATE = 'GCS_Text_to_BigQuery'
        # The full path to the Dataflow template
        GCSPATH = 'gs://dataflow-templates/latest/{template}'.format(template=TEMPLATE)
        # The file used to define the Schema mapping between the data file and the BigQuery table
        SCHEMA_FILE = 'schema.json'
        # The file used to define the execution variables of your Dataflow job
        RUN_FILE = 'run.js'
        # The GCP zone to create the Dataflow job in
        ZONE = 'us-central1-f'


        # Parameters passed to the RESTful API's to create the Dataflow job using the preceeding variables
        BODY = {
            "jobName": "{jobname}".format(jobname=JOBNAME),
            "parameters": {
                "javascriptTextTransformFunctionName": "transform",
                "JSONPath": "gs://"+BUCKET+"/"+JOBSOURCEDIR+"/"+SCHEMA_FILE,
                "javascriptTextTransformGcsPath": "gs://"+BUCKET+"/"+JOBSOURCEDIR+"/"+RUN_FILE,
                "inputFilePattern":"gs://"+BUCKET+"/"+filename,
                "outputTable":BQ_OUTPUT_TABLE,
                "bigQueryLoadingTemporaryDirectory": "gs://"+BUCKET+"/"+TEMPDIR
                },
            "environment": {
                "zone": ZONE
            }
        }

        # Define the request and submit to the Dataflow RESTful API
        request = service.projects().templates().launch(projectId=PROJECT, gcsPath=GCSPATH, body=BODY)
        response = request.execute()

        # Catoure the response from the Dataflow API and log to Stackdriver
        print(response)

    # Exit the script if the uploaded file is not a valid data file
    else:
        print(filename+' is not a csv file. Exiting')
        exit()
