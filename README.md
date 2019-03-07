# GCP Automated Pipeline Example

This code can be used to automate the creation of a Dataflow pipeline on GCP by simply uploading a datafile into a GCS bucket.

In this example, the associated GCS bucket is expected to contian at least two subdirectories, one for the Dataflow job source files (defined as the JOBSOURCEDIR variable) and one for the temporary processing files (defined as the TEMPDIR variable). All datafiles can be loaded directly into the top level GCS bucket. The example layout of the GCS bucket should resemble the following:

GCS Bucket<br/>
|<br/>
|--source<br/>
...|--run.js<br/>
...|--schema.json<br/>
|--temp<br/>
...|--(empty)<br/>


You can change the layout of the folders and location of the datafiles to whatever you like, just make sure to update the associated variables of the scirpt and validate the correct path information under the "BODY = {" section.
