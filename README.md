## Google Fit Health Data Processing with Dataflow and Visualization

### Overview

In this lab, I'm going to learn how to:

- Build a batch Extract-Transform-Load pipeline in Apache Beam, which takes raw data from Google Fit health data from Google Cloud Storage and writes it to Google BigQuery.
- Run the Apache Beam Pipeline on Cloud Dataflow.

### Advantages of Dataflow

Cloud dataflow is a Google Cloud service that provides stream and batch data processing at scale. Use Dataflow to create data pipelines that read from one or more sources, transform the data, and write the data to a destination.

- Fully-managed: when you run a Dataflowjob, the Dataflow allocates a pool of worker VMs to execute the pipeline. Auto-scaling.
- Portable: It's written in Java, Python and Go. You can run the code on Apache Flink or Apache Spark without rewriting the code.

The data I'm going to use for this lab is not big data.
I'm going to use dataflow and bigQuery for practice and integrate health big data to the system in the future.

### Setup and requirements

1. In the Google Cloud console, on the Navigation menu (Navigation menu icon), select IAM & Admin > IAM.

2. At the top of the roles table, below View by Principals, click Grant Access.
   For New principals, type: {project-number}-compute@developer.gserviceaccount.com
   Replace {project-number} with your project number.
   For Role, select Project (or Basic) > Editor. Click Save.
   [IAM](./images/health-lab-IAM.png)

3. Set up ADC as described in https://cloud.google.com/docs/authentication/provide-credentials-adc?hl=ja#how-to

```
gcloud auth application-default login
```

### Save Json data to Cloud Storage

1. Log into your Google Account. Go to the Google Takeout page. Select Fit to export data.
   [Takeout](./images/health-lab-takeout.png)

2. Save [Downloads/Takeout/Fit/All Data/raw_com.google.body.temperature_com.google.and.json] data to a bucket in Cloud Storage.
   [Export files](./images/health-lab-export.png)
   [Cloud Storage](./images/health-lab-gcs.png)

### Create Dataset in BigQuery

1. Create a health dataset with multi-region US.

### Write and ETL pipeline from scratch

Before we begin editing the actual pipeline code, ensure that you have installed the necessary dependencies and set up a virtual environment.
We use a virtual environment to get advnatages:

- easier to manage packages: you can use different version of packages depending on a project
- reducing risk of package conflicts and errors
- easier to create and delete an environment

In the terminal, create a virtual environment

```
sudo apt-get update && sudo apt-get install -y python3-venv
python3 -m venv df-env
source df-env/bin/activate
```

Next, install the packages we will need to execute your pipeline:

```
python3 -m pip install -q --upgrade pip setuptools wheel
python3 -m pip install apache-beam[gcp]
```

Finally, ensure that the Dataflow API is enabled:

```
gcloud services enable dataflow.googleapis.com
```

### Run a ETL pipeline to verify that it works

1. Create temperature.py in the current folder. Following packages are imported.

```
import argparse
import time
import logging
import json
import apache_beam as beam
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.runners import DataflowRunner, DirectRunner
```

2. Read command line arguments

```
    # Command line arguments
    parser = argparse.ArgumentParser(description='Load from Json into BigQuery')
    parser.add_argument('--project',required=True, help='Specify Google Cloud project')
    parser.add_argument('--region', required=True, help='Specify Google Cloud region')
    parser.add_argument('--runner', required=True, help='Specify Apache Beam Runner')

    opts = parser.parse_args()

    # Setting up the Beam pipeline options
    options = PipelineOptions()
    options.view_as(GoogleCloudOptions).project = opts.project
    options.view_as(GoogleCloudOptions).region = opts.region
    options.view_as(GoogleCloudOptions).staging_location = opts.stagingLocation
    options.view_as(GoogleCloudOptions).temp_location = opts.tempLocation
    options.view_as(GoogleCloudOptions).job_name = '{0}{1}'.format('body-temperature-',time.time_ns())
    options.view_as(StandardOptions).runner = opts.runner
```

3. Read a file in Cloud Storage as input and a table in BigQuery as output

```
    # Static input and output
    input = 'gs://{0}/derived_com.google.body.temperature_com.google.json'.format(opts.project)
    output = '{0}:health.body_temperature'.format(opts.project)

```

4. Define table schema for BigQuery

```

    # Table schema for BigQuery
    table_schema = {
        "fields": [
            {
                "name": "startDatetime",
                "type": "STRING"
            },
            {
                "name": "temperature",
                "type": "FLOAT"
            },
            {
                "name": "insertDatetime",
                "type": "STRING"
            },
        ]
    }

```

### Write to a sink

There are 4P in Apache Beam data pipeline.

- PCollection: immutable dataset. It's the input and output of each step of the pipeline.
- PTransform: operations to transform data
- Pipeline: define one job, what to do, how to transform, and how to write.
- Pipeline Runner: backend to process this job.

The code below creates an initial input as PCollection. Read a Fit data from Google Clous Storage. "ParseJson" transforms the data and return an output PCollection. WriteToBQ get the PCollection to save it to BigQuery.

```
    (p
        | 'Start' >> beam.Create([None])
        | 'ParseJson' >> beam.ParDo(ParseJsonFile())
        | 'WriteToBQ' >> beam.io.WriteToBigQuery(
            output,
            schema=table_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
    )
```

We also define ParsejsonFile class to transform data

```
class ParseJsonFile(beam.DoFn):

    def process(self, element):
        from google.cloud import storage
        from datetime import datetime
        from collections import defaultdict

        bucket_name = 'health-lab'
        file_name = 'raw_com.google.body.temperature_com.google.and.json'

        # initialize a client
        storage_client = storage.Client()

        # get a bucket
        bucket = storage_client.bucket(bucket_name)

        # get the blob file from the bucket
        blob = bucket.blob(file_name)

        # download its content as a string
        blob = blob.download_as_string().decode('utf-8')

        # parse a json string and convert it into a dictionary
        content_dict = json.loads(blob)

        for row in content_dict["Data Points"]:

            fitValue = row['fitValue']
            if fitValue[0]['value']['fpVal']:
                startTimeNanos = datetime.fromtimestamp(row['startTimeNanos'] / 1e9)
                startDatetime = startTimeNanos.strftime('%Y-%m-%dT%H:%M:%S')
                temperature = fitValue[0]['value']['fpVal']
                insertDatetime = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')

                insert_dict = defaultdict(dict)
                insert_dict['startDatetime'] = startDatetime
                insert_dict['temperature'] = temperature
                insert_dict['insertDatetime'] = insertDatetime

                # merge all key:value pair as dictionary
                insert_dict = {
                    'startDatetime': startDatetime,
                    'temperature': temperature,
                    'insertDatetime': insertDatetime
                }

                yield insert_dict
```

Final code is in temperature.py

### Run a pipeline

Return to the terminal, execute the command to the pipeline locally to test the code

```
python3 temperature.py   --project=${PROJECT_ID}   --region=us-central1  --runner=DataflowRunner
```

Dataset should be ready in bigQuery.
[Dataset](./images/health-lab-dataset.png)

Run the pipeline using Google Cloud Dataflow. Change DirectRunner to DataflowRunner

```
python3 temperature.py   --project=${PROJECT_ID}   --region=us-central1  --runner=DataflowRunner
```
