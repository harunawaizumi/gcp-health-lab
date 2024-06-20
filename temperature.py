import argparse
import time
import logging
import json
import apache_beam as beam
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.runners import DataflowRunner, DirectRunner


class ParseJsonFile(beam.DoFn):
    def process(self, element):
        from google.cloud import storage
        from datetime import datetime
        from collections import defaultdict

        bucket_name = "health-lab"
        file_name = "raw_com.google.body.temperature_com.google.and.json"

        # initialize a client
        storage_client = storage.Client()

        # get a bucket
        bucket = storage_client.bucket(bucket_name)

        # get the blob file from the bucket
        blob = bucket.blob(file_name)

        # download its content as a string
        blob = blob.download_as_string().decode("utf-8")

        # parse a json string and convert it into a dictionary
        content_dict = json.loads(blob)

        for row in content_dict["Data Points"]:
            fitValue = row["fitValue"]
            if fitValue[0]["value"]["fpVal"]:
                startTimeNanos = datetime.fromtimestamp(row["startTimeNanos"] / 1e9)
                startDatetime = startTimeNanos.strftime("%Y-%m-%dT%H:%M:%S")
                temperature = fitValue[0]["value"]["fpVal"]
                insertDatetime = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

                insert_dict = defaultdict(dict)
                insert_dict["startDatetime"] = startDatetime
                insert_dict["temperature"] = temperature
                insert_dict["insertDatetime"] = insertDatetime

                # merge all key:value pair as dictionary
                insert_dict = {
                    "startDatetime": startDatetime,
                    "temperature": temperature,
                    "insertDatetime": insertDatetime,
                }

                yield insert_dict


def run():
    # Command line arguments
    parser = argparse.ArgumentParser(description="Load from Json into BigQuery")
    parser.add_argument("--project", required=True, help="Specify Google Cloud project")
    parser.add_argument("--region", required=True, help="Specify Google Cloud region")
    parser.add_argument("--runner", required=True, help="Specify Apache Beam Runner")

    opts = parser.parse_args()

    # Setting up the Beam pipeline options
    options = PipelineOptions()
    options.view_as(GoogleCloudOptions).project = opts.project
    options.view_as(GoogleCloudOptions).region = opts.region
    options.view_as(GoogleCloudOptions).job_name = "{0}{1}".format(
        "body-temperature-", time.time_ns()
    )
    options.view_as(StandardOptions).runner = opts.runner

    # Static input and output
    input = "gs://{0}/derived_com.google.body.temperature_com.google.json".format(
        opts.project
    )
    output = "{0}:health.body_temperature".format(opts.project)

    # Table schema for BigQuery
    table_schema = {
        "fields": [
            {"name": "startDatetime", "type": "STRING"},
            {"name": "temperature", "type": "FLOAT"},
            {"name": "insertDatetime", "type": "STRING"},
        ]
    }

    # Create the pipeline
    p = beam.Pipeline(options=options)

    """

    Steps:
    1) Create PCollection
    2) Transform Json data
    3) Write PCollection to health.body_temperature table in BigQuery

    """

    (
        p
        | "Start" >> beam.Create([None])
        | "ParseJson" >> beam.ParDo(ParseJsonFile())
        | "WriteToBQ"
        >> beam.io.WriteToBigQuery(
            output,
            schema=table_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        )
    )

    logging.getLogger().setLevel(logging.INFO)
    logging.info("Building pipeline ...")

    p.run()


if __name__ == "__main__":
    run()
