export JAVA_HOME="${HOME}/Desktop/coding/DE_learning/DataEngineering/data_talks_camp/week5/jdk-11.0.1"
export PATH="${JAVA_HOME}/bin:${PATH}"

export SPARK_HOME="${HOME}/Desktop/coding/DE_learning/DataEngineering/data_talks_camp/week5/spark-3.3.3-bin-hadoop3"
export PATH="${SPARK_HOME}/bin:${PATH}"





{
  "reference": {
    "jobId": "job-dc3da140",
    "projectId": "arcane-grin-394118"
  },
  "placement": {
    "clusterName": "first-cluster"
  },
  "status": {
    "state": "DONE",
    "stateStartTime": "2023-10-04T13:22:58.464767Z"
  },
  "yarnApplications": [
    {
      "name": "test",
      "state": "FINISHED",
      "progress": 1,
      "trackingUrl": "http://first-cluster-m.c.arcane-grin-394118.internal.:8088/proxy/application_1696424314830_0001/"
    }
  ],
  "statusHistory": [
    {
      "state": "PENDING",
      "stateStartTime": "2023-10-04T13:21:53.853953Z"
    },
    {
      "state": "SETUP_DONE",
      "stateStartTime": "2023-10-04T13:21:53.908213Z"
    },
    {
      "state": "RUNNING",
      "details": "Agent reported job success",
      "stateStartTime": "2023-10-04T13:21:54.374230Z"
    }
  ],
  "driverControlFilesUri": "gs://dataproc-staging-europe-west6-253231033131-ecqldpnw/google-cloud-dataproc-metainfo/4179f587-73e0-4b8a-ada1-9c6276dd013d/jobs/job-dc3da140/",
  "driverOutputResourceUri": "gs://dataproc-staging-europe-west6-253231033131-ecqldpnw/google-cloud-dataproc-metainfo/4179f587-73e0-4b8a-ada1-9c6276dd013d/jobs/job-dc3da140/driveroutput",
  "jobUuid": "b4a1cf3e-d3c4-4cd9-b945-e716e4ad21a9",
  "done": true,
  "pysparkJob": {
    "mainPythonFileUri": "gs://dtc_data_lake_arcane-grin-394118/06_spark_sql.py",
    "args": [
      "--input_green=gs://dtc_data_lake_arcane-grin-394118/pq/green/2021/*/",
      "--input_yellow=gs://dtc_data_lake_arcane-grin-394118/pq/yellow/2021/*/",
      "--output=gs://dtc_data_lake_arcane-grin-394118/report-2021"
    ]
  }
}


gcloud dataproc jobs submit pyspark \
    --cluster=first-cluster \
    --region=europe-west6 \
    gs://dtc_data_lake_arcane-grin-394118/06_spark_sql.py \
    -- \
        --input_green=gs://dtc_data_lake_arcane-grin-394118/pq/green/2021/*/ \
        --input_yellow=gs://dtc_data_lake_arcane-grin-394118/pq/yellow/2021/*/ \
        --output=gs://dtc_data_lake_arcane-grin-394118/report-2021


trips_data_all.reports-2020

gcloud dataproc jobs submit pyspark \
    --cluster=first-cluster \
    --region=europe-west6 \
    --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar \
    gs://dtc_data_lake_arcane-grin-394118/cluster_to_bq.py \
    -- \
        --input_green=gs://dtc_data_lake_arcane-grin-394118/pq/green/2021/*/ \
        --input_yellow=gs://dtc_data_lake_arcane-grin-394118/pq/yellow/2021/*/ \
        --output=trips_data_all.reports-2020