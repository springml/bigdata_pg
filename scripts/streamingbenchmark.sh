#!/bin/bash


PROJECT_ID=dataflowtesting-218212
BUCKET=dataflow-results-ini
PIPELINE_FOLDER=gs://dataflow-staging-ini/dataflow/pipelines/streaming-benchmark

RUNNER=DataflowRunner
 
mvn compile exec:java \
 -Dexec.mainClass=com.google.cloud.pso.pipeline.StreamingBenchmark \
	-Dexec.cleanupDaemonThreads=false \
	-Dexec.args="--project=${PROJECT_ID} \
        --stagingLocation=${PIPELINE_FOLDER}/staging \
        --tempLocation=${PIPELINE_FOLDER}/temp \
        --runner=${RUNNER} \
        --zone=us-east1-d \
        --autoscalingAlgorithm=THROUGHPUT_BASED \
        --maxNumWorkers=5 \
        --qps=50000 \
        --schemaLocation=gs://dataflow-results-ini/sample-data/game-event-schema.json \
        --topic=projects/dataflowtesting-218212/topics/streamdemo"

