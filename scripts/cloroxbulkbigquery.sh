#!/bin/bash


PROJECT=dataflowtesting-218212
DATASET=dataflowtesting-218212:examples.manualpartition
MAIN=com.google.cloud.training.dataanalyst.javahelp.CloroxBulkBigQuery
BUCKET=dataflow-results-ini

echo "project=$PROJECT dataset=$DATASET bucket=$BUCKET  main=$MAIN"

export PATH=/usr/lib/jvm/java-8-openjdk-amd64/bin/:$PATH
mvn compile -e exec:java \
 -Dexec.mainClass=$MAIN \
      -Dexec.args="--project=$PROJECT \
      --stagingLocation=gs://$BUCKET/staging/ \
      --tempLocation=gs://$BUCKET/staging/ \
      --qps=101000 \
      --dataset=$DATASET \
      --runner=DataflowRunner \
      --autoscalingAlgorithm=THROUGHPUT_BASED \
      --maxNumWorkers=5"
