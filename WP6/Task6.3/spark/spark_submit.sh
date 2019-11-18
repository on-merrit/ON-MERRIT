#!/usr/bin/env bash

if [ "$#" -ne 1 ]; then
    echo "Usage: $0 job_name" >&2
    echo "Example: $0 word_count" >&2
    exit 1
fi

echo "Received the following arguments:"
echo "Job name: $1"

spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --queue project.core \
    --py-files config.json,jobs.zip \
    --name "$1" \
    --executor-memory 16g \
    --executor-cores 2 \
    --driver-memory 16g \
    --num-executors 64 \
    --conf spark.kryoserializer.buffer.max=1024m \
    --conf spark.rpc.message.maxSize=2047 \
    --conf spark.yarn.submit.waitAppCompletion=false \
    --conf spark.driver.maxResultSize=32g \
    main.py \
    --job $1
