#!/bin/bash

# Ensure required environment variables are set
if [ -z "$BUCKET_PATH" ]; then
    echo "Error: BUCKET_PATH environment variable is not set"
    exit 1
fi

if [ -z "$AWS_ACCESS_KEY_ID" ] || [ -z "$AWS_SECRET_ACCESS_KEY" ]; then
    echo "Warning: AWS credentials not set in environment"
fi

# Run the Python script
python3 data_ingestion.py