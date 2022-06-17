#!/usr/bin/env bash
bq mk --project_id=$GCP_PROJECT --location=europe-west2  --force=true --dataset {{ params.dataset }}