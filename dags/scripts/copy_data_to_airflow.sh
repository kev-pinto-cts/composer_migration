#!/usr/bin/env bash
# Attach Data to Container
gsutil -m cp -r '{{ var.value.source_location }}/*'   gs://${GCS_BUCKET}/data/{{ params.root_staging_folder }}/encrypted/
env