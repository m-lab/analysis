#!/bin/bash
# List all objects in measurement-lab

project=measurement-lab

datasets=`bq ls --project_id $project | sed -e '0,/-----/d'`

echo $datasets

for ds in $datasets; do
  echo "======= $ds"
  bq show --project_id $project $ds
done
