# Docker container for running python BQ inspection tools in a standardized environment

from jupyter/datascience-notebook
# See: https://github.com/jupyter/docker-stacks/blob/master/datascience-notebook/Dockerfile
# This includes Julia and other things that we don't need

run pip install --upgrade google-cloud-bigquery google-cloud-storage zstandard

workdir /home/inspector
COPY tmp /home/inspector

ENTRYPOINT ["/bin/bash", "-c"]
