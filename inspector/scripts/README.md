# Scripts for validating MLab Tables and Views


Setup and invocation:
```
# Fetch the cover script
wget https://raw.githubusercontent.com/m-lab/analysis/master/inspector/scripts/dockerPython
chmod +x dockerPython

# Authenticate your account.
gcloud auth login

# Set application default credentials.
gcloud auth application-default login

# Set default project.
gcloud config set project measurement-lab

# Inspect a table or view to be sure that it meets basic correctness criteria:
dockerPython inspect \<project.dataset.name\>

# Inspect a dataset of tables and/or views
dockerPython inspect \<project.dataset\>

The options --minimal and --extended can be added for faster or slower queries
```
Notes:

    - If a test fails because the inferred column mappings are incorrect, please drop me a note.
      (mattmathis)

(Docker setup was borrowed from the
 ["Pilot Exit Blog Post"](https://www.measurementlab.net/blog/global-pilot-success/) )
