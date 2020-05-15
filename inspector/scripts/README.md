# Scripts for validating MLab Tables and Views


Setup and invocation:
```
# Authenticate your account.
gcloud auth login

# Set application default credentials.
gcloud auth application-default login

# Set default project.
gcloud config set project measurement-lab

# Inspect a table or view to be sure that it meets basic correctness criteria:
dockerPython [build] ./inspectTable.py \<project.dataset.name\>

# Inspect a dataset (of tables and/or views)
dockerPython  [build] ./inspectTable.py \<project.dataset\>
```
If a test fails because the inferred column mappings are incorrect,
drop me a note.  mattmathis

The optional build is only needed the first time and when things change.
If the build is needed, it has to be invoked in the source directory.


(Docker setup was borrowed from the
 ["Pilot Exit Blog Post"](https://www.measurementlab.net/blog/global-pilot-success/) )
