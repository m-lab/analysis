# Tools for Validating MLab data

See: [MLab BQ Inspection and View Test Plan](https://docs.google.com/document/d/1SkrYGzhpLijALxHzArNUlbCtMJVoWdAoyHRFsSKYvGk)

Required setup (lifted from from Peter's
 ["Pilot Exit Blog Post"](https://www.measurementlab.net/blog/global-pilot-success/) )


Setup and invocation:
```
# Authenticate your account.
gcloud auth login

# Set application default credentials.
gcloud auth application-default login

# Set default project.
gcloud config set project measurement-lab

# Build and launch the container with Jupyter and other tools
./doDockerPyplot [build]
# The build is only needed the first time and when things change
# cut&paste the URL or cookie into a browser

```
