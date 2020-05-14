# Tools for exploring MLab data in Jupyter/PyPlot

Setup and invocation:
```
# Authenticate your account.
gcloud auth login

# Set application default credentials.
gcloud auth application-default login

# Set default project.
gcloud config set project measurement-lab

# Launch dockerizd Jupyter
doDockerPyplot [build]
# copy and paste the URL or cookie into a browser

# Launch a shell in the dockerizd environment intended for Jupyter
doDockerPyplot [build] shell
```

The optional build is only needed the first time and when things change.
If the build is needed, it has to be invoked in the source directory.

(Docker setup was borrowed from from Peter's
 ["Pilot Exit Blog Post"](https://www.measurementlab.net/blog/global-pilot-success/) )
