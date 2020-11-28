# Tools for exploring MLab data in Jupyter/PyPlot

Setup and invocation:
```
# Authenticate your account.
gcloud auth login

# Set application default credentials.
gcloud auth application-default login

# Set default project.
gcloud config set project measurement-lab

# Launch dockerizd Jupyter  (The main usecase)
# NB: this does sudo inside and will ask for your password
doDockerPyplot [build]
# copy and paste the URL or cookie into a browser

# Launch a shell in the dockerizd environment intended for Jupyter
# (For debugging only)
doDockerPyplot [build] shell
```

The optional build is only needed the first time and when python or gcloud librarys change.
If the build is needed, it must be invoked in the source directory.
If the build in not needed, you only need to have doDockerPyplot in your path.

(Docker setup was borrowed from the
 ["Pilot Exit Blog Post"](https://www.measurementlab.net/blog/global-pilot-success/) )
