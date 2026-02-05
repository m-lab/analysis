#!/bin/bash

# Abort on errors
set -e

# Force sudo early
sudo true
if [ "x$1" = "xbuild" ]; then
    echo docker build
    sudo docker build -t mm-pcap-inspector .
    echo build complete
fi

# NB: background to protect from accidental signals
sudo docker run -p 127.0.0.1:9000:8080 \
     -v /tmp/.X11-unix:/tmp/.X11-unix \
     -v /run/user/1000/gdm/Xauthority:/root/.Xauthority \
     -v $HOME/.config/gcloud:/content/.config \
     -v $HOME/work/gcsCache:/content/gcsCache \
     -v $HOME/work/pcapCache:/content/pcapCache \
     -e DISPLAY=$DISPLAY \
     -h $HOSTNAME \
     mm-pcap-inspector
#     us-docker.pkg.dev/colab-images/public/runtime

exit 0

     -v $HOME/.config/gcloud/application_default_credentials.json:/.config/gcloud/application_default_credentials.json \
