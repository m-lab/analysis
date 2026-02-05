# Dockerfile to add tcptrace, xplot, etc to a local colab runtime
FROM us-docker.pkg.dev/colab-images/public/runtime

# Install some debugging tools
RUN apt-get install xterm

# tcptrace and xplot
RUN apt-get install xplot-xplot.org tcptrace
