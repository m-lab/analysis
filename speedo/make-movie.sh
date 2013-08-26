#!/bin/bash

echo 'Making movie' $1
echo 'Cleaning up old images'
mkdir -p tmp
rm tmp/*
echo 'Converting, annotating, and morphing'
convert $1*.bmp -delay 30 -morph 25 \
    -fill white -undercolor '#00000080' -gravity North \
    -pointsize 48 -annotate +0+5 '%t' \
    tmp/%05d.map.bmp
echo 'Making the movie'
avconv -y -i tmp/%05d.map.bmp -c:v libx264 -preset slow -crf 24 $1_map.mp4
