#!/bin/bash

echo 'Making movie'
echo 'Converting'
mkdir -p tmp
rm tmp/*
convert out/*.bmp -delay 10 -fill white -undercolor '#00000080' -gravity East \
                  -pointsize 18 -annotate +0+5 '%t' tmp/%05d.bmp
echo 'Building'
avconv -y -i tmp/%05d.bmp -c:v libx264 -preset slow -crf 24 usage.mp4
