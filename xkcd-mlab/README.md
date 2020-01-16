# xkcd-mlab

Make a cool animated gif of MLab's Internet-wide NDT performance data for
2009-2019.

Currently, as long as your bigquery client is set up correctly to use the
measurement-lab project, you have the imagemagick libraries installed, and you
can compile go code, then all you should have to do is type
```sh
make
```
and you should get a great animated gif named `years.gif`, as well as higher res
images named `2009.png`, `2010.png`, ..., `2019.png`!
