#!/usr/bin/env python3


import BQhelper as bq

print("loaded")

bq.project = "mlab-sandbox"
bq.dataset = 'mattmathis'
bq.UnitTestRunQuery()
bq.UnitTestWriteQuery()
