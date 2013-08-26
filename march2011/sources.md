#Data sources and Validation Information

##Sources from the FCC

###March 2011
- [March 2011 (Validated)](http://data.fcc.gov/download/measuring-broadband-america/validated-march-data-2011.tar.gz) (2.41 GB download, 11.5 GB expanded)
- [February 2011 (Raw)](http://data.fcc.gov/download/measuring-broadband-america/raw-bulk-data-feb-2011.tar.gz) (1.92 GB download, 5.79 GB expanded
- [March 2011 (Raw)](http://data.fcc.gov/download/measuring-broadband-america/raw-bulk-data-mar-2011.tar.gz) (3.94 GB download, 20.2 GB expanded)
- [April 2011 (Raw)](http://data.fcc.gov/download/measuring-broadband-america/raw-bulk-data-apr-2011.tar.gz) (3.96 GB download, 20.3 GB expanded)
- [May 2011 (Raw)](http://data.fcc.gov/download/measuring-broadband-america/raw-bulk-data-may-2011.tar.gz) (4.24 GB download, 21.5 GB expanded)
- [June 2011 (Raw)](http://data.fcc.gov/download/measuring-broadband-america/raw-bulk-data-jun-2011.tar.gz) (3.98 GB download, 20.2 GB expanded)

###April 2012
- [April 2012 (Validated)](https://s3.amazonaws.com/fcc-april-data/fcc_data_tables_april_2012_csv.tar.gz)

##Sources in BigQuery
Raw and validated data for the 2011 SamKnows tests are available in the M-lab project in Google BigQuery. The data is stored in a set of tables in the `fcc_samknows_data` database. There is raw data available for February, March, April and May 2011. This is available in tables named like `yyyy_mm_<tablename>`. There is validated data available for March 2011. This is available in tables named like `2011_03_validated_<tablename>`.
