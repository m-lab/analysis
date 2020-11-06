while read old new ; do
      newtable=`basename $new .sql`
      oldtable=`basename $old .sql`
cat << zzEOFzz > $old
-- View library.$oldtable has been moved
--      to intermediate_ndt.$newtable
--
-- TODO: remove this stub
--
SELECT * FROM \`{{.ProjectID}}.intermediate_ndt.$newtable\` WHERE FALSE
zzEOFzz
done << EOF
ndt_unified_ndt5_downloads.sql extended_ndt5_downloads.sql 
ndt_unified_ndt5_uploads.sql extended_ndt5_uploads.sql
ndt_unified_ndt7_downloads.sql extended_ndt7_downloads.sql
ndt_unified_ndt7_uploads.sql extended_ndt7_uploads.sql
ndt_unified_web100_downloads.sql extended_web100_downloads.sql
ndt_unified_web100_uploads.sql extended_web100_uploads.sql
EOF
