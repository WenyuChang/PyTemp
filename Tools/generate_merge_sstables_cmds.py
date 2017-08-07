sstables = """

/var/lib/cassandra/data/aosmd_us_geo_crtn01/bucket_contents_new-861c0bf083f511e6b04f1bcbf28d010c/aosmd_us_geo_crtn01-bucket_contents_new-ka-21303-Data.db
/var/lib/cassandra/data/aosmd_us_geo_crtn01/bucket_contents_new-861c0bf083f511e6b04f1bcbf28d010c/aosmd_us_geo_crtn01-bucket_contents_new-ka-21480-Data.db
/var/lib/cassandra/data/aosmd_us_geo_crtn01/bucket_contents_new-861c0bf083f511e6b04f1bcbf28d010c/aosmd_us_geo_crtn01-bucket_contents_new-ka-21452-Data.db
/var/lib/cassandra/data/aosmd_us_geo_crtn01/bucket_contents_new-861c0bf083f511e6b04f1bcbf28d010c/aosmd_us_geo_crtn01-bucket_contents_new-ka-20577-Data.db
/var/lib/cassandra/data/aosmd_us_geo_crtn01/bucket_contents_new-861c0bf083f511e6b04f1bcbf28d010c/aosmd_us_geo_crtn01-bucket_contents_new-ka-21076-Data.db
/var/lib/cassandra/data/aosmd_us_geo_crtn01/bucket_contents_new-861c0bf083f511e6b04f1bcbf28d010c/aosmd_us_geo_crtn01-bucket_contents_new-ka-21567-Data.db
/var/lib/cassandra/data/aosmd_us_geo_crtn01/bucket_contents_new-861c0bf083f511e6b04f1bcbf28d010c/aosmd_us_geo_crtn01-bucket_contents_new-ka-21162-Data.db
/var/lib/cassandra/data/aosmd_us_geo_crtn01/bucket_contents_new-861c0bf083f511e6b04f1bcbf28d010c/aosmd_us_geo_crtn01-bucket_contents_new-ka-20869-Data.db
/var/lib/cassandra/data/aosmd_us_geo_crtn01/bucket_contents_new-861c0bf083f511e6b04f1bcbf28d010c/aosmd_us_geo_crtn01-bucket_contents_new-ka-21398-Data.db

"""

sstables = sstables.strip().split('\n')
sstables = [sstable for sstable in sstables]
final_sstables = []
ks = ''
table = ''
for sstable in sstables:
    sstable = sstable.split('/')[-1]
    final_sstables.append(sstable)
    ks = sstable.split('-')[0]
    table = sstable.split('-')[1]
sstables = ','.join(final_sstables)


print 'curl -s http://localhost:8778/jolokia/write/org.apache.cassandra.db:columnfamily=%s,keyspace=%s,type=ColumnFamilies/CompactionStrategyClass/org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy' % (table, ks)
print
print
print 'run -b org.apache.cassandra.db:type=CompactionManager forceUserDefinedCompaction %s' % sstables
print
print
print 'curl -s http://localhost:8778/jolokia/write/org.apache.cassandra.db:columnfamily=%s,keyspace=%s,type=ColumnFamilies/CompactionStrategyClass/org.apache.cassandra.db.compaction.LeveledCompactionStrategy' % (table, ks)
