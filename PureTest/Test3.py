import datetime
from cassandra import ReadTimeout, ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import dict_factory, SimpleStatement
from cassandra.policies import WhiteListRoundRobinPolicy, TokenAwarePolicy, DCAwareRoundRobinPolicy


seeds = [
    'cass-aos2-sntc-n101.ecs2.sntc01.ecsops.com',
    'cass-aos2-sntc-n102.ecs2.sntc01.ecsops.com',
    'cass-aos2-sntc-n103.ecs2.sntc01.ecsops.com',
]

import datetime
from cassandra import ReadTimeout, ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import dict_factory, SimpleStatement

cluster = Cluster(
    seeds,
    load_balancing_policy=TokenAwarePolicy(DCAwareRoundRobinPolicy()),
    control_connection_timeout=180,
    connect_timeout=180
)
session = cluster.connect()
session.row_factory = dict_factory
session.default_fetch_size = 10
session.default_timeout = 180


try:
    count = 0
    # print "SELECT key FROM aosmd_us_central_1.object_versions WHERE bucket = '%s' AND key = '%s'" % (bucket, key)
    query = SimpleStatement(
        "SELECT key, md, is_delete_marker FROM aosmd_us_geo_sntc01.bucket_contents_new WHERE bucket = 'f1013cc63b24c9cc-bf5b977907cd075e-d0$1' AND key_hash = 2 AND key > '' AND key <= '/cset/hdr/ffffffff/0000000000480000'ORDER BY key ASC",
        consistency_level=ConsistencyLevel.LOCAL_QUORUM)
    ret = session.execute(query)
    for r in ret:
        count += 1
        print r['key']
except Exception, ex:
    print str(ex)
