import datetime
from cassandra import ReadTimeout, ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import dict_factory, SimpleStatement
from cassandra.policies import WhiteListRoundRobinPolicy, TokenAwarePolicy, DCAwareRoundRobinPolicy



versions = [
    "twinstrata06e5e4ae-60ba-11e6-952a-001e67b8b287-18u$1 e414e672-60ba-11e6-b048-001e67b8b287-g00g2e9gtwrfv",
    "twinstrata06e5e4ae-60ba-11e6-952a-001e67b8b287-18u$1 e513ad06-60ba-11e6-b2ec-001e67b8b287-95hc3irj6gvs9",
    "twinstrata06e5e4ae-60ba-11e6-952a-001e67b8b287-18u$1 e76d3356-60ba-11e6-952a-001e67b8b287-30d6uu7wb6rzk",
    "twinstrata06e5e4ae-60ba-11e6-952a-001e67b8b287-18u$1 e92117b2-60ba-11e6-b048-001e67b8b287-f0j1d1z3l3nrp",
]


seeds = [
    'cass-a01-s01.ecs2.chcg01.ecsops.com',
    'cass-a01-s02.ecs2.chcg01.ecsops.com',
    'cass-a01-s03.ecs2.chcg01.ecsops.com',
    'cass-a01-s04.ecs2.chcg01.ecsops.com',
    'cass-a01-s06.ecs2.chcg01.ecsops.com',
    'cass-a01-s07.ecs2.chcg01.ecsops.com',
    'cass-a01-s08.ecs2.chcg01.ecsops.com',
    'cass-a01-s10.ecs2.chcg01.ecsops.com',
    'cass-a01-s11.ecs2.chcg01.ecsops.com',
    'cass-a01-s12.ecs2.chcg01.ecsops.com',
    'cass-a01-s13.ecs2.chcg01.ecsops.com',
    'cass-a01-s14.ecs2.chcg01.ecsops.com',
    'cass-a01-s15.ecs2.chcg01.ecsops.com',
    'cass-a01-s16.ecs2.chcg01.ecsops.com',
]

import datetime
from cassandra import ReadTimeout, ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import dict_factory, SimpleStatement

cluster = Cluster(
    seeds,
    load_balancing_policy=TokenAwarePolicy(DCAwareRoundRobinPolicy()),
    # control_connection_timeout=180,
    # connect_timeout=180
)
session = cluster.connect()
session.row_factory = dict_factory
# session.default_fetch_size = 10
# session.default_timeout = 180


count = 0
for version in versions:
    try:
        count = 0
        bucket, key = version.split(' ')
        # print "SELECT key FROM aosmd_us_central_1.object_versions WHERE bucket = '%s' AND key = '%s'" % (bucket, key)
        query = SimpleStatement(
            "SELECT version FROM aosmd_us_central_1.object_versions WHERE bucket = '%s' AND key = '%s'" % (bucket, key),
            consistency_level=ConsistencyLevel.ALL)
        ret = session.execute(query)
        for r in ret:
            # print count, r['version']
            # query1 = SimpleStatement(
            #     "SELECT * FROM aosmd_us_central_1.object_versions WHERE bucket = '%s' AND key = '%s' AND version = '%s'" % (bucket, key, r['version']),
            #     consistency_level=ConsistencyLevel.ALL)
            # ret1 = session.execute(query1)
            count += 1
    except Exception, ex:
        print '%s:%s (%s) Failing...%s' % (bucket, key, count, str(ex))
    finally:
        print '%s:%s (%s)' % (bucket, key, count)