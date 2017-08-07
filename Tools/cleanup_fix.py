key = """twinstrata06e5e4ae-60ba-11e6-952a-001e67b8b287-18u$1 e513ad06-60ba-11e6-b2ec-001e67b8b287-95hc3irj6gvs9"""

key = key.split("\n")
keys = [kk.split(' ') for kk in key]

import datetime
import json
import time
from cassandra.policies import WhiteListRoundRobinPolicy

seeds = [
    'cass-a01-s01.ecs2.chcg01.ecsops.com',
    'cass-a01-s02.ecs2.chcg01.ecsops.com',
    'cass-a01-s03.ecs2.chcg01.ecsops.com',
    'cass-a01-s04.ecs2.chcg01.ecsops.com',
    'cass-a01-s06.ecs2.chcg01.ecsops.com',
    'cass-a01-s07.ecs2.chcg01.ecsops.com',
    'cass-a01-s08.ecs2.chcg01.ecsops.com',
    'cass-a01-s09.ecs2.chcg01.ecsops.com',
    'cass-a01-s10.ecs2.chcg01.ecsops.com',
    'cass-a01-s11.ecs2.chcg01.ecsops.com',
    'cass-a01-s12.ecs2.chcg01.ecsops.com',
    'cass-a01-s13.ecs2.chcg01.ecsops.com',
    'cass-a01-s14.ecs2.chcg01.ecsops.com',
    'cass-a01-s15.ecs2.chcg01.ecsops.com',
    'cass-a01-s16.ecs2.chcg01.ecsops.com',
]

from cassandra import ReadTimeout, ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import dict_factory, SimpleStatement

cluster = Cluster(
    seeds,
    load_balancing_policy=WhiteListRoundRobinPolicy(seeds),
    control_connection_timeout=180,
    connect_timeout=180
)
session = cluster.connect()
session.row_factory = dict_factory
session.default_fetch_size = 100
session.default_timeout = 180

count = 0
for key in keys:
    #print key[0], key[1]
    stat = "select version from aosmd_us_central_1.object_versions where bucket = '%s' and key = '%s';" % (key[0], key[1])
    query = SimpleStatement(stat, consistency_level=ConsistencyLevel.LOCAL_QUORUM)
    res = session.execute(query)
    count = 0
    for r in res:
        count += 1
        try:
            version = r['version']
            stat1 = "select * from aosmd_us_central_1.object_versions where bucket = '%s' and key = '%s' and version = '%s';" % (key[0], key[1], version)
            print stat1
            query1 = SimpleStatement(stat, consistency_level=ConsistencyLevel.LOCAL_QUORUM)
            ret = session.execute(query1)
        except Exception, ex:
            print '%s failed' % version
        else:
            print '%s success' % version
    print count