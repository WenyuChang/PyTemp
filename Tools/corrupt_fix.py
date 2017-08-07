import sys
import os
import traceback
import datetime
import copy
from cassandra import ReadTimeout, ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import dict_factory, SimpleStatement

seeds = ['cass-a01-s01.ecs2.chcg01.ecsops.com',
         'cass-a01-s02.ecs2.chcg01.ecsops.com',
         'cass-a01-s03.ecs2.chcg01.ecsops.com',]

#keyspace = 'aosmd_us_perf_1'
keyspace = 'aosmd_us_geo_crtn01'


class object_md(object):
    def __init__(self, owner, storage_class, last_modified, size, etag):
        self.owner = owner
        self.storage_class = storage_class
        self.last_modified = last_modified
        self.size = size
        self.etag = etag


class location(object):
    def __init__(self, storage, cloudlet, shard, bucket, key):
        self.storage = storage
        self.cloudlet = cloudlet
        self.shard = shard
        self.bucket = bucket
        self.key = key


class object_md_extra(object):
    def __init__(self, date, content_encoding, content_type, cache_control, expires, sse_c_algorithm, sse_c_key_salted_hmac, sse_c_iv):
        self.date = date
        self.content_encoding = content_encoding
        self.content_type = content_type
        self.cache_control = cache_control
        self.expires = expires
        self.sse_c_algorithm = sse_c_algorithm
        self.sse_c_key_salted_hmac = sse_c_key_salted_hmac
        self.sse_c_iv = sse_c_iv


def _check_and_fix(session, bucket, key):
    stat = 'select version from %s.objects where bucket = %s and key = %s;' % (keyspace, '%s' , '%s')
    stat = SimpleStatement(stat, consistency_level=ConsistencyLevel.ALL)
    rs = session.execute(stat, (bucket, key))
    version = ''
    for r in rs:
        if r['version'] is None or len(r['version']) == 0:
            continue
        else:
            version = r['version']
            break

    if version is None or len(version) == 0:
        try:
            print '[FIX] %s:%s is already deleted...going to delete' % (bucket, key)
            stat = 'delete from %s.object_versions where bucket = %s and key = %s;' % (keyspace, '%s', '%s')
            stat = SimpleStatement(stat, consistency_level=ConsistencyLevel.ALL)
            session.execute(stat, (bucket, key))
        except Exception, ex:
            print '[FIX] Failed to delete %s:%s. Error msg i %s' % (bucket, key, str(ex))
        return

    # # Check write time of latest version
    # try:
    #     stat = 'select writetime(is_delete_marker) as writetime from %s.object_versions where bucket = %s and key = %s and version = %s;'  % (keyspace, '%s' , '%s', '%s')
    #     stat = SimpleStatement(stat, consistency_level=ConsistencyLevel.ALL)
    #     rs = session.execute(stat, (bucket, key, version))
    #     writetime = rs[0]['writetime'] if rs is not None else None
    #     if writetime is not None:
    #         writetime = datetime.datetime.fromtimestamp(writetime / 1e3)
    #         print '%s:%s:%s %s' % (bucket, key, version, writetime)
    # except Exception, ex:
    #     if type(ex) is ReadTimeout:
    #         print '[FIX] %s:%s:%s is queried timeout...' % (bucket, key, version)
    #     else:
    #         print '[FIX] Please manually check this %s:%s:%s. Error msg is %s' % (bucket, key, version, str(ex))
    #     return

    latest_rec = None
    try:
        stat = 'select * from %s.object_versions where bucket = %s and key = %s and version = %s;'  % (keyspace, '%s' , '%s', '%s')
        stat = SimpleStatement(stat, consistency_level=ConsistencyLevel.ALL)
        rs = session.execute(stat, (bucket, key, version))
        latest_rec = rs[0] if rs is not None else None
    except Exception, ex:
        if type(ex) is ReadTimeout:
            print '[FIX] %s:%s:%s is queried timeout...' % (bucket, key, version)
        else:
            print '[FIX] Please manually check this %s:%s:%s. Error msg is %s' % (bucket, key, version, str(ex))
        return

    if latest_rec is None:
        print '[FIX] Please manually check this %s:%s:%s. There seems no such record'  % (bucket, key, version)
        return

    try:
        print '[FIX] Starting to delete: \n %s' % latest_rec
        stat = 'delete from %s.object_versions where bucket = %s and key = %s;' % (keyspace, '%s' , '%s')
        stat = SimpleStatement(stat, consistency_level=ConsistencyLevel.ALL)
        session.execute(stat, (bucket, key))
    except Exception, ex:
        print '[FIX] Failed to delete %s:%s. Error msg i %s' % (bucket, key, str(ex))

    try:
        cols = []
        cols_aux = []
        values = []
        for k, value in latest_rec.items():
            cols.append(k)
            cols_aux.append('%s')
            values.append(value)

        cols = ','.join(cols)
        cols_aux = ','.join(cols_aux)

        stat = 'insert into %s.object_versions (%s) values (%s)' % (keyspace, cols, cols_aux)
        stat = SimpleStatement(stat, consistency_level=ConsistencyLevel.EACH_QUORUM)
        session.execute(stat, values)
    except Exception, ex:
        print '[FIX] Failed to re-insert %s:%s. Error msg %s' % (bucket, key, str(ex))
        traceback.print_stack()
    else:
        print '[FIX] %s:%s is already fixed...' % (bucket, key)


def _execute(input_file):
    if not os.path.exists(input_file):
        print '%s not exists...' % input_file
        return

    keys = []
    with open(input_file, 'rb') as f:
        contents = f.read()
        keys_str = contents.split('\n')
        keys.extend([key.split(' ') for key in keys_str])

    if len(keys) == 0:
        print 'No keys to execute...'
        return

    cluster = Cluster(seeds)
    session = cluster.connect()
    session.row_factory = dict_factory
    session.default_fetch_size = 100
    session.default_timeout = 120

    cluster.register_user_type(keyspace, 'object_md', object_md)
    cluster.register_user_type(keyspace, 'location', location)
    cluster.register_user_type(keyspace, 'object_md_extra', object_md_extra)

    keys_bk = keys[0:-1]
    for key in keys:
        if len(key) <= 1:
            continue
        _check_and_fix(session, key[0], key[1])

    print '$' * 30
    for key in keys_bk:
        if len(key) <= 1:
            continue
        stat = "select version from %s.object_versions where bucket = '%s' and key = '%s';" % (keyspace, key[0], key[1])
        print stat
    print '$' * 30


if __name__ == '__main__':
    file_name = sys.argv[1]
    _execute(file_name)
