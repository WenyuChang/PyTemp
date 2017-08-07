versions = """twinstrata06e5e4ae-60ba-11e6-952a-001e67b8b287-18u\$1 e414e672-60ba-11e6-b048-001e67b8b287-g00g2e9gtwrfv
twinstrata06e5e4ae-60ba-11e6-952a-001e67b8b287-18u\$1 e513ad06-60ba-11e6-b2ec-001e67b8b287-95hc3irj6gvs9
twinstrata06e5e4ae-60ba-11e6-952a-001e67b8b287-18u\$1 e76d3356-60ba-11e6-952a-001e67b8b287-30d6uu7wb6rzk
twinstrata06e5e4ae-60ba-11e6-952a-001e67b8b287-18u\$1 e92117b2-60ba-11e6-b048-001e67b8b287-f0j1d1z3l3nrp"""
versions = versions.strip()
versions = [version for version in versions.split('\n')]

for version in versions:
    count = 0
    bucket, key = version.split(' ')
    print 'nodetool getendpoints aosmd_us_central_1 object_versions %s:%s' % (bucket, key)
    print 'nodetool getsstables aosmd_us_central_1 object_versions %s:%s' % (bucket, key)
    print