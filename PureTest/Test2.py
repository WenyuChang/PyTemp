nodes = [
"10.122.1.11",
"10.122.1.12",
"10.122.1.13",
"10.122.1.14",
"10.122.1.15",
"10.122.1.16",
"10.122.1.17",
"10.122.1.18",
"10.122.1.20",
"10.122.1.21",
"10.122.1.22",
"10.122.1.23",
"10.122.1.24",
"10.122.1.25",
"10.122.1.26",
"10.122.1.27"
]


nodes.reverse()


for node in nodes:
    print 'nodetool -h %s repair -pr -- aosmd_us_central_1 objects' % node
    print 'echo "finished repair %s"' % node
    print 'date'