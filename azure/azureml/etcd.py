import os
etcd_cmd = '''
/usr/local/bin/etcd --data-dir "/var/lib/etcd" --enable-v2 --listen-client-urls "http://0.0.0.0:2379" --advertise-client-urls "http://0.0.0.0:2379" --initial-cluster-state "new"
'''
os.system(etcd_cmd)