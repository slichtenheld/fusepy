#!/usr/bin/env python
from __future__ import print_function, absolute_import, division

import logging

from collections import defaultdict
from sys import argv, exit
from time import time


if not hasattr(__builtins__, 'bytes'):
    bytes = str

import xmlrpclib, pickle
from xmlrpclib import Binary
import sys

import pprint


class Wrapper:
    def __init__(self, metaport, dataport):
        print("initialized wrapper")
        self.metaproxy = xmlrpclib.ServerProxy("http://localhost:"+str(metaport))
        self.dataproxy = xmlrpclib.ServerProxy("http://localhost:"+str(dataport))
        print("supported dataproxy methods: {}".format(self.dataproxy.system.listMethods()))

    def corrupt(self, path):
        try:
            print(path)
            inode = pickle.loads(self.metaproxy.get((Binary(path))).data)["inode"]
            print(inode)
            d = self.dataproxy.corrupt_block(Binary(str(inode)))
            print("result {}".format(d))
            return True
        except:
            return False


if __name__ == '__main__':
    print(len(argv))

    if len(argv) < 4:
        print('usage: %s <metaserver port#> <dataserver port#> <path>' % argv[0])
        sys.exit(1)

    else:
        meta_port = argv[1]
        data_port = argv[2]
        path = argv[3]

    print("metaserver port: " + meta_port)
    print("dataserver port: " + str(data_port))
    w = Wrapper(meta_port, data_port)
    if w.corrupt(path):
        print("corruption successful")
    else:
        print("corruption unsuccessful")