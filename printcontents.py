#!/usr/bin/env python
from __future__ import print_function, absolute_import, division

import logging

from collections import defaultdict
from errno import ENOENT
from stat import S_IFDIR, S_IFLNK, S_IFREG
from sys import argv, exit
from time import time

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn

if not hasattr(__builtins__, 'bytes'):
    bytes = str

import metaserver, dataserver, xmlrpclib, pickle
from xmlrpclib import Binary
import sys

import pprint

class Wrapper:
    def __init__(self,url):
        print("Starting proxy at " + str(url))
        self.proxy = xmlrpclib.ServerProxy(url)

    def test(self):
        return self.proxy.test()

    def print_content(self):
        return self.proxy.print_content()

    def print_replicas(self):
        return self.proxy.print_replicas()


class Printer:

    def __init__(self, metaport, dataport):
        self.metaproxy = Wrapper("http://localhost:" + str(metaport))
        self.dataproxy = []
        for port in dataport:
            self.dataproxy.append( Wrapper("http://localhost:" + str(port)) )
        #self.dataproxy = Wrapper("http://localhost:" + str(dataport))
        try:
            print("MetaServer connected: " + str(self.metaproxy.test()))
        except:
            print("METASERVER NOT CONNECTED")
        for proxy in self.dataproxy:
            try:
                print("DataServer connected: " + str(proxy.test()))
            except:
                print("DATASERVER NOT CONNECTED")

    def print_contents(self):
        self.metaproxy.print_content()
        for proxy in self.dataproxy:
            try:
                proxy.print_replicas()
            except:
                print('Proxy not connected')
        return
    

if __name__ == '__main__':
    print(len(argv))

    if len(argv) < 3:
        print('usage: %s <metaserver port#> <dataserver port#>' % argv[0])
        
    else :
        meta_port = argv[1]
        data_port = argv[2:] # dataport is list
    print("metaserver port: " + meta_port)
    print("dataserver port: " + str(data_port))

    p = Printer(meta_port,data_port)
    p.print_contents()