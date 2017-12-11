#!/usr/bin/env python
"""
Author: David Wolinsky
Version: 0.03

Description:
The XmlRpc API for this library is:
    get(base64 key)
        Returns the value associated with the given key using a dictionary
            or an empty dictionary if there is no matching key
        Example usage:
            rv = rpc.get(Binary("key"))
            print rv => Binary
            print rv.data => "value"
    put(base64 key, base64 value)
        Inserts the key / value pair into the hashtable, using the same key will
            over-write existing values
        Example usage:  rpc.put(Binary("key"), Binary("value"))
    print_content()
        Print the contents of the HT
    read_file(string filename)
        Store the contents of the Hahelperable into a file
    write_file(string filename)
        Load the contents of the file into the Hahelperable

Changelog:
        0.03 - Modified to remove timeout mechanism for data.
"""

import sys, SimpleXMLRPCServer, getopt, pickle, time, threading, xmlrpclib, unittest
from datetime import datetime, timedelta
from xmlrpclib import Binary

# FIXME
import pprint

# Binary Notes
# .data = binary data encapsulated by the Binary instance. The data is provided as an 8-bit string.

# Presents a HT interface
class MetaServer:
    def __init__(self):
        self.data = {}
        self.inode = 0
        print("MetaServer initialized")

    def makeInode(self):
        self.inode = self.inode + 1
        print("MAKING INODE",self.inode)
        return self.inode 


    def test(self):
        print("__TEST__")
        return True   

    def count(self):
        return len(self.data)

    # Retrieve something from the HT
    def get(self, key): #key is Binary object
        # Default return value
        rv = {}
        # If the key is in the data structure, return properly formated results
        key = key.data
        if key in self.data:
            rv = Binary(self.data[key])

        return rv # xmlrpc uses dicts to pass data

    # Insert something into the HT
    def put(self, key, value): # key and value are Binary objects
        #print("__PUT__")
        # Remove expired entries
        self.data[key.data] = value.data
        return True

    def getkeys(self):
        return self.data.keys()

    def rem(self,key): # key is a Binary object
        #print("__REM__")
        self.data.pop(key.data,None)
        return True

    # Print the contents of the hashtable
    def print_content(self):
        pp = pprint.PrettyPrinter()
        pp4 = pprint.PrettyPrinter(indent=4)
        print("___META_PRINTCONTENTS_BEGIN__")
        for key, value in self.data.items():
            pp.pprint(key)
            temp = pickle.loads(value)
            pp4.pprint(temp)
        print("___META_PRINTCONTENTS__END___")
        return True

    # # Load contents from a file
    # def read_file(self, filename):
    #     f = open(filename.data, "rb")
    #     self.data = pickle.load(f)
    #     f.close()
    #     return True

    # # Write contents to a file
    # def write_file(self, filename):
    #     f = open(filename.data, "wb")
    #     pickle.dump(self.data, f)
    #     f.close()
    #     return True

# Start the xmlrpc server
def serve(port):
    file_server = SimpleXMLRPCServer.SimpleXMLRPCServer(('localhost', port))
    file_server.register_introspection_functions()
    sht = MetaServer()
    file_server.register_function(sht.test)
    file_server.register_function(sht.getkeys)
    file_server.register_function(sht.get)
    file_server.register_function(sht.put)
    file_server.register_function(sht.rem)
    file_server.register_function(sht.print_content)
    file_server.register_function(sht.makeInode)
    #file_server.register_function(sht.read_file)
    #file_server.register_function(sht.write_file)
    file_server.serve_forever()

def main():
    if len(sys.argv) != 2:
        print('usage: %s <metaserver port#> ' % sys.argv[0])
        exit(1)

    port = int(sys.argv[1])
    
    print ("METASERVER PORT: " + str(port))
    serve(port)

if __name__ == "__main__":
    main()