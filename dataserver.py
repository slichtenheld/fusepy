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
import pickle
from zlib import crc32
import shelve
#FIXME
import pprint

import signal
def signal_handler(signal, frame):
        print('Exiting... (you pressed Ctrl+C)')
        sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

import time

# Binary Notes
# .data = binary data encapsulated by the Binary instance. The data is provided as an 8-bit string.

# contacts all neighbors to build dict of all inodes as keys and largest block# as answers
# its dirty i know


# Presents a HT interface
class DataServer:
    def __init__(self, neighbordataports, server_idx):
        #time.sleep(server_idx)
        self.idx = server_idx
        #self.dataports = dataports
        self.neighbor_proxy = []
        for idx, portNum in enumerate(neighbordataports):
            if idx == server_idx:
                self.neighbor_proxy.append(None)
                print("skipping idx " + str(idx))
                continue
            self.neighbor_proxy.append( xmlrpclib.ServerProxy("http://localhost:" + str(portNum)) )
        print(str(len(self.neighbor_proxy))+" neighbors")


        filename = "datastorage" + str(self.idx)
        self.replicas = shelve.open(filename,writeback=True)
        self.needs_recovery = False
        if self.replicas == {}:
            print("initializing storage")
            self.replicas['0'] = {}
            self.replicas['1'] = {}
            self.replicas['2'] = {}
            self.needs_recovery = True
            #inodesMaxBlocks = make_inodes_maxblks(self.neighbor_proxy) 
            #recreate_storage(self.neighbor_proxy,inodesMaxBlocks,self.idx)


        #self.replicas = [{},{},{}] # 3 replicas stored as dicts of fixed array
        #print("DataServer initialized")

        print("DataServer initialized")

    def make_inodes_maxblks(self, neighbor_proxy_list):
        neighbor_inodeblks = []
        all_inodes = []
        all_inodes_maxblks = {}
        try:
            for n in neighbor_proxy_list:
                if n is not None:
                    try:
                        neighbor_inodeblks.append(n.get_inode_blknum())
                    except:
                        print("connection refused")
            # print (neighbor_inodeblks)

            for n in neighbor_inodeblks:
                all_inodes = list(set(all_inodes) | set(n.keys()))
            # print(all_inodes)

            for inode in all_inodes:
                for n in neighbor_inodeblks:
                    all_inodes_maxblks[inode] = max(all_inodes_maxblks.get(inode, 0), n.get(inode, 0))
        except:
            print("MAKE INODES MAXBLKS FAILED")

        return all_inodes_maxblks

    def recreate_storage(self, neighbor_proxy_list, inodesAndMaxBlocks, serveridx):
        # once have dictionary of all inodes with max blocks
        datastorageTemp = [{}, {}, {}]

        N = len(neighbor_proxy_list)  # need to include yourself
        inodeList = inodesAndMaxBlocks.keys()
        for replicaNum in range(0, 3):  # for each replica
            for inode in inodeList:
                for blockNum in range(0, int(inodesAndMaxBlocks[inode])):
                    if ((int(inode) + replicaNum + int(
                            blockNum)) % N == serveridx):  # see which inodes and blocks responsible for
                        # see which other servers have other replicas
                        gotBlock = False
                        for i in range(0, 3):  # for each replica
                            if i == replicaNum:
                                continue
                            if gotBlock is False:
                                blockLocated = (int(inode) + i + int(blockNum)) % N
                                print("reading from server " + str(blockLocated) + " replica " + str(i) + ", inode: "
                                      + str(inode) + ", block: " + str(blockNum))

                                # ask respective servers for respective blocks
                                try:
                                    value = neighbor_proxy_list[blockLocated].get_block(
                                            Binary(str(i)), Binary(inode), Binary(str(blockNum))
                                            )
                                    value = pickle.loads(value.data)
                                    print("storing to replica {}, inode {}, block {}".format(replicaNum, inode, blockNum))
                                    self.replicas[str(replicaNum)].setdefault(str(inode), {})[str(blockNum)] = value
                                    self.replicas.sync()
                                    break
                                except:
                                    pass


    def recover(self):
        if self.needs_recovery:
            inodesMaxBlocks = self.make_inodes_maxblks(self.neighbor_proxy)
            print(inodesMaxBlocks)
            self.recreate_storage(self.neighbor_proxy, inodesMaxBlocks, self.idx)


    def test(self):
        print("__TEST__")
        return True   

    def count(self):
        print("COUNT", len(self.data))
        return len(self.data)
        
    # replica_num, inode, block_num are all Binary objects
    def get_block(self, replica_num, inode, block_num):
        replica_num = replica_num.data
        inode = inode.data
        block_num = block_num.data
        print("reading from replica " + str(replica_num) +", inode: "
            + str(inode) + ", block: " + str(block_num))

        try:
            return Binary(pickle.dumps(self.replicas[replica_num][inode][block_num]))
        except:
            print("block not found")
            return {}

    # replica_num, inode, block_num, value are all binary objects
    def put_block(self, replica_num, inode, block_num, value):
        print("__PUT_BLOCK__")
        replica_num = replica_num.data
        #print("Replica Num: " + str(replica_num))
        inode = inode.data
        #print("Inode: " + inode)
        block_num = block_num.data
        #print("block_num: " + block_num)
        value = value.data 
        #print("value: " + value)
        #print("storing to replica " + str(replica_num) +", inode: " 
        #    + str(inode) + ", block: " + str(block_num) + ", data: " + str(value))
        self.replicas[replica_num].setdefault(inode,{})[block_num]={"value": value, "checksum": crc32(value)}
        self.replicas.sync()
        self.print_replicas()
        return True

    def corrupt_block(self, inode):
        print("__CORRUPT_BLOCK__")
        inode = inode.data
        for replica, contents in self.replicas.items():
            if inode in contents:
                block_to_corrupt = min(contents[inode].keys())
                print("corrupting block {} of inode {} in replica {}".format(block_to_corrupt, inode, replica))
                if self.replicas[replica][inode][block_to_corrupt]["checksum"] == 0:
                    self.replicas[replica][inode][block_to_corrupt]["checksum"] = 1
                else:
                    self.replicas[replica][inode][block_to_corrupt]["checksum"] = 0
                self.replicas.sync()
                return True
        return False

    def rem(self,key): # key is a Binary object
        print("__REM__")
        for i in range(0,3): # FIXME, make replicas not static
            self.replicas[str(i)].pop(key.data,None)
        self.replicas.sync()
        return True

    def print_replicas(self):
        pp = pprint.PrettyPrinter()
        pp.pprint(self.replicas)
        for i in range(0,3): # 3 replicas
            print("    REPLICA " + str(i))
            #print replicas[str(i)]
            for key in self.replicas[str(i)].keys():
                print("        inode: " + key)
                for block, value in self.replicas[str(i)][key].items():
                    print("            block " + str(block) + ": " + str(value))
        print("<><><><><><><><><><><><><><>")
        return True

    # returns dictionary where keys are inodes and data is max block number
    def get_inode_blknum(self):

        temp = {}
        for i in range(0,3): # 3 replicas
            for inode, block in self.replicas[str(i)].items():
                temp[inode] = max(int(temp.get(inode,0)) , max(map(int, block.keys())) )
        print("GET inode+maxblksize: " + str(temp))
        # dict.get(key[,default])
        return temp # sending dictionaries supported, no pickle and Binary needed

# Start the xmlrpc server
def serve(data_ports, server_idx): # dataports is list of ints

    file_server = SimpleXMLRPCServer.SimpleXMLRPCServer(('localhost', data_ports[server_idx]))
    file_server.register_introspection_functions()

    # only pass other ports to server
    #data_ports.pop(server_idx)
    print("all ports: " + str(data_ports))

    sht = DataServer(data_ports,server_idx)
    file_server.register_function(sht.test) 
    file_server.register_function(sht.get_block)
    file_server.register_function(sht.put_block)
    file_server.register_function(sht.print_replicas)
    file_server.register_function(sht.corrupt_block)
    file_server.register_function(sht.rem)
    file_server.register_function(sht.get_inode_blknum)
    delaythread = DelayThread(sht)
    delaythread.start()
    file_server.serve_forever()


class DelayThread(threading.Thread):
    def __init__(self, sht):
        threading.Thread.__init__(self)
        self.sht = sht

    def run(self):
        print("haha")
        time.sleep(0.5)
        self.sht.recover()

def main():

    if len(sys.argv) < 3:
        print('usage: %s <server index> <dataserver port#> ... <dataserver port#N>' % sys.argv[0])
        exit(1)

    server_idx = int(sys.argv[1])
    time.sleep(float(server_idx)/10)
    data_ports = map(int,sys.argv[2:]) # get list of ints of port numbers
    print(data_ports)
    if (server_idx > ( len(data_ports) - 1 ) ):
        print('usage: %s <server index> <dataserver port#> ... <dataserver port#N>' % sys.argv[0])
        exit(1)

    print("server index: " + str(server_idx))
    print ("port #: " + str(data_ports[server_idx]))
    print("dataserver ports: " + str(data_ports))
    serve(data_ports, server_idx)

if __name__ == "__main__":
    main()