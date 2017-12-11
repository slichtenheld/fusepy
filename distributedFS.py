#!/usr/bin/env python
from __future__ import print_function, absolute_import, division

import logging

from collections import defaultdict
from errno import ENOENT
from stat import S_IFDIR, S_IFLNK, S_IFREG
from sys import argv, exit
from time import time

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn

import metaserver, dataserver, xmlrpclib, pickle
from xmlrpclib import Binary
import sys
from zlib import crc32
import random
import math

import pprint
from time import sleep

BLKSIZE = 8

if not hasattr(__builtins__, 'bytes'):
    bytes = str

"""
FOR FINAL PROJECT
PUT ALL CHANGES IN DATAWRAPPER - few exceptions
"""

"""
KNOWN BUGS
    - can still cd into directory without x permission
"""


def split_to_list(split_this):
    arr = []
    temp = split_this
    while len(temp) > BLKSIZE:
        arr.append(temp[0:BLKSIZE])
        temp = temp[BLKSIZE:]
    if len(temp) > 0:
        arr.append(temp)
    return arr

def join_from_list(join_this):
    temp = bytes()  # put these methods after the "if not hasattr" so that it gets the right type
    for piece in join_this:
        temp = temp + piece
    return temp


# DO NOT TOUCH NO CHANGES TO META SHOULD BE NECESSARY
class MetaWrapper:
    def __init__(self,url):
        print("Starting META proxy at " + str(url))
        self.proxy = xmlrpclib.ServerProxy(url)
        self.retryNum = 5

    def makeInode(self): # ONLY WORKS FOR DATASERVER!!!!
        for i in range(0,self.retryNum):    
            try:
                return self.proxy.makeInode()
            except: 
                continue       
        return 0

    def test(self):
        for i in range(0,self.retryNum):
            try:
                return self.proxy.test()
            except:
                continue
        return False

    def getkeys(self):
        for i in range(0,self.retryNum):
            try:
                return self.proxy.getkeys()
            except:
                continue
        return []

    # key is string, data is object
    def put(self, key, data): 
        for i in range(0,self.retryNum):
            try:
                return self.proxy.put(Binary(key),Binary(pickle.dumps(data)))
            except:
                continue
        return None

    # key is string, returns object
    def get(self, key):
        for i in range(0,self.retryNum):
            try:
                return pickle.loads(self.proxy.get((Binary(key))).data)
            except:
                #print ("ERROR: Accessing file that doesn't exist")
                continue
        return None

    def rem(self, key):
        #print("__REM__")
        for i in range(0,self.retryNum):
            try:
                return self.proxy.rem((Binary(key)))
            except:
                continue
        return False

    def print_content(self):
        #print("__PRINT_CONTENT__")
        for i in range(0,self.retryNum):
            try:
                return self.proxy.print_content()
            except:
                continue
        return False

def split_path(path):
    parent_path, file_name = path.rsplit('/',1)
    if not parent_path: # if parent is root, parent_path will just be '' after split
        parent_path = '/'
    return parent_path, file_name



# ALL CHANGES SHOULD HAPPEN HERE
class DataWrapper:
    def __init__(self,portList):
        print("Starting DATA proxies for " + str(portList))
        self.proxy = [] # create list for all proxies

        for idx, portNum in enumerate(portList):
            self.proxy.append( xmlrpclib.ServerProxy("http://localhost:" + str(portNum)) )
            

    def test(self):
        test = []
        for idx, p in enumerate(self.proxy):
            print("DataWrapper testing Proxy", idx)
            try:
                test.append(self.proxy[idx].test())
            except:
                test.append(False)

        return test

    # key is string, data is object # FIXME or is data string?
    def put(self, key, data): 
        print("DataWrapper PUT")
        key = str(key) # Binary() takes string input
        print(str(len(self.proxy)) + " dataservers")

        inode = str(key)
        N = len(self.proxy)
        starting_server = int(inode)%N
        print("Starting server: " + str(starting_server))
        ################################
        # happens when initializing files
        # FIXME: necessary? Also always fails
        if data == "": 
            print ("initializing block0")
            for i in range(0,3): # 3 replicas
                for r in range(0,3): # try to store data 3 times
                    try:
                        self.proxy[(starting_server+i)%N].put_block( 
                            Binary(str(i)), #replica_num
                            Binary(inode),  #inode
                            Binary(str(block_num)), #block num 
                            Binary( "" ) ) # data
                        print("Initialized inode on dataproxy: "+ str((starting_server+i)%N))
                        break
                    except:
                        print("Failed to init inode on dataproxy: "+ str((starting_server+i)%N))
                        continue
            return
        ################################
        # writing data to files
        for block_num, d in enumerate(split_to_list(data)):
            for replica_num in range(0,3): # for each replica 0, 1, 2
                block_stored = False
                while block_stored is False:
                    try:
                        self.proxy[(starting_server+replica_num+block_num)%N].put_block( 
                            Binary(str(replica_num)), #replica_num
                            Binary(inode),  #inode
                            Binary(str(block_num)), #block num 
                            Binary(str(d) ) ) # data
                        print("PUT dataserver " + str((starting_server+replica_num+block_num)%N) + 
                            ", replica " + str(replica_num) + " | inode: " + str(inode) + 
                            ", block: " + str(block_num) + ", data:  " + str(d) )
                            #last element will print newline to screen as well
                        block_stored = True
                    except:
                        pass
        print("BLOCK PUT FINISHED")
        return


    # key is string(inode), returns Binary object
    def get(self, key, size, offset=0):
        print("DataWrapper GET | inode: " + str(key) +
            ", offset: " + str(offset) + ", size: " + str(size) )
        key = str(key)

        #####################################
        #####################################
        inode = str(key)
        N = len(self.proxy)

        starting_block = int(offset/BLKSIZE)
        first_byte_of_starting_block = starting_block*BLKSIZE
        offset_from_start_of_starting_block = offset-first_byte_of_starting_block
        num_blocks_to_retrieve = int(math.ceil(float(size+offset_from_start_of_starting_block)/BLKSIZE))
        blocks_to_retrieve = range(starting_block, num_blocks_to_retrieve)
        print("blocks to retrieve: {}".format(blocks_to_retrieve))

        starting_server = (int(inode)+starting_block)%N

        temp = []
        print("Starting server: " + str(starting_server))
        for block in blocks_to_retrieve:
            block_retrieved = False
            while block_retrieved is False: # retry forever until at least one copy of block was retrieved
                print(block)

                replicas_needing_rewrite = []
                replicas_to_try = range(0,3)
                random.shuffle(replicas_to_try)
                for trynum, replica in enumerate(replicas_to_try):
                    print("TRYING GET_" + str(trynum) + ": dataserver: " + str((starting_server+block+replica)%N) +
                                ",replica " + str(replica) + ",inode: " + str(inode) +
                                ",block_num: " + str(block))
                    try:
                        result = pickle.loads(self.proxy[(starting_server + block + replica) % N].get_block(
                            Binary(str(replica)),  # replica_num
                            Binary(inode),  # inode
                            Binary(str(block))  # block num
                        ).data)

                        print(str(result))
                        result_data = result["value"]
                        result_checksum = result["checksum"]
                        result_checksum_calc = crc32(result_data)

                        if result_checksum_calc != result_checksum:
                            replicas_needing_rewrite.append(replica)
                            continue

                        for replica_to_rewrite in replicas_needing_rewrite:
                            print("rewriting replica: {} for inode: {} block: {}".format(replica_to_rewrite, inode, block))
                            self.proxy[(starting_server + replica_to_rewrite + block) % N].put_block(
                                Binary(str(replica_to_rewrite)),  # replica_num
                                Binary(inode),  # inode
                                Binary(str(block)),  # block num
                                Binary(str(result_data)))  # data

                        temp.append(result_data)
                        block_retrieved = True
                        print("GOT: " + str(temp[-1]))
                        break
                    except:
                        continue
                                      # infinite loop described below shouldn't occur if you don't try to read past the end of the file
                # if r == maxtries-1: # CAREFUL COULD CAUSE ENDLESS LOOP BECAUSE SIZE FUCKED UP IN OUTERMOST FOR LOOP
                #     break

        print(temp)
        print(join_from_list(temp))
        print("BLOCK GET FINISHED")
        return join_from_list(temp)

    def rem(self, key):
        key = str(key)
        #print("__REM__")
        for p in self.proxy:
            p.rem((Binary(key)))
        return

    def print_content(self):
        #print("__PRINT_CONTENT__")
        for p in self.proxy:
            p.print_replicas
        return


class Memory(LoggingMixIn, Operations):

    def __init__(self, metaport, dataports):
        # connect metaservers and dataservers
        self.metaproxy = MetaWrapper("http://localhost:" + str(metaport)) # FIXME: make like dataproxy
        try:
            print("MetaServer connected: " + str(self.metaproxy.test()))
        except:
            print("METASERVER NOT CONNECTED!!!!")
        self.dataproxy = DataWrapper(dataports) # create object with all data servers
        dataproxytest = self.dataproxy.test()
        print("DataServers connected: " + str(dataproxytest))
        if False in dataproxytest:
            print("DATASERVER NOT CONNECTED!!!!")

        # initialize filedescriptor
        self.fd = 0 
        now = time()

        #initialize metaserver if not initialized
        if not self.metaproxy.get('/'):
            print("INITIALIZING METASERVER!!!")
            self.metaproxy.put('/',dict(st_mode=(S_IFDIR | 0o755), st_ctime=now,
                            st_mtime=now, st_atime=now, st_nlink=2, files=[]))
        else:
            print("METASERVER ALREADY INITIALIZED!!!")

        return


    def chmod(self, path, mode):
        #print("----CHMOD: " + path + " MODE: " + str(mode) )
        file = self.metaproxy.get(path)
        file['st_mode'] &= 0o770000
        file['st_mode'] |= mode
        self.metaproxy.put(path,file)
        return 0

    def chown(self, path, uid, gid):
        #print("----CHOWN: " + path + "")
        file = self.metaproxy.get(path)
        file['st_uid'] = uid
        file['st_gid'] = gid
        self.metaproxy.put(path,file)

    def create(self, path, mode):
        print("CREATE:", path)
        
        inode_t = self.metaproxy.makeInode()
        self.dataproxy.put(inode_t,'')
        #self.dataproxy.print_content()
        #self.metaproxy.print_content()

        parent_path, new_file_name = split_path(path)

        parent = self.metaproxy.get(parent_path) # get parent dir
        parent['files'].append(new_file_name)
        self.metaproxy.put(parent_path,parent) # update parent in database
        self.metaproxy.put(path, dict(st_mode=(S_IFREG | mode), st_nlink=1,
                                st_size=0, st_ctime=time(), st_mtime=time(),
                                st_atime=time() , inode=inode_t))
        self.fd += 1
        return self.fd

    def getattr(self, path, fh=None):
        #print("----GETATTR: " + path + "")
        file = self.metaproxy.get(path)
        if not file: # if file doesn't exist
            raise FuseOSError(ENOENT)
        return file 

    def getxattr(self, path, name, position=0):
        #print("----GETXATTR: " + path + " NAME: " + str(name) + " POS: " + str(position))
        attrs = self.metaproxy.get(path)
        if attrs is not None:
            attrs = attrs.get('attrs',{})
        else:
            attrs = {}

        try:
            return attrs[name]
        except KeyError:
            return ''

    # FIXME: untested
    def listxattr(self, path):
        #print("_-_-LISTXATTR: " + path)
        attrs = self.metaproxy.get(path).get('attrs',{})
        return attrs.keys()

    def mkdir(self, path, mode):
        #print("**MKDIR: " + path + "")

        parent_path, new_dir_name = split_path(path)

        parent = self.metaproxy.get(parent_path) # get parent dir
        parent['st_nlink'] += 1 # update parent dir
        parent['files'].append(new_dir_name) # add name to files

        self.metaproxy.put(parent_path,parent) # store updated parent dir
        
        #pp = pprint.PrettyPrinter(indent=4)
        #pp.pprint(parent)

        # add new dir
        self.metaproxy.put(path,dict(st_mode=(S_IFDIR | mode), st_nlink=2,
                                st_size=0, st_ctime=time(), st_mtime=time(),
                                st_atime=time(), files=[]) )

    def open(self, path, flags):
        #print("_OPEN: " + path + "")
        self.fd += 1
        return self.fd

    def read(self, path, size, offset, fh):
        print("READ: " + path + "")
        inode = self.metaproxy.get(path)['inode']
        file_size = self.metaproxy.get(path).get('st_size', 0)
        if size > file_size:
            size = file_size
        return self.dataproxy.get(inode,size,offset)

    def readdir(self, path, fh):
        print("****READDIR: " + path)
        cur_dir = self.metaproxy.get(path)
        print (cur_dir)
        return ['.','..'] + [ x for x in sorted(cur_dir.get('files',[]))]

    def readlink(self, path):
        print("****READLINK: " + path + "")
        pp = pprint.PrettyPrinter(indent=4)
        metadata = self.metaproxy.get(path)
        while metadata==None: # FIXME: eliminate endless loop possibility
            metadata = self.metaproxy.get(path)

        print("____metadata:")
        pp.pprint(metadata)
        inode = metadata['inode']
        print("____inode:")
        pp.pprint(inode)
        data = self.dataproxy.get(inode,metadata['st_size']) # no offset necessary
        print("____data:")
        pp.pprint(data)
        return data
        #return self.dataproxy.get(self.metaproxy.get(path)['inode'])

    def removexattr(self, path, name):
        #print("-_-_REMOVEXATTR: " + path + "")
        file = self.metaproxy.get(path)
        attrs = file.get('attrs',{})
        try:
            del attrs[name]
        except KeyError:
            pass        # Should return ENOATTR
        self.metaproxy.put(path,file)

    # since use inodes for data, do not have to move anything in data server
    def rename(self, old, new):
        #print("RENAME: " + old + " to " + new )
        file = self.metaproxy.get(old)

        old_parent_path, old_file_name = split_path(old)
        new_parent_path, new_file_name = split_path(new)
        #print(old_parent_path,old_file_name)
        #print(new_parent_path,new_file_name)

        #   update old parent
        old_parent = self.metaproxy.get(old_parent_path)
        old_parent['files'].remove(old_file_name)

        #   update new parent
        if old_parent_path!=new_parent_path:
            new_parent = self.metaproxy.get(new_parent_path)
        else:
            new_parent = old_parent

        new_parent['files'].append(new_file_name)

        # update actual path
        # check if directory or file
        if file['st_mode'] & 0o770000 == S_IFREG: # just a file
            #print('Moving file')
            self.metaproxy.rem(old)
            self.metaproxy.put(new,file)
        elif file['st_mode'] & 0o770000 == S_IFDIR:
            old_parent['st_nlink'] -= 1
            new_parent['st_nlink'] += 1
            keys = self.metaproxy.getkeys()
            #print(keys)
            for key in keys:
                if old in key:
                    temp = self.metaproxy.get(key)
                    self.metaproxy.rem(key)

                    new_path = key.replace(old, new, 1)
                    #print("OLD PATH: " + key + ", NEW PATH: " + new_path)

                    self.metaproxy.put(new_path,temp)
                    #self.metaproxy.print_content()
        self.metaproxy.put(old_parent_path,old_parent)
        self.metaproxy.put(new_parent_path,new_parent)
        return

    def rmdir(self, path):
        self.metaproxy.rem(path)
        parent_path, dir_name = split_path(path)
        parent = self.metaproxy.get(parent_path)
        parent['files'].remove(dir_name)
        parent['st_nlink'] -= 1
        self.metaproxy.put(parent_path,parent)

    def setxattr(self, path, name, value, options, position=0):
        #print("-_-_SETXATTR: " + path + "")
        file = self.metaproxy.get(path)
        attrs = file.get('attrs',{})
        attrs[name] = value
        self.metaproxy.put(path,file)

    def statfs(self, path):
        #print("_STATFS: " + path + "")
        return dict(f_bsize=512, f_blocks=4096, f_bavail=2048)

    def symlink(self, target, source):
        print("**SYMLINK target: " + target + " source: " + source)
        inode_t = self.metaproxy.makeInode()
        self.metaproxy.put(target,dict(st_mode=(S_IFLNK | 0o777), st_nlink=1,
                                    st_size=len(source), inode = inode_t))
        parent_path, link_name = split_path(target)
        parent = self.metaproxy.get(parent_path)
        parent['files'].append(link_name)
        self.metaproxy.put(parent_path, parent)
        self.dataproxy.put(inode_t, source)
        self.metaproxy.print_content()
        self.dataproxy.print_content()
        return
        

    def truncate(self, path, length, fh=None):
        print("TRUNCATE: " + path + "")
        file = self.metaproxy.get(path)
        contents = self.dataproxy.get(file['inode'],file['st_size'])
        contents = contents[:length]
        self.dataproxy.put(file['inode'],contents)
        file['st_size'] = length
        self.metaproxy.put(path,file)

    def unlink(self, path):
        #print("**UNLINK: " + path + "")
        parent_path, file_name = split_path(path)
        inode_t = self.metaproxy.get(path)['inode']
        self.dataproxy.rem(inode_t)
        self.metaproxy.rem(path)

        parent = self.metaproxy.get(parent_path)
        parent['files'].remove(file_name)
        self.metaproxy.put(parent_path,parent)

    def utimens(self, path, times=None):
        #print("----UTIMENS: " + path )
        now = time()
        atime, mtime = times if times else (now, now)
        file = self.metaproxy.get(path)
        file['st_atime'] = atime
        file['st_mtime'] = mtime
        self.metaproxy.put(path,file)

    def write(self, path, data, offset, fh):
        print("WRITE: " + path )
        file = self.metaproxy.get(path)
        contents = self.dataproxy.get(file['inode'],file['st_size'])
        contents = contents[:offset] + data
        self.dataproxy.put(file['inode'],contents)
        #self.dataproxy.print_content()
        file['st_size'] = len(contents)

        self.metaproxy.put(path,file)
        return len(data)


if __name__ == '__main__':
    if len(argv) < 4:
        print('usage: %s <mountpoint> <metaserver port#> <dataserver port#> ... <dataserver port#N>' % argv[0])
        exit(1)
    mountpoint = argv[1]
    meta_port = argv[2]
    data_ports = argv[3:]
    print("mountpoint: " + mountpoint)
    print("metaserver port: " + meta_port)
    print("dataserver ports: " + str(data_ports))
    #logging.basicConfig(level=logging.DEBUG)
    fuse = FUSE(Memory(meta_port,data_ports), mountpoint, foreground=True, debug=False) #FIXME: debug = true