#!/usr/bin/env python
import shelve
import sys
import pprint

def main():
    if len(sys.argv) != 2 :
        print('usage: %s <datastorage filename>' % sys.argv[0])
        exit()
    print("Warning: will print empty dict if file doesn't exist")
    data = shelve.open(sys.argv[1])
    #pp = pprint.PrettyPrinter(indent=4)
    #pp.pprint(data)
    print("___________________________")
    for i in range(0,3): # 3 replicas
        print("REPLICA " + str(i))
        #print data[str(i)]
        for key in data[str(i)].keys():
            print("    inode: " + key)
            for block, value in data[str(i)][key].items():
                #pass
                print("        block " + str(block) + ": " + value)
    

if __name__ == "__main__":
    main()