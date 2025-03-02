#-*- coding: utf-8 -*-
import time
from kazoo.client import KazooClient
from kazoo.recipe.watchers import ChildrenWatch
import re
import os
import sys
import operator


class ElectionMaster(object):

    def __init__(self, client_id):
        self.client_id = client_id
        #self.current_host = "172.18.0.4"
        self.current_host = "127.0.0.1"
        self.zk = KazooClient(hosts=self.current_host+':21811')
        #print(self.current_host)
        self.leadernode = "/leader"
        self.validator_children_watcher = ChildrenWatch(client=self.zk,
                                                        path=self.leadernode,
                                                        func=self.detectLeader)
        self.zk.start()
        self.host_seq_list = []

    def detectLeader(self, childrens):
        print("childern:", childrens)
        self.host_seq_list = [i.split("_") for i in childrens]
        sorted_host_seqvalue = sorted(self.host_seq_list, key=operator.itemgetter(1))
        print("sorted_host_seqvalue", sorted_host_seqvalue)
        if sorted_host_seqvalue and sorted_host_seqvalue[0][0] == self.client_id:
            print("I am current leader :",self.client_id)
            self.do_something()
        else:
            print("I am a worker :",self.client_id)

    def do_something(self):
        print("'[do_something] '")

    def create_node(self):
        self.zk.create(os.path.join(self.leadernode, "%s_" % self.client_id), b"host:", ephemeral=True, sequence=True, makepath=True)

    def __del__(self):
        self.zk.close()





if __name__ == '__main__':
    client_id = sys.argv[1]
    detector = ElectionMaster(client_id)
    detector.create_node()
    input("wait to quit:\n")
    
    #time.sleep(10)
