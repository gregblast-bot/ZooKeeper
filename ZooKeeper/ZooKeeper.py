import argparse
import time
from kazoo.client import KazooClient
from kazoo.recipe.watchers import ChildrenWatch
from flask import Flask, request, jsonify
import os
import operator
import requests

app = Flask(__name__)

class ElectionMaster(object):

    def __init__(self, client_id, zk_host, zk_port):
        self.client_id = client_id
        self.current_host = zk_host
        self.zk = KazooClient(hosts=f"{zk_host}:{zk_port}")
        self.leadernode = "/election"
        self.validator_children_watcher = ChildrenWatch(client=self.zk,
                                                        path=self.leadernode,
                                                        func=self.detectLeader)
        self.zk.start()
        self.host_seq_list = []
        self.data_store = {}  # Dictionary to store key-value pairs
        self.replicas = []  # List of replica addresses

    def detectLeader(self, childrens):
        print("children:", childrens)
        self.host_seq_list = [i.split("_") for i in childrens]
        sorted_host_seqvalue = sorted(self.host_seq_list, key=operator.itemgetter(1))
        print("sorted_host_seqvalue", sorted_host_seqvalue)
        if sorted_host_seqvalue and sorted_host_seqvalue[0][0] == self.client_id:
            print("I am current leader:", self.client_id)
            self.do_something()
        else:
            print("I am a worker:", self.client_id)

    def do_something(self):
        print("'[do_something]'")

    def create_node(self):
        self.zk.create(os.path.join(self.leadernode, f"server_{self.client_id}_"), b"host:", ephemeral=True, sequence=True, makepath=True)

    def __del__(self):
        self.zk.close()

    def read(self, key):
        return self.data_store.get(key, "")

    def add_update(self, key, value):
        if self.is_leader():
            self.data_store[key] = value
            self.propagate_update(key, value)
        else:
            print("Not the leader. Cannot perform Add/Update operation.")

    def is_leader(self):
        sorted_host_seqvalue = sorted(self.host_seq_list, key=operator.itemgetter(1))
        return sorted_host_seqvalue and sorted_host_seqvalue[0][0] == self.client_id

    def propagate_update(self, key, value):
        for replica in self.replicas:
            try:
                response = requests.post(replica + "/update", json={"key": key, "value": value})
                if response.status_code == 200:
                    print(f"Successfully propagated update to {replica}")
                else:
                    print(f"Failed to propagate update to {replica}: {response.status_code}")
            except requests.exceptions.RequestException as e:
                print(f"Error propagating update to {replica}: {e}")

@app.route('/read', methods=['GET'])
def read():
    key = request.args.get('key')
    value = detector.read(key)
    return jsonify({"key": key, "value": value})

@app.route('/update', methods=['POST'])
def update():
    data = request.get_json()
    key = data['key']
    value = data['value']
    detector.add_update(key, value)
    return jsonify({"status": "success"})

# Main method
if __name__ == '__main__':
    # Process arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', required=True, help='Host IP')
    parser.add_argument('--port', required=True, help='Port')
    parser.add_argument('--zookeeper', required=True, help='ZooKeeper IP')
    parser.add_argument('--zookeeper_port', required=True, help='ZooKeeper Port')
    args = parser.parse_args()

    client_id = args.host + ":" + args.port
    detector = ElectionMaster(client_id, args.zookeeper, args.zookeeper_port)
    detector.create_node()

    app.run(host=args.host, port=int(args.port))