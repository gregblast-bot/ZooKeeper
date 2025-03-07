import subprocess
import time
import requests

# Runs the Docker container with the ZooKeeper services .
def start_docker():
    print("Composing Docker Environment...")
    subprocess.run(["docker-compose", "up", "-d"], check=True)
    time.sleep(10) # wait ten

# Shuts down the Docker container with the ZooKeeper services. 
def stop_docker():
    print("Stopping Docker Environment...")
    subprocess.run(["docker-compose", "down"], check=True)

# Run each instance asynchronously by using Popen.
def start_server(host, port, zookeeper_ip, zookeeper_port):
    print(f"Starting Server On {host}:{port}...")
    return subprocess.Popen(["python", "ZooKeeper.py", "--host", host, "--port", port, "--zookeeper", zookeeper_ip, "--zookeeper_port", zookeeper_port])

# Terminate each instance and wait for them to finish.                       
def stop_server(process):
    print(f"Stopping Server...")
    process.terminate()
    process.wait()

def add_update(host, port, key, value):
    url = f"http://{host}:{port}/update"
    response = requests.post(url, json={"key": key, "value": value})  # Use requests.post
    print(f"Add/Update response: {response.json()}")

def read_key(host, port, key):
    url = f"http://{host}:{port}/read"
    response = requests.get(url, params={"key": key})  # Use requests.get
    print(f"Read response: {response.json()}")

def main():
    zookeeper_ip = "127.0.0.1"
    zookeeper_port = "21811"
    host = "127.0.0.1"
    ports = ["5000", "5001", "5002"]

    try:
        start_docker()

        servers = [start_server(host, port, zookeeper_ip, zookeeper_port) for port in ports]
        time.sleep(10)  # Wait for servers to start and elect a leader

        print("Testing Add and Read...")
        add_update(host, ports[0], "key1", "value1")
        for port in ports:
            read_key(host, port, "key1")

        print("Testing Leader Election...")
        stop_server(servers[0])
        time.sleep(10)  # Wait for new leader election
        add_update(host, ports[1], "key2", "value2")
        for port in ports[1:]:
            read_key(host, port, "key2")

        print("Testing Stale Read...")
        servers[0] = start_server(host, ports[0], zookeeper_ip, zookeeper_port)
        time.sleep(10)  # Wait for server to start
        read_key(host, ports[0], "key2")
        add_update(host, ports[1], "key2", "new_value2")
        read_key(host, ports[0], "key2")

        time.sleep(10)

    # Handle an exception
    except Exception as e:
        print(f"Exception: {e}")

    finally:
        for server in servers:
            stop_server(server)
        stop_docker()

# Main method
if __name__ == "__main__":
    main()