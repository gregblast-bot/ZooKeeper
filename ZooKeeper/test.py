import subprocess
import time
import requests

# Runs the Docker container with the ZooKeeper services
def start_docker():
    print("Composing Docker Environment...")
    subprocess.run(["docker-compose", "up", "-d"], check=True)
    time.sleep(10) # wait ten

# Shuts down the Docker container with the ZooKeeper services 
def stop_docker():
    print("Stopping Docker Environment...")
    subprocess.run(["docker-compose", "down"], check=True)

# Run each instance asynchronously by using Popen
def start_server(host, port, zookeeper_ip, zookeeper_port):
    print(f"Starting Server On {host}:{port}...")
    server = subprocess.Popen(["python", "ZooKeeper.py", "--host", host, "--port", port, "--zookeeper", zookeeper_ip, "--zookeeper_port", zookeeper_port])
    return port, server

# Terminate each instance and wait for them to finish                      
def stop_server(process):
    print(f"Stopping Server...")
    process.terminate()
    process.wait()

# Send request and print add_update response
def add_update(host, port, key, value):
    url = f"http://{host}:{port}/update"
    response = requests.post(url, json={"key": key, "value": value})
    print(f"Add/Update response: {response.json()}")

# Send request and print read_key response
def read_key(host, port, key):
    url = f"http://{host}:{port}/read"
    response = requests.get(url, params={"key": key})
    print(f"Read response: {response.json()}")

# Send request and print kill response
def kill(host, port):
    url = f"http://{host}:{port}/kill"
    response = requests.get(url)
    print(f"Kill response: {response.json()}")
    return response.json()

# Main method
def main():
    zookeeper_ip = "127.0.0.1"
    zookeeper_port = "21811"
    host = "127.0.0.1"
    ports = ["5000", "5001", "5002"]

    try:
        start_docker()

        servers = [start_server(host, port, zookeeper_ip, zookeeper_port) for port in ports]
        time.sleep(10)  # Wait for servers to start and elect a leader

        print("\033[32mTesting Add and Read...\033[0m")
        for i in range(3):
            add_update(host, ports[i], f"key{i}", f"value{i}")
            time.sleep(10) # Wait
            read_key(host, ports[i], "key1") # key1 only exists on port 5001
            time.sleep(10) # Wait
            read_key(host, ports[i], f"key{i}") # check existing keys on all ports
            time.sleep(10) # Wait

        print("\033[32mTesting Leader Election...\033[0m")
        for port, server in servers:
            respnse = kill(host, port)
            is_leader = respnse.get("is_leader", False)
            if is_leader:
                print("Killing the leader, electing a new one.")
                stop_server(server)
                time.sleep(30)  # Wait for new leader election

        add_update(host, ports[1], "key2", "value2")
        for port in ports[1:]:
            read_key(host, port, "key2")

        print("\033[32mTesting Stale Read...\033[0m")
        servers[0] = start_server(host, ports[0], zookeeper_ip, zookeeper_port)
        time.sleep(10)  # Wait for server to start
        read_key(host, ports[0], "key2")
        add_update(host, ports[1], "key2", "new_value2")
        read_key(host, ports[0], "key2")

        time.sleep(10)

    # Handle an exception, so many exceptions... :/
    except Exception as e:
        print(f"Exception: {e}")

    finally:
        for _, server in servers:
            stop_server(server)
        stop_docker()

# Main method
if __name__ == "__main__":
    main()