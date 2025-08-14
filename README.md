flask

Build and run the docker container:

Build the image
docker build -t distributed-system-sim .

Run the container
docker run -d -p 5001:5001 --name ds-cluster distributed-system-sim

Run Command
docker run -it --rm -p 5001:5001 distributed-system-sim



Rebuild and reset easily

docker rm -f ds-cluster
docker build -t distributed-system-sim .
docker run -d -p 5001:5001 --name ds-cluster distributed-system-sim

Volume Binding
-v $(pwd)/cluster.db:/app/cluster.db

docker run -d -p 5001:5001 -v $(pwd)/cluster.db:/app/cluster.db --name ds-cluster distributed-system-sim



# KubernetesKubesim
Run The Main File :- python3 distributedsystems.py 

List Nodes 



List nodes with CLI.PY :-  python3 cli.py --list-nodes
 
List nodes with curl :- curl -X GET http://127.0.0.1:5001/list_nodes



Add Nodes



Add node info with CLI.PY :-  python3 cli.py --add-node node1 6
Add nodes with curl:- curl -X POST http://127.0.0.1:5001/add_node -H "Content-Type: application/json" -d '{"node_id": "node1", "cpu": 4}'



Send Heartbeat (To Prevent Failure)

Heartbeat with curl:- 
curl -X POST http://127.0.0.1:5001/heartbeat -H "Content-Type: application/json" -d '{"node_id": "node1"}'

You can simulate automatic heartbeats externally using a simple watch or loop script:

while true; do curl -X POST http://127.0.0.1:5001/heartbeat -H "Content-Type: application/json" -d '{"node_id":"node1"}'; sleep 5; done


Launch Pods


Launch node with CLI.PY :-  python3 cli.py --launch-pod pod1 2
Launch Pod with curl:- curl -X POST http://127.0.0.1:5001/launch_pod -H "Content-Type: application/json" -d '{"pod_id": "pod2", "cpu": 1}'


Launch Pods With Scheduling Algorithms

curl -X POST http://localhost:5001/launch_pod \
-H "Content-Type: application/json" \
-d '{"pod_id": "pod1", "cpu": 3, "strategy": "first_fit"}'

curl -X POST http://localhost:5001/launch_pod \
-H "Content-Type: application/json" \
-d '{"pod_id": "pod2", "cpu": 3, "strategy": "best_fit"}'

curl -X POST http://localhost:5001/launch_pod \
-H "Content-Type: application/json" \
-d '{"pod_id": "pod5", "cpu": 3, "strategy": "worst_fit"}'


List Pods

python3 cli.py --list-pods

curl http://127.0.0.1:5001/pod_usage


# Simulate node failure
python3 cli.py --fail-node node1

# Recover failed node
python3 cli.py --recover-node node1














