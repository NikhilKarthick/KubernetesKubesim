from flask import Flask, request, jsonify
import threading
import time
import subprocess
import logging
import sqlite3

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

db_file = "cluster.db"
lock = threading.Lock()

# Initialize Database and cleanup on restart
def init_db():
    with sqlite3.connect(db_file) as conn:
        cursor = conn.cursor()

        cursor.execute("DROP TABLE IF EXISTS nodes")
        cursor.execute("""
            CREATE TABLE nodes (
                node_id TEXT PRIMARY KEY,
                cpu INTEGER,
                last_heartbeat REAL,
                status TEXT DEFAULT 'healthy'
            )
        """)

        cursor.execute("DROP TABLE IF EXISTS pods")
        cursor.execute("""
            CREATE TABLE pods (
                pod_id TEXT PRIMARY KEY,
                cpu INTEGER,
                node_id TEXT,
                status TEXT DEFAULT 'pending',
                FOREIGN KEY (node_id) REFERENCES nodes (node_id)
            )
        """)

        conn.commit()

init_db()

def run_command(cmd):
    subprocess.run(cmd, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

def schedule_pod(pod_id, cpu_req, strategy="best_fit"):
    with sqlite3.connect(db_file) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT node_id, cpu, status FROM nodes WHERE status = 'healthy'")
        nodes = cursor.fetchall()

        assigned_node = None

        if strategy == "first_fit":
            # First node that can fit the pod
            for node_id, cpu, status in nodes:
                if cpu >= cpu_req:
                    assigned_node = node_id
                    break

        elif strategy == "best_fit":
    # Node with minimum available CPU that can still handle the request
            best_node = None
            min_cpu = float("inf")
            for node_id, cpu, status in nodes:
                if cpu >= cpu_req and cpu < min_cpu:
                    min_cpu = cpu
                    best_node = node_id
            assigned_node = best_node


        elif strategy == "worst_fit":
            # Node with most leftover CPU after allocation
            worst_node = None
            max_leftover = -1
            for node_id, cpu, status in nodes:
                if cpu >= cpu_req and (cpu - cpu_req) > max_leftover:
                    max_leftover = cpu - cpu_req
                    worst_node = node_id
            assigned_node = worst_node

        if assigned_node:
            cursor.execute("SELECT cpu FROM nodes WHERE node_id = ?", (assigned_node,))
            available_cpu = cursor.fetchone()[0]
            new_cpu = available_cpu - cpu_req
            cursor.execute("UPDATE nodes SET cpu = ? WHERE node_id = ?", (new_cpu, assigned_node))
            cursor.execute("UPDATE pods SET node_id = ?, status = 'running' WHERE pod_id = ?", (assigned_node, pod_id))
            conn.commit()
            return assigned_node

        return None

def auto_reschedule(strategy="best_fit"):
    while True:
        time.sleep(15)
        with sqlite3.connect(db_file) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT pod_id, cpu FROM pods WHERE node_id IS NULL")
            failed_pods = cursor.fetchall()
            for pod_id, cpu in failed_pods:
                assigned_node = schedule_pod(pod_id, cpu, strategy)
                if assigned_node:
                    logging.info(f"Rescheduled pod {pod_id} to Node {assigned_node} using {strategy}")

@app.route('/add_node', methods=['POST'])
def add_node():
    data = request.json
    node_id, cpu = data['node_id'], data['cpu']
    with sqlite3.connect(db_file) as conn:
        cursor = conn.cursor()
        cursor.execute("INSERT INTO nodes VALUES (?, ?, ?, 'healthy')", (node_id, cpu, time.time()))
        conn.commit()
    return jsonify({'message': f'Node {node_id} added'})

@app.route('/launch_pod', methods=['POST'])
def launch_pod():
    data = request.json
    pod_id = data['pod_id']
    cpu_req = data['cpu']
    strategy = data.get('strategy', 'best_fit')  # default

    with sqlite3.connect(db_file) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT 1 FROM pods WHERE pod_id = ?", (pod_id,))
        if cursor.fetchone():
            return jsonify({'error': f'Pod {pod_id} already exists'}), 400

        current_time = time.time()
        cursor.execute("""
            SELECT SUM(cpu) FROM nodes WHERE ? - last_heartbeat <= 30
        """, (current_time,))
        total_available_cpu = cursor.fetchone()[0] or 0

        if total_available_cpu < cpu_req:
            return jsonify({'error': 'Insufficient cluster-wide resources to schedule pod'}), 400

        cursor.execute("""
            INSERT INTO pods (pod_id, cpu, node_id, status)
            VALUES (?, ?, NULL, 'pending')
        """, (pod_id, cpu_req))
        conn.commit()

    assigned_node = schedule_pod(pod_id, cpu_req, strategy)
    if assigned_node:
        return jsonify({'message': f'Pod {pod_id} launched on Node {assigned_node} using {strategy}'})
    else:
        return jsonify({'error': f'No single node has enough resources right now with {strategy}'}), 400

@app.route('/heartbeat', methods=['POST'])
def heartbeat():
    data = request.json
    node_id = data['node_id']
    with sqlite3.connect(db_file) as conn:
        cursor = conn.cursor()
        cursor.execute("UPDATE nodes SET last_heartbeat = ?, status = 'healthy' WHERE node_id = ?", (time.time(), node_id))
        conn.commit()
    return jsonify({'message': 'Heartbeat received'})

@app.route('/list_nodes', methods=['GET'])
def list_nodes():
    with sqlite3.connect(db_file) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT node_id, cpu, last_heartbeat, status FROM nodes")
        node_list = cursor.fetchall()
    return jsonify([{ 'node_id': node_id, 'cpu': cpu, 'last_heartbeat': last_heartbeat, 'status': status} for node_id, cpu, last_heartbeat, status in node_list])

@app.route('/pod_usage', methods=['GET'])
def pod_usage():
    with sqlite3.connect(db_file) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT pod_id, cpu, node_id, status FROM pods")
        pod_list = cursor.fetchall()
    return jsonify([{ 'pod_id': pod_id, 'cpu': cpu, 'node_id': node_id, 'status': status} for pod_id, cpu, node_id, status in pod_list])

@app.route('/fail_node', methods=['POST'])
def fail_node():
    data = request.json
    node_id = data['node_id']
    with sqlite3.connect(db_file) as conn:
        cursor = conn.cursor()
        cursor.execute("UPDATE nodes SET status = 'unhealthy' WHERE node_id = ?", (node_id,))
        cursor.execute("UPDATE pods SET node_id = NULL, status = 'pending' WHERE node_id = ?", (node_id,))
        conn.commit()
    logging.warning(f"Node {node_id} has been manually marked as failed.")
    return jsonify({'message': f'Node {node_id} marked as failed'})

@app.route('/recover_node', methods=['POST'])
def recover_node():
    data = request.json
    node_id = data['node_id']
    with sqlite3.connect(db_file) as conn:
        cursor = conn.cursor()
        cursor.execute("UPDATE nodes SET status = 'healthy', last_heartbeat = ? WHERE node_id = ?", (time.time(), node_id))
        conn.commit()
    logging.info(f"Node {node_id} has been manually recovered.")
    return jsonify({'message': f'Node {node_id} marked as healthy'})

def heartbeat_checker():
    while True:
        time.sleep(10)
        with sqlite3.connect(db_file) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT node_id FROM nodes WHERE ? - last_heartbeat > 30", (time.time(),))
            failed_nodes = cursor.fetchall()
            for (node_id,) in failed_nodes:
                logging.warning(f"Node {node_id} failed!")
                cursor.execute("UPDATE nodes SET status = 'unhealthy' WHERE node_id = ?", (node_id,))
                cursor.execute("UPDATE pods SET node_id = NULL, status = 'pending' WHERE node_id = ?", (node_id,))
            conn.commit()

def auto_heartbeat():
    while True:
        time.sleep(5)
        with sqlite3.connect(db_file) as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE nodes SET last_heartbeat = ?", (time.time(),))
            conn.commit()

# Start background threads
threading.Thread(target=heartbeat_checker, daemon=True).start()
threading.Thread(target=auto_heartbeat, daemon=True).start()
threading.Thread(target=auto_reschedule, kwargs={'strategy': 'best_fit'}, daemon=True).start()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
