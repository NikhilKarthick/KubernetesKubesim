from flask import Flask, request, jsonify
import threading
import time
import subprocess
import logging
import sqlite3

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

db_file = "cluster.db"
lock = threading.RLock()

# ---------------- Database Init ----------------
def init_db():
    with sqlite3.connect(db_file, check_same_thread=False) as conn:
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

        cursor.execute("DROP TABLE IF EXISTS settings")
        cursor.execute('''CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT
        )''')

        # Ensure a default strategy exists
        cursor.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('strategy', 'best_fit')")
        cursor.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('leader', 'none')")

        conn.commit()

init_db()

# ---------------- Strategy Helper ----------------
def get_current_strategy():
    with sqlite3.connect(db_file, check_same_thread=False) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT value FROM settings WHERE key='strategy'")
        row = cursor.fetchone()
        return row[0] if row else "best_fit"

# ---------------- Scheduling ----------------
def run_command(cmd):
    subprocess.run(cmd, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

def schedule_pod(pod_id, cpu_req):
    strategy = get_current_strategy()
    with lock, sqlite3.connect(db_file, check_same_thread=False) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT node_id, cpu, status FROM nodes WHERE status = 'healthy'")
        nodes = cursor.fetchall()

        assigned_node = None

        if strategy == "first_fit":
            for node_id, cpu, status in nodes:
                if cpu >= cpu_req:
                    assigned_node = node_id
                    break

        elif strategy == "best_fit":
            best_node = None
            min_cpu = float("inf")
            for node_id, cpu, status in nodes:
                if cpu >= cpu_req and cpu < min_cpu:
                    min_cpu = cpu
                    best_node = node_id
            assigned_node = best_node

        elif strategy == "worst_fit":
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
            logging.info(f"Pod {pod_id} scheduled on Node {assigned_node} with {strategy}")
            return assigned_node

        return None

def auto_reschedule():
    while True:
        time.sleep(15)
        strategy = get_current_strategy()
        with lock, sqlite3.connect(db_file, check_same_thread=False) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT pod_id, cpu FROM pods WHERE node_id IS NULL")
            failed_pods = cursor.fetchall()
            for pod_id, cpu in failed_pods:
                assigned_node = schedule_pod(pod_id, cpu)
                if assigned_node:
                    logging.info(f"[AUTO] Rescheduled pod {pod_id} to Node {assigned_node} using {strategy}")

# ---------------- API Endpoints ----------------
@app.route('/add_node', methods=['POST'])
def add_node():
    data = request.json
    node_id, cpu = data['node_id'], data['cpu']
    with lock, sqlite3.connect(db_file, check_same_thread=False) as conn:
        cursor = conn.cursor()
        cursor.execute("INSERT INTO nodes VALUES (?, ?, ?, 'healthy')", (node_id, cpu, time.time()))
        conn.commit()
    return jsonify({'message': f'Node {node_id} added'})

@app.route('/scale', methods=['POST'])
def scale():
    count = int(request.args.get("count", 1))
    responses = []
    for i in range(count):
        node_id = f"node_auto_{int(time.time())}_{i}"
        cpu = 10  # default CPU per node
        with lock, sqlite3.connect(db_file, check_same_thread=False) as conn:
            cursor = conn.cursor()
            cursor.execute("INSERT INTO nodes VALUES (?, ?, ?, 'healthy')", (node_id, cpu, time.time()))
            conn.commit()
        responses.append(node_id)
    return jsonify({'message': f'Scaled up by {count} nodes', 'nodes': responses})

@app.route('/launch_pod', methods=['POST'])
def launch_pod():
    data = request.json
    pod_id = data['pod_id']
    cpu_req = data['cpu']

    with lock, sqlite3.connect(db_file, check_same_thread=False) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT 1 FROM pods WHERE pod_id = ?", (pod_id,))
        if cursor.fetchone():
            return jsonify({'error': f'Pod {pod_id} already exists'}), 400

        current_time = time.time()
        cursor.execute("SELECT SUM(cpu) FROM nodes WHERE ? - last_heartbeat <= 30", (current_time,))
        total_available_cpu = cursor.fetchone()[0] or 0

        if total_available_cpu < cpu_req:
            return jsonify({'error': 'Insufficient cluster-wide resources'}), 400

        cursor.execute("INSERT INTO pods (pod_id, cpu, node_id, status) VALUES (?, ?, NULL, 'pending')",
                       (pod_id, cpu_req))
        conn.commit()

    assigned_node = schedule_pod(pod_id, cpu_req)
    if assigned_node:
        return jsonify({'message': f'Pod {pod_id} launched on Node {assigned_node} using {get_current_strategy()}'})
    else:
        return jsonify({'error': 'No node can handle pod'}), 400

@app.route('/leader', methods=['GET'])
def leader():
    with lock, sqlite3.connect(db_file, check_same_thread=False) as conn:
        cursor = conn.cursor()

        # Fetch current leader from settings
        cursor.execute("SELECT value FROM settings WHERE key='leader'")
        row = cursor.fetchone()
        current_leader = row[0] if row else "none"

        if current_leader:
            # Check if current leader is still healthy
            cursor.execute("SELECT 1 FROM nodes WHERE node_id=? AND status='healthy'", (current_leader,))
            if cursor.fetchone():
                # Leader is still alive → stick with it
                return jsonify({'leader': current_leader})

        # No leader, or current leader failed → elect new one
        cursor.execute("SELECT node_id FROM nodes WHERE status='healthy' ORDER BY node_id ASC LIMIT 1")
        row = cursor.fetchone()
        new_leader = row[0] if row else "none"

        if new_leader:
            # Save the new leader back to settings
            cursor.execute(
                "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
                ("leader", new_leader)
            )
            conn.commit()

    return jsonify({'leader': new_leader})

   


@app.route('/set_strategy', methods=['POST'])
def set_strategy():
    strategy = request.json.get("strategy", "best_fit")
    with sqlite3.connect(db_file) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)
        cursor.execute(
            "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
            ("strategy", strategy)
        )
        conn.commit()
    return jsonify({'message': f'Strategy set to {strategy}'})

@app.route("/get_strategy", methods=["GET"])
def get_strategy():
    return jsonify({"strategy": get_current_strategy()})

@app.route('/metrics', methods=['GET'])
def metrics():
    with lock, sqlite3.connect(db_file, check_same_thread=False) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*), SUM(cpu) FROM nodes WHERE status='healthy'")
        node_count, total_cpu = cursor.fetchone()
        cursor.execute("SELECT COUNT(*) FROM pods WHERE status='running'")
        running_pods = cursor.fetchone()[0]
    return jsonify({
        'healthy_nodes': node_count or 0,
        'total_cpu': total_cpu or 0,
        'running_pods': running_pods
    })

@app.route('/list_nodes', methods=['GET'])
def list_nodes():
    with lock, sqlite3.connect(db_file, check_same_thread=False) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT node_id, cpu, last_heartbeat, status FROM nodes")
        node_list = cursor.fetchall()
    return jsonify([{ 'node_id': node_id, 'cpu': cpu, 'last_heartbeat': last_heartbeat, 'status': status} 
                    for node_id, cpu, last_heartbeat, status in node_list])

@app.route('/pod_usage', methods=['GET'])
def pod_usage():
    with lock, sqlite3.connect(db_file, check_same_thread=False) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT pod_id, cpu, node_id, status FROM pods")
        pod_list = cursor.fetchall()
    return jsonify([{ 'pod_id': pod_id, 'cpu': cpu, 'node_id': node_id, 'status': status} 
                    for pod_id, cpu, node_id, status in pod_list])

@app.route('/remove_node/<node_id>', methods=['DELETE'])
def remove_node(node_id):
    with lock, sqlite3.connect(db_file, check_same_thread=False) as conn:
        cursor = conn.cursor()
        # Check if node exists
        cursor.execute("SELECT * FROM nodes WHERE node_id=?", (node_id,))
        row = cursor.fetchone()

        if not row:
            return jsonify({"error": f"Node {node_id} not found"}), 404

        # Delete the node
        cursor.execute("DELETE FROM nodes WHERE node_id=?", (node_id,))
        conn.commit()

    return jsonify({"message": f"Node {node_id} removed successfully"})


@app.route('/fail_node', methods=['POST'])
def fail_node():
    data = request.json
    node_id = data['node_id']
    with lock, sqlite3.connect(db_file, check_same_thread=False) as conn:
        cursor = conn.cursor()
        cursor.execute("UPDATE nodes SET status = 'unhealthy' WHERE node_id = ?", (node_id,))
        cursor.execute("UPDATE pods SET node_id = NULL, status = 'pending' WHERE node_id = ?", (node_id,))
        conn.commit()
    logging.warning(f"Node {node_id} marked failed; pods evicted")
    return jsonify({'message': f'Node {node_id} failed'})

@app.route('/recover_node', methods=['POST'])
def recover_node():
    data = request.json
    node_id = data['node_id']
    with lock, sqlite3.connect(db_file, check_same_thread=False) as conn:
        cursor = conn.cursor()
        cursor.execute("UPDATE nodes SET status = 'healthy', last_heartbeat = ? WHERE node_id = ?",
                       (time.time(), node_id))
        conn.commit()
    logging.info(f"Node {node_id} recovered")
    return jsonify({'message': f'Node {node_id} healthy again'})

@app.route("/heartbeat", methods=["POST"])
def heartbeat():
    data = request.get_json()
    node = data.get("node_id")

    if not node:
        return jsonify({"error": "Missing node field"}), 400

    with lock, sqlite3.connect(db_file) as conn:
        conn.execute(
            "UPDATE nodes SET last_heartbeat = ? WHERE node_id = ?",
            (int(time.time()), node)
        )
        conn.commit()

    return jsonify({"message": f"Heartbeat received from {node}"}), 200

# ---------------- Background Threads ----------------
def heartbeat_checker():
    while True:
        time.sleep(10)
        with lock, sqlite3.connect(db_file, check_same_thread=False) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT node_id FROM nodes WHERE ? - last_heartbeat > 30", (time.time(),))
            failed_nodes = cursor.fetchall()
            for (node_id,) in failed_nodes:
                logging.warning(f"Node {node_id} failed (timeout)!")
                cursor.execute("UPDATE nodes SET status = 'unhealthy' WHERE node_id = ?", (node_id,))
                cursor.execute("UPDATE pods SET node_id = NULL, status = 'pending' WHERE node_id = ?", (node_id,))
            conn.commit()

def auto_heartbeat():
    while True:
        time.sleep(5)
        with lock, sqlite3.connect(db_file, check_same_thread=False) as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE nodes SET last_heartbeat = ?", (time.time(),))
            conn.commit()

threading.Thread(target=heartbeat_checker, daemon=True).start()
threading.Thread(target=auto_heartbeat, daemon=True).start()
threading.Thread(target=auto_reschedule, daemon=True).start()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001, debug=True)
