import traceback
import os

from flask import Flask, jsonify, request
from datetime import datetime
import json
import subprocess

from index_builder.indexing_service import IndexingService
from models import data_model
import uuid
import logging
from logging.handlers import RotatingFileHandler
from waitress import serve
from urllib3 import HTTPConnectionPool

# To ensure that we are getting the host IP, use the host mode on ECS service.
def getIp():
    process = subprocess.Popen(["hostname", "-I"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()
    host_ip = str(stdout.decode()).split(" ")[0]
    print(f"Host IP is: {host_ip}")
    return host_ip

PORT=6005
# Create logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Console handler (stdout)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

# File handler
file_handler = RotatingFileHandler(
    '/app/logs/remote-index-build-service.log',
    maxBytes=1024 * 1024,
    backupCount=5
)
file_handler.setLevel(logging.INFO)

# Format
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
file_handler.setFormatter(formatter)

# Add handlers
logger.addHandler(console_handler)
logger.addHandler(file_handler)

logger = logging.getLogger(__name__)

app = Flask(__name__)

indexing_service = IndexingService()


coordinator_node_url = os.getenv('COORDINATOR_NODE_URL', '')
coordinator_node_protocol = os.getenv('COORDINATOR_NODE_PROTOCOL', 'http')
coordinator_node_port = int(os.getenv('COORDINATOR_NODE_PORT', "6006"))
register_with_coordinator = 0

@app.route('/', methods=['GET'])
def hello():
    return jsonify({
        "message": "Hello from Vector Index Build Service Worker!",
        "timestamp": datetime.now().isoformat()
    }), 200

@app.route('/heart_beat')
def heart_beat():
    return jsonify({
        "message": "Hello from Vector Index Build Service Coordinator!",
        "timestamp": datetime.now().isoformat()
    }), 200


@app.route('/job/<string:job_id>')
def job(job_id: str):
    job_deatils = indexing_service.get_job_status(job_id)
    if job_deatils is None:
        return jsonify({"error": f"Job not found with Id {job_id}"}), 404
    return jsonify({
        "status": job_deatils.status,
        "result": job_deatils.result,
        "error": job_deatils.error
    })


@app.route('/jobs', methods=['GET'])
def get_jobs():
    jobs = indexing_service.get_jobs()
    return json.dumps(jobs, default=lambda o: o.__dict__, indent=4)


@app.route('/create_index', methods=['POST'])
def create_index():
    try:
        logger.info(f"Received request: %s ", request.json)
        create_index_request = data_model.build_create_index_request(request.json)
    except Exception as e:
        logger.error(traceback.format_exc())
        return jsonify({"error": "Invalid request"}), 400

    indexing_job_id = str(uuid.uuid4())

    job_details = indexing_service.create_job(indexing_job_id, create_index_request)
    indexing_service.start_job(job_details.id, create_index_request)
    return jsonify({"job_id": job_details.id, "status": job_details.status}), 201


def register_worker():
    logger.info("Registering the worker with coordinator node")

    if len(coordinator_node_url) == 0:
        logger.error(f"coordinator_node_url value is empty : {coordinator_node_url}")
        raise Exception(f"coordinator_node_url value is empty : {coordinator_node_url}")

    client_pool = HTTPConnectionPool(host=coordinator_node_url, port=coordinator_node_port, maxsize=1)
    
    host_ip = getIp()
    if len(host_ip) == 0 :
        logger.error("Host IP is empty. throwing an error")
        raise Exception("Host IP is empty. throwing an error")

    register_worker = {
        "workerList": [
            {
                "workerURL": host_ip,
                "workerPort": PORT
            }
        ]
    }

    response = client_pool.request("POST", "/register_worker", body=json.dumps(register_worker), headers={'Content-Type': 'application/json'})
    response = response.json()
    logger.info("Response for registering worker is : {}", response)


if __name__ == '__main__':
    if register_with_coordinator == 1:
        register_worker()
    serve(app, host="0.0.0.0", port=PORT)
