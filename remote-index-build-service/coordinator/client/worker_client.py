import concurrent.futures
import logging
from dataclasses import dataclass

from urllib3 import HTTPConnectionPool
import json

from util.common import ThreadSafeRoundRobinIterator


@dataclass
class Worker:
    host:str
    port: int

class WorkerClient:

    def __init__(self, worker):
        self.logger = logging.getLogger(__name__)
        self.client_pool = HTTPConnectionPool(host=worker.host, port=worker.port, maxsize=10)
        self.worker = worker

    def get_job(self, job_id: str):
        return self.client_pool.request("GET", f"/job/{job_id}", headers={'Content-Type': 'application/json'})

    def create_index(self, createIndexRequest):
        self.logger.info(f"createIndexRequest is : {createIndexRequest}")
        response = self.client_pool.request("POST", "/create_index", body=json.dumps(createIndexRequest), headers={'Content-Type': 'application/json'})
        return response.json()

    def get_jobs(self):
        jobs = self.client_pool.request("GET", "/jobs", headers={'Content-Type': 'application/json'})
        jobs = jobs.json()
        self.logger.info(f"Jobs are : {jobs}")
        return jobs


class WorkerService:

    def __init__(self, workers: list):
        self.logger = logging.getLogger(__name__)
        self.workers = workers
        self._build_worker_client()

    def _build_worker_client(self):
        self.worker_clients = []
        for worker in self.workers:
            self.worker_clients.append(WorkerClient(worker))
        self.round_robin_iterator = ThreadSafeRoundRobinIterator(self.worker_clients)


    def get_job(self, job_id: str):
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(self.worker_clients)) as executor:
            futures = []
            for worker_client in self.worker_clients:
                future = executor.submit(worker_client.get_job, job_id)
                future.add_done_callback(lambda x: x.result().release_conn())
                futures.append(future)

            for future in futures:
                response = future.result()
                if response.status == 200:
                    return response.json()
                else:
                    self.logger.info(f"No job found for the {job_id} : {response.status} {response.reason}")
            raise Exception(f"Error in get_job for job_id {job_id}")


    def create_index(self, createIndexRequest):
        worker_client = self.round_robin_iterator.get_next()
        self.logger.info("in create_index call")
        response = worker_client.create_index(createIndexRequest)
        self.logger.info(f"response is : {response}")
        return response

    def get_jobs(self):
        jobs = {}
        logging.info("in get_jobs call")
        for worker_client in self.worker_clients:
            client_jobs = worker_client.get_jobs()
            for job in client_jobs:
                jobs[job] = client_jobs[job]
        self.logger.info(f"jobs are : {jobs}")
        return jobs