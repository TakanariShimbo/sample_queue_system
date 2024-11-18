import os
import time
import threading
from typing import Literal

import requests


API_SERVER_ADDRESS = "localhost"
API_SERVER_PORT = "8000"


def submit_job(texts: list[str], priority: Literal["high", "low"]) -> list[str]:
    data = []
    for text in texts:
        data.append({"text": text})

    response = requests.post(
        url=f"http://{API_SERVER_ADDRESS}:{API_SERVER_PORT}/add-job/{priority}-priority",
        json={"data": data},
    )

    if response.status_code == 200:
        job_ids = []
        body = response.json()
        for data in body["data"]:
            job_id = data["job_id"]
            job_ids.append(job_id)
            print(f"{job_id}: submitted to {priority}")
        return job_ids
    else:
        raise Exception(f"Failed to submit job: {response.text}")


def request_result(job_id: str):
    response = requests.get(
        url=f"http://{API_SERVER_ADDRESS}:{API_SERVER_PORT}/get-result/{job_id}",
    )

    body = response.json()
    if response.status_code == 200:
        print(f"{job_id}: Finish")
        return body["data"]["embedding"]
    elif response.status_code == 202:
        n_wait = body["data"]["n_wait"]
        print(f"{job_id}: Wait {n_wait}")
        return None
    else:
        raise Exception(f"Failed to get job result: {response.text}")


def observe_submited_job(job_id):
    while True:
        time.sleep(2.5)
        embdedding = request_result(job_id=job_id)
        if embdedding is not None:
            break


if __name__ == "__main__":
    job_ids_low = submit_job(texts=[f"sample text", f"sample text"], priority="low")
    job_ids_high = submit_job(texts=[f"sample text", f"sample text"], priority="high")

    threads = []
    for job_id in job_ids_low + job_ids_high:
        t = threading.Thread(target=observe_submited_job, args=(job_id,))
        t.start()
        threads.append(t)
        time.sleep(0.01)

    for t in threads:
        t.join()
