import os
import time
import threading
from typing import Literal

from dotenv import load_dotenv
import requests


load_dotenv()
FASTAPI_IP_ADDRESS = os.environ["FASTAPI_IP_ADDRESS"]
FASTAPI_PORT = os.environ["FASTAPI_PORT"]


def submit_job(texts: list[str], priority: Literal["high", "low"]) -> list[str]:
    data = []
    for text in texts:
        data.append({"text": text})

    response = requests.post(
        url=f"http://{FASTAPI_IP_ADDRESS}:{FASTAPI_PORT}/add-job/{priority}-priority",
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


def request_result(job_id: str, priority: Literal["high", "low"]):
    response = requests.get(
        url=f"http://{FASTAPI_IP_ADDRESS}:{FASTAPI_PORT}/get-result/{priority}-priority/{job_id}",
    )

    body = response.json()
    if response.status_code == 200:
        print(f"{job_id}: Finish")
        return body["data"]["embedding"]
    elif response.status_code == 202:
        n_wait = body["data"]["n_wait"]
        print(f"{job_id}: Wait {priority}-{n_wait}")
        return None
    else:
        raise Exception(f"Failed to get job result: {response.text}")


def observe_submited_job(job_id, priority: Literal["high", "low"]):
    while True:
        time.sleep(2.5)
        embdedding = request_result(job_id=job_id, priority=priority)
        if embdedding:
            break


if __name__ == "__main__":
    job_ids_low = submit_job(texts=[f"sample text", f"sample text"], priority="low")
    job_ids_high = submit_job(texts=[f"sample text", f"sample text"], priority="high")

    threads_low = []
    for job_id in job_ids_low:
        t = threading.Thread(target=observe_submited_job, args=(job_id, "low"))
        t.start()
        threads_low.append(t)
        time.sleep(0.01)

    threads_high = []
    for job_id in job_ids_high:
        t = threading.Thread(target=observe_submited_job, args=(job_id, "high"))
        t.start()
        threads_high.append(t)
        time.sleep(0.01)

    for t in threads_low + threads_high:
        t.join()
