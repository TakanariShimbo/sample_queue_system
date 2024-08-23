import time
import os
import threading

import requests
from dotenv import load_dotenv


load_dotenv()
FASTAPI_IP_ADDRESS = os.environ["FASTAPI_IP_ADDRESS"]
FASTAPI_PORT = os.environ["FASTAPI_PORT"]


def submit_embedding_request(texts: list[str]) -> list[str]:
    data = []
    for text in texts:
        data.append({"text": text})

    response = requests.post(
        url=f"http://{FASTAPI_IP_ADDRESS}:{FASTAPI_PORT}/low_priority",
        json={"data": data},
    )

    if response.status_code == 200:
        job_ids = []
        for res in response.json():
            job_id = res["job_id"]
            job_ids.append(job_id)
            print(f"Job submitted successfully. Job ID: {job_id}")
        return job_ids
    else:
        raise Exception(f"Failed to submit job: {response.text}")


def get_job_result(job_id: str):
    response = requests.get(
        url=f"http://{FASTAPI_IP_ADDRESS}:{FASTAPI_PORT}/status/{job_id}",
    )

    if response.status_code == 200:
        return response.json()
    elif response.status_code == 404:
        return None
    else:
        raise Exception(f"Failed to get job result: {response.text}")


def process_job(job_id):
    result = None
    while result is None:
        time.sleep(2.5)
        result = get_job_result(job_id)
    print(f"Result for job {job_id}")


if __name__ == "__main__":
    job_ids = submit_embedding_request(texts=[f"sample text 1", f"sample text 2"])

    threads = []

    for job_id in job_ids:
        t = threading.Thread(target=process_job, args=(job_id,))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()
