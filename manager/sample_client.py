import os
import time
import threading

from dotenv import load_dotenv
import requests


load_dotenv()
FASTAPI_IP_ADDRESS = os.environ["FASTAPI_IP_ADDRESS"]
FASTAPI_PORT = os.environ["FASTAPI_PORT"]


def submit_embedding_request(texts: list[str]) -> list[str]:
    data = []
    for text in texts:
        data.append({"text": text})

    response = requests.post(
        url=f"http://{FASTAPI_IP_ADDRESS}:{FASTAPI_PORT}/add-job/low-priority",
        json={"data": data},
    )

    if response.status_code == 200:
        job_ids = []
        body = response.json()
        for data in body["data"]:
            job_id = data["job_id"]
            job_ids.append(job_id)
            print(f"Job submitted successfully. Job ID: {job_id}")
        return job_ids
    else:
        raise Exception(f"Failed to submit job: {response.text}")


def get_job_result(job_id: str):
    response = requests.get(
        url=f"http://{FASTAPI_IP_ADDRESS}:{FASTAPI_PORT}/get-result/low-priority/{job_id}",
    )

    body = response.json()
    if response.status_code == 200:
        print(f"Finish: {job_id}")
        return body["data"]["embedding"]
    elif response.status_code == 202:
        n_wait = body["data"]["n_wait"]
        print(f"Wait{n_wait}: {job_id}")
        return None
    else:
        raise Exception(f"Failed to get job result: {response.text}")


def process_job(job_id):
    while True:
        time.sleep(2.5)
        embdedding = get_job_result(job_id=job_id)
        if embdedding:
            break


if __name__ == "__main__":
    job_ids = submit_embedding_request(texts=[f"sample text 1", f"sample text 2", f"sample text 3"])

    threads = []

    for job_id in job_ids:
        t = threading.Thread(target=process_job, args=(job_id,))
        t.start()
        threads.append(t)
        time.sleep(0.01)

    for t in threads:
        t.join()
