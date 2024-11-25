import argparse

import requests


API_SERVER_ENDPOINT = "http://localhost:8000"


def get_result(job_id: str):
    response = requests.get(
        url=f"{API_SERVER_ENDPOINT}/get-result/{job_id}",
    )

    body = response.json()
    if response.status_code == 200:
        print(f"{job_id}: finish, result is {body['data']['embedding']}")
    elif response.status_code == 202:
        n_wait = body["data"]["n_wait"]
        print(f"{job_id}: Wait {n_wait}")
    else:
        raise Exception(f"Failed to get job result: {response.text}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Get the result of a submitted job.")
    parser.add_argument("--id", required=True, help="ID of the job to get the result for")

    job_id = parser.parse_args().id

    get_result(job_id=job_id)
