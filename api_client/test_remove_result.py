import argparse

import requests


API_SERVER_ENDPOINT = "http://localhost:8000"


def remove_result(job_id: str):
    response = requests.delete(
        url=f"{API_SERVER_ENDPOINT}/remove-result/{job_id}",
    )

    if response.status_code == 200:
        print(f"{job_id}: remove job result")
    else:
        raise Exception(f"Failed to remove job result: {response.text}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Get the result of a submitted job.")
    parser.add_argument("--id", required=True, help="ID of the job to get the result for")

    job_id = parser.parse_args().id

    remove_result(job_id=job_id)
