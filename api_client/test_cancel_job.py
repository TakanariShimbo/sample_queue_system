import argparse

import requests


API_SERVER_ENDPOINT = "http://localhost:8000"


def cancel_job(job_id: str):
    response = requests.delete(
        url=f"{API_SERVER_ENDPOINT}/cancel-job/{job_id}",
    )

    if response.status_code == 200:
        print(f"{job_id}: job cancelled successfully")
    else:
        raise Exception(f"Failed to cancel job: {response.text}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Cancel a submitted job.")
    parser.add_argument("--id", required=True, help="ID of the job to cancel")

    job_id = parser.parse_args().id

    cancel_job(job_id=job_id)
