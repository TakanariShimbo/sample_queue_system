import argparse
from typing import Literal

import requests


API_SERVER_ENDPOINT = "http://localhost:8000"


def add_job(
    text: str,
    priority: Literal["high", "low"],
) -> None:

    response = requests.post(
        url=f"{API_SERVER_ENDPOINT}/add-job/{priority}-priority",
        json={
            "data": [
                {"text": text},
            ]
        },
    )

    if response.status_code == 200:
        body = response.json()
        for data in body["data"]:
            job_id = data["job_id"]
            print(f"{job_id}: submitted to {priority}")
    else:
        raise Exception(f"Failed to submit job: {response.text}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Submit text for processing.")
    parser.add_argument("--text", required=True, help="Text to be processed")
    parser.add_argument("--priority", choices=["high", "low"], default="low", help="Priority of the job")

    text = parser.parse_args().text
    priority = parser.parse_args().priority

    add_job(text=text, priority=priority)
