FROM python:3.11-slim

WORKDIR /work

COPY ./requirements.txt /work
RUN apt-get clean && apt-get update
RUN apt-get upgrade -y --fix-missing

ENV TZ=Asia/Tokyo
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

RUN pip install --no-cache-dir -r requirements.txt
