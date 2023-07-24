# syntax=docker/dockerfile:1

FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt requirements.txt

RUN apt-get update \
    && apt-get install libmariadb3 libmariadb-dev -y \
    && apt-get clean \
    && pip3 install --user -r requirements.txt

CMD [ "python3", "main.py"]
