FROM python:3.12-alpine AS base

WORKDIR /app

COPY requirements.txt .

RUN pip3 install -r requirements.txt

FROM base as Prod

COPY . /app

ENTRYPOINT ["python /app/worker.py"]